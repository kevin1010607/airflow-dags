# Airflow-Dags

## Todo
- (Priority) model dag 還不行正常運作，因為 spark 相關的所有設定像是 vpn, java runtime 全部都需要在 docker image 中設置好。
  - 可能需要將 airflow image 和 pod_template image 分成不同的 image。
  - 不確定 pod_template 是不是只會套用在 worker 上，還沒測試。
- (Future) gitSync 的 repo 會有大小限制，直接 sync `git@github.com:kevin1010607/MLOps-ASMPT.git` 會有 error。
  - 應該是要修改 `values.yaml` 中的 dags.gitSync.resources。
- (Future) 目前使用的 CeleryExecutor，官方說 原本就有 default 的 pvc，所以暫時沒有加上 pv 和 pvc，也沒有 enable logs.persistence。
  - If you are using CeleryExecutor, workers persist logs by default to a volume claim created with a volumeClaimTemplate.
  - 如果 enable logs.persistence 會有 error。
- (Solved) 用 `docker/Dockerfile` 建立了 customized 的 airflow image，也安裝了需要的 python package，但 dag 還是 import error。
  - k8s node 上會 cache 著之前用過的 docker image，如果重新 docker build 相同名稱和 tag，並 push 到 remote，node 一樣會優先使用 local 的 docker image，不會去 remote 下載。
  - 原因是 image.airflow.pullPolicy 為 IfNotPresent，所以要嘛改成 Always，或是上去 node 上把舊的 docker image 刪掉，或是直接重開 cluster。
- (Solved) webserver 的啟動時間非常長，目前大約是 2 分鐘，如果 health check 的時間過短，就會一直沒辦法 run 起來，因為在 run 起來前就被 k8s restart。
  - 增加 webserver.livenessProbe.initialDelaySeconds。
  - 增加 webserver.startupProbe.failureThreshold 和 webserver.startupProbe.periodSeconds，兩個相乘的數字要大於 webserver 啟動時間的秒數。

## Install Airflow with Helm on K8s

### Dependency
```bash
# Install kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.23.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind
kind --version

# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x ./kubectl
sudo mv ./kubectl /usr/local/bin/kubectl
kubectl version --client

# Install docker
sudo apt update
sudo apt upgrade
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce
sudo systemctl status docker --no-pager
sudo usermod -aG docker ${USER}
docker version

# Install helm
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 ./get_helm.sh
./get_helm.sh
helm version

# Install yq
sudo add-apt-repository ppa:rmescandon/yq
sudo apt update
sudo apt install -y yq
yq --version
```

### Create k8s cluster
```bash
# Create the cluster
cd k8s
kind create cluster \
    --name airflow-cluster \
    --config kind-cluster.yaml
kubectl cluster-info

# Create airflow namespace
kubectl create namespace airflow
kubectl get namespaces
```

### Build customized docker image (only once)
```bash
USERNAME=kevin1010607
CHART_VERSION=1.15.0
AIRFLOW_VERSION=2.9.3

cd k8s/docker/chart-${CHART_VERSION}-airflow-${AIRFLOW_VERSION}
docker build --no-cache -t ${USERNAME}/airflow-custom:${AIRFLOW_VERSION} .
docker push ${USERNAME}/airflow-custom:${AIRFLOW_VERSION}
```

### Add necessary secret to k8s cluster
```bash
# Add ssh git secret
kubectl create secret generic airflow-ssh-git-secret \
    --from-file=gitSshKey=$HOME/.ssh/id_ed25519 \
    --namespace airflow
kubectl get secret airflow-ssh-git-secret \
    -o jsonpath="{.data.gitSshKey}" \
    --namespace airflow \
    | base64 --decode

# Add webserver secret
kubectl create secret generic airflow-webserver-secret \
    --from-literal=webserver-secret-key=$(python3 -c 'import secrets; print(secrets.token_hex(16))') \
    --namespace airflow
kubectl get secret airflow-webserver-secret \
    -o jsonpath="{.data.webserver-secret-key}" \
    --namespace airflow \
    | base64 --decode
```

### Use helm to install airflow
```bash
# Add airflow repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm search repo airflow --versions

# Apply customized setting on airflow
export USERNAME=kevin1010607
export CHART_VERSION=1.15.0
export AIRFLOW_VERSION=2.9.3
helm show values apache-airflow/airflow --version ${CHART_VERSION} > values.yaml
yq eval -i '
  .defaultAirflowRepository = env(USERNAME) + "/airflow-custom" |
  .defaultAirflowTag = env(AIRFLOW_VERSION) |
  .airflowVersion = env(AIRFLOW_VERSION) |
  .images.airflow.repository = env(USERNAME) + "/airflow-custom" |
  .images.airflow.tag = env(AIRFLOW_VERSION) |
  .images.pod_template.repository = env(USERNAME) + "/airflow-custom" |
  .images.pod_template.tag = env(AIRFLOW_VERSION) |
  .webserverSecretKeySecretName = "airflow-webserver-secret" |
  .webserver.livenessProbe.initialDelaySeconds = 25 |
  .webserver.startupProbe.failureThreshold = 10 |
  .webserver.startupProbe.periodSeconds = 12 |
  .dags.gitSync.enabled = true |
  .dags.gitSync.repo = "git@github.com:kevin1010607/airflow-dags.git" |
  .dags.gitSync.branch = "main" |
  .dags.gitSync.subPath = "dags" |
  .dags.gitSync.sshKeySecret = "airflow-ssh-git-secret" |
  .dags.gitSync.knownHosts = "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
' values.yaml

# Install customized airflow
helm install airflow apache-airflow/airflow \
    --version ${CHART_VERSION} \
    --namespace airflow \
    -f values.yaml \
    --debug

# Upgrade airflow
helm upgrade \
    --install airflow apache-airflow/airflow \
    --namespace airflow \
    -f values.yaml \
    --debug
```

### Webserver port forwarding
```bash
# Run in the background
kubectl port-forward svc/airflow-webserver 8080:8080 \
    --address 10.121.252.191 \
    --namespace airflow \
    2>&1 > /dev/null &

# Stop port forwarding
sudo kill -9 $(ps -aux | grep "kubectl port-forward" | grep -v grep | awk '{print $2}' | head -n1)
```

### Other
```bash
# Get infomation
kubectl get pods -n airflow
kubectl get svc -n airflow
kubectl get pv -n airflow
kubectl get pvc -n airflow
kubectl get sts -n airflow
kubectl get secret -n airflow
kubectl get deployment -n airflow
kubectl describe pod/airflow-worker-0 -n airflow

# Restart deployment
kubectl rollout restart deployment airflow-webserver  -n airflow
kubectl rollout restart deployment airflow-scheduler  -n airflow

# Delete the airflow cluster
kubectl delete ns/airflow
kind delete cluster -n airflow-cluster

# Check image on the nodes
docker exec -it airflow-cluster-worker crictl images
docker exec -it airflow-cluster-worker crictl rmi docker.io/kevin1010607/airflow-custom:2.9.3
```

## Script
```bash
cd scripts

# Install dependency
./dependency-install.sh

# Install airflow on k8s
./airflow-setup.sh

# Delete airflow on k8s
./airflow-clear.sh
```

## Reference
- https://marclamberti.com/blog/airflow-on-kubernetes-get-started-in-10-mins
- https://artifacthub.io/packages/helm/airflow-helm/airflow/8.3.2
- https://airflow.apache.org/docs/helm-chart/stable/index.html
- https://airflow.apache.org/docs/helm-chart/stable/manage-logs.html

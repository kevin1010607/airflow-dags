# Stop port forwarding
sudo kill -9 $(ps -aux | grep "kubectl port-forward" | grep -v grep | awk '{print $2}' | head -n1)

# Delete the airflow cluster
kubectl delete ns/airflow --force
kind delete cluster -n airflow-cluster

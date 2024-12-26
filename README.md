# Airflow-Dags
## Place for codes with git synchronization
---
## Solved problems
- (Solved) 用 `docker/Dockerfile` 建立了 customized 的 airflow image，也安裝了需要的 python package，但 dag 還是 import error。
  - k8s node 上會 cache 著之前用過的 docker image，如果重新 docker build 相同名稱和 tag，並 push 到 remote，node 一樣會優先使用 local 的 docker image，不會去 remote 下載。
  - 原因是 image.airflow.pullPolicy 為 IfNotPresent，所以要嘛改成 Always，或是上去 node 上把舊的 docker image 刪掉，或是直接重開 cluster。
- (Solved) webserver 的啟動時間非常長，目前大約是 2 分鐘，如果 health check 的時間過短，就會一直沒辦法 run 起來，因為在 run 起來前就被 k8s restart。
  - 增加 webserver.livenessProbe.initialDelaySeconds。
  - 增加 webserver.startupProbe.failureThreshold 和 webserver.startupProbe.periodSeconds，兩個相乘的數字要大於 webserver 啟動時間的秒數。

### select kubernetes cluster
```shell
# select cluster to deploy 
kubectx do-nyc3-do-owshq-scorpius
k get nodes
```

### install apache nessie
```shell
# create namespace
k create namespace catalog

# add the nessie helm repo
# https://projectnessie.org/try/helm/
helm repo add nessie-helm https://charts.projectnessie.org
helm repo update

# install helm chart
# https://github.com/projectnessie/nessie/blob/main/helm/nessie/readme.md
helm install -n nessie-ns nessie nessie-helm/nessie
```

### install {spark-on-k8s} operator
```shell
# spok = spark on kubernetes
# https://github.com/GoogleCloudPlatform/spark-on-k8s-operator

# create namespace
k create namespace processing

# install operator {GoogleCloudPlatform/spark-on-k8s-operator}
helm repo update
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark spark-operator/spark-operator --namespace processing

# get helm
helm -ls --all-namespaces
```

### building image {docker image}
```shell
# location
/Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/kubernetes

# build, tag & push image
docker build . -t etl-device-subscription-iceberg:3.2.1
docker tag etl-device-subscription-iceberg:3.2.1 owshq/etl-device-subscription-iceberg:3.2.1
docker push owshq/etl-device-subscription-iceberg:3.2.1
```

### deploy {app}
```sh
# create & verify cluster role perms
k apply -f /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/kubernetes/crb-spark-operator-processing.yaml -n processing
k describe clusterrolebinding crb-spark-operator-processing

# deploy spark application
kubens processing
k apply -f /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/kubernetes/etl-device-subscription.yaml -n processing

# get yaml detailed info
# verify submit
k get sparkapplications
k get sparkapplications etl-device-subscription -o=yaml
k describe sparkapplication etl-device-subscription

# [schedule app]
k apply -f /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/kubernetes/sch-etl-device-subscription.yaml -n processing
k get scheduledsparkapplication
k describe scheduledsparkapplication etl-device-subscription

# verify logs in real-time
# port forward to spark ui
POD=etl-device-subscription-driver
k port-forward $POD 4040:4040
```
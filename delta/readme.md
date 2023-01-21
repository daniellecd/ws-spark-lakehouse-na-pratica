```sh
# install & upgrade minio cli client
brew install minio/stable/mc
brew upgrade minio/stable/mc

# minio web console
console = http://159.203.146.203:9090/
endpoint = http://45.55.126.192
access_key = minio
secret_key = minio123

# minio configuration
mc config host add minio http://159.203.146.203:9090/ minio minio123

# list info
mc ls minio
mc tree minio
```

```sh
# libraries to copy to jars folder
# local environment [copy]
# [jars loc] = /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/delta/jars
cp delta-core_2.12-1.2.1.jar /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/venv/lib/python3.9/site-packages/pyspark/jars/
cp delta-storage-1.2.1.jar /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/venv/lib/python3.9/site-packages/pyspark/jars/

# app name = etl-enriched-users-analysis
# location of dockerfile [build image]
Dockerfile

# build image
# tag image
# push image to registry
docker build . -t etl-enriched-users-analysis:3.1.1
docker tag etl-enriched-users-analysis:3.1.1 owshq/etl-enriched-users-analysis:3.1.1
docker push owshq/etl-enriched-users-analysis:3.1.1

# select cluster to deploy 
kubectx do-sfo3-do-owshq-svc-scorpius
k top nodes

# verify spark operator
kubens processing  
helm ls -n processing
kgp -n processing

# create & verify cluster role binding perms
k apply -f /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/delta/crb-spark-operator-processing.yaml -n processing
k describe clusterrolebinding crb-spark-operator-processing

# deploy spark application [kubectl] for testing purposes
# [deploy application]
kubens processing
k apply -f /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/delta/etl-enriched-users-analysis.yaml -n processing

# get yaml detailed info
# verify submit
k get sparkapplications
k get sparkapplications etl-enriched-users-analysis -o=yaml
k describe sparkapplication etl-enriched-users-analysis

# [schedule app]
k apply -f /Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/delta/sch-etl-enriched-users-analysis.yaml -n processing
k get scheduledsparkapplication
k describe scheduledsparkapplication etl-enriched-users-analysis

# verify logs in real-time
# port forward to spark ui
POD=etl-enriched-users-analysis-driver
k port-forward $POD 4040:4040

# housekeeping
k delete SparkApplication etl-enriched-users-analysis -n processing
k delete Scheduledsparkapplication etl-enriched-users-analysis -n processing
k delete clusterrolebinding crb-spark-operator-processing
helm uninstall spark -n processing
```
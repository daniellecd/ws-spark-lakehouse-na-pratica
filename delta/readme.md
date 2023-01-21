```sh
# install & upgrade minio cli client
brew install minio/stable/mc
brew upgrade minio/stable/mc

# minio web console
console = http://137.184.246.244:9090/
endpoint = http://143.198.244.43
access_key = QVhXm9fWeFutyKBe
secret_key = 1jZrIaYbBtxd9t2HDyuqGk6wIE6nNb3s

# minio configuration
mc config host add minio http://137.184.246.244:9090/ QVhXm9fWeFutyKBe 1jZrIaYbBtxd9t2HDyuqGk6wIE6nNb3s

# list info
mc ls minio
mc tree minio
```

```sh
# libraries to copy to jars folder
# local environment [copy]
# [jars loc] = /usr/local/lib/python3.9/site-packages/pyspark/jars 
cp -a /Users/luanmorenomaciel/BitBucket/owshq-svc-spark/etl-enriched-users-analysis/delta/jars/* /usr/local/lib/python3.9/site-packages/pyspark/jars

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
k apply -f /Users/luanmorenomaciel/BitBucket/owshq-svc-spark/etl-enriched-users-analysis/crb-spark-operator-processing.yaml -n processing
k describe clusterrolebinding crb-spark-operator-processing

# deploy spark application [kubectl] for testing purposes
# [deploy application]
kubens processing
k apply -f /Users/luanmorenomaciel/BitBucket/owshq-svc-spark/etl-enriched-users-analysis/etl-enriched-users-analysis.yaml -n processing

# get yaml detailed info
# verify submit
k get sparkapplications
k get sparkapplications etl-enriched-users-analysis -o=yaml
k describe sparkapplication etl-enriched-users-analysis

# [schedule app]
k apply -f /Users/luanmorenomaciel/BitBucket/owshq-svc-spark/etl-enriched-users-analysis/sch-etl-enriched-users-analysis.yaml -n processing
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

### add jars
```shell
# delta = 1.2.1
https://mvnrepository.com/artifact/io.delta/delta-core_2.12/1.2.1

# iceberg = 0.13.2
https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.2_2.12/0.13.2

# copy jars to spark's folder
/usr/local/lib/python3.9/site-packages/pyspark/jars/
cp -a /Users/luanmorenomaciel/BitBucket/owshq-svc-spark/etl-enriched-users-analysis/iceberg/jars/* /usr/local/lib/python3.9/site-packages/pyspark/jars
ls | grep iceberg
```
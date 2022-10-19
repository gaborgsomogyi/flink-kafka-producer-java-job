# flink-kafka-producer-java-job

Flink Kafka producer job for k8s operator.

## How to build
In order to build the project one needs maven and java.
Please make sure to set Flink version in
* `pom.xml` file with `flink.version` parameter
* `Dockerfile` file with `FROM` parameter
* `flink-kafka-producer.yaml` file with `flinkVersion` parameter
```
mvn clean install

# Build the docker image into minikube
eval $(minikube docker-env)
docker build -t flink-kafka-producer:latest .
```

## How to deploy
```
kubectl apply -f flink-kafka-producer.yaml
```

## How to delete
```
kubectl delete -f flink-kafka-producer.yaml
```

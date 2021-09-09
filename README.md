# executor-check-mysql-data
executor-check-mysql-data





# 构建

```
yum install -y wegt unzip
mkdir executor-check-mysql-data && cd executor-check-mysql-data
wget https://github.com/laashub-soa/executor-check-mysql-data/archive/refs/tags/v0.0.1.zip
unzip v0.0.1.zip && cd executor-logging-loki-ruler-0.0.1

docker build -t tanshilindocker/executor-check-mysql-data:0.0.1 -f deploy/Dockerfile .
docker login  --username="" --password=""
docker push  tanshilindocker/executor-check-mysql-data:0.0.1
```


HADOOP_VERSION = 3.2.1
SPARK_VERSION = 2.4.4

build.log: Dockerfile requirements.txt
	docker build --build-arg HADOOP_VERSION=$(HADOOP_VERSION) --build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--tag datafaucet/pyspark-notebook:spark_$(SPARK_VERSION)-hadoop_$(HADOOP_VERSION) . > build.log

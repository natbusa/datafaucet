./setup.sh

docker run -it \
  --network datafaucet\
  -v $(pwd)/kafka-consumer-test.ipynb:/home/jovyan/work/kafka-consumer-test.ipynb \
  -v $(pwd)/logging.ipynb:/home/jovyan/work/logging.ipynb \
  -v $(pwd)/../../../datafaucet:/home/jovyan/work/datafaucet \
  --workdir /home/jovyan/work/ \
  -p 8888:8888 \
  datafaucet/pyspark-notebook:spark_2.4.4-hadoop_3.2.1

./teardown.sh
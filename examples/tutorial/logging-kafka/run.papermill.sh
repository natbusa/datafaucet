./setup.sh

docker run \
  --network datafaucet \
  -v $(pwd)/logging.ipynb:/home/jovyan/work/logging.ipynb \
  -v $(pwd)/../../../datafaucet:/home/jovyan/work/datafaucet \
  --workdir /home/jovyan/work/ \
  datafaucet/pyspark-notebook:spark_2.4.4-hadoop_3.2.1 \
  start.sh papermill --log-level INFO --log-output logging.ipynb logging.output.ipynb

docker run \
  --network datafaucet \
  -v $(pwd)/kafka-consumer-test.ipynb:/home/jovyan/work/kafka-consumer-test.ipynb \
  -v $(pwd)/../../../datafaucet:/home/jovyan/work/datafaucet \
  --workdir /home/jovyan/work/ \
  datafaucet/pyspark-notebook:spark_2.4.4-hadoop_3.2.1 \
  start.sh papermill --log-level INFO --log-output kafka-consumer-test.ipynb kafka-consumer-test.output.ipynb

./teardown.sh
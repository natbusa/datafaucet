pushd ../../../docker/compose/kafka
docker-compose down
popd

docker network rm datafaucet

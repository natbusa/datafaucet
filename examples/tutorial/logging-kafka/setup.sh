# always a clean start
./teardown.sh

## setup
docker network create datafaucet

pushd ../../../docker/compose/kafka
docker-compose down
docker-compose up -d
popd

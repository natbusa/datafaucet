SHELL=/bin/bash

build:
	docker build -t natbusa/jupyter:latest .

run:
	docker run -v `pwd`/src:/home/jovyan/src \
	           -v `pwd`/data:/home/jovyan/data \
	           -p 8888:8888 natbusa/jupyter start.sh \
						 jupyter lab &

clean:
	find . -name '.ipynb_checkpoints' -exec rm -rf  {} +
	find . -name 'spark-warehouse' -exec rm -rf {} +

.PHONY: clean

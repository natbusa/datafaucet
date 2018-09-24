SHELL=/bin/bash

build:
	docker build -t {{cookiecutter.user_name}}/{{cookiecutter.project_name}}:latest .

run:
	docker run -v `pwd`/src:/home/jovyan/src \
	           -v `pwd`/data:/home/jovyan/data \
	           -p 8888:8888 {{cookiecutter.user_name}}/{{cookiecutter.project_name}} start.sh \
						 jupyter lab &

clean:
	find . -name '.ipynb_checkpoints' -exec rm -rf  {} +
	find . -name 'spark-warehouse' -exec rm -rf {} +

.PHONY: clean

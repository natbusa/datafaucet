SHELL=/bin/bash
CONDA_ROOT=`which conda | sed -n 's/\(.*\)\/bin\/conda/\1/p'`

build-docker:
	docker build -f ci/Dockerfile -t natbusa/titanic:latest .

build-local:
	conda env update -f binder/environment.yml

run-local:
		 (source $(CONDA_ROOT)/bin/activate titanic && jupyter lab &)

run-docker:
	docker run -v `pwd`/src:/home/jovyan/src \
	           -v `pwd`/data:/home/jovyan/data \
	           -p 8888:8888 natbusa/titanic start.sh \
						 jupyter lab &

clean:
	find . -name '.ipynb_checkpoints' -exec rm -rf  {} +
	find . -name 'spark-warehouse' -exec rm -rf {} +

.PHONY: clean

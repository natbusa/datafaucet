test:
	py.test tests --tb=short

install:
	pip install --editable . --upgrade

tox:
	@tox

clean:
	find . -name '__pycache__' -exec rm -rf  {} +
	find . -name '.ipynb_checkpoints' -exec rm -rf  {} +
	find . -name 'spark-warehouse' -exec rm -rf {} +
	rm -rf 'datafaucet.egg-info' datafaucet.zip

build:
	make clean
	mkdir -p datafaucet/spark/dist
	zip -rq datafaucet.zip datafaucet
	mv datafaucet.zip datafaucet/spark/dist
	python3 setup.py sdist bdist_wheel

.PHONY: test tox clean build

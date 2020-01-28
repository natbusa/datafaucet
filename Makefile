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
	rm -rf 'datafaucet.egg-info'

build:
	python3 setup.py sdist bdist_wheel

.PHONY: test tox

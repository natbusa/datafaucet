test:
	py.test tests --tb=short

dev:
	pip install --editable . --upgrade

tox:
	@tox

clean:
	find . -name '__pycache__' -exec rm -rf  {} +
	find . -name '.ipynb_checkpoints' -exec rm -rf  {} +
	find . -name 'spark-warehouse' -exec rm -rf {} +
	rm -rf 'datafaucet.egg-info'

.PHONY: test tox

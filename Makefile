test:
	py.test tests --tb=short

develop:
	pip install --editable .

tox-test:
	@tox

release:
	find . -name '.ipython' -exec rm -rf  {} +
	find . -name '.jupyter' -exec rm -rf  {} +
	find . -name '.config' -exec rm -rf  {} +
	find . -name '.local' -exec rm -rf  {} +
	find . -name '.ivy2' -exec rm -rf  {} +
	find . -name '.ipynb_checkpoints' -exec rm -rf  {} +
	find . -name 'spark-warehouse' -exec rm -rf {} +
	(cd datalabframework/templates; zip -r etl.zip etl)
	(cd datalabframework/templates; zip -r titanic.zip titanic)

.PHONY: test

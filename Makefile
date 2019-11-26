test:
	py.test tests --tb=short

dev:
	pip install --editable .

tox:
	@tox

clean:
	find . -name '.ipython' -exec rm -rf  {} +
	find . -name '.jupyter' -exec rm -rf  {} +
	find . -name '.config' -exec rm -rf  {} +
	find . -name '.local' -exec rm -rf  {} +
	find . -name '.ivy2' -exec rm -rf  {} +
	find . -name '.ipynb_checkpoints' -exec rm -rf  {} +
	find . -name 'spark-warehouse' -exec rm -rf {} +
	rm -rf 'datafaucet.egg-info'

.PHONY: test tox

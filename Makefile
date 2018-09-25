test:
	py.test tests --tb=short

develop:
	pip install --editable .

tox-test:
	@tox

.PHONY: test

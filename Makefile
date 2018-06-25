test:
	py.test tests --tb=short

develop:
	pip install --editable .

tox-test:
	@tox

release:
	python scripts/make-release.py

.PHONY: test

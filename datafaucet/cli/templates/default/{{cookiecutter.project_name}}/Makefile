SHELL=/bin/bash

PROJECT_ROOT = $(pwd)

run:
	cd $(PROJECT_ROOT)/src && datafaucet run main.ipynb

clean:
	find $(PROJECT_ROOT) -name '.ipynb_checkpoints' -exec rm -rf  {} +
	find $(PROJECT_ROOT) -name 'spark-warehouse' -exec rm -rf {} +
	rm -rf $(PROJECT_ROOT)/data/correlation.csv

.DEFAULT_GOAL := demo
.PHONY: demo clean

venv:
	python3 -m venv ./venv

hooks:
	bash bin/setup_hooks.sh

.PHONY: install
install:
	pip install -e .

.PHONY: test
test:
	pytest

.PHONY: clean
clean:
	find . -iname '*.pyc' -delete

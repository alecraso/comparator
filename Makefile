.DEFAULT_GOAL := build

.PHONY: venv
venv:
	python -m venv ./venv || virtualenv venv

hooks:
	bash bin/setup_hooks.sh

.PHONY: build
build:
	python setup.py sdist

.PHONY: install
install:
	python setup.py install

.PHONY: test
test:
	python setup.py test

.PHONY: clean
clean:
	find . -iname '*.pyc' -delete

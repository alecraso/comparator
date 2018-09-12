.DEFAULT_GOAL := build

.PHONY: venv
venv:
	python3 -m venv ./venv

hooks:
	bash bin/setup_hooks.sh

.PHONY: build
build:
	python3 setup.py sdist

.PHONY: install
install:
	python3 setup.py install

.PHONY: test
test:
	python3 setup.py test

.PHONY: clean
clean:
	find . -iname '*.pyc' -delete

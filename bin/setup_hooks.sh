#!/bin/bash

pip3 install flake8
cd ./.git/hooks || exit 1
[[ ! -L ./pre-commit  ]] && ln -s ../../bin/pre-commit pre-commit || exit 0

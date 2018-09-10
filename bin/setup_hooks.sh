#!/bin/bash

pip3 install flake8
cd ./.git/hooks || exit 1
ln -s ../../bin/pre-commit pre-commit

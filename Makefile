install:
	pip install -e .

test:
	pytest

test-html:
	pytest --cov-report=html
	open htmlcov/index.html

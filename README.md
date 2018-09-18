![Comparator](https://raw.githubusercontent.com/aaronbiller/comparator/master/docs/comparator.jpg "Comparator")
# Comparator

[![pypi](https://img.shields.io/pypi/v/comparator.svg)](https://pypi.org/project/comparator/)
[![versions](https://img.shields.io/pypi/pyversions/comparator.svg)](https://pypi.org/project/comparator/)
[![CircleCI](https://circleci.com/gh/aaronbiller/comparator/tree/master.svg?style=shield)](https://circleci.com/gh/aaronbiller/comparator/tree/master)
[![Coverage Status](https://coveralls.io/repos/github/aaronbiller/comparator/badge.svg?branch=master)](https://coveralls.io/github/aaronbiller/comparator?branch=master)

Comparator is a utility for comparing the results of queries run against two databases. Future development will include support for APIs, static files, and more.


## Installation
```bash
pip install comparator
```

## Usage
### Overview
```python
from comparator import Comparator
from comparator.config import DbConfig
from comparator.db import PostgresDb

conf = DbConfig()

l = PostgresDb(**conf.default)
r = PostgresDb(**conf.other_db)
query = 'SELECT * FROM my_table ORDER BY 1'

c = Comparator(l, r, query)
c.run_comparisons()
```
```
[('first_eq_comp', True)]
```

### Included Comparisons
There are some basic comparisons included, and they can be imported and passed using constants.
```python
from comparator.comps import BASIC_COMP, LEN_COMP

c = Comparator(l, r, query, comparisons=[BASIC_COMP, LEN_COMP])
c.run_comparisons()
```
```
[('basic_comp', True), ('len_comp', True)]
```

### Queries and Exceptions
It's possible to run different queries against each database. You can raise exceptions if that's your speed.
```python
lq = 'SELECT * FROM my_table ORDER BY 1'
rq = 'SELECT id, uuid, name FROM reporting.my_table ORDER BY 1'
comparisons = [BASIC_COMP, LEN_COMP]

c = Comparator(l, r, left_query=lq, right_query=rq, comparisons=comparisons)

for name, success in c.compare():
    if not success:
        raise Exception('{} check failed!'.format(name))
```

### Custom Comparisons
Finally, you'll probably want to define your own comparison checks. You can do so by defining functions that accept `left` and `right` args, which, if coming from one of the included database classes, will be a list of tuples representing your query result. Perform whatever magic you like, and return a boolean.
```python
def left_is_longer(left, right):
    # Return True if left contains more rows than right
    return len(left) > len(right)


def totals_are_equal(left, right):
    # Return True if sum(left) == sum(right)
    sl = sr = 0
    for row in left:
        sl += int(row[1])
    for row in right:
        sr += int(row[1])
    return sl == sr


c = Comparator(l, r, query, comparisons=[left_is_longer, totals_are_equal])
c.run_comparisons()
```
```
[('left_is_longer', False), ('totals_are_equal', True)]
```

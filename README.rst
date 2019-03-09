|Comparator|

COMPARATOR
==========

|pypi| |versions| |CircleCI| |Coverage Status|

Comparator is a utility for comparing the results of queries run against
two databases. Future development will include support for APIs, static
files, and more.

Installation
------------

.. code:: bash

   pip install comparator

Usage
-----

Overview
~~~~~~~~

.. code:: python

   from spackl import db

   import comparator as cpt

   conf = db.Config()
   l = db.Postgres(**conf.default)
   r = db.Postgres(**conf.other_db)
   query = 'SELECT * FROM my_table ORDER BY 1'

   c = cpt.Comparator(l, query, r)
   c.run_comparisons()

::

   [('basic_comp', True)]

Included Comparisons
~~~~~~~~~~~~~~~~~~~~

There are some basic comparisons included, and they can be imported and
passed using constants.

.. code:: python

   from comparator.comps import BASIC_COMP, LEN_COMP

   c = cpt.Comparator(l, query, r, comps=[BASIC_COMP, LEN_COMP])
   c.run_comparisons()

::

   [('basic_comp', True), ('len_comp', True)]

Queries and Exceptions
~~~~~~~~~~~~~~~~~~~~~~

It’s possible to run different queries against each database. You can
raise exceptions if that’s your speed.

.. code:: python

   lq = 'SELECT * FROM my_table ORDER BY 1'
   rq = 'SELECT id, uuid, name FROM reporting.my_table ORDER BY 1'
   comparisons = [BASIC_COMP, LEN_COMP]

   c = cpt.Comparator(l, lq, r, rq, comps=comparisons)

   for result in c.compare():
       if not result:
           raise Exception('{} check failed!'.format(result.name))

Custom Comparisons
~~~~~~~~~~~~~~~~~~

You’ll probably want to define your own comparison checks. You can do so
by defining functions that accept ``left`` and ``right`` args, which correspond
to the results of the queries against your "left" and "right" data source,
respectively. Perform whatever magic you like, and return a boolean (or not… your choice).

.. code:: python

   def left_is_longer(left, right):
       # Return True if left contains more rows than right
       return len(left) > len(right)


   def totals_are_equal(left, right):
       # Return True if sum(left) == sum(right)
       sl, sr = 0, 0
       for row in left:
           sl += int(row[1])
       for row in right:
           sr += int(row[1])
       return sl == sr


   c = cpt.Comparator(l, query, r, comps=[left_is_longer, totals_are_equal])
   c.run_comparisons()

::

   [('left_is_longer', False), ('totals_are_equal', True)]

Access Comparator and Query Results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The results of both queries and comparisons can be checked using
standard operators, as well as for “truthiness” (ex:
``failures = [result.name for result in c.compare() if result is False]``).

Comparisons do not always need to return a boolean. Accessing the
resulting value of such a comparison is simple.

.. code:: python

   def len_diff(left, right):
       return len(left) - len(right)


   c = cpt.Comparator(l, query, r, comps=len_diff)
   res = c.run_comparisons()[0]
   if res == 0:
       print('They match')
   elif res < 0:
       print('Left is shorter by {}'.format(res.result))
   else:
       print('Left is longer by {}'.format(res.result))

It's recommended that you use the ``spackl`` package for instantiating your
"left" and "right" data source objects (``pip install spackl``). This package
was originally part of ``comparator``, and provides the following functionality:

Query results are contained in the ``QueryResult`` class, which provides
simple yet powerful ways to look up and access the output of the query.
Data can be retrieved as a dict, list, json string, or pandas DataFrame.
Rows/columns can be accesed by index, attribute, or key. Iterating on
the ``QueryResult`` returns a ``QueryResultRow``, which has the same
lookup functionality, as well as standard operators (<, >, =, etc).

.. code:: python

   from spackl import db

   conf = db.Config()
   pg = db.Postgres(**conf.default)
   res = pg.query(query_string)

   res          # [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}, {'a': 7, 'b': 8, 'c': 9}]

   res.a        # (1, 4, 7)
   res['a']     # (1, 4, 7)
   res[0]       # QueryResultRow : (1, 2, 3)

   res[0].a     # 1
   res[0]['a']  # 1
   res[0][0]    # 1

   res.dict()   # {'a': (1, 4, 7), 'b': (2, 5, 8), 'c': (3, 6, 9)}
   res.list()   # [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
   res.first()  # QueryResultRow : (1, 2, 3)

These result sets can be used to great effect in comparison callables.
For example, accessing the result of a query as a pandas DataFrame
allows for an endless variety of checks/manipulations do be done on a
single query output.

Support is being added to ``spackl`` to allow for querying from files and APIs
using the same methods, allowing for easy comparison between many disparate
data sources. Stay tuned.

.. |Comparator| image:: https://raw.githubusercontent.com/aaronbiller/comparator/master/docs/comparator.jpg
.. |pypi| image:: https://img.shields.io/pypi/v/comparator.svg
   :target: https://pypi.org/project/comparator/
.. |versions| image:: https://img.shields.io/pypi/pyversions/comparator.svg
   :target: https://pypi.org/project/comparator/
.. |CircleCI| image:: https://circleci.com/gh/aaronbiller/comparator/tree/master.svg?style=shield
   :target: https://circleci.com/gh/aaronbiller/comparator/tree/master
.. |Coverage Status| image:: https://coveralls.io/repos/github/aaronbiller/comparator/badge.svg?branch=master
   :target: https://coveralls.io/github/aaronbiller/comparator?branch=master

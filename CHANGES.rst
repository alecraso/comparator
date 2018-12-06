CHANGELOG
=========

0.4.0rc3 (2018-12-05)
---------------------

- Adds better transaction handling in the PostgresDb class
- Cleans up calls to connect() in the Db classes

0.4.0rc2 (2018-12-05)
---------------------

- BREAKING - ``QueryPair`` arguments order has changed (``QueryPair(left, lquery, right, rquery)``)
- ``QueryPair``, ``Comparator``, and ``ComparatorSet`` no longer require a "right" Db

0.4.0rc1 (2018-11-07)
---------------------

- DEPRECATED - the ``from_list`` method on ``ComparatorSet``
- adds the ``QueryPair`` class
- BREAKING - ``Comparator`` and ``ComparatorSet`` are instantiated with ``QueryPair`` objects
- BREAKING - ``ComparatorSet.from_dict()`` requires the dict as the first argument
- BREAKING - ``QueryResult.keys()`` and ``QueryResult.values()`` both return generators
- the ``rquery`` passed to a ``QueryPair`` can be formatted with the ``lquery`` query result
- adds the ``QueryResultCol`` class
- adds the ``append``, ``pop``, ``extend``, and ``filter`` methods on ``QueryResult``
- downgrades pandas version requirement to >=0.22.0
- improves docstrings on ``QueryResult`` methods
- adds slice handling to ``QueryResult``
- adds ``empty`` property to ``QueryResult``

0.3.2 (2018-10-04)
------------------

- adds MANIFEST.in for readme and changes

0.3.1 (2018-10-03)
------------------

- adds ``creds_file`` to possible BigQueryDb init kwargs

0.3.0 (2018-10-03)
------------------

-  DEPRECATED - the ``query_df`` method on ``BaseDb`` and subclasses
-  DEPRECATED - the ``output`` kwarg for Comparator results
-  adds the ``execute`` method on ``BaseDb`` and subclasses
-  adds the ``QueryResult`` and ``QueryResultRow`` classes
-  adds the ``ComparatorSet`` class
-  adds ``list_tables`` and ``delete_table`` methods to ``BigQueryDb``
-  cleans up some python 2/3 compatability using six

0.2.1 (2018-09-19)
------------------

-  officially support Python 2.7, 3.6, and 3.7

0.2.0 (2018-09-18)
------------------

-  adds ``query_df`` methods for returning pandas DataFrames
-  adds ``output`` kwarg to Comparator to allow calling the ``query_df`` method

0.1.0 (2018-09-12)
------------------

-  initial release

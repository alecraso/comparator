# Changelog

## 0.3.0 (2018-10-03)
- DEPRECATED - the `query_df` method on `BaseDb` and subclasses
- DEPRECATED - the `output` kwarg for Comparator results
- add the `execute` method on `BaseDb` and subclasses
- add the QueryResult and QueryResultRow classes
- add the ComparatorSet class
- add `list_tables` and `delete_table` methods to `BigQueryDb`
- cleans up some python 2/3 compatability using six

## 0.2.1 (2018-09-19)
- officially support Python 2.7, 3.6, and 3.7

## 0.2.0 (2018-09-18)
- add `query_df` methods for returning pandas DataFrames
- add `output` kwarg to Comparator to allow calling the `query_df` method

## 0.1.0 (2018-09-12)
- initial release
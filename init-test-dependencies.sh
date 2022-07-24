#!/bin/sh

# Initialise dependencies for unit tests

mysql -h localhost --protocol=TCP -uroot -ptestpwd < scripts/mysql-test.sql


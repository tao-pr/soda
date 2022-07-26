#!/bin/sh

# Initialise dependencies for unit tests
# NOTE: password is exposed to commandline on purpose because this is for test only
mysql -h localhost --protocol=TCP -uroot -ptestpwd < scripts/mysql-test.sql


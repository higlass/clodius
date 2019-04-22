#!/usr/bin/env bash
set -o errexit

start() { echo travis_fold':'start:$1; echo $1; }
end() { echo travis_fold':'end:$1; }
die() { set +v; echo "$*" 1>&2 ; sleep 1; exit 1; }
# Race condition truncates logs on Travis: "sleep" might help.
# https://github.com/travis-ci/travis-ci/issues/6018

start flake8
# TODO:
# - Get more files to lint cleanly.
# - Reduce the number of errors which are ignored everywhere else.
flake8 --config=.flake8-ignore
flake8 test/tsv_to_mrmatrix_test.py
flake8 scripts/tsv_to_mrmatrix.py
end flake8

start download
./get_test_data.sh
end download

start test
nosetests test
end test

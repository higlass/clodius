#!/usr/bin/env bash
set -o errexit

start() { echo travis_fold':'start:$1; echo $1; }
end() { echo travis_fold':'end:$1; }
die() { set +v; echo "$*" 1>&2 ; sleep 1; exit 1; }
# Race condition truncates logs on Travis: "sleep" might help.
# https://github.com/travis-ci/travis-ci/issues/6018

start flake8
# TODO: Remove the special cases from this file:
flake8 --config=.flake8-ignore
# TODO: Add more files to this list:
flake8 test/tsv_to_mrmatrix_test.py \
       scripts/tsv_to_mrmatrix.py
# TODO: When everything is covered,
# just lint the whole directory once,
# instead of listing special cases.
end flake8

start download
./get_test_data.sh
end download

start test
nosetests test
end test

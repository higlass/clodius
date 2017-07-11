.PHONY: clean clean-build build publish

clean: clean-pyc clean-build

clean-build:
	rm -rf build/
	rm -rf dist/

build: clean-build
	python setup.py sdist
	python setup.py bdist_wheel

publish: build
	python setup.py register
	python setup.py sdist upload
	python setup.py bdist_wheel upload

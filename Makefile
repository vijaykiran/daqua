env:
	poetry install

deps:
	poetry install

run:
	poetry run daqua

dist: build
	poetry build

help:
	poetry run daqua --help

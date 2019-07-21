#!/bin/bash
PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS='notebook' PYSPARK_PYTHON=$PWD/venv/bin/python pyspark

#!/usr/bin/env bash

# If only to document what's needed...
# run from  base directory, CCDA_OMOP_by_Python

# Setup mamba virtual environment
#mamba create --name default python=3.9 lxml duckdb
#mamba activate default
#....doesn't include foundry packages.

mamba install duckdb
mamba install lxml
mamba install pandas
mamab install numpy

echo "installing lxml"
mamba install -y -q lxml

echo "installing duckdb"
mamba install -y -q duckdb

mkdir output 2> /dev/null
mkdir logs 2> /dev/null
ln -s ../CCDA-data/resources .

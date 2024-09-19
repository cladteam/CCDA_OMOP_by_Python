#!/usr/bin/env bash

# If only to document what's needed...
# run from  base directory, CCDA_OMOP_by_Python


mamba install -y -q lxml
mamba install -y -q duckdb

mkdir output 2> /dev/null
mkdir logs 2> /dev/null
ln -s ../CCDA-data/resources .

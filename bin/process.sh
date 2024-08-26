#!/usr/bin/env bash

rm -f logs/*
rm -f output/*
python3 prototype_2/layer_datasets.py -d ../CCDA-data/resources
python3 omop/setup_omop.py


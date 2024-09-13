#!/usr/bin/env bash

rm -f logs/*
rm -f output/*
python3 -m prototype_2.layer_datasets -d ../CCDA-data/resources
python3 -m omop.setup_omop


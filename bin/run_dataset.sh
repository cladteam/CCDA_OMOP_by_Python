#!/usr/bin/env bash


mkdir logs 2> /dev/null
mkdir output 2> /dev/null

rm -f logs/*
rm -f output/*

# Ex. run the third batch of three, and export
python3 -m prototype_2.layer_datasets -ds ccda_response_files -l 3 -s 6 -x


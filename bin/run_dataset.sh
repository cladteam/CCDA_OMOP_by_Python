#!/usr/bin/env bash


mkdir logs 2> /dev/null
mkdir output 2> /dev/null

rm -f logs/*
rm -f output/*

python3 -m prototype_2.layer_datasets -ds ccda_response_files  -x -l 1000



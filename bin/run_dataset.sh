#!/usr/bin/env bash


mkdir logs 2> /dev/null
mkdir output 2> /dev/null

rm -f logs/*Person*
rm -f logs/*Measure*
rm -f logs/*Observa*
rm -f logs/*Visit*
rm -f logs/*Care*
rm -f logs/*Location*
rm -f logs/*Provider*
rm -f logs/*Procedure*
rm -f logs/*Medicat*
rm -f logs/log_config*
ls logs
rm -f logs/*
rm -f output/*

# Ex. run the third batch of three, and export
#python3 -m prototype_2.layer_datasets -ds ccda_response_files -l 3 -s 6 -x

# base run: 1000 files
date > batch_1.txt
git status >> batch_1.txt
echo "STARTING" >> batch_1.txt
python3 -m prototype_2.layer_datasets -ds ccda_response_files -l 30 -s 0 -x  >> batch_1.txt
date >> batch_1.txt




# it  not like nohup! TBD
#nohup python3 -m prototype_2.layer_datasets -ds ccda_response_files -l 1000 -s 0 -x &
#tail -f nohup.out

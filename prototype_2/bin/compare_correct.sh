#!/usr/bin/env bash

for file in *.csv
do
    echo $file
    diff correct_output/$file $file
done

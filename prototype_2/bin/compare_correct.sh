#!/usr/bin/env bash

for file in *.csv
do
    echo $file
    diff $file correct_output/$file
done

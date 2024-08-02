#!/usr/bin/env bash

for file in output/*.csv
do
    base_file=$(basename $file)
    diff prototype_2/correct_output/$base_file $base_file
    if [[ $? == 0 ]]; then
        echo "$fiel OK"
    fi
done

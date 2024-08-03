#!/usr/bin/env bash

for file in output/*.csv
do
    base_file=$(basename $file)
    echo ""
    echo "$file  <-->  prototype_2/correct_output/$base_file" 
    diff $file prototype_2/correct_output/$base_file 
    if [[ $? == 0 ]]; then
        echo "OK"
    fi
done

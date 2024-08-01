#!/usr/bin/env bash

for file in *.csv
do
    if [[  $file != 'metadata.csv' && $file != 'metadata_w_intermediate.csv' ]]; then
        echo $file
        diff correct_output/$file $file
        if [[ $? == 0 ]]; then
            echo "OK"
        fi
    fi
done

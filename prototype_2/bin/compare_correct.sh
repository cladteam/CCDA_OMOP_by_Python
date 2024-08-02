#!/usr/bin/env bash

for file in *.csv
do
        echo $file
        diff correct_output/$file $file
        if [[ $? == 0 ]]; then
            echo "OK"
        fi
	echo "==============="
done

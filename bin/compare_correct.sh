#!/usr/bin/env bash

err_count=0
for file in output/*.csv
do
    base_file=$(basename $file)
    echo ""
    echo "$file  <-->  prototype_2/correct_output/$base_file" 
    diff $file prototype_2/correct_output/$base_file 
    err_count=$(( $err_count + $? ))
done

echo "counted $err_count errors"

exit $err_count

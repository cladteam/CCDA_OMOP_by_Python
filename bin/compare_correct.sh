#!/usr/bin/env bash

err_count=0
file_count=0
for file in output/*.csv
do
    base_file=$(basename $file)
    echo ""
    echo "$file  <-->  prototype_2/correct_output/$base_file"
    diff $file prototype_2/correct_output/$base_file
    errval=$?
    if [[ $errval > 0 ]] ; then
        err_count=$(( $err_count + 1 ))
        echo "$file produced an error $errval"
        echo -n "A head: "
        head -1 $file
        echo -n "B head: "
        head -1 prototype_2/correct_output/$base_file
    else
        echo -n  "OK"
        wc -l $file
    fi
    file_count=$(( $file_count + 1))
done

echo "counted $err_count errors of $file_count files"

exit $err_count

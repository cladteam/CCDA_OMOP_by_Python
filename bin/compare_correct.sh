#!/usr/bin/env bash

err_count=0
file_count=0
missing_count=0
short_correct_count=0
for file in $(ls prototype_2/correct_output/*.csv  | grep -v domain_)
do
    base_file=$(basename $file)
    if [[ -f output/$base_file ]]
    then
        diff prototype_2/correct_output/$base_file output/$base_file 
        errval=$?
        if [[ $errval > 0 ]] ; then
            err_count=$(( $err_count + 1 ))
            echo "CORRECT <--> NEW "
            echo " prototype_2/correct_output/$base_file output/$base_file "
            echo "$file produced an error $errval"
            echo -n "A head: "
            head -1 $file
            echo -n "B head: "
            head -1 prototype_2/correct_output/$base_file
        else
            echo -n  "OK"
            wc -l $file
        fi
    else
        missing_count=$(( $missing_count + 1))
	correct_length=$( wc -l prototype_2/correct_output/$base_file | awk '{print $1}')
	if [[ $correct_length == '1' ]]
	then
	    echo "MISSING, but OK $file is length 1 in correct_output"
	    short_correct_count=$(( $short_correct_count + 1 ))
	else
            echo "NO OUTPUT for $file, \"$correct_length\" "
	fi
    fi
    file_count=$(( $file_count + 1))
done

echo "counted $err_count errors of $file_count files"
real_problem_count=$(( $missing_count - $short_correct_count ))
echo "counted $missing_count missing of $file_count files, $short_correct_count of which are just headers in the correct output, $real_problem_count + $err_count to look at"
non_reconciled_visit_id_count=$(grep RECONCILE output/* | grep -v domain_ | wc -l | awk '{print $1}'  )
if [[ $non_reconciled_visit_id_count > 0 ]] 
then
   echo "$non_reconciled_visit_id_count rows with bogus or non-reconciled visit_ids in the following files"
   grep RECONCILE output/* | grep -v domain_ | awk -F: '{print $1}' | sort -u
fi
exit $err_count

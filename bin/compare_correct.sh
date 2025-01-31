#!/usr/bin/env bash

MY_TEMP='compare_correct_tempfile'
err_count=0
file_count=0
missing_count=0
short_correct_count=0
for file in $(ls prototype_2/correct_output/*.csv  | grep -v domain_)
do
    base_file=$(basename $file)
    compare_file="logs/compare_${base_file}.log"
    date  > $compare_file
    if [[ -f output/$base_file ]]
    then
	echo "" >> $compare_file
        diff prototype_2/correct_output/$base_file output/$base_file  > $MY_TEMP
        errval=$?
        if [[ $errval > 0 ]] ; then
            err_count=$(( $err_count + 1 ))
            echo "CORRECT <--> NEW " >> $compare_file
            echo " prototype_2/correct_output/$base_file  <-->  output/$base_file " >> $compare_file
            echo "$file produced an error $errval" >> $compare_file
            echo -n "A head: " >> $compare_file
            head -1 $file >> $compare_file
            echo -n "B head: " >> $compare_file
            head -1 prototype_2/correct_output/$base_file >> $compare_file
	    cat $MY_TEMP >> $compare_file
	    echo ""  >> $compare_file
        else
            echo -n  "OK" 
            wc -l $file
        fi
	rm $MY_TEMP
    else
        missing_count=$(( $missing_count + 1))
	correct_length=$( wc -l prototype_2/correct_output/$base_file | awk '{print $1}')
	if [[ $correct_length == '1' ]]
	then
		echo "MISSING, but OK (no data expected) $file is length 1 in correct_output"
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

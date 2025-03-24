#!/usr/bin/env bash

python3 -m  prototype_2.find_paths  > temp_find_paths_output.txt
diff prototype_2/test_find_paths.txt temp_find_paths_output.txt > temp_diff.txt
success=$?
if [[ $success == 0 ]]; then
    echo "OK"
else
    echo "FAIL"
    cat temp_diff.txt
fi

echo "------------------------------------"
cat temp_find_paths_output.txt


rm temp_diff.txt
rm temp_find_paths_output.txt

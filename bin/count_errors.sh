#!/usr/bin/env bash

echo ""

count=0
for file in logs/log_domain*Person.log
do
    echo -n "$file  "
    grep ERROR $file 2> /dev/null | wc -l
    NUM=$(grep ERROR $file 2> /dev/null | wc -l )
    count=$(( $count + $NUM ))
done
person_errors=$count
echo "Person Errors: $count"
echo ""

count=0
for file in logs/log_domain*Visit.log
do 
    echo -n "$file  "
    grep ERROR $file 2> /dev/null | wc -l
    NUM=$(grep ERROR $file 2> /dev/null | wc -l )
    count=$(( $count + $NUM ))
done
echo "Visit Errors: $count"
echo ""

count=0
for file in logs/log_domain*Observation.log
do 
    echo -n "$file  "
    grep ERROR $file 2> /dev/null | wc -l
    NUM=$(grep ERROR $file 2> /dev/null | wc -l )
    count=$(( $count + $NUM ))
done
echo "Observation Errors: $count"
echo ""

count=0
for file in logs/log_domain*Measurement.log
do 
    echo -n "$file  "
    grep ERROR $file 2> /dev/null | wc -l
    NUM=$(grep ERROR $file 2> /dev/null | wc -l )
    count=$(( $count + $NUM ))
done
echo "Measurement Errors: $count"
echo ""

count=0
for file in logs/log_domain*Drug.log
do 
    echo -n "$file  "
    grep ERROR $file 2> /dev/null | wc -l
    NUM=$(grep ERROR $file 2> /dev/null | wc -l )
    count=$(( $count + $NUM ))
done
echo "Drug Errors: $count"
echo ""

count=0
for file in logs/log_domain*Condition.log
do 
    echo -n "$file  "
    grep ERROR $file 2> /dev/null | wc -l
    NUM=$(grep ERROR $file 2> /dev/null | wc -l )
    count=$(( $count + $NUM ))
done
echo "Condition Errors: $count"
echo ""


# failing when there person errors for now
echo "Person Errors: $person_errors"
#exit $person_errors
exit 0


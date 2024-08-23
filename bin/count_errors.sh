#!/usr/bin/env bash

count=0
for file in logs/log_domain*Person.log
do
    echo -n $file
    grep ERROR $file | wc -l 
    NUM=$(grep ERROR $file | wc -l )
    count=$(( $count + $NUM ))
done
echo "Person Errors: $count"

count=0
for file in logs/log_domain*Visit.log
do 
    echo -n $file
    grep ERROR $file | wc -l 
    NUM=$(grep ERROR $file | wc -l )
    count=$(( $count + $NUM ))
done
echo "Visit Errors: $count"

count=0
for file in logs/log_domain*Observation.log
do 
    echo -n $file
    grep ERROR $file | wc -l 
    NUM=$(grep ERROR $file | wc -l )
    count=$(( $count + $NUM ))
done
echo "Observation Errors: $count"

count=0
for file in logs/log_domain*Measurement.log
do 
    echo -n $file
    grep ERROR $file | wc -l 
    NUM=$(grep ERROR $file | wc -l )
    count=$(( $count + $NUM ))
done
echo "Measurement Errors: $count"

count=0
for file in logs/log_domain*Drug.log
do 
    echo -n $file
    grep ERROR $file | wc -l 
    NUM=$(grep ERROR $file | wc -l )
    count=$(( $count + $NUM ))
done
echo "Drug Errors: $count"

count=0
for file in logs/log_domain*Condition.log
do 
    echo -n $file
    grep ERROR $file | wc -l 
    NUM=$(grep ERROR $file | wc -l )
    count=$(( $count + $NUM ))
done
echo "Condition Errors: $count"


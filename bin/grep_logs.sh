#!/usr/bin/env bash

# Grep the logs for a message taht summarizes bad fields.
# Also for all too common errors.

STATUS=0

echo "ERROR SUMMARIES"
grep "DOMAIN Fields with errors" logs/*
if (( $? == 0 ))
then
    STATUS=1
fi

echo ""
echo "ERROR  tuple indices..."
grep "tuple indices must be integers or slices"  logs/* | uniq
if (( $? == 0 ))
then
    STATUS=1
fi

exit $STATUS

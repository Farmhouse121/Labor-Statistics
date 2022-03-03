#!/bin/bash
python=/usr/local/bin/python3
series=/Users/grahamgiller/Documents/Xcode/Labor\ Statistics/src/fetch.py
weights=/Users/grahamgiller/Documents/Xcode/Labor\ Statistics/src/getweights.py

for section in ap cu su
do
    echo `date`: fetching data for $section
    $python "$series" --update $section || exit $?
done

$python "$weights" --update --weight=2

echo `date`: Done.

#!/bin/zsh
python=/Users/alexcastro/anaconda3/envs/labor-statistics/bin/python
dir=/Users/alexcastro/Development/Labor-Statistics
series=${dir}/src/fetch.py
weights=${dir}/src/getweights.py

for section in ap cu su
do
    echo `date`: fetching data for $section
    $python "$series" --update --local $section || exit $?
done

$python "$weights" --update --weight=2

echo `date`: Done.

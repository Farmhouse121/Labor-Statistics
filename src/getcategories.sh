#!/bin/bash
url="https://www.bls.gov/cpi/tables/relative-importance"
data="/Users/${USER}/Dropbox/Data/CPI Taxonomies"
((year=1987))
[[ ! -e "$data" ]] && mkdir "$data"

while ((year <= $(date +%Y)))
do
    uri="${url}/${year}.txt"
    echo "$(date): fetching data for $year from $uri"
    curl "${uri}" > "${data}/${year}.txt" || exit 1
    ((++year))
done

echo "$(date): Done."

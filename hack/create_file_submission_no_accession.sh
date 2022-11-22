#!/bin/bash


if [ $# -lt 2 ]; then
  echo "usage: $0 <manifest_file> <source_bucket>"
  exit 1
fi

MANIFEST_FILE=$1
SOURCEBUCKET=$2

JSON_LINE='{"sourcePath":"SOURCEBUCKETFILEPATH","targetPath":"FILEPATH","description":"FILEID"}'

FILE_IDS=`tail -n +2  ${MANIFEST_FILE} | grep -v archived | awk -F'\t' '{print $7}'`

 while IFS= read -r line; do
    FILEPATH=`echo $line | cut -d'/' -f 2-`
    FILEID=`echo $FILEPATH | awk -F/ '{print $NF}' | sed "s/\.gz//g" | sed "s/\.tsv//g" | sed "s/\.tar//g" | sed "s/\.g[t|f]f$//g" | sed "s/\.fasta//g" | sed "s/^737K-arc-v1_\(.*\).txt$/737K-arc-v1(\1)/"`
    echo "$JSON_LINE" | sed "s|FILEPATH|$FILEPATH|g" | sed "s|SOURCEBUCKET|$SOURCEBUCKET|g" | sed "s|FILEID|$FILEID|g"
done <<< "$FILE_IDS"

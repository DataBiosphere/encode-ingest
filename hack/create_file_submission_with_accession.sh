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
    FILEID=`echo $line | awk -F/ '{print $NF}' | awk -F. '{print $1}'`
    echo "$JSON_LINE" | sed "s|FILEPATH|$FILEPATH|g" | sed "s|SOURCEBUCKET|$SOURCEBUCKET|g" | sed "s|FILEID|$FILEID|g"
done <<< "$FILE_IDS"

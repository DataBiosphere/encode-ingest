#!/bin/bash
set -x
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <manifest_file>" 
  exit 1
fi

MANIFEST_FILE=$1
LINES=`tail -n +2  ${MANIFEST_FILE} | grep -v archived | cut -f1,7`

JSON_LINE='{"sourcePath":"SOURCE_FILEPATH","targetPath":"TARGET_FILEPATH","description":"ACCESSION"}'


 while IFS= read -r line; do
    ACCESSION=`echo $line | cut -w -f1`
    
    if [[ $ACCESSION == ENCFF* ]]; then
        # change the s3 to gs
        SOURCE_FILEPATH=`echo $line | cut -w -f2 | sed "s/s3/gs/"`

        # we want to reduce the number of directories we have in the target path so remove the initial bucket name (cut command)
        # then we want to substitute all the / for _ except between the date and the uuid (the 3rd /), so replace them all then revert back the 3rd one
        TARGET_FILEPATH=`echo $SOURCE_FILEPATH | cut -d'/' -f 4- | sed "s|/|_|g" | sed "s|_|/|3"`

        FILENAME=`echo $SOURCE_FILEPATH | awk -F/ '{print $NF}'`
            
        if [[ $FILENAME == ENCFF* ]]; then
            FILEID=`echo $FILENAME | awk -F. '{print $1}'`
        else
            # encode has several files where the name of the file is slightly modified from the path of the file. this is brittle and risky
            FILEID=`echo $FILENAME | sed "s/\.gz//g" | sed "s/\.tsv//g" | sed "s/\.tar//g" | sed "s/\.g[t|f]f$//g" | sed "s/\.fasta//g" | sed "s/^737K-arc-v1_\(.*\).txt$/737K-arc-v1(\1)/"`
        fi

        if [[ $FILEID != $ACCESSION ]]; then
            echo "$ACCESSION,$FILEID,$TARGET_FILEPATH"
        fi
    fi

    # in the next command, we can't use '/' as the s delimiter because the variables have /s in them when expanded
    #echo "$JSON_LINE" | sed "s|SOURCE_FILEPATH|$SOURCE_FILEPATH|g" | sed "s|TARGET_FILEPATH|$TARGET_FILEPATH|g" | sed "s|ACCESSION|$ACCESSION|g"
done <<< "$LINES"

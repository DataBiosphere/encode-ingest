#!/bin/bash

if [ $# -lt 4 ]; then
  echo "usage: $0 <project-id> <dataset-name> <tag-name> <num-files>"
  exit 1
fi

PROJECT_ID=$1
DATASET_NAME=$2
TAG_NAME=$3
NUM_FILES=$4

JSON_LINE='{"file_id":"__FILE__","file_ref":"__REF__"}'
 
function transform() {
  # $1 should be the variable with the missing ids, $2 is the text to echo after the number of found files
  while IFS= read -r line; do
    REF=`echo "$line" | awk '{print $1}'`
    FILE_ID=`echo "$line" | awk '{print $NF}' | awk -F'/' '{print $NF}' | awk -F'.' '{print $1}' | sed 's/\(.*\)bed/\1/'`
    echo "$JSON_LINE" | sed "s/__FILE__/${FILE_ID}/g" | sed "s/__REF__/${REF}/g"
  done <<< "$1"
}
 
FILE_DATA_SQL="SELECT file_id, checksum_crc32c, checksum_md5, target_path  FROM datarepo_load_history WHERE load_tag=\"${TAG_NAME}\" and state=\"succeeded\""
FILE_DATA=`bq --project_id=${PROJECT_ID} --format=sparse query --dataset_id=${DATASET_NAME} -n $NUM_FILES "${FILE_DATA_SQL}" | tail -n+3`
transform "$FILE_DATA"


#!/bin/bash
 

if [ $# -lt 4 ]; then
  echo "usage: $0 <project-id> <dataset-name> <tag-name> <output-file"
  exit 1
fi

PROJECT_ID=$1
DATASET_NAME=$2
TAG_NAME=$3
OUTPUT_FILE=$4

rm ${OUTPUT_FILE}

JSON_LINE='{"file_id":"FILE","file_ref":"REF"}'
 
function transform() {
  # $1 should be the variable with the missing ids, $2 is the text to echo after the number of found files
  while IFS= read -r line; do
    REF=`echo "$line" | awk '{print $1}'`
    FILE_ID=`echo "$line" | awk '{print $NF}' | awk -F'/' '{print $NF}' | awk -F'.' '{print $1}'`
    echo "$JSON_LINE" | sed "s/FILE/${FILE_ID}/g" | sed "s/REF/${REF}/g" >> ${OUTPUT_FILE}
  done <<< "$1"
}
 
FILE_DATA_SQL="SELECT file_id, checksum_crc32c, checksum_md5, target_path  FROM datarepo_load_history WHERE load_tag=\"${TAG_NAME}\" and state=\"succeeded\""
FILE_DATA=`bq --project_id=${PROJECT_ID} --format=sparse query --dataset_id=${DATASET_NAME} -n 250000 "${FILE_DATA_SQL}" | tail -n+3`
transform "$FILE_DATA"


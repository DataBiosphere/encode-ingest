#!/bin/bash


if [ $# -lt 3 ]; then
  echo "usage: $0 <project-id> <dataset-name> <fileids-file>"
  exit 1
fi

PROJECT_ID=$1
DATASET_NAME=$2
FILE_IDS=`cat $3`

JSON_LINE='{"file_id":"__FILE__","file_ref":"__REF__"}'
 
function get_file_data() {
  # $1 should be the variable with the missing ids, $2 is the text to echo after the number of found files
  while IFS= read -r line; do
    FILE_DATA_SQL="SELECT file_id FROM datarepo_load_history WHERE state=\"succeeded\" AND target_path LIKE \"%$line%\""
    REF=`bq --project_id=${PROJECT_ID} --format=sparse query --dataset_id=${DATASET_NAME} "${FILE_DATA_SQL}" | tail -n1 | tr -d "[:space:]"`
    echo "$JSON_LINE" | sed "s/__FILE__/${line}/g" | sed "s/__REF__/${REF}/g"
  done <<< "$1"
}
 
get_file_data "$FILE_IDS"


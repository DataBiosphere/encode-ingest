#!/bin/bash

if [ $# -lt 2 ]; then
  echo "usage: $0 <project-id> <dataset-name>"
  exit 1
fi

PROJECT_ID=$1
DATASET_NAME=$2

JSON_LINE='{"file_id":"FILE","label":"FILE","xref":["https://www.encodeproject.org/files/FILE/"],"data_modality":[],"audit_labels":[],"file_type":"Unavailable","quality_metrics":[],"library_id":[],"uses_sample_biosample_id":[],"donor_id":[],"derived_from_file_id":[]}'

MISSING_FILE_IDS=`bq --project_id=${PROJECT_ID} --format=sparse query --dataset_id=${DATASET_NAME} -n 5000 --nouse_legacy_sql "SELECT distinct(alignment_used_file) as MissingFile FROM alignmentactivity, UNNEST(used_file_id) alignment_used_file LEFT OUTER JOIN file AS file ON alignment_used_file=file_id WHERE file_id IS NULL" | grep ENCFF | tr -d "[:blank:]"`
#| awk '{print "{\"file_id\":\""$1"\",\"file_type\":\"unavailable\"}"}'
while IFS= read -r line; do
    echo "$JSON_LINE" | sed "s/FILE/$line/g"
done <<< "$MISSING_FILE_IDS"


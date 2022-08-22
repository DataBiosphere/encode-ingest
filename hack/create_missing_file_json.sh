#!/bin/bash

if [ $# -lt 3 ]; then
  echo "usage: $0 <project-id> <dataset-name> <outputfilename>"
  exit 1
fi

PROJECT_ID=$1
DATASET_NAME=$2
OUTPUT_FILE=$3
TMP_FILE="${OUTPUT_FILE}.tmp"

rm ${OUTPUT_FILE}
rm ${TMP_FILE}

JSON_LINE='{"file_id":"FILE","label":"FILE","xref":["https://www.encodeproject.org/files/FILE/"],"data_modality":[],"audit_labels":[],"file_type":"Unavailable","quality_metrics":[],"library_id":[],"uses_sample_biosample_id":[],"donor_id":[],"derived_from_file_id":[]}'


function transform() {
  # $1 should be table name, $2 shoulde be field name
  MISSING_FILE_SQL="SELECT distinct(linked_file)  as MissingFile FROM $1, UNNEST($2) linked_file WHERE linked_file NOT IN (SELECT file_id FROM file)"
  MISSING_FILE_IDS=`bq --project_id=${PROJECT_ID} --format=sparse query --dataset_id=${DATASET_NAME} -n 250000 --nouse_legacy_sql "${MISSING_FILE_SQL}" | tail -n+3 | tr -d "[:blank:]"`
  NUM_MISSING=0
  while IFS= read -r line; do
      if [ ! $line = "" ]; then
        NUM_MISSING=$((NUM_MISSING+1))
        echo "$JSON_LINE" | sed "s/FILE/$line/g" >> ${TMP_FILE}
      fi
  done <<< "$MISSING_FILE_IDS"
  echo "Found ${NUM_MISSING} missing files from attribute $2 in table $1"
}

transform "alignmentactivity" "generated_file_id"
transform "alignmentactivity" "used_file_id"
transform "analysisactivity" "derived_from_file_id"
transform "analysisactivity" "generated_file_id"
transform "assayactivity" "generated_file_id"
transform "experimentactivity" "used_file_id"
transform "experimentactivity" "generated_file_id"
transform "file" "derived_from_file_id"
transform "file" "paired_with_file_id"
transform "referencefileset" "derived_from_file_id"
transform "referencefileset" "generated_file_id"
transform "referencefileset" "original_file_id"
transform "referencefileset" "related_file_id"
transform "sequencingactivity" "generated_file_id"
transform "stepactivity" "used_file_id"
transform "stepactivity" "generated_file_id"

TOTAL_NUM=`wc -l ${TMP_FILE}`
sort ${TMP_FILE} | uniq > ${OUTPUT_FILE}
echo "Found ${TOTAL_NUM} files before removing dupes; have `wc -l ${OUTPUT_FILE}` after removing dupes"

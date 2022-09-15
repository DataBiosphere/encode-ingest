#!/bin/bash

if [ $# -lt 2 ]; then
  echo "usage: $0 <project-name> <dataset-name> [findability]"
  exit 1
fi

PROJECT_NAME=$1
DATASET_NAME=$2

if [[ $# -eq 3 && $3 = "findability" ]]; then
  FINDABILITY_ONLY=1
fi


function count_unnest() {
  # $1 should be table name, $2 shoulde be field name
  MISSING_FILE_SQL="SELECT count(distinct(linked_file)) as MissingFile FROM $1, UNNEST($2) linked_file WHERE linked_file NOT IN (SELECT file_id FROM file)"
  NUM_MISSING=`bq --project_id=${PROJECT_NAME} --format=sparse query --dataset_id=${DATASET_NAME} --nouse_legacy_sql "${MISSING_FILE_SQL}" | tail -n+3 | tr -d "[:blank:]"`
  echo "Found ${NUM_MISSING} missing files from attribute $2 in table $1"
}

function count() {
  MISSING_FILE_SQL="SELECT count(distinct($2)) FROM $1 WHERE $2 NOT IN (SELECT file_id FROM file)"
  NUM_MISSING=`bq --project_id=${PROJECT_NAME} --format=sparse query --dataset_id=${DATASET_NAME} --nouse_legacy_sql "${MISSING_FILE_SQL}" | tail -n+3 | tr -d "[:blank:]"`
  echo "Found ${NUM_MISSING} missing files from attribute $2 in table $1"
}

count_unnest "alignmentactivity" "generated_file_id"
count_unnest "alignmentactivity" "used_file_id"
count_unnest "assayactivity" "generated_file_id"
count_unnest "sequencingactivity" "generated_file_id"

if [ -z $FINDABILITY_ONLY ]; then
  count_unnest "analysisactivity" "used_file_id"
  count_unnest "analysisactivity" "generated_file_id"
  count_unnest "experimentactivity" "used_file_id"
  count_unnest "experimentactivity" "generated_file_id"
  count_unnest "file" "derived_from_file_id"
  count_unnest "referencefileset" "derived_from_file_id"
  count_unnest "referencefileset" "generated_file_id"
  count_unnest "referencefileset" "original_file_id"
  count_unnest "referencefileset" "related_file_id"
  count_unnest "stepactivity" "used_file_id"
  count_unnest "stepactivity" "generated_file_id"
  count "file" "paired_with_file_id"
fi

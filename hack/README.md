
## Directions for ingesting missing file data

    ./create_missing_file_json.sh <project> <dataset> > missing_file_ingest.json

Copy the file to a google bucket, replace the PATH, TAG, and PROFILE_ID in the json request below, and then use 
swagger `https://data.terra.bio/api/repository/v1/datasets/<dataset_id>/ingest` to send the json which will load the 
missing file metadata.
    
    {
    "table": "file",
    "path": "__PATH__",
    "format": "json",
    "load_tag": "__TAG__",
    "profile_id": "__PROFILE_ID__",
    "max_bad_records": 0,
    "max_failed_file_loads": 0,
    "ignore_unknown_values": true,
    "resolve_existing_files": false,
    "updateStrategy": "append"
    }

You can check to see if there are any missing files by running 

    ./check_for_missing_files.sh <project> <dataset> > missing_file_ingest.json


## Directions for loading the files into TDR

Download the manifest from encode `encode_file_manifest.tsv`
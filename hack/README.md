# Process for ingesting encode
## Create the dataset and set permissions

Create a dataset in TDR. For the schema tables, from a clone of `https://github.com/DataBiosphere/encode-ingest.git`, run

	sbt generateJadeSchema
	
and then copy the table section from `schema/target/schema.json` into the json below at `<table_data>`.

Then use the swagger at `https://data.terra.bio/swagger-ui.html#/datasets/createDataset`

	{
		"name": "encode_in_anvil_v6",
		"description": "some findability fixes, with file data",
		"defaultProfileId": "0e6dd763-e8ef-4aad-bc99-f0fc7bbf2a76",
		"schema": {
		"tables": [<table_data>]
		},
		"cloudPlatform": "gcp",
		"enableSecureMonitoring": false,
		"experimentalSelfHosted": false,
		"properties": {},
		"dedicatedIngestServiceAccount": false
	}


## Directions for ingesting missing file data

    ./create_missing_file_json.sh <project> <dataset> > missing_file_ingest.json

Note: currently the script does not handle bq errors. Look at the output file to see if the correct actions were performed.

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

    ./check_for_missing_files.sh <project> <dataset> 


## Directions for loading the files into TDR

Download the manifest from encode `encode_file_manifest.tsv`

Run

	./create_file_submission <manifest_file> <source_bucket> > output.json
	
This reads the manifest and creates a json entry for each file that needs to be ingested. It skips archived files. Copy the output file to a google bucket, replace the variables in the json below, and then use the swagger endpoint to load the files into the dataset: `https://data.terra.bio/swagger-ui.html#/datasets/bulkFileLoad` 

	{
		"profileId": "__PROFILE_ID__",
		"loadTag": "TAG_NAME",
		"maxFailedFileLoads": 10,
		"loadControlFile": "__GSPATH_TO_FILE__"
	}
	
### Update the file entries with the DRS IDs assigned by the data repo

One way to get the DRS Ids that have been assigned to the files that were uploaded is to use the swagger enpoint: `https://data.terra.bio/swagger-ui.html#/datasets/getLoadHistoryForLoadTag`. Another way is to query the `datarepo_load_history` table directly which is what the script below does. Run this script to build the json that will update the file entries that now have DRS IDs. 

	./create_DRS_upload.sh <project-id> <dataset-name> <tag-name> <num-files> > drs_upload.json
	
Copy the output file to a google bucket, replace the variables below and use the swagger endpoint to load the DRS ref IDs. (Note: it's important to use the updateStrategy `merge`).
	
	 {
	    "table": "file",
	    "path": "__DRS_UPLOAD_FILE__",
	    "format": "json",
	    "load_tag": "__TAG__",
	    "profile_id": "__PROFILE_ID__",
	    "max_bad_records": 0,
	    "max_failed_file_loads": 0,
	    "ignore_unknown_values": true,
	    "resolve_existing_files": false,
	    "updateStrategy": "merge"
    }
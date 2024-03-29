# Process for ingesting encode
## Create the dataset and set permissions

Create a dataset in TDR. For the schema tables, from a clone of `https://github.com/DataBiosphere/encode-ingest.git`, run

	sbt generateJadeSchema
	
and then copy the contents of the file `pbcopy < schema/target/schema.json` into the json below at `<table_data>`*use `pbcopy` to load the contents into your buffer without opening the file*.

Then use the swagger at `https://data.terra.bio/swagger-ui.html#/datasets/createDataset`

	{
		"name": "encode_in_anvil_v6",
		"description": "some findability fixes, with file data",
		"defaultProfileId": "0e6dd763-e8ef-4aad-bc99-f0fc7bbf2a76",
		"schema": <table_data>,
		"cloudPlatform": "gcp",
		"enableSecureMonitoring": false,
		"experimentalSelfHosted": true,
		"properties": {},
		"dedicatedIngestServiceAccount": true
	}

Grant steward access to these users

	encode-argo-runner@broad-dsp-monster-prod.iam.gserviceaccount.com
    ahaessly@broadinstitute.org
    prod-dsp-data-ingest@firecloud.org
    monster@firecloud.org
    kreinold@broadinstitute.org
    rcox@broadinstitute.org

## Run argo workflow

See the build section in [README](https://github.com/DataBiosphere/encode-ingest/#build-and-deploy-code-chages-from-branch) to build updated version of the ingest code

### Steps in monster-deploy repo

1. Clone the monster-deploy repo `https://github.com/broadinstitute/monster-deploy.git`
1. Checkout branch `ah-encode-test`
1. Edit `environments/prod/helm/orchestration-workflows/encode/values.yaml` and fill in the dataset info and the version number you want deployed.

	*Do not prefix the dataset name with `datarepo_`. The data project will start with `datarepo-`*
	
	Set the schedule to kick off a few minutes in the future.

		enable: true
		schedule: '52 14 * * *'
		argo:
			env: prod
			vaultPrefix: secret/dsde/monster/prod/command-center
			namespace: encode
			artifactBucket: broad-dsp-monster-encode-prod-argo-archive
		repo:
			profileId: 0e6dd763-e8ef-4aad-bc99-f0fc7bbf2a76
			datasetId: <dataset-id>
			datasetName: <dataset-name>
			dataProject: <data-project>
			url: https://data.terra.bio
		chart:
			git: false
			ref: 1.0.<x>
			
	
1. Connect to non-split VPN
1. Deploy the updated config by running `./hack/apply-orchestration-workflow prod encode`
1. Copy your google access token `gcloud auth print-access-token`
1. Log into argo `https://argo.monster-prod.broadinstitute.org/` and in the authentication box type `Bearer <token>` replacing `<token>` with the token you just copied. 

	*This expires about every 30 minutes and you have to go through this annoying process again*
	
1. Wait for the workflow to start and then **check to make sure the correct version is running!!!**
	1. Click into the workflow (sometimes you have to go to archived workflows to see the latest)
	1. Select the 4th task `run-extraction`
	1. Click on the `YAML` button and about 10 lines down in the `Current Node` section you should see the expected version. If this starts too soon after the new version is deployed, it sometimes runs the old version. If this happens, terminate the workflow and click `resubmit`. Then verify the correct version is running.

NOTE: If the Argo Workflow succeeds, it lands in the Archived Workflows section and does not appear in the main landing page.
	 
## Directions for ingesting missing file data

Once the workflow successfully completes, we patch the dataset file table with entries for some archived and restricted files. *Make sure your dataset_name starts with `datarepo_`*

    ./create_missing_file_json.sh <project_id> <dataset_name> <output-file>.json

***Note: currently the script does not handle bq errors. Look at the output file to see if the correct actions were performed.***

Copy the output file to a google bucket, replace the PATH, TAG, and PROFILE_ID in the json request below, and then use 
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
    NOTE: Should update strategy here be to "merge" or to "replace"?

You can check to see if there are any missing files by running 

    ./check_for_missing_files.sh <project> <dataset>
    NOTE: Don't forget to prepend the word "datarepo_" in front of the name of the dataset.


## Directions for loading the files into TDR


### Create the json to load all the ENCODE FILES
This takes a long time and could be refactored not to use so many separate sed an awk commands. Also I believe the format needs to change based on us loading the files using the ingest endpoint. Also, the files on s3 change frequently so we should really re-sync from s3 which then requires a new load json. Finally, when we were using the bulk load endpoint, the system could not handle more than ~40k files at a time. So we used the split unix command to break up the large file into files with at most 40k lines.

**TODO** Merge create_file_submission_from_manifest.sh and create_file_submission_from_manifest_use_accession.sh



1. Re-sync from the s3 bucket. (This will takes days!)

1. Download the manifest from encode `encode_file_manifest.tsv`

1. Run

	`./create_file_submission_from_manifest_use_accession.sh <manifest_file> > encode-fileload.json` 
	
	*This reads the manifest and creates a json entry for each file that needs to be ingested. It skips archived files.*

1. Copy the `encode-fileload.json` file to a google bucket.


### Bulk load the files
*** DO NOT USE THIS ENDPOINT ***
Use ingestDataset instead!!!

1. Replace the variables in the json below, and then use the swagger endpoint to create the DRS references and update the files table for the self-hosted files: `https://data.terra.bio/swagger-ui.html#/datasets/{id}/ingest` 


		{
    		"table": "file",
    		"path": "__UPLOAD_FILE_PATH__",
    		"format": "json",
    		"load_tag": "__TAG_NAME__",
    		"profile_id": "__PROFILE_ID__",
    		"max_bad_records": 0,
    		"max_failed_file_loads": 0,
    		"ignore_unknown_values": true,
    		"resolve_existing_files": false,
    		"updateStrategy": "merge"
		}
	
## Define full view snapshot

	{
	  "name": "encode_in_anvil_v7_full_view",
	  "description": "Full snapshot of the dataset",
	  "contents": [
	    {
	      "datasetName": "encode_in_anvil_v7",
	      "mode": "byFullView"
	}
	  ],
	  "readers": [
	    "ahaessly@broadinstitute.org", "kreinold@broadinstitute.org", "rcox@broadinstitute.org", "schaluva@broadinstitute.org"
	
	  ],
	  "profileId": "0e6dd763-e8ef-4aad-bc99-f0fc7bbf2a76",
	  "properties": {}
	}
   
## Asset Definition
   
   
	   	{
	  "name": "biosample_findability_subset_v1",
	  "tables": [
	    {
	      "name": "activity",
	      "columns": [
		"activity_id",
		"activity_type",
		"used_file_id",
		"generated_file_id"
	      ]
	    },
	    {
	      "name": "alignmentactivity",
	      "columns": [
		"alignmentactivity_id",
		"activity_type",
		"data_modality",
		"generated_file_id",
		"used_file_id",
		"reference_assembly"
	      ]
	    },
	    {
	      "name": "antibody",
	      "columns": [
		"antibody_id",
		"target"
	      ]
	    },
	    {
	      "name": "assayactivity",
	      "columns": [
		"assayactivity_id",
		"activity_type",
		"antibody_id",
		"assay_type",
		"data_modality",
		"generated_file_id",
		"used_biosample_id"
	      ]
	    },
	    {
	      "name": "biosample",
	      "columns": [
		"biosample_id",
		"anatomical_site",
		"apriori_cell_type",
		"biosample_type",
		"diagnosis_id",
		"disease",
		"donor_age_at_collection_unit",
		"donor_age_at_collection_lower_bound",
		"donor_age_at_collection_upper_bound",
		"donor_id",
		"part_of_dataset_id"
	      ]
	    },
	    {
	      "name": "dataset",
	      "columns": [
		"dataset_id",
		"consent_group",
		"data_use_permission",
		"owner",
		"principal_investigator",
		"registered_identifier",
		"title",
		"data_modality"
	      ]
	    },
	    {
	      "name": "diagnosis",
	      "columns": [
		"diagnosis_id",
		"disease",
		"diagnosis_age_unit",
		"diagnosis_age_lower_bound",
		"diagnosis_age_upper_bound",
		"onset_age_unit",
		"onset_age_lower_bound",
		"onset_age_upper_bound",
		"phenotype",
		"phenopacket"
	      ]
	    },
	    {
	      "name": "donor",
	      "columns": [
		"donor_id",
		"organism_type",
		"part_of_dataset_id",
		"phenotypic_sex",
		"reported_ethnicity",
		"genetic_ancestry",
		"diagnosis_id"
	      ]
	    },
	    {
	      "name": "file",
	      "columns": [
		"file_id",
		"data_modality",
		"file_format",
		"file_ref",
		"reference_assembly",
		"label"
	      ]
	    },
	    {
	      "name": "project",
	      "columns": [
		"project_id",
		"funded_by",
		"generated_dataset_id",
		"principal_investigator",
		"title",
		"registered_identifier"
	      ]
	    },
	    {
	      "name": "sequencingactivity",
	      "columns": [
		"sequencingactivity_id",
		"activity_type",
		"assay_type",
		"data_modality",
		"generated_file_id",
		"used_biosample_id"
	      ]
	    },
	    {
	      "name": "variantcallingactivity",
	      "columns": [
		"variantcallingactivity_id",
		"activity_type",
		"used_file_id",
		"generated_file_id",
		"reference_assembly",
		"data_modality"
	      ]
	    }
	  ],
	  "rootTable": "biosample",
	  "rootColumn": "biosample_id",
	  "follow": [
	      "from_activity.used_file_id_to_file.file_id",
	      "from_activity.generated_file_id_to_file.file_id",
	      "from_alignmentactivity.used_file_id_to_file.file_id",
	      "from_alignmentactivity.generated_file_id_to_file.file_id",
	      "from_assayactivity.antibody_id_to_antibody.antibody_id",
	      "from_assayactivity.generated_file_id_to_file.file_id",
	      "from_assayactivity.used_biosample_id_to_biosample.biosample_id",
	      "from_biosample.diagnosis_id_to_diagnosis.diagnosis_id",
	      "from_biosample.donor_id_to_donor.donor_id",
	      "from_biosample.part_of_dataset_id_to_dataset.dataset_id",
	      "from_donor.part_of_dataset_id_to_dataset.dataset_id",
	      "from_donor.diagnosis_id_to_diagnosis.diagnosis_id",
	      "from_project.generated_dataset_id_to_dataset.dataset_id",
	      "from_sequencingactivity.generated_file_id_to_file.file_id",
	      "from_sequencingactivity.used_biosample_id_to_biosample.biosample_id",
	      "from_variantcallingactivity.used_file_id_to_file.file_id",
	      "from_variantcallingactivity.generated_file_id_to_file.file_id"
	  ]
	}

--------------------------------

## OUTDATED INSTRUCTIONS
### Update the file entries with the DRS IDs assigned by the data repo

One way to get the DRS Ids that have been assigned to the files that were uploaded is to use the swagger enpoint: `https://data.terra.bio/swagger-ui.html#/datasets/getLoadHistoryForLoadTag`. Another way is to query the `datarepo_load_history` table directly which is what the script below does. Run this script to build the json that will update the file entries that now have DRS IDs. 

	./create_DRS_upload_from_tag.sh <project-id> <dataset-name> <tag-name> <num-files> > drs_upload.json
	
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
   

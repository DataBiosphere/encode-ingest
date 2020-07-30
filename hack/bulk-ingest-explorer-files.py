from collections import namedtuple
import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
from polling import poll
from time import perf_counter
import sys
import re


# This script bulk-ingests files from a google bucket, with the following arguments:
# - A profileID (associated with the billing account)
# - The environment to use (dev/prod), will default to dev
# - The google bucket path of control file (a json file which lists source and target paths)
# - An optional dataset id (if none is provided, a temporary dataset will be used and deleted afterwards)

# read in arguments
profile_id = sys.argv[1]
is_production = (sys.argv[2] == 'prod') # default to dev
control_file_path = sys.argv[3]
dataset_id = sys.argv[4] if sys.argv[4:] else None # optional

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])
authed_session = AuthorizedSession(credentials)
Counts = namedtuple('Counts', ['succeeded', 'failed', 'not_tried'])
jade_base_url = 'https://jade-terra.datarepo-prod.broadinstitute.org/' if is_production \
    else 'https://jade.datarepo-dev.broadinstitute.org/'

# define the name and schema of a temporary dataset (if no dataset id is provided)
# datasets must have at least one table with at least one column
use_temp_dataset = dataset_id is None
test_dataset_name = 'encode_explorer_ingest_test'
test_dataset_schema = {
    "tables": [
        {
            "name": "test_table_name",
            "columns": [
                {
                    "name": "test_column",
                    "datatype": "STRING"
                }
            ]
        }
    ]
}


# Get the status of the given job: either "running", "succeeded", or "failed".
def check_job_status(job_id: str):
    response = authed_session.get(f"{jade_base_url}/api/repository/v1/jobs/{job_id}")
    if response.ok:
        return response.json()["job_status"]
    else:
        raise HTTPError("Bad response, got code of: {}".format(response.status_code))


# Check whether the given job is done.
def is_done(job_id: str) -> bool:
    status = check_job_status(job_id)
    is_done = status in ["succeeded", "failed"]
    if is_done:
        print(f"Job {status}.")
    return is_done


def get_job_result(job_id: str):
    response = authed_session.get(f'{jade_base_url}/api/repository/v1/jobs/{job_id}/result')
    if response.ok:
        return response.json()
    else:
        raise HTTPError(f'Failed to get the result. Got response : {response.json()}')

# check that the response is okay, then poll the status until it finishes
# return the final result of the job
def wait_for_result(response):
    if response.ok:
        job_id = response.json()['id']
        print("Waiting for the job to finish...")
        # check every 10 seconds until the job is finished (wait up to 48 hours)
        poll(lambda: is_done(job_id), step=10, timeout=172800)
        result = get_job_result(job_id)
        print(f"Result: {result}")
        return result
    else:
        raise HTTPError(f"Failed with response : {response.json()}")


# Create a new (temporary) jade dataset. If successful, return the id of the dataset.
# The kwargs should include the defaultProfileId, name, and schema to use in dataset creation.
def create_dataset(**kwargs):
    print("\nCreating a temporary dataset.")
    response = authed_session.post(f'{jade_base_url}/api/repository/v1/datasets', json=kwargs)
    return wait_for_result(response)['id']


# Delete the temporary jade dataset.
def delete_dataset(dataset_id: str):
    print("\nDeleting the temporary dataset.")
    response = authed_session.delete(f'{jade_base_url}/api/repository/v1/datasets/{dataset_id}')
    wait_for_result(response)


# submit a bulk file ingest request. If successful, return the id of the job.
def request_bulk_file_ingest(dataset_id: str, **kwargs):
    print("\nSubmitting the bulk file ingest request")
    start_time = perf_counter()
    response = authed_session.post(f'{jade_base_url}/api/repository/v1/datasets/{dataset_id}/files/bulk', json=kwargs)
    print(f"Response: {response.json()}")
    result = wait_for_result(response)
    time_elapsed = perf_counter() - start_time
    print(f"Bulk file ingest (jobId: {response.json()['id']}) finished in {time_elapsed} seconds.")
    # get the total number of files in each status type (succeeded, failed, not tried).
    counts = Counts(succeeded=result['succeededFiles'], failed=result['failedFiles'], not_tried=result['notTriedFiles'])
    print(f'Bulk file ingest failed on {counts.failed} files ({counts.not_tried} not tried, {counts.succeeded} successful)')


# create a new dataset with default values if no dataset id was specified
if use_temp_dataset:
    dataset_id = create_dataset(name=test_dataset_name, defaultProfileId=profile_id, schema=test_dataset_schema)
    print(f"Created dataset with ID: {dataset_id}")

# generate the load tag from the control file name
load_tag_regex = r'.*/([^/]*).json' # match the part after the last "/" but before ".json"
load_tag = re.search(load_tag_regex, control_file_path).group(1)

# submit the bulk file ingest job
request_bulk_file_ingest(
    dataset_id,
    profileId=profile_id,
    loadControlFile=control_file_path,
    loadTag=load_tag,
    maxFailedFileLoads=1000000
)

# delete dataset, if a temporary dataset was created
if use_temp_dataset:
    delete_dataset(dataset_id)
    print(f"Deleted dataset with ID: {dataset_id}")

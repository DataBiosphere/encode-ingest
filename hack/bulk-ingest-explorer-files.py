from collections import namedtuple
import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import sys
import re
import time

# This script bulk-ingests files from a google bucket, with the following arguments:
# - A profileID (associated with the billing account)
# - The environment to use (dev/prod), will default to dev
# - A comma-separated list of google bucket paths for control files (json files which list source and target paths)
# - An optional dataset id (if none is provided, a temporary dataset will be used and deleted afterwards)

# read in arguments
profile_id = sys.argv[1]
is_production = (sys.argv[2] == 'prod') # default to dev
control_file_paths = sys.argv[3].split(',')
dataset_id = sys.argv[4] if sys.argv[4:] else None # optional

# define hard-coded values
credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])
Counts = namedtuple('Counts', ['succeeded', 'failed', 'not_tried'])
jade_base_url = 'https://jade-terra.datarepo-prod.broadinstitute.org/' if is_production \
    else 'https://jade.datarepo-dev.broadinstitute.org/'
authed_session = AuthorizedSession(credentials)
use_temp_dataset = dataset_id is None
default_dataset_name = 'encode-explorer-ingest-test'
default_dataset_schema = {
    "assets": [],
    "relationships": [],
    "tables": []
}

# submit a bulk file ingest request. If successful, return the id of the job.
def submit_job(dataset_id: str, **kwargs):
    response = authed_session.post(f'{jade_base_url}/api/repository/v1/datasets/{dataset_id}/files/bulk', json=kwargs)
    if response.ok:
        return response.json()['id']
    else:
        raise HTTPError(f'Bad response, got code of: {response.status_code}')


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
    return status in ["succeeded", "failed"]


# Get more detailed status information for a bulk file ingest.
# Return the total number of files in each status type (succeeded, failed, not tried).
def get_counts(job_id: str) -> Counts:
    response = authed_session.get(f'{base_url}/api/repository/v1/jobs/{job_id}/result')
    if response.ok:
        json = response.json()
        return Counts(
            succeeded=json['succeededFiles'],
            failed=json['failedFiles'],
            not_tried=json['notTriedFiles'])
    else:
        raise HTTPError(f'Bad response, got code of: {response.status_code}')


# Create a new jade dataset. If successful, return the id of the job.
# The kwargs should include the defaultProfileId, name, and schema to use in dataset creation.
# TODO figure out how to get / return the dataset id
def create_dataset(**kwargs):
    response = authed_session.post(f'{jade_base_url}/api/repository/v1/datasets', json=kwargs)
    if response.ok:
        return response.json()['id']
    else:
        raise HTTPError(f'Bad response, got code of: {response.status_code}')

# Create a new jade dataset. If successful, return the id of the job.
def delete_dataset(id: str):
    response = authed_session.delete(f'/api/repository/v1/datasets/{id}')
    if response.ok:
        return response.json()['id']
    else:
        raise HTTPError(f'Bad response, got code of: {response.status_code}')

# Monitor the job status until it's done, then print the final status and time taken
def monitor_job(job_id: str):
    start_time = time.clock()
    # check status every 10 seconds and stop after 2 hours have passed if still not complete
    polling.poll(lambda: is_done(job_id), step=10, timeout=720)
    time_elapsed = time.clock() = start_time
    print(f'Job {job_id} finished in {time_elapsed} seconds.')


# create a new dataset with default values if no dataset id was specified
if use_temp_dataset:
    dataset_id = create_dataset(name=default_dataset_name, defaultProfileId=profile_id, schema=default_dataset_schema)

# submit the bulk file ingest job for each
job_ids = []
for control_file_path in control_file_paths:
    # generate the load tag from the file name (ex. "part-00000-of-00020")
    load_tag = re.search('/(part.*).json', control_file_path).group(1)
    job_id = submit_job(dataset_id, profileId=profile_id, loadControlFile=control_file_path, loadTag=load_tag)
    job_ids.append(job_id)
    # poll the job until it's done (eventually need to make this launch as an async task)
    monitor_job(job_id)

# print out the final results
for job_id in job_ids:
    counts = get_counts(job_id)
    print(f'Bulk load for {job_id} failed on {counts.failed} files ({counts.not_tried} not tried, {counts.succeeded} successful)')

if use_temp_dataset:
    delete_dataset(dataset_id)

# To Do:
# - figure out: do I need to set max-failures value when submitting the job? (not sure what the default value is)
# - dataset id as an optional argument (using the argparse library)
# - set up asynch monitoring
# - set up a bucket with a subset of file ingest requests

# To Test:
# - Are the args read in correctly? - done!
# - Is the dataset created and deleted successfully?
# - Is the bulk ingest launched and monitored well? Does it succeed?
# - Can the dataset still be deleted after the bulk ingest succeeds?

import google.auth
import re
import sys
from collections import namedtuple
from google.auth.transport.requests import AuthorizedSession
from polling import poll
from requests.exceptions import HTTPError
from time import perf_counter


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


# Get the status of the given job: either 'running', 'succeeded', or 'failed' using the retrieveJob endpoint.
def check_job_status(job_id: str):
    response = authed_session.get(f'{jade_base_url}/api/repository/v1/jobs/{job_id}')
    if response.ok:
        return response.json()['job_status']
    else:
        raise HTTPError('Bad response, got code of: {}'.format(response.status_code))


# Get the result of a given job using the retrieveJobResult endpoint.
def get_job_result(job_id: str):
    response = authed_session.get(f'{jade_base_url}/api/repository/v1/jobs/{job_id}/result')
    if response.ok:
        return response.json()
    else:
        raise HTTPError(f'Failed to get the result. Got response : {response.json()}')


# Check whether the given job is done.
# Return true if the status is 'succeeded' or 'failed', return false if the status is 'running'.
def is_done(job_id: str) -> bool:
    status = check_job_status(job_id)
    done = status in ['succeeded', 'failed']
    if done:
        print(f'Job {status}.')
    return done


# check that the response is okay, then poll the status until it finishes
# return the final result of the job
def wait_for_result(response):
    if response.ok:
        job_id = response.json()['id']
        print(f'Waiting for job (ID: {job_id}) to finish...')
        # check every 10 seconds until the job is finished (wait up to 48 hours)
        poll(lambda: is_done(job_id), step=10, timeout=172800)
        result = get_job_result(job_id)
        return result
    else:
        raise HTTPError(f'Failed with response : {response.json()}')


# Create a new (temporary) jade dataset. If successful, return the id of the dataset.
# The kwargs should include the defaultProfileId, name, and schema to use in dataset creation.
def create_temp_dataset(profile_id: str):
    print('\nCreating a temporary dataset.')
    args = {
        'name': 'encode_explorer_ingest_test',
        'defaultProfileId': profile_id,
        'schema': {'tables': [{'name': 'test_table_name', 'columns': [{'name': 'test_column', 'datatype': 'STRING'}]}]}
    }
    response = authed_session.post(f'{jade_base_url}/api/repository/v1/datasets', json=args)
    result = wait_for_result(response)
    result_dataset_id = result['id']
    print(f'Created dataset with ID: {result_dataset_id}')
    return result_dataset_id


# Delete the temporary jade dataset.
def delete_dataset(target_dataset_id: str):
    print('\nDeleting the temporary dataset.')
    response = authed_session.delete(f'{jade_base_url}/api/repository/v1/datasets/{target_dataset_id}')
    wait_for_result(response)
    print(f'Deleted dataset with ID: {target_dataset_id}')


# submit a bulk file ingest request. If successful, return the id of the job.
def request_bulk_file_ingest(target_dataset_id: str, load_tag: str, profile_id: str, control_file_path: str):
    print('\nSubmitting the bulk file ingest request')
    args = {
        'loadTag': load_tag,
        'profileId': profile_id,
        'loadControlFile': control_file_path,
        'maxFailedFileLoads': 1000000  # the jade api will eventually have 'unlimited' as the default
    }
    # launch the job and wait for it to finish
    start_time = perf_counter()
    response = authed_session.post(f'{jade_base_url}/api/repository/v1/datasets/{target_dataset_id}/files/bulk', json=args)
    result = wait_for_result(response)
    time_elapsed = perf_counter() - start_time

    # print result information
    print(f'Bulk file ingest finished in {time_elapsed} seconds.')
    counts = Counts(succeeded=result['succeededFiles'], failed=result['failedFiles'], not_tried=result['notTriedFiles'])
    print(f'File result counts: {counts.failed} failed, {counts.not_tried} not tried, {counts.succeeded} succeeded')


# create a temporary dataset if none was provided
use_temp_dataset = dataset_id is None
if use_temp_dataset:
    dataset_id = create_temp_dataset(profile_id)

# generate the load tag from the control file name
load_tag_regex = r'.*/([^/]*).json' # match the part after the last '/' but before '.json'
load_tag = re.search(load_tag_regex, control_file_path).group(1)

# submit the bulk file ingest job
request_bulk_file_ingest(dataset_id, load_tag, profile_id, control_file_path)

# delete dataset, if a temporary dataset was created
if use_temp_dataset:
    delete_dataset(dataset_id)

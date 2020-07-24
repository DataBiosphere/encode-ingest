from collections import namedtuple
import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import sys

# arguments & static values
profile_id = sys.argv[1]
is_production = (sys.argv[2] == 'prod') # default to dev
control_file_paths = sys.argv[3].split(',')
dataset_id = sys.argv[4] # TODO use argparse to treat this as an optional arg
credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])
Counts = namedtuple('Counts', ['succeeded', 'failed', 'not_tried'])
jade_base_url = 'https://jade-terra.datarepo-prod.broadinstitute.org/' if is_production \
    else 'https://jade.datarepo-dev.broadinstitute.org/'
authed_session = AuthorizedSession(credentials)

default_dataset_schema = {
    "assets": [],
    "relationships": [],
    "tables": []
}
default_dataset_name = 'encode-explorer-ingest-test'

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
def create_dataset(**kwargs):
    response = authed_session.post(f'{jade_base_url}/api/repository/v1/datasets', json=kwargs)
    if response.ok:
        return response.json()['id']
    else:
        raise HTTPError(f'Bad response, got code of: {response.status_code}')


# create a new dataset with default values if no dataset id was specified
if dataset_id is not None:
    create_dataset(name=default_dataset_name, defaultProfileId=profile_id, schema=default_dataset_schema)

# submit the bulk file ingest job for each
job_ids = []
for control_file_path in control_file_paths:
    # submit bulk file ingest job (need to set max failures?, use the file name as the load tag?)
    load_tag = "" # TODO decide how to set this
    job_id = submit_job(dataset_id, profileId=profile_id, loadControlFile=control_file_path, loadTag=load_tag)
    job_ids.append(job_id)

# check the job status until it's finished
# while not all jobs are done,
#  - Wait some period of time (5 minutes?)
#  - Check whether the entire job finished
#  - Get the counts of file statuses for each (does this work before the job is done? Or only after?)

# check status every 10 seconds and stop after 2 hours have passed if still not complete
# polling.poll(lambda: is_done(job_id), step=10, step_function=step_function, timeout=720)

# print out the final results
# counts = get_counts(job_id)
# print(f'Bulk load for {job_id} failed on {counts.failed} files ({counts.not_tried} not tried, {counts.succeeded} successful)')

# TODO: if a dummy dataset was used, delete it




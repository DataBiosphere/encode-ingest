from collections import namedtuple
import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import sys

# arguments & static values
dataset_id = sys.argv[1]
profile_id = sys.argv[2]
is_production = (sys.argv[3] == 'prod') # default to dev
control_file_paths = sys.argv[4].split(',')
timeout = 30 # 30 seconds
credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])
Counts = namedtuple('Counts', ['succeeded', 'failed', 'not_tried'])
jade_base_url = 'https://jade-terra.datarepo-prod.broadinstitute.org/' if is_production \
    else 'https://jade.datarepo-dev.broadinstitute.org/'
authed_session = AuthorizedSession(credentials)

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

# TODO write method to set up jade dataset

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

# print out the final results
print('\nSuccess! Final results:')
for job_id in job_ids:
    counts = get_counts(job_id)
    print(f'Bulk load for {job_id} failed on {counts.failed} files ({counts.not_tried} not tried, {counts.succeeded} successful)')




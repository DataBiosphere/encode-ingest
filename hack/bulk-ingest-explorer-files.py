from collections import namedtuple
import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import sys

# arguments & generated values
dataset_id = sys.argv[1]
profile_id = sys.argv[2]
is_production = (sys.argv[3] == 'prod')
jade_base_url = '' if is_production else 'https://jade.datarepo-dev.broadinstitute.org/'

# static values
credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])
Counts = namedtuple('Counts', ['succeeded', 'failed', 'not_tried'])
control_file_prefix = 'gs://broad-encode-migration-storage/explorer-backfill'
timeout = 30 # 30 seconds

# dynamically generated values (should be moved inside for loop)
control_file = control_file_prefix + "" # update to use file names that have been read in from bucket
load_tag = None # generate for each
max_failures = 1 # set to the number of lines in control file

authed_session = AuthorizedSession(credentials)

def submit_job(dataset_id: str, **kwargs):
    response = authed_session.post(f'{jade_base_url}/api/repository/v1/datasets/{dataset_id}/files/bulk', json=kwargs)
    if response.ok:
        return response.json()['id']
    else:
        raise HTTPError(f'Bad response, got code of: {response.status_code}')

def check_job_status(job_id: str):
    response = authed_session.get(f"{jade_base_url}/api/repository/v1/jobs/{job_id}")
    if response.ok:
        return response.json()["job_status"]
    else:
        raise HTTPError("Bad response, got code of: {}".format(response.status_code))


def is_done(job_id: str) -> bool:
    # if "running" then we want to keep polling, so false
    # if "succeeded" then we want to stop polling, so true
    # if "failed" then we want to stop polling, so true
    status = check_job_status(job_id)
    return status in ["succeeded", "failed"]

def is_success(job_id: str):
    # need to spit out the lowercase strings instead of real bools, allows the workflow to know if it succeeded or not
    if check_job_status(job_id) == "succeeded":
        return "true"
    else:
        raise ValueError("Job ran but did not succeed.")

# TODO query status until done

# print(submit_job(dataset_id, profileId=profile_id, loadControlFile=control_file, loadTag=load_tag, maxFailedFileLoads=max_failures))
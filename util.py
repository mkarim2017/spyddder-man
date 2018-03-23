#!/usr/bin/env python 
import os, time, json, requests, logging

from hysds_commons.job_utils import resolve_hysds_job


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


BASE_PATH = os.path.dirname(__file__)


def resolve_s1_slc(identifier, download_url, project):
    """Resolve S1 SLC using ASF datapool (ASF or NGAP). Fallback to ESA."""

    # determine best url and corresponding queue
    vertex_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(identifier)
    r = requests.head(vertex_url, allow_redirects=True)
    if r.status_code == 403:
        url = r.url
        queue = "{}-job_worker-small".format(project)
    elif r.status_code == 404:
        url = download_url
        queue = "factotum-job_worker-scihub_throttled"
    else:
        raise RuntimeError("Got status code {} from {}: {}".format(r.status_code, vertex_url, r.url))
    return url, queue


def resolve_source(ctx_file):
    """Resolve best URL from acquisition."""

    # read in context
    with open(ctx_file) as f:
        ctx = json.load(f)

    # ensure acquisition
    if ctx['dataset_type'] != "acquisition":
        raise RuntimeError("Invalid dataset type: {}".format(ctx['dataset_type']))

    # route resolver and return url and queue
    if ctx['dataset'] == "acquisition-S1-IW_SLC":
        url, queue = resolve_s1_slc(ctx['identifier'], ctx['download_url'], ctx['project'])
    else:
        raise NotImplementedError("Unknown acquisition dataset: {}".format(ctx['dataset']))

    return ( ctx['spyddder_extract_version'], queue, url, ctx['archive_filename'], 
             ctx['identifier'], time.strftime('%Y-%m-%d' ) )


def extract_job(spyddder_extract_version, queue, localize_url, file, prod_name,
                prod_date, wuid=None, job_num=None):
    """Map function for spyddder-man extract job."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type and disk space reqs
    job_type = "job-spyddder-extract:{}".format(spyddder_extract_version)

    # resolve hysds job
    params = {
        "localize_url": localize_url,
        "file": file,
        "prod_name": prod_name,
        "prod_date": prod_date,
    }
    job = resolve_hysds_job(job_type, queue, params=params, job_name="%s-%s" % (job_type, prod_name))

    # save to archive_filename if it doesn't match url basename
    if os.path.basename(localize_url) != file:
        job['payload']['localize_urls'][0]['local_path'] = file

    # add workflow info
    job['payload']['_sciflo_wuid'] = wuid
    job['payload']['_sciflo_job_num'] = job_num
    print("job: {}".format(json.dumps(job, indent=2)))

    return job

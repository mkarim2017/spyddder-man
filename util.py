#!/usr/bin/env python 
import os, time, json, logging


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


BASE_PATH = os.path.dirname(__file__)


def resolve_source(ctx_file):
    """Resolve best URL from acquisition."""

    # read in context
    with open(ctx_file) as f:
        ctx = json.load(f)

    # determine best url
    best_url = ctx['download_url']

    # determine queue
    queue = "factotum-job_worker-scihub_throttled"

    # prod date
    prod_date = time.strftime('%Y-%m-%d')
    
    return ( queue,
             best_url,
             ctx['archive_filename'],
             ctx['identifier'],
             prod_date,
           )


def extract_job(queue, localize_url, file, prod_name, prod_date, wuid=None, job_num=None):
    """Map function for spyddder-man extract job."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type and disk space reqs
    job_version = "dev"
    job_type = "job-spyddder-extract:{}".format(job_version)
    disk_usage = "50GB"

    # set localize urls
    localize_urls = [{
        "url": localize_url,
        "local_path": file,
    }]

    # get container mappings and dependency images from job spec
    job_spec_file = os.path.abspath(os.path.join(BASE_PATH, 'docker', 'job-spec.json.sling_extract_acquisition'))
    with open(job_spec_file) as f:
        job_spec = json.load(f)
    container_mappings = job_spec.get('imported_worker_files', {})
    dependency_images = job_spec.get('dependency_images', [])

    return {
        "job_name": "%s-%s" % (job_type, prod_name),
        "job_type": "job:%s" % job_type,
        "job_queue": queue,
        "container_mappings": container_mappings,
        "soft_time_limit": 14400,
        "time_limit": 14700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job spec for dependencies
            "job_specification": {
              "dependency_images": dependency_images,
            },
  
            # job params
            "file": file,
            "prod_name": prod_name,
            "prod_date": prod_date,

            # v2 cmd
            "_command": "/home/ops/verdi/ops/spyddder-man/extract.py",

            # disk usage
            "_disk_usage": disk_usage,

            # localize urls
            "localize_urls": localize_urls,
        }
    } 

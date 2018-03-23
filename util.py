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
    prod_date = time.stftime('%Y-%m-%d')
    
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
    job_type = "generate_l0b_lr_raw:%s" % job_version
    disk_usage = "50GB"

    # set job queue based on project
    project = "swot"

    # set localize urls
    localize_urls = []
    if l0a_lr_prime0 is not None:
        localize_urls.append({'url': os.path.join(l0a_lr_prime0_url, "%s.tif" % l0a_lr_prime0) })
        localize_urls.append({'url': os.path.join(l0a_lr_prime0_url, "%s.met.json" % l0a_lr_prime0)})
    if l0a_lr_prime1 is not None:
        localize_urls.append({'url': os.path.join(l0a_lr_prime1_url, "%s.tif" % l0a_lr_prime1) })
        localize_urls.append({'url': os.path.join(l0a_lr_prime1_url, "%s.met.json" % l0a_lr_prime1)})
    if l0a_lr_prime2 is not None:
        localize_urls.append({'url': os.path.join(l0a_lr_prime2_url, "%s.tif" % l0a_lr_prime2) })
        localize_urls.append({'url': os.path.join(l0a_lr_prime2_url, "%s.met.json" % l0a_lr_prime2)})

    # get container mappings and dependency images from job spec
    job_spec_file = os.path.abspath(os.path.join(BASE_PATH, '..', 'docker', 'job-spec.json.PGE_L0B_LR_RAW'))
    with open(job_spec_file) as f:
        job_spec = json.load(f)
    container_mappings = job_spec.get('imported_worker_files', {})
    dependency_images = job_spec.get('dependency_images', [])

    return {
        "job_name": "%s-%s" % (job_type, l0b_lr_raw_id),
        "job_type": "job:%s" % job_type,
        "job_queue": queue,
        "container_mappings": container_mappings,
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job spec for dependencies
            "job_specification": {
              "dependency_images": dependency_images,
            },
  
            # job params
            "l0a_lr_prime0": l0a_lr_prime0,
            "l0a_lr_prime1": l0a_lr_prime1,
            "l0a_lr_prime2": l0a_lr_prime2,
            "l0b_lr_raw_id": l0b_lr_raw_id,
            "ancillary1": ancillary1,
            "ancillary2": ancillary2,

            # v2 cmd
            "_command": "/home/ops/verdi/ops/swot-pcm/PGE_L0B_LR_RAW/run_pcm_int.sh",

            # disk usage
            "_disk_usage": disk_usage,

            # localize urls
            "localize_urls": localize_urls,
        }
    } 

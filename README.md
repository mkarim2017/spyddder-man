# spyddder-man
*S*imple *PY*thon *D*ata *D*iscoverer, *D*ownloader, and *E*xtracto*R*...*MAN*!

## Release
- current release: release-20170525

## Requirements
- HySDS
- Osaka

## sling.py
- Download file, push to repository and submit job for extraction and ingest
- Credentials need to go into .netrc, e.g.:
```
$ cat ~/.netrc
machine scihub.esa.int login <username> password <password>
macdef init

```
- Usage:
```
usage: sling.py [-h] [--oauth_url OAUTH_URL] [-f | -e]
                download_url repo_url prod_name {zip,tbz2,tgz} prod_date

Sling data from a source to a destination: 1) download data from a source and
verify, 2) push verified data to repository, 3) submit extract-ingest job.
HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.

positional arguments:
  download_url          download file URL (credentials stored in .netrc)
  repo_url              repository file URL
  prod_name             product name to use for canonical product directory
  {zip,tbz2,tgz}        download file type to verify
  prod_date             product date to use for canonical product directory

optional arguments:
  -h, --help            show this help message and exit
  --oauth_url OAUTH_URL
                        OAuth authentication URL (credentials stored in
                        .netrc)
  -f, --force           force download from source, upload to repository, and
                        extract-ingest job submission; by default, nothing is
                        done if the repo_url exists
  -e, --force_extract   force extract-ingest job submission; if repo_url
                        exists, skip download from source and use whatever is
                        at repo_url
```
- Example:
```
$ ./sling.py https://scihub.esa.int/dhus/odata/v1/Products\(\'99562704-541d-4781-8b51-a02e0bb04903\'\)/\$value davs://aria2-dav.jpl.nasa.gov/incoming/sentinel1a/raw/2015/08/27/S1A_IW_RAW__0SSV_20150827T001823_20150827T001855_007441_00A407_03D5.zip S1A_IW_RAW__0SSV_20150827T001823_20150827T001855_007441_00A407_03D5 zip 2015-08-27
[2015-08-27 22:19:23,325: INFO/sling] Downloading https://scihub.esa.int/dhus/odata/v1/Products('99562704-541d-4781-8b51-a02e0bb04903')/$value to S1A_IW_RAW__0SSV_20150827T001823_20150827T001855_007441_00A407_03D5.zip.
[2015-08-27 22:19:23,333: INFO/_new_conn] Starting new HTTPS connection (1): scihub.esa.int
^[[2015-08-27 22:22:33,783: INFO/upload] Uploading S1A_IW_RAW__0SSV_20150827T001823_20150827T001855_007441_00A407_03D5.zip to davs://aria2-dav.jpl.nasa.gov/incoming/sentinel1a/raw/2015/08/27/S1A_IW_RAW__0SSV_20150827T001823_20150827T001855_007441_00A407_03D5.zip
[2015-08-27 22:22:33,785: INFO/_new_conn] Starting new HTTPS connection (1): aria2-dav.jpl.nasa.gov
[2015-08-27 22:22:33,809: INFO/_new_conn] Starting new HTTPS connection (2): aria2-dav.jpl.nasa.gov
[2015-08-27 22:22:33,828: INFO/_new_conn] Starting new HTTPS connection (3): aria2-dav.jpl.nasa.gov
[2015-08-27 22:22:33,848: INFO/_new_conn] Starting new HTTPS connection (4): aria2-dav.jpl.nasa.gov
[2015-08-27 22:22:33,866: INFO/_new_conn] Starting new HTTPS connection (5): aria2-dav.jpl.nasa.gov
[2015-08-27 22:22:33,885: INFO/_new_conn] Starting new HTTPS connection (6): aria2-dav.jpl.nasa.gov
[2015-08-27 22:22:33,904: INFO/_new_conn] Starting new HTTPS connection (7): aria2-dav.jpl.nasa.gov
[2015-08-27 22:22:33,923: INFO/_new_conn] Starting new HTTPS connection (8): aria2-dav.jpl.nasa.gov
```

## extract.py
- Bootstrap canonical product generation
- Usage:
```
usage: extract.py [-h] file prod_name prod_date

Bootstrap the generation of a canonical product by downloading data from the
repository, creating the skeleton directory structure for the product and
leveraging the configured metadata extractor defined for the product in
datasets JSON config.

positional arguments:
  file        localized product file
  prod_name   product name to use for canonical product directory
  prod_date   product date to use for canonical product directory

optional arguments:
  -h, --help  show this help message and exit
```
- Example:
```
$ ./extract.py S1A_IW_RAW__0SSV_20150827T001823_20150827T001855_007441_00A407_03D5.zip S1A_IW_SLC__1SSV_20150319T001030_20150319T001101_005093_006678_6B9B 2015-08-27
```

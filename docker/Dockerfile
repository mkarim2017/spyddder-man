FROM hysds/pge-base

MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="spyddder man"

ARG git_oauth_token
ARG id
ARG gid

# provision
USER root
RUN set -ex && \
  usermod -u ${id} ops && \
  groupmod -g ${gid} ops && \
  chown -R ops:ops /home/ops/ /data && \
  usermod -g ${gid} ops

# copy config
#COPY .aws /home/ops/.aws
#COPY .netrc /home/ops/.netrc

# copy packages
COPY . /home/ops/verdi/ops/spyddder-man

# as ops user
USER ops

# create work directory
RUN set -ex \
 && mkdir -p /data/work \
 && chmod -R 755 /data \
 && chown -R ops:ops /data

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

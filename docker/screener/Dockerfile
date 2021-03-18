ARG base_image=gcr.io/panoptes-exp/panoptes-plate-solver
FROM ${base_image}

ENV PATH="/root/miniconda3/bin:${PATH}"
ENV HUNTSMAN_LOG_DIR=/home/huntsman-logs

USER root
# update apt-get
RUN apt-get update -y && \
  apt-get install -y wget ca-certificates git python3-pip vim && \
  # Install conda
  wget \
  https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
  mkdir /root/.conda && \
  bash Miniconda3-latest-Linux-x86_64.sh -b && \
  rm -f Miniconda3-latest-Linux-x86_64.sh && \
  conda init bash && \
  . ~/.bashrc && \
  conda create -n screener python=3.7 numpy astropy ipython pymongo matplotlib pyyaml pandas && \
  conda activate screener && \
  pip install astroquery panoptes-utils && \
  # Downgrade coverage version to avoid pytest issues
  pip uninstall -y coverage && \
  pip install coverage==4.5.4 && \
  cd /home/ && mkdir -p huntsman-drp && \
  # Make directories for logs and set permissions
  mkdir -p "${HUNTSMAN_LOG_DIR}"

# Copy huntsman-drp into the image and install
COPY --chown="${USER}:${USER}" ../../. /home/huntsman-drp
RUN cd /home/huntsman-drp && python setup.py develop

WORKDIR /home
ADD bash-config.sh .
RUN cat bash-config.sh >> ~/.bashrc
CMD ["/bin/bash"]

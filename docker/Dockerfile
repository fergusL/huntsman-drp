ARG base_image=lsstsqre/centos:7-stack-lsst_distrib-v21_0_0
FROM ${base_image}

# Build args
ARG user=lsst

# Environment variables
ENV LSST_HOME=/opt/lsst/software/stack
ENV USER=${user}
ENV HUNTSMAN_LOG_DIR="${LSST_HOME}"/huntsman-logs

USER root
RUN yum install -y wget vim sudo && \
  # Setup user with sudo privileges
  usermod -a -G wheel ${user} && \
  echo "${user} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers && \
  # Install git-LFS
  export GITLFS=git-lfs-linux-amd64-v2.10.0.tar.gz && \
  cd "${LSST_HOME}/_build" && \
  wget "https://github.com/git-lfs/git-lfs/releases/download/v2.10.0/${GITLFS}" && \
  tar -zxf ${GITLFS} && \
  ./install.sh && \
  rm install.sh && \
  rm -rf ${GITLFS} && \
  #Setup git-LFS for LSST (https://pipelines.lsst.io/v/DM-11077/install/git-lfs.html)
  echo "[credential \"https://lsst-sqre-prod-git-lfs.s3-us-west-2.amazonaws.com\"]" >> ~/.gitconfig && \
  echo -e "\thelper=store" >> ~/.gitconfig && \
  echo "[credential \"https://s3.lsst.codes\"]" >> ~/.gitconfig && \
  echo -e "\thelper=store" >> ~/.gitconfig && \
  touch ~/.git-credentials && \
  echo "https://:@lsst-sqre-prod-git-lfs.s3-us-west-2.amazonaws.com" >> ~/.git-credentials && \
  echo "https://:@s3.lsst.codes" >> ~/.git-credentials && \
  # Install extra python stuff into LSST conda env
  source ${LSST_HOME}/loadLSST.bash && \
  pip install ipython pymongo astroquery panoptes-utils && \
  # Copy obs_huntsman into the image (install handled by EUPS)
  cd "${LSST_HOME}" && \
  git clone https://github.com/AstroHuntsman/obs_huntsman.git && \
  # Make directory for huntsman-drp repository
  mkdir "${LSST_HOME}"/huntsman-drp && \
  # Make directories for logs and set permissions
  mkdir -p "${HUNTSMAN_LOG_DIR}" && chown -R "${user}:${user}" "${HUNTSMAN_LOG_DIR}"

# Copy huntsman-drp into the image (install handled by EUPS)
COPY --chown="${USER}:${USER}" . "${LSST_HOME}"/huntsman-drp

USER ${user}
WORKDIR ${LSST_HOME}
ADD docker/bash-config.sh .
RUN cat bash-config.sh >> ~/.bashrc
CMD ["/bin/bash"]

ARG base_image=lsstsqre/centos:7-stack-lsst_distrib-v20_0_0
FROM ${base_image}

# Build args
ARG user=lsst

# Environment variables
ENV LSST_HOME /opt/lsst/software/stack
ENV USER ${user}

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
  pip install ipython pymongo astroquery panoptes-utils tqdm && \
  # Downgrade coverage version to avoid pytest issues
  pip uninstall -y coverage && \
  pip install coverage==4.5.4 && \

  # Install astrometry.net outside of the LSST conda environment
  conda deactivate && \
  yum install -y epel-release && \
  yum install -y bzip2-devel cfitsio-devel cairo-devel libjpeg-devel libpng-devel \
    libXrender-devel xorg-x11-proto-devel zlib-devel netpbm-devel \
    netpbm-progs wcslib-devel.x86_64 swig.x86_64 python3-devel gcc make file fpack && \
  # install python dependencies (outside of the lsst conda env)
  python3 -m pip install fitsio astropy && \
  # add the directories of installed dependencies to PATH
  export PATH="/usr/lib:/usr/lib64:/usr/include:${PATH}" && \
  export PYTHONPATH="/usr/bin/python3:${PYTHONPATH}" && \
  export alias python=python3 && \
  # clone the repo:
  git clone https://github.com/dstndstn/astrometry.net.git && \
  cd astrometry.net/util && \
  ORIGINAL_LINE_START='NETPBM_INC.*' && \
  REPLACE_WITH='NETPBM_INC ?= -I\/usr\/include\/netpbm\/' && \
  sed -i "s/$ORIGINAL_LINE_START/$REPLACE_WITH/g" makefile.netpbm && \
  ORIGINAL_LINE_START='NETPBM_LIB.*' && \
  REPLACE_WITH='NETPBM_LIB ?= -L\/usr\/lib64 -lnetpbm' && \
  sed -i "s/$ORIGINAL_LINE_START/$REPLACE_WITH/g" makefile.netpbm && \
  cd .. && \
  # compile/install:
  make config > ./config_results && \
  make && \
  make extra && \
  make py && \
  make install PYTHON_SCRIPT="/usr/bin/python3"

USER ${user}
WORKDIR ${LSST_HOME}
ENV PATH="/usr/local/astrometry/bin:${PATH}"
ADD bash-config.sh .
RUN cat bash-config.sh >> ~/.bashrc
CMD ["/bin/bash"]

FROM jupyter/scipy-notebook

# Hadoop and spark versions
ARG HADOOP_VERSION=3.2.1
ARG SPARK_VERSION=2.4.4

# apache mirrors and dist
ARG APACHE_DIST=https://apache.org/dist
ARG APACHE_MIRROR=https://dist.apache.org/repos/dist/release

USER root
SHELL [ "/bin/bash", "-c" ]

# update the system
RUN apt-get -y update;

# install system utils
RUN apt-get install --no-install-recommends -y vim gnupg curl tzdata;

# install sasl and ssh-client
RUN apt-get install --no-install-recommends -y \
  openssh-client libsasl2-modules libsasl2-dev;

# install java
RUN apt-get install --no-install-recommends -y \
  ca-certificates-java openjdk-8-jre-headless;

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# install monitoring utils
RUN apt-get install --no-install-recommends -y \
  iproute2 net-tools telnet dnsutils iputils-* htop iftop;

# clean apt cache
RUN set -eux; apt-get clean; rm -rf /var/lib/apt/lists/*;

ENV HADOOP_HOME=/opt/hadoop
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native
ENV PATH="${HADOOP_HOME}/bin:${PATH}"

RUN set -eux; \
    cd /tmp;  \
    wget -qO - "${APACHE_DIST}/hadoop/common/KEYS" | gpg --import -; \
    wget       "${APACHE_MIRROR}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" --progress=bar:force; \
    wget -qO - "${APACHE_DIST}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.asc" | gpg --verify - "hadoop-${HADOOP_VERSION}.tar.gz"; \
    tar -xzf "hadoop-${HADOOP_VERSION}.tar.gz" -C /opt --owner root --group root --no-same-owner; \
    rm  -rf  "hadoop-${HADOOP_VERSION}.tar.gz" "$HOME/.gnupg"; \
    ln  -s   "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}"

ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.7-src.zip"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN set -eux; \
    cd /tmp;  \
    wget -qO - "${APACHE_DIST}/spark/KEYS" | gpg --import -; \
    wget       "${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz" --progress=bar:force; \
    wget -qO - "${APACHE_DIST}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz.asc" | gpg --verify - "spark-${SPARK_VERSION}-bin-without-hadoop.tgz"; \
    tar -xzf "spark-${SPARK_VERSION}-bin-without-hadoop.tgz" -C /opt --owner root --group root --no-same-owner; \
    rm  -rf  "spark-${SPARK_VERSION}-bin-without-hadoop.tgz" "$HOME/.gnupg"; \
    ln  -s   "/opt/spark-${SPARK_VERSION}-bin-without-hadoop" ${SPARK_HOME};

####################
# RUN mkdir -p /usr/local/share/jupyter /usr/local/etc/jupyter; \
#    chown $NB_UID:$NB_GID /usr/local/share/jupyter /usr/local/etc/jupyter;
####################

COPY conf/spark-env.sh ${SPARK_HOME}/conf/spark-env.sh
COPY scripts/preload_spark_packages.sh /tmp/preload_spark_packages.sh

# create default hadoop logs dir
RUN mkdir ${HADOOP_HOME}/logs
RUN chown -R $NB_UID ${HADOOP_HOME}/logs

USER $NB_UID

# preload the spark packages and populate /home/jovyan/.ivy2
RUN /tmp/preload_spark_packages.sh

## install jupyterlab and extension
RUN set -eux; \
  conda install -y -c plotly plotly=4.1.0; \
  conda install -y -c conda-forge jupyterlab nbdime ipyleaflet; \
  jupyter labextension install \
    @jupyter-widgets/jupyterlab-manager \
    jupyterlab_bokeh \
    jupyter-leaflet \
    nbdime-jupyterlab; \
  nbdime config-git --enable --global;

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

## cleanup
#RUN set -eux; \
#    conda clean -tipsy; \
#    npm cache clean --force; \
#    rm -rf $CONDA_DIR/share/jupyter/lab/staging; \
#    rm -rf /home/$NB_USER/.cache/yarn; \
#    rm -rf /home/$NB_USER/.node-gyp; \
#    fix-permissions $CONDA_DIR; \
#    fix-permissions /home/$NB_USER

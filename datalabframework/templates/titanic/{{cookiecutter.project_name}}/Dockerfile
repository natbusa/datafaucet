FROM jupyter/pyspark-notebook

# Only ADD the binder dir, to avoid polluting the cache
USER root
COPY binder /home/$NB_USER/binder
RUN fix-permissions /home/$NB_USER/binder

# Run the env update in as non-privileged user
USER $NB_USER
RUN conda env update -q -n base -f /home/$NB_USER/binder/environment.yml && \
    conda clean -tipsy && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# Ready to go: switch to user $NB_USER
USER $NB_USER

#!/bin/bash

## cleanup
rm -rf ./build

## create build directory
mkdir -p ./build/src/

# copy into build dir, till we move buildpacks to binder and fix the Dockerfile
cp -Ra * ./build/src
rm -rf ./build/buildi

# generate Dockerfile & auxiliary files
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
conda env update -q --name repo2docker -f $DIR/environment.localbuild.yml
source activate repo2docker
jupyter-repo2docker --no-run --no-build --debug . 2> dockerfile.tmp
sed -n -e '/FROM/,$p' dockerfile.tmp > build/Dockerfile
rm dockerfile.tmp

# # copy/move build packs and Dockerfiles
R2D_LIBPATH=$(dirname $(which repo2docker) )/../lib/python3.6/site-packages/repo2docker
cp -Ra $R2D_LIBPATH/buildpacks/* build
cp -Ra $R2D_LIBPATH/buildpacks/python build
source deactivate

# start docker image build process
URL=$(git remote show -n origin | grep Fetch | cut -d: -f2-)
REPO_PROTO="$(echo $URL | grep :// | sed -e's,^\(.*://\).*,\1,g')"
REPO_URL="$(echo ${URL/$REPO_PROTO/})"
REPO_PATH="$(echo $REPO_URL | grep / | cut -d/ -f2-)"
REPO_BRANCH="$(git branch | grep \* | cut -d ' ' -f2)"
REPO_VERSION=$(git rev-parse --short HEAD)
REPO_COMMIT_USER=$(git log -1 --pretty=format:'%an')
IMAGE_PREFIX='r2d'
IMAGE_NAME="$(echo $IMAGE_PREFIX ${REPO_PATH%.git} | tr -c [a-zA-Z0-9-_] - | sed '$s/.$//' )"
echo building image $IMAGE_NAME:$REPO_VERSION # building image r2d-natbusa-dsp-titanic:7828369
docker build ./build --build-arg NB_USER=jovyan --build-arg NB_UID=1000 -t $IMAGE_NAME:$REPO_VERSION


# #run
# docker run -p 8888:8888  $IMAGE_NAME:$REPO_VERSION jupyter notebook --ip 0.0.0.0 --port 8888
#
# # background run
# docker run $IMAGE_NAME:$REPO_VERSION jupyter nbconvert --to notebook --execute src/notebooks/main.ipynb --stdout

#!/bin/bash

pushd repo
URL=$(git remote show -n origin | grep Fetch | cut -d: -f2-)

REPO_PROTO="$(echo $URL | grep :// | sed -e's,^\(.*://\).*,\1,g')"
REPO_URL="$(echo ${URL/$REPO_PROTO/})"
REPO_PATH="$(echo $REPO_URL | grep / | cut -d/ -f2-)"
REPO_BRANCH="$(git branch | grep \* | cut -d ' ' -f2)"
REPO_VERSION=$(git rev-parse --short HEAD)

IMAGE_NAME="$(echo ${REPO_PATH%.git} $REPO_BRANCH | tr -c [a-zA-Z0-9-_] - | sed '$s/.$//' )"
echo building image $IMAGE_NAME:$REPO_VERSION 

jupyter-repo2docker --no-build --debug --user-id 1000 --user-name jovyan --image $IMAGE_NAME:$REPO_VERSION . 2> dockerfile.tmp
sed -n -e '/FROM/,$p' dockerfile.tmp > ../Dockerfile
rm dockerfile.tmp
popd

#copy files to output dir build/src
cp -Ra repo build/src

# copy/move build packs and Dockerfiles
R2D_PATH=/usr/local/lib/python3.6/site-packages/repo2docker
cp -Ra $R2D_PATH/buildpacks/conda build
cp -Ra $R2D_PATH/buildpacks/python build
mv Dockerfile build/

# debug
cat build/Dockerfile
ls -Rla build

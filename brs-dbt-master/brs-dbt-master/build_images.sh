#!/bin/sh
set -ex

# mandatory parameters
CI_REGISTRY_IMAGE=${CI_REGISTRY_IMAGE:?'CI_REGISTRY_IMAGE variable missing.'}
CI_COMMIT_SHA=${CI_COMMIT_SHA:?'CI_COMMIT_SHA variable missing.'}

for d in ./clients/* ; do
    d=${d%*/}
    d=${d##*/}
    echo "Building image for client $d"
    docker pull $CI_REGISTRY_IMAGE:$d-latest || true
    docker build --cache-from $CI_REGISTRY_IMAGE:$d-latest --build-arg ssh_prv_key="$BITBUCKET_SSH_PRIV_KEY" --build-arg ssh_pub_key="$BITBUCKET_SSH_PUB_KEY" --build-arg CLIENT=$d --tag $CI_REGISTRY_IMAGE:$CI_COMMIT_SHA --tag $CI_REGISTRY_IMAGE:$d-latest .
    docker push $CI_REGISTRY_IMAGE:$CI_COMMIT_SHA
    docker push $CI_REGISTRY_IMAGE:$d-latest
done

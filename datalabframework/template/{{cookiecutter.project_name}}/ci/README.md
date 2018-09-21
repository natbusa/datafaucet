# CI pipeline

This is the pipeline to manage data science repos.

## How to access the service

The Helm install of concourse can be accessed:

  * Within your cluster, at the following DNS name at port 8080:  `concourse-web.<namespace>.svc.cluster.local`

  * From outside the cluster, run these commands in the same shell:
```
# assuming the k8s namespace is called 'dsf'
CONCOURSE_POD_NAME=$(kubectl get pods --namespace dsf -l "app=concourse-web" \
                                      -o jsonpath="{.items[0].metadata.name}")
kubectl port-forward --namespace dsf $CONCOURSE_POD_NAME 8080:8080 &
``

2. Login with the following credentials

   - Username: concourse
   - Password: concourse
```

## Set, Activate and Run a pipeline

```
CONCOURSE_URL=http://127.0.0.1:8080
fly -t ds login -c $CONCOURSE_URL

fly -t ds set-pipeline --pipeline datascience --config pipeline.yml
fly -t ds unpause-pipeline --pipeline datascience

# one-off trigger job
fly -t ds trigger-job -j datascience/datascience

# monitor builds
fly watch
```

## To do:

implement the following:

  - import credentials for git user
  - https://github.com/swce/keyval-resource
  - run the image and collect kpi's and metrics

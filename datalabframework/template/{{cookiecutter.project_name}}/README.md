# Titanic

This repo is meant as a blueprint to structure data science projects 
which are easy to manage, reproducable, and will governed with hooks for 
replay, lint, collaboration data and result gathering, in a format
which is easily accessible both to humans as machines.

Accessing data resources:

data resources are accessed both for read and write by aliases.
Aliases are defined in the metadata.yml files under the path.
Resource metadata files can be located anywhere in the project tree.
It's allowed to have multiple aliases referring to the same resource.

```
data:
    resources:
        <aliasname>:
            ...
```

Aliases are always relative to the module where they are declared.
For instance if the notebook needs to access a local alias just refer to that.
Internally, resources are referred to appended to the path 
starting from the project `rootpath`

example of absolute aliases:
    `.elements.extract.input`  
    `.pipelines.insight.ml.randomforest.training.train`  
    `.input`
    
example of relative aliases:
    `train`


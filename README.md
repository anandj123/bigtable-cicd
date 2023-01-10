# Overview
This application will allow healthcare providers to apply schema changes to BigTable using Github Actions (CI/CD pipeline).

This application checks the directory for bigtable_schema_x.yaml files and creates a table if it doesn't already exist in BigTable. This is done with the use of Github actions and Google Cloud platform tools to apply schema changes to any BigTable instance during CI/CD processing.

# Architecture 

![Architecture](./img/arch.png)

# Build instructions
### Prerequisite
The following prerequisite is required for the build

1. [Installed Google Cloud SDK](https://cloud.google.com/sdk/docs/install)

### Create BigTable schema files

1. Use the same naming convention for all schema files
```sh

bigtable_schema_<table_id>.yaml

```

2. Create a YAML file with the following syntax
```sh

project_id: <project_id>
instance_id: <instance_name>
table_id: <table_name>
column_families:
- name: <column_family_name1>
  max_versions_rule: 2
  max_age_rule: 7
- name: <column_family_name2>
  max_versions_rule: 2
  max_age_rule: 7

```
|Variable Name|Description|
|---|---|
|project_id|Provide your project id. |
|instance_id| Provide your Bigtable insance id. |
|table_id| Provide your Bigtable table id. |

|Column Families Variable Name|Description|
|---|---|
|name|Provide the name of the column family|
|max_versions_rule|Configure the maximum number of versions for cells in a table|
|max_age_rule|Configure the maximum age for cells in a table|

### Github Action
Once the schema files are committed to the git repository, the github action will run a python script will scan the git repository for the files using the naming convention. Then, it will read all the configuration parameters listed above and create the tables in Bigtable.

# Output 
The output in Bigtable will look like the following 

![Bigtable Table](./img/output.png)


# References

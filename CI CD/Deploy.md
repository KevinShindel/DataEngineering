### Deploying tactics

- Manually ( suing UI, time-consuming)
- Programmatically (REST API / SDK)
- [Terraform](https://registry.terraform.io/providers/databricks/databricks/latest/docs) ( challenging )
- DAB (YAML + CLI)

### Declarative Automation Bundles

1. Using YAML files to specify artifacts, resources and configurations
2. CLI - have specific bundle commands to validate, deploy and run DAB`s

### Project schema

project_dir
        |
        _ resources ( additional DAB's)
        |
        _ src ( contains the source files, notebooks, *.py etc )
        |
        _ tests ( unit / integration tests for data pipeline )
        |
        _ databricks.yaml ( single based top-level bundle mapping )

### Default Bundle Template
Exist templates:
- default-python ( Python-based projects )
- default-sql (SQL-based projects )
- dbt-sql ( involving Data Build Tool )
- Mlops-stacks ( for ML operation stacks)

Template generation example: 

```shell
databricks bundle init default-python
```

### Top-Lvl mappings

```yaml
# databricks.yaml
bundle:
  name: demo-dataengineering-bundle

###############################################################
# Additional YAML configurations to include for the development
# - includes all yaml files in folders
###############################################################
include:
  - resources/*.yaml
  - jobs/*.yaml

###############################################################
# Variables name
# - Custom variables for the bundle
###############################################################
variables:
  admin_email:
    description: Email
    default: admin@databricks.com
  my_lab_user_name:
    description: Add you lab username
    default: ${workspace.current_user.short_name}
  catalog:
    description: Unity Catalog name
    default: main
  catalog_dev:
    description: Dev catalog for the project.
    default: ${var.my_lab_user_name}_dev
  bronze_path:
    description: Bronze layer storage path
    default: /Volumes/main/bronze
  notebook_path:
    description: Base notebook folder
    default: /Workspace/Shared/dataengineering
    my_cluster:
      description: My cluster
      type: complex
      default:
        spark_version: "15.4.x-scala2.11"
        node_type_id: "Standard_DS3_v2"
        num_workers: 2
      my_cluster_id:
        description: "Get the cluster ID using a lookup var"
        lookup:
          cluster: myclustername

workspaces:
  dev: https://adb-1111111111111111.1.azuredatabricks.net
  stage: https://adb-333333333333333.3.azuredatabricks.net
  prod: https://adb-2222222222222222.2.azuredatabricks.net

###############################################################
# Internal lib
# - Build an internal library using wheel
###############################################################

artifacts:
  default:
    type: whl
    path: .
    build: python -m pip wheel --wheel-dir dist .

resources:
  jobs:
    etl_job:
      email_notifications:
        on_failure: 
          - ${var.admin_email}
      name: etl-job-${bundle.target}
      tasks:
        - task_key: load_bronze
          notebook_task:
            notebook_path: ${var.notebook_path}/01_load_bronze
      permissions:
        - level: CAN_MANAGE
          group_name: data-engineers

permissions:
  - level: CAN_VIEW
    group_name: analysts
  - level: CAN_MANAGE
    group_name: data-engineers

sync:
  include:
    - notebooks/**
    - src/**
    - resources/**
  exclude:
    - .git/**
    - tests/**
    - "*.md"

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: ${workspaces.dev}
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: dev_catalog

  prod:
    mode: production
    workspace:
      host: ${workspaces.prod}
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: prod_catalog
```

### Validate / Deploy / Run DAB`s

```shell
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev l1_simple_dab
```

### Remove artifacts ( all bundles, artifacts, jobs etc.)

```shell
databricks bundle destroy --auto-approve```
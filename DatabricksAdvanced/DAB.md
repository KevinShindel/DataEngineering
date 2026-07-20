

## Structure of DAB

project_name - |
                |-> resources/ <- Additional YAML configs
                |-> src/ <- contains source files ( notebooks, python files, etc)
                |-> tests/ <- unit and integration tests
                |-> databricks.yml <- top-level bundle


The **databricks.yml** file is a required bundle configuration file used to deploy your
Databricks assets. This file must:
- Be expressed in YAML format.
- Contain at minimum the top-level bundle mapping.
- Contain at least one (and only one) bundle configuration file named **databricks.yml**.


### Top Level Mappings Structure

```yaml
bundle:
  name: demo01_bundle
resources:
  jobs:
    job1_simple_lab:
      name: job1_simple_lab
      tasks:
        - task_key: create_bronze_table
          notebook_path: ./src/bronze/create_bronze_table.ipynb
          source: WORKSPACE
targets:
  development:
    mode: development
    default: true
    workspace: 
      host: https://dev.cloud.databricks.com/
    production:
      mode: production
      workspace: 
        host: https://prod.cloud.databricks.com/

```


### Level of DAB

| Level       | Obligatory | Description                                                                       |
|-------------|------------|-----------------------------------------------------------------------------------|
| bundle      | REQUIRED   | Defines bundle metadata such as the project name.                                 | 
| resources   | REQUIRED   | Declares deployable assets like jobs, pipelines, and other resources.             | 
| targets     | REQUIRED   | Specifies deployment environments (for example, dev and prod) and their settings. | 
| variables   | OPTIONAL   | Holds reusable parameter values for configuration.                                | 
| workspace   | OPTIONAL   | Sets default workspace-level settings used by the bundle.                         | 
| permissions | OPTIONAL   | Assigns access permissions to bundle resources.                                   | 
| artifacts   | OPTIONAL   | Configures build outputs and files to package or publish.                         | 
| include     | OPTIONAL   | Imports additional YAML files into the bundle configuration.                      | 
| sync        | OPTIONAL   | Controls local file synchronization behavior with the workspace.                  | 


### DAB delivery

```shell
databricks bundle validate
databricks bundle summary
databricks bundle deploy -t production
databricks bundle run -t development l1_simple_dab
databricks bundle destroy --auto-approve

```


### Default Variable in DAB

- ${bundle.name} - Name of bundle 
- ${bundle.target} - Target environment
- ${workspace.file_path} - Path to the workspace file
- ${workspace.root_path} - Root path of the workspace
- ${resources.jobs.<job-name>.id} - ID of the job
- ${resources.models.<model-name>.name} - Name of the model
- ${resources.pipelines.<pipeline-name>.name} - Name of the pipeline


#### Simple custom variable 

```yaml
variables:
  my_lab_user_name:
    description: Your user name
    default: labuser23904
```


#### Complex custom variable

```yaml
variables: 
  my_cluster:
    description: "My cluster"
    type: complex
    default:
      spark_version: "15.4.x-scala2.11"
      node_type_id: "S
```


#### Using variables

```yaml
variables:
  my_lab_user_name:
    description: Your user name
    default: labuser23904
  catalog_dev:
    description: Development catalog reference
    default: ${var.my_lab_user_name}_1_dev #  labuser23904_1_dev
  catalog_prod:
    description: Production catalog reference
    default: ${var.my_lab_user_name}_3_prod #  labuser23904_3_prod
```

### Lookup Variables

> Dynamically Retrieve an Object’s Value

```yaml
my_cluster_id:
    description: "Get the cluster ID using a lookup variable"
    lookup:
     cluster: my_cluster_name
```

| Var Name                 | Description                       | Example                       |
|--------------------------|-----------------------------------|-------------------------------|
| alert                    | Alert variable                    | $var.alert                    |
| cluster_policy           | Cluster policy variable           | $var.cluster_policy           |
| cluster                  | Cluster variable                  | $var.cluster                  |
| dashboard                | Dashboard variable                | $var.dashboard                |
| instance_pool            | Instance pool variable            | $var.instance_pool            |
| job                      | Job variable                      | $var.job                      |
| metastore                | Metastore variable                | $var.metastore                |
| notification_destination | Notification destination variable | $var.notification_destination |
| pipeline                 | Pipeline variable                 | $var.pipeline                 |
| query                    | Query variable                    | $var.query                    |
| service_principal        | Service principal variable        | $var.service_principal        |
| warehouse                | Warehouse variable                | $var.warehouse                |


### Templates 

Use a Databricks default bundle template to
create your bundle
• Templates:
• default-python
• default-sql
• dbt-sql
• Mlops-stacks

| Template        | Description                                                                                            |
|-----------------|--------------------------------------------------------------------------------------------------------| 
| default-python  | The default Python template for Notebooks and Lakeflow                                                 |
| default-sql     | The default SQL template for .sql files that run with Databricks SQL                                   |
| default-minimal | The minimal template, for advanced users                                                               |
| default-scala   | The default Scala template for JAR jobs                                                                |
| dbt-sql         | The dbt SQL template (databricks.com/blog/delivering-cost-effective-data-real-time-dbt-and-databricks) |
| mlops-stacks    | The Databricks MLOps Stacks template (github.com/databricks/mlops-stacks)                              |
| pydabs          | A variant of the 'default-python' template that defines resources in Python instead of YAML            |
| local           | A local file system path with a template directory                                                     |
| git             | A Git repository URL, e.g. https://github.com/my/repository                                            |


**Custom Bundle Template**

- databricks bundle init /projects/templates/test-template
- To use a custom bundle template, pass its local path or remote URL to the Databricks CLI bundle init command.
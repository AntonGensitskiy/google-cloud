# Google BigQuery Merge Table Action

Description
-----------
This action allows user to merges two BigQuery tables.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be 
provided and can be set to 'auto-detect'. Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided. 
The service account key can be found on the Dashboard in the Cloud Platform Console. Make sure the 
account key has permission to access BigQuery and Google Cloud Storage. The service account key file 
needs to be available on every node in your cluster and must be readable by all users running the job.

Properties
----------
**Project ID**: Google Cloud Project ID, which uniquely identifies a project. It can be found on the 
Dashboard in the Google Cloud Platform Console. This is the project that the BigQuery job will run in.

**Source Dataset**: Dataset the source table belongs to. A dataset is contained within a specific 
project. Datasets are top-level containers that are used to organize and control access to tables and views.

**Source Table**: Name of the source table.

**Destination Dataset**: Dataset the destination table belongs to. A dataset is contained within a specific 
project. Datasets are top-level containers that are used to organize and control access to tables and views.

**Destination Table**: Name of the destination table.

**Table Key**: List of fields that determines relation between source and destination tables.

**Service Account File Path**: Path on the local file system of the service account key used for 
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. When running on other 
clusters, the file must be present on every node in the cluster.
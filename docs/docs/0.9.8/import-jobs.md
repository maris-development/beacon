
# Import Jobs

Using the import jobs feature, you can let Beacon import many datasets using the same configuration. This is useful when you have many datasets that you want to import using the same configuration. Beacon will try to import as many datasets as possible, and will report any errors that occur during the import process.

## Creating an import job

To create an import job, you can call the rest api endpoint `/admin/api/jobs/import-job` with a json body containing the configuration for the import job using a `POST` request. The following is an example of a json body for an import job configuration:

```json
{
  "driver_name": "sea-surface-satellite",
  "directory": "/datasets",
  "regex": ".*\\.nc",
  "identifier_generation": "incremental",
  "num_threads": 4,
  "include_subdirectories": true,
  "job_name": "sea-surface-satellite-import-job"
}
```

## Viewing import jobs

To view the finished and current running jobs, you can call the rest api endpoint `/admin/api/jobs/job-runner-status` using a `GET` request.
This returns all the currently finished and running jobs in JSON.

## Cancel an import job

To cancel an import job, you can call the rest api endpoint `/admin/api/jobs/cancel-job/{job_name}` with a json body containing the job name to cancel using a `DELETE` request. Replace `job_name` with the name of the import job. This will cancel the job.

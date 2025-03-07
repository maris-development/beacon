# Import Job


## Creating an import job

To create an import job, you can call the rest api endpoint `/admin/api/jobs/import-job` with a json body containing the configuration for the import job using a `POST` request. The following is an example of a json body for an import job configuration:

```json
{
  "directory": "./datasets",
  "regex": ".*\\.nc",
  "identifier_generation": "incremental",
  "include_subdirectories": true,
  "job_name": "sea-surface-satellite-import-job",
  "netcdf": {}
}
```

## Viewing import jobs

To view the finished and current running jobs, you can call the rest api endpoint `/admin/api/jobs/job-runner-status` using a `GET` request.
This returns all the currently finished and running jobs in JSON.

## Cancel an import job

To cancel an import job, you can call the rest api endpoint `/admin/api/jobs/cancel-job/{job_name}` with a json body containing the job name to cancel using a `DELETE` request. Replace `job_name` with the name of the import job. This will cancel the job.

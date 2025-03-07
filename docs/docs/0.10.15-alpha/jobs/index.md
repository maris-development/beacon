# Jobs

Beacon provides a job system that allows users to run jobs in the background. This is useful for long-running tasks that you don't want to block the main thread of the server. Currently Beacon supports the following types of jobs:
- [Import job](import-job.md) - importing many datasets into Beacon through a single API call.

## View current running jobs

To view the finished and current running jobs, you can call the rest api endpoint `/admin/api/jobs/job-runner-status` using a `GET` request.
This returns all the currently finished and running jobs in JSON.

## Cancel a job

To cancel a job, you can call the rest api endpoint `/admin/api/jobs/cancel-job/{job_name}` with a json body containing the job name to cancel using a `DELETE` request. Replace `job_name` with the name of the job. This will cancel the job.
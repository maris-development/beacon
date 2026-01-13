# Getting Started with the Beacon Data Lake

To quickly get started with the Beacon Data Lake, we have created a example repository that contains a complete setup of the Beacon Data Lake using Docker Compose. This example repository is available at [beacon-example](https://github.com/maris-development/beacon-example).

Install docker and docker-compose on your machine. You can find the installation instructions for Docker and Docker Compose in the official documentation:

- [Docker Installation](https://docs.docker.com/get-docker/)

To run the example repository, follow these steps:

- Clone the repository:

```bash
git clone https://github.com/maris-development/beacon-example.git
```

## Local File System Example

Example of running Beacon Data Lake with datasets stored locally on your machine.

- Navigate into the cloned directory:

```bash
cd beacon-example/local-file-system
```

- Start the Docker containers:

```bash
docker-compose up -d
```

- Open your web browser and navigate to `http://localhost:8080/swagger` to access the Beacon Swagger Page.

## S3 Cloud Storage Example (MinIO)

Example of running Beacon Data Lake with datasets stored in an S3-compatible cloud storage. This example uses MinIO as the S3-compatible storage solution but can be adapted to other S3-compatible services such as Amazon S3.

:::tip
You can run Beacon against existing non-owned S3 buckets that allow for anonymous access.
:::

- Navigate into the cloned directory:

```bash
cd beacon-example/cloud-storage-minio
```

- Start the Docker containers:

```bash
docker-compose up -d
```

- Open your web browser and navigate to `http://localhost:8080/swagger` to access the Beacon Swagger Page.
- Open your web browser and navigate to `http://localhost:9001` to access the MinIO Web Interface.

By default, the example will have made a bucket called `beacon-bucket`.
All changes made to the bucket will be reflected in the Beacon Data Lake. Simply upload files to the `beacon-bucket` using the MinIO Web Interface or S3-compatible tools. The Beacon Data Lake will automatically detect and index these files.

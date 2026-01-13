# Getting Started with Beacon

To quickly get started with Beacon, we provide Docker Compose examples that allow you to run Beacon Data Lake either with datasets stored on your local file system or in an S3-compatible cloud storage.
Setting these up only takes a few minutes/seconds.

:::tip
To simplify the setup process, you can clone our ready-to-use Beacon examples from the [beacon-example repository](https://github.com/maris-development/beacon-example). This repository contains pre-configured Docker Compose files and instructions to help you get started quickly.
:::

## Prerequisites

Install docker and docker-compose on your machine. You can find the installation instructions for Docker and Docker Compose in the official documentation:

- [Docker Installation](https://docs.docker.com/get-docker/)

## Beacon - Local File System

Create a docker compose file with the following content:

```yaml
version: "3.8"

services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        container_name: beacon
        restart: unless-stopped
        ports:
            - "8080:8080" # Adjust the port mapping as needed
        environment:
            - BEACON_ADMIN_USERNAME=admin # Replace with your admin username
            - BEACON_ADMIN_PASSWORD=securepassword # Replace with your admin password
            - BEACON_VM_MEMORY_SIZE=4096 # Adjust memory allocation as needed (in MB)
            - BEACON_DEFAULT_TABLE=default # Set default table name
            - BEACON_LOG_LEVEL=INFO # Adjust log level
            - BEACON_HOST=0.0.0.0 # Set IP address to listen on
            - BEACON_PORT=8080 # Set port number
        volumes:
            - ./datasets:/beacon/data/datasets # Adjust the volume mapping as required. Folder containing your datasets.
            - ./tables:/beacon/data/tables # Adjust the volume mapping as required. This enable persistence of created tables.
```

:::warning
Make sure to replace the environment variable values such as `BEACON_ADMIN_USERNAME` and `BEACON_ADMIN_PASSWORD` with your desired credentials for accessing the Beacon admin interface.
:::

Once you have created the `docker-compose.yml` file, you can start the Docker container by running the following command in the directory where the `docker-compose.yml` file is located:

```bash
docker-compose up -d
```

Open your web browser and navigate to `http://localhost:8080/swagger` to access the Beacon Swagger Page.
You can now start adding your datasets to the local folder `./datasets` that you mounted as `/beacon/data/datasets` in the Beacon container. Beacon will detect these files automatically and make them available for querying.

To start querying your datasets, we recommend using the Beacon Python Client, which you can find in the [Beacon Python Client documentation](../1.5.0-install/data-lake/python-sdk.md).
The Python Client makes it easy to connect to your Beacon instance and execute SQL or JSON queries against your datasets.

## Beacon - S3 Cloud Storage

:::tip
If you are looking for a ready to deploy docker compose that runs Beacon with MinIO as S3-compatible storage, check out the [beacon-example repository](https://github.com/maris-development/beacon-example/tree/main/cloud-storage-minio).
It contains a complete example that creates a MinIO instance alongside Beacon and connects them together. Simply upload your datasets to the MinIO instance and start querying them using Beacon.
:::

Making Beacon work with S3 compatible cloud storage is straightforward. It just takes a few environment variables. Below is an example of how to set up Beacon to use MinIO as the S3-compatible storage solution. You can adapt this example to other S3-compatible services such as Amazon S3 by changing the endpoint and credentials accordingly.

Create a docker compose file with the following content:

::: code-group

```yaml  [s3_auth.docker-compose.yml]{19-23}
version: "3.8"

services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        container_name: beacon
        restart: unless-stopped
        ports:
            - "8080:8080" # Adjust the port mapping as needed
        environment:
            - BEACON_ADMIN_USERNAME=admin # Replace with your admin username
            - BEACON_ADMIN_PASSWORD=securepassword # Replace with your admin password
            - BEACON_VM_MEMORY_SIZE=4096 # Adjust memory allocation as needed (in MB)
            - BEACON_DEFAULT_TABLE=default # Set default table name
            - BEACON_LOG_LEVEL=INFO # Adjust log level
            - BEACON_HOST=0.0.0.0 # Set IP address to listen on
            - BEACON_PORT=8080 # Set port number

            - AWS_ENDPOINT=http://minio:9000 # Set your S3 endpoint here. Adjust to your S3 provider. // [!code ++]
            - AWS_ACCESS_KEY_ID=minioadmin # Set your S3 access key here. Can be left empty for anonymous access. // [!code ++]
            - AWS_SECRET_ACCESS_KEY=minioadmin # Set your S3 secret key here. Can be left empty for anonymous access. // [!code ++]
            - BEACON_S3_BUCKET=beacon-bucket # Set your S3 bucket name here. Make sure the bucket exists. // [!code ++]
            - BEACON_S3_DATA_LAKE=true # Enable S3 data lake mode. This will make Beacon use S3 as data lake storage. // [!code ++]
        volumes:
            - ./tables:/beacon/data/tables # Adjust the volume mapping as required. This enable persistence of created tables.
```

```yaml  [s3_anonymous.docker-compose.yml]{19-23}
version: "3.8"

services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        container_name: beacon
        restart: unless-stopped
        ports:
            - "8080:8080" # Adjust the port mapping as needed
        environment:
            - BEACON_ADMIN_USERNAME=admin # Replace with your admin username
            - BEACON_ADMIN_PASSWORD=securepassword # Replace with your admin password
            - BEACON_VM_MEMORY_SIZE=4096 # Adjust memory allocation as needed (in MB)
            - BEACON_DEFAULT_TABLE=default # Set default table name
            - BEACON_LOG_LEVEL=INFO # Adjust log level
            - BEACON_HOST=0.0.0.0 # Set IP address to listen on
            - BEACON_PORT=8080 # Set port number

            - AWS_ENDPOINT=http://minio:9000 # Set your S3 endpoint here. Adjust to your S3 provider. // [!code ++]
            - AWS_SKIP_SIGNATURE=true # Set to true to skip request signing for anonymous access. // [!code ++]
            - BEACON_S3_BUCKET=beacon-bucket # Set your S3 bucket name here. Make sure the bucket exists. // [!code ++]
            - BEACON_S3_DATA_LAKE=true # Enable S3 data lake mode. This will make Beacon use S3 as data lake storage. // [!code ++]
        volumes:
            - ./tables:/beacon/data/tables # Adjust the volume mapping as required. This enable persistence of created tables.
```

:::

:::warning
Make sure to replace the environment variable values such as `BEACON_ADMIN_USERNAME` and `BEACON_ADMIN_PASSWORD` with your desired credentials for accessing the Beacon admin interface.
:::

:::tip
If you are using an S3-compatible service that allows for anonymous access, you can leave the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables empty. Beacon will then attempt to connect to the S3 service without authentication and access the specified bucket. This is useful if the S3 bucket is publicly accessible.
:::

Once you have created the `docker-compose.yml` file, you can start the Docker container by running the following command in the directory where the `docker-compose.yml` file is located:

```bash
docker-compose up -d
```

Open your web browser and navigate to `http://localhost:8080/swagger` to access the Beacon Swagger Page.
You can now start adding your datasets to the S3 bucket that you specified in the `BEACON_S3_BUCKET` environment variable. Beacon will detect these files automatically and make them available for querying.

To start querying your datasets, we recommend using the Beacon Python Client, which you can find in the [Beacon Python Client documentation](../1.5.0-install/data-lake/python-sdk.md)
The Python Client makes it easy to connect to your Beacon instance and execute SQL or JSON queries against your datasets.

## Ready-Made Examples

We provide ready-made Docker Compose examples in the [beacon-example repository](https://github.com/maris-development/beacon-example).
This repository contains pre-configured Docker Compose files and instructions to help you get started quickly.

Start by cloning the repository:

```bash
git clone https://github.com/maris-development/beacon-example.git
```

See below for two example setups: one for local file system storage and one for S3-compatible cloud storage using MinIO.

### Local File System Example

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

### S3 Cloud Storage Example (MinIO)

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

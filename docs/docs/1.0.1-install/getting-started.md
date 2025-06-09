# Getting Started with the Beacon Data Lake

To quickly get started with the Beacon Data Lake, we have created a example repository that contains a complete setup of the Beacon Data Lake using Docker Compose. This example repository is available at [beacon-example](https://github.com/maris-development/beacon-example).

Install docker and docker-compose on your machine. You can find the installation instructions for Docker and Docker Compose in the official documentation:
- [Docker Installation](https://docs.docker.com/get-docker/)

To run the example repository, follow these steps:

- Clone the repository:

```bash
git clone https://github.com/maris-development/beacon-example.git
```

- Navigate into the cloned directory:

```bash
cd beacon-example
```

- Start the Docker containers:

```bash
docker-compose up -d
```

- Open your web browser and navigate to `http://localhost:8080/swagger` to access the Beacon Swagger Page.

The example contains the following examples to query the Beacon Data Lake:

- A jupyter notebook that demonstrates how to use the Beacon Data Lake with Python.

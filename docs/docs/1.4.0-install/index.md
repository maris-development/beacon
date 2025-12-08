# 🛠️ Installation

Welcome to the installation guide for Beacon. In this chapter, we will walk you through the installation process of Beacon using a Docker image. By leveraging Docker, you can quickly and easily deploy Beacon on various operating systems without worrying about complex dependencies or compatibility issues.

To proceed with the installation, make sure you have Docker installed on your system. Docker provides comprehensive installation guides for various operating systems on their official website (https://www.docker.com/). Follow the instructions specific to your operating system to set up Docker.

Once you have Docker up and running, we will guide you through the steps required to install Beacon using its Docker image. We will cover pulling the image from the Docker registry, and launching Beacon within a Docker container.

<h2><svg style='display: inline-block; vertical-align:middle; position:relative; bottom:3px;' xmlns="http://www.w3.org/2000/svg" aria-label="Docker" role="img" viewBox="0 0 512 512" width="32px" height="32px" fill="#000000" stroke="#000000"><g id="SVGRepo_bgCarrier" stroke-width="0"></g><g id="SVGRepo_tracerCarrier" stroke-linecap="round" stroke-linejoin="round"></g><g id="SVGRepo_iconCarrier">
<path stroke="#066da5" stroke-width="38" d="M296 226h42m-92 0h42m-91 0h42m-91 0h41m-91 0h42m8-46h41m8 0h42m7 0h42m-42-46h42"></path><path fill="#066da5" d="m472 228s-18-17-55-11c-4-29-35-46-35-46s-29 35-8 74c-6 3-16 7-31 7H68c-5 19-5 145 133 145 99 0 173-46 208-130 52 4 63-39 63-39"></path></g></svg> Docker </h2>

In this chapter, we will guide you through the step-by-step installation procedure to set up Beacon using Docker. By following these instructions, you will be able to pull the Beacon Docker image from our GitHub repository, deploy it as a Docker container, configure port mapping, and map a directory for your datasets using volume mapping.

Let's get started with the installation process:

## Deploy Using Docker Compose

To deploy Beacon using Docker Compose, you need to create a `docker-compose.yml` file with the following content:

::: code-group

```yaml [docker-compose.yml]
version: "3.8"

services:
    beacon:
        image: ghcr.io/maris-development/beacon:community-latest
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
            - ./data/datasets:/beacon/data/datasets # Adjust the volume mapping as required
            - ./data/tables:/beacon/data/tables # Adjust the volume mapping as required. This enable persistence of created tables
```

:::

## Verify the Installation

* Open a web browser and enter http://localhost:8080/swagger/ in the address bar.
If the installation was successful, you should see the Beacon Swagger Interface indicating that Beacon is running.
Congratulations! You have successfully installed Beacon using Docker. You can now start leveraging its powerful features.

In case you encounter any issues during the installation process, please refer to the troubleshooting section of this documentation or reach out to our support team for assistance.

## Troubleshooting

If you encounter any issues during the installation process, please refer to the following troubleshooting steps:

* Ensure that Docker is installed correctly on your system.
* Verify that you have downloaded the correct Beacon Docker image file.
* Check the Docker logs for any error messages that may indicate the cause of the issue.
* Confirm that the port mapping and volume mapping are correctly configured in the Docker run command.
* If you are still facing issues, please reach out to our support team for further assistance. We have a dedicated slack channel <a href="https://join.slack.com/t/beacontechnic-wwa5548/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg" rel="no-referrer"> slack channel </a> where you can ask questions and get help from our team members and the community.

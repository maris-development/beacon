# 🛠️ Installation

Welcome to the installation guide for Beacon. In this chapter, we will walk you through the installation process of Beacon using a Docker image. By leveraging Docker, you can quickly and easily deploy Beacon on various operating systems without worrying about complex dependencies or compatibility issues.

To proceed with the installation, make sure you have Docker installed on your system. Docker provides comprehensive installation guides for various operating systems on their official website (https://www.docker.com/). Follow the instructions specific to your operating system to set up Docker.

Once you have Docker up and running, we will guide you through the steps required to install Beacon using its Docker image. We will cover pulling the image from the Docker registry, and launching Beacon within a Docker container.

<h2><svg style='display: inline-block; vertical-align:middle; position:relative; bottom:3px;' xmlns="http://www.w3.org/2000/svg" aria-label="Docker" role="img" viewBox="0 0 512 512" width="32px" height="32px" fill="#000000" stroke="#000000"><g id="SVGRepo_bgCarrier" stroke-width="0"></g><g id="SVGRepo_tracerCarrier" stroke-linecap="round" stroke-linejoin="round"></g><g id="SVGRepo_iconCarrier">
<path stroke="#066da5" stroke-width="38" d="M296 226h42m-92 0h42m-91 0h42m-91 0h41m-91 0h42m8-46h41m8 0h42m7 0h42m-42-46h42"></path><path fill="#066da5" d="m472 228s-18-17-55-11c-4-29-35-46-35-46s-29 35-8 74c-6 3-16 7-31 7H68c-5 19-5 145 133 145 99 0 173-46 208-130 52 4 63-39 63-39"></path></g></svg> Docker </h2>

In this chapter, we will guide you through the step-by-step installation procedure to set up Beacon using Docker. By following these instructions, you will be able to download the Beacon Docker image from our GitHub repository, deploy it as a Docker container, configure port mapping, and map a directory for your datasets using volume mapping.

Let's get started with the installation process:

Step 1: Download the Beacon Docker Image

* Open your web browser and navigate to our GitHub repository (https://github.com/maris-development/beacon/releases).
* Locate the Beacon Docker image file, typically named beacon-docker.x.x.x.tar.
* Click on the file to initiate the download process. Choose a suitable location on your machine to save the file.

Step 2: Load the Docker Image

* Open a terminal or command prompt on your machine.
* Navigate to the directory where you downloaded the Beacon Docker image file.
* Load the image into Docker by running the following command:

```bash
docker load -i beacon-x.x.x.tar
```

Replace beacon-x.x.x.tar with the actual name of the downloaded file if it differs.

Step 3: Deploy the Beacon Docker Container

* With the Beacon Docker image loaded, you can now deploy it as a container. Run the following command:

```bash
docker run -d -p 5001:5001 -v /path/to/datasets:/beacon/datasets beacon-image
```

The -d flag runs the container in detached mode, allowing it to run in the background should you want it to.

The -p 5001:5001 flag maps port 5001 from the container to the same port on your host machine. Adjust the port numbers as per your requirements.

The -v /path/to/datasets:/datasets flag maps a directory on your host machine containing your datasets to a directory inside the container. This way beacon can load in datasets from your host machine.

* Replace /path/to/dataset with the actual path to your dataset directory.
* Finally, beacon-image represents the name of the loaded Docker image. Make sure to use the correct image name.

### Optional: Mounting the Beacon Data Directory (e.g., for Backup)

If you want to mount the directory that beacon creates for the internal representation of the data (e.g., for backup), you can add another -v flag to the command:

```bash
docker run -d -p 5001:5001 -v /path/to/datasets:/beacon/datasets -v /path/to/beacon/data:/beacon/data beacon-image
```

::: warning
It's recommended to use Docker Volumes if performance is a concern. Docker Volumes are usually more performant than bind mounts.
:::

Step 4: Verify the Installation

* Open a web browser and enter http://localhost:5001/swagger/ in the address bar.
If the installation was successful, you should see the Beacon Swagger Interface indicating that Beacon is running.
Congratulations! You have successfully installed Beacon using Docker. You can now start leveraging its powerful features.

In case you encounter any issues during the installation process, please refer to the troubleshooting section of this documentation or reach out to our support team for assistance.


### Activation

To activate Beacon, you need to set the following environment variables:

- `BEACON_TOKEN=YOUR_TOKEN` - The token to activate Beacon.

This check will be done on startup and can only be used to start a single Beacon Instance. You can request another token if you would like to spawn another instance of Beacon at https://beacon.maris.nl/.


### Troubleshooting

If you encounter any issues during the installation process, please refer to the following troubleshooting steps:
- Ensure that Docker is installed correctly on your system.
- Verify that you have downloaded the correct Beacon Docker image file.
- Check the Docker logs for any error messages that may indicate the cause of the issue.
- Confirm that the port mapping and volume mapping are correctly configured in the Docker run command.
- If you are still facing issues, please reach out to our support team for further assistance. We have a dedicated slack channel <a href="https://join.slack.com/t/beacontechnic-wwa5548/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg" rel="no-referrer"> slack channel </a> where you can ask questions and get help from our team members and the community.

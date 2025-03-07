
# Install

Welcome to the installation guide for Beacon. In this chapter, we will walk you through the installation process of Beacon using a Docker image. By leveraging Docker, you can quickly and easily deploy Beacon on various operating systems without worrying about complex dependencies or compatibility issues.

Docker is a popular containerization platform that allows you to package an application and its dependencies into a standardized unit called a container. With Beacon's Docker image, you can streamline the installation process, ensuring consistency and portability across different environments.

Installing Beacon with Docker offers several advantages. Firstly, it provides a self-contained environment, isolating Beacon and its dependencies from the host system. This isolation ensures that the installation does not interfere with other applications or libraries on your machine. Moreover, Docker enables reproducible deployments, allowing you to easily replicate the same environment on multiple machines or share it with colleagues.

To proceed with the installation, make sure you have Docker installed on your system. Docker provides comprehensive installation guides for various operating systems on their official website (https://www.docker.com/). Follow the instructions specific to your operating system to set up Docker.

Once you have Docker up and running, we will guide you through the steps required to install Beacon using its Docker image. We will cover pulling the image from the Docker registry, configuring the necessary parameters, and launching Beacon within a Docker container.

## Docker

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

Step 4: Verify the Installation

* Open a web browser and enter http://localhost:5001/swagger/ in the address bar.
If the installation was successful, you should see the Beacon Swagger Interface indicating that Beacon is running.
Congratulations! You have successfully installed Beacon using Docker. You can now start leveraging its powerful features.

In case you encounter any issues during the installation process, please refer to the troubleshooting section of this documentation or reach out to our support team for assistance.
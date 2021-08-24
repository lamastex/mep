# Project MEP in the Sky!

Sky Computing Infrastructure as Code for *Project MEP: Meme Evolution Programme*, mainly to collect tweets scalably in an AI/BI-ready lakehouse architecture.

## Terraform for Twitter streaming
[Terraform](https://terraform.io) is a framework for Infrastructure as Code (IaC), meaning that it enables the user to write human-readable specifications for complex infrastructure. This is an example showcasing how Terraform can be used to deploy the [Scala library](../../sc/tw/) in this repository and use it to write the Twitter Stream into files that are then copied to cloud storage in S3.

### Directory structure and execution
There are two Terraform projects present in this showcase. 

1. The project in [server/](./server/) is for launching an EC2 instance with the necessary IAM role to access S3,preparing the instance for deployment of the second project, and sets up for transfer of files to S3.
  * Note that the server cannot be successfully deployed without the necessary files in the credentials directory. See details below.
2. The project in [docker/](./docker/) is for deploying a docker container to the newly created instance, which starts collecting the Twitter stream and writes it to files.

The server must be deployed before the docker container, since the container attaches to the server. It is necessary to do this in two separate projects since there is no way to make Terraform wait for the server to finish deploying before starting to deploy the docker container.

Hence, the way to start the project when all files are present is the following:
1. `cd` into the `server/` directory and deploy the project by running `terraform apply` and typing `yes` when prompted. Note that the EC2 instance launched is **not** part of AWS Free Tier.
2. Wait for the server to fully deploy and complete the initialization described in the [cloud-init configuration](./server/cloud-init.yml). This usually takes 3-5 minutes in total. The progress can be checked by using the [ssh script](./server/launch-ssh.sh) to ssh into the EC2 instance and running `htop`. If the machine is idle for several seconds, the script is finished.
3. `cd` into the `docker/` directory and deploy the container by running `terraform apply`, again typing `yes` when prompted.

To terminate the stream, run `terraform destroy` in the `docker/` directory, typing `yes` when prompted.
To terminate the EC2 instance, first terminate the stream, and then run `terraform destroy` in the `server/` directory, typing `yes` when prompted.

### Credentials and other resources
In order to launch the EC2 instance and successfully prepare it for the Twitter stream, a number of files need to be present in the directory `server/credentials/`. All necessary files are given as `.template`, meaning that the actual files should follow that template to work.
* `application.conf` contains the Twitter developer credentials.
* `aws-credentials` contains the credentials for AWS.
* `aws-key.pub` contains the public RSA ssh key for access to the created instance.
* `aws-ssh-key-pair.pem` contains the private RSA ssh key for access to the created instance.
* `aws-s3-bucket` contains the name of the S3 bucket to put files into. Files are put into `s3://bucket-name/twitter/ingest/`.
* `config-repo-owner` contains the owner of the GitHub repository with configuration details for the Twitter stream.
* `config-repository` contains the name of the GitHub repository with configuration details for the Twitter stream.
* `github-ssh.pub` contains the public ED25519 ssh key connected to a GitHub account with access to the configuration repository.
* `github-ssh` contains the private ED25519 ssh key connected to a GitHub account with access to the configuration repository.

#### Configuration repository
The configuration repository must have the directory `ingestion/` in the root of the repository. Inside `ingestion/`, the following files must be present. The contents of `ingestion/` are moved to `/home/ubuntu/twitter-config/` as part of the cloud-init script.
* `copyJsonToS3.sh`, a shell script that copies files to S3 using the AWS cli.
* `monitor-directory.sh`, a shell script that uses `inotifywait` to monitor the directory where completed files are put by the Scala library, calling `copyJsonToS3.sh` when a new file appears.
* `twitter-stream.conf`, a configuration file for the Twitter stream. See the Scala library for details.
  * The cloud-init script creates directories `/home/ubuntu/files-being-written-to/` and `/home/ubuntu/files-complete/`, which can be used as temporary writing directory and completed files directory.
* (optional) A file with Twitter handles for the stream to follow.

This showcase was done by [Johannes Graner](https://github.com/johannes-graner) as part of an employment as Data Engineering Science Intern at [Combient Mix AB](https://combient.com/mix) under the supervision of [Raazesh Sainudiin](https://github.com/lamastex).

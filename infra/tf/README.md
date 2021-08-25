## Terraform for Twitter streaming
[Terraform](https://terraform.io) is a framework for Infrastructure as Code (IaC), meaning that it enables the user to write human-readable specifications for complex infrastructure. This is an example showcasing how Terraform can be used to deploy the [Scala library](../../sc/tw/) in this repository and use it to write the Twitter Stream into files that are then copied to cloud storage in S3.

### Directory structure and execution
There are two Terraform projects present in this showcase. 

The project in [server/](./server/) is for launching an EC2 instance with the necessary IAM role to access S3,preparing the instance for deployment of the second project, and sets up for transfer of files to S3.
  * Note that the server cannot be successfully deployed without the necessary files in the credentials directory. See details below.

To terminate the stream and the instance, run `terraform destroy` in the `server/` directory, typing `yes` when prompted.

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

ssh -o StrictHostKeychecking=no -i credentials/aws-ssh-key-pair.pem ubuntu@$(terraform output -raw public_dns)

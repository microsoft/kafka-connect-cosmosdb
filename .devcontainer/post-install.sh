#!/bin/sh

# Workaround to get docker working as nonroot inside a devcontainer on a local machine
# NOTE: The main docker-debian.sh script https://github.com/microsoft/vscode-dev-containers/blob/master/script-library/docker-debian.sh
# did not work out of the box for setting up docker as nonroot
sudo chmod 660 /var/run/docker.sock
sudo chgrp docker /var/run/docker.sock

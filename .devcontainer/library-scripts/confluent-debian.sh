#!/usr/bin/env bash
#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------
#
# Docs: https://docs.confluent.io/platform/current/installation/installing_cp/deb-ubuntu.html
#
# Syntax: ./confluent-debian.sh [confluent platform version to install] [confluent CLI version to install]

CONFLUENT_VERSION=${1:-"6.0"}
CONFLUENT_CLI_VERSION=${2:-"v1.16.0"}

set -e

if [ "$(id -u)" -ne 0 ]; then
    echo -e 'Script must be run as root. Use sudo, su, or add "USER root" to your Dockerfile before running this script.'
    exit 1
fi

export DEBIAN_FRONTEND=noninteractive

# Install curl, apt-transport-https, or gpg if missing
if ! dpkg -s apt-transport-https curl ca-certificates > /dev/null 2>&1 || ! type gpg > /dev/null 2>&1; then
    if [ ! -d "/var/lib/apt/lists" ] || [ "$(ls /var/lib/apt/lists/ | wc -l)" = "0" ]; then
        apt-get update
    fi
    apt-get -y install --no-install-recommends apt-transport-https curl ca-certificates gnupg2 
fi

# Install Confluent Platform
echo "deb [arch=amd64] https://packages.confluent.io/deb/${CONFLUENT_VERSION} stable main" > /etc/apt/sources.list.d/confluent.list
curl -sL https://packages.confluent.io/deb/${CONFLUENT_VERSION}/archive.key | (OUT=$(apt-key add - 2>&1) || echo $OUT)
apt-get update
apt-get install -y confluent-platform

# Install the Confluent CLI
wget -O confluent-cli-install.sh https://cnfl.io/cli
sh confluent-cli-install.sh $CONFLUENT_CLI_VERSION
rm confluent-cli-install.sh

# Set Log directory for Confluent
export LOG_DIR=/var/log/kafka

echo "Done!"

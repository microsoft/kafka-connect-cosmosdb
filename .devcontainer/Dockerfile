ARG VARIANT="11"
FROM mcr.microsoft.com/vscode/devcontainers/java:0-${VARIANT}

ENV DEBIAN_FRONTEND=noninteractive

ARG USERNAME="vscode"

RUN curl https://packages.microsoft.com/config/debian/10/packages-microsoft-prod.deb --output /tmp/packages-microsoft-prod.deb && \
    dpkg -i /tmp/packages-microsoft-prod.deb && \
    apt-get update && \
    apt-get -y install --no-install-recommends apt-utils dialog && \
    apt-get -y install --no-install-recommends apt-transport-https ca-certificates software-properties-common libssl-dev libffi-dev \
        build-essential gnupg-agent dnsutils httpie bash-completion curl wget git unzip jq lsb-release procps gnupg2 powershell && \
    apt-get -y upgrade

COPY .devcontainer/library-scripts /tmp/library-scripts/

# [Option] Install Maven
ARG INSTALL_MAVEN="true"
ARG MAVEN_VERSION="3.6.3"
RUN if [ "${INSTALL_MAVEN}" = "true" ]; then su vscode -c "source /usr/local/sdkman/bin/sdkman-init.sh && sdk install maven \"${MAVEN_VERSION}\""; fi

# [Option] Install Azure CLI
ARG  INSTALL_AZURE_CLI="true"
RUN  if [ "$INSTALL_AZURE_CLI" = "true" ]; then bash /tmp/library-scripts/azcli-debian.sh; fi

# [Option] Install Confluent Platform and CLI
ARG  INSTALL_CONFLUENT="true"
ARG  CONFLUENT_VERSION="6.0"
ARG  CONFLUENT_CLI_VERSION="v1.16.0"
ENV  LOG_DIR=/home/$USERNAME/logs
RUN  if [ "$INSTALL_CONFLUENT" = "true" ]; then bash /tmp/library-scripts/confluent-debian.sh "${CONFLUENT_VERSION}" "${CONFLUENT_CLI_VERSION}"; fi

# [Option] Install Docker CLI
ARG  INSTALL_DOCKER="true"
RUN  if [ "${INSTALL_DOCKER}" = "true" ]; then bash /tmp/library-scripts/docker-debian.sh "${USERNAME}"; fi

USER $USERNAME

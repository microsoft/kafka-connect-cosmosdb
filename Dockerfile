# ----- Base Java - Check Dependencies ----
FROM openjdk:8u212-b04-jdk-stretch AS base
# Env variables
ENV SCALA_VERSION=2.12.8
ENV SBT_VERSION=1.2.8
ENV HOME=/app
WORKDIR $HOME

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt

#
# ----Build the app ----
FROM base AS build
ADD . $HOME
RUN sbt compile

#
# ----Run Unit Tests ----
FROM build AS unittest
RUN sbt test

#
# ---- Publish the App ----
FROM  unittest AS release
EXPOSE 8888
CMD sbt run



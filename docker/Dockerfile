# Start from Ubuntu base (or use debian:11-slim for smaller size)
FROM ubuntu:20.04

# Avoid interactive prompts during build
ENV DEBIAN_FRONTEND=noninteractive

# Install required packages
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    curl \
    bash \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Create spark user (security best practice)
RUN useradd -m -s /bin/bash spark

# Create directories
RUN mkdir -p /opt/spark && \
    mkdir -p /shared && \
    chown -R spark:spark /opt/spark /shared && \
    chmod 777 /shared

# Copy YOUR custom Spark distribution tarball
COPY spark-3.3.0-SNAPSHOT-bin-custom.tgz /tmp/spark.tgz

# Extract Spark to /opt/spark
RUN tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz && \
    chown -R spark:spark /opt/spark

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip

# Switch to spark user
USER spark

# Set working directory
WORKDIR /opt/spark

# Expose ports
EXPOSE 7077 8080 8081 4040

# Default command (can be overridden in docker-compose)
CMD ["/bin/bash"]

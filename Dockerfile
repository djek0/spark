# ==============================================================================
# Stage 1: Base runtime image
# ==============================================================================
FROM eclipse-temurin:11-jre

# What this does:
# - FROM: Specifies the base image to build upon
# - eclipse-temurin:11-jre: Official OpenJDK 11 runtime (JRE only, no compiler)
# - This gives us a Linux OS + Java 11 pre-installed

# ==============================================================================
# Install system dependencies
# ==============================================================================
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    procps \
    && rm -rf /var/lib/apt/lists/*

# What this does:
# - RUN: Executes commands inside the container during build
# - apt-get update: Updates package lists (like 'apt update' on Ubuntu)
# - apt-get install -y: Installs packages without prompting for confirmation
#   - python3: Python interpreter (for PySpark)
#   - python3-pip: Python package manager (for PySpark dependencies)
#   - procps: Process utilities (ps, top, etc. - useful for debugging)
# - && rm -rf /var/lib/apt/lists/*: Removes package cache to reduce image size

# ==============================================================================
# Set environment variables
# ==============================================================================
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# What this does:
# - ENV: Sets environment variables in the container
# - SPARK_HOME=/opt/spark: Standard location for Spark installation
# - PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin: 
#   Adds Spark's bin and sbin directories to PATH so you can run
#   'spark-submit', 'spark-shell', etc. without full paths

# ==============================================================================
# Copy pre-built Spark distribution
# ==============================================================================
COPY spark-*-bin-custom_param.tgz /tmp/spark.tgz

# What this does:
# - COPY: Copies files from your host machine into the container
# - spark-*-bin-custom_param.tgz: Source file (the wildcard * matches version number)
# - /tmp/spark.tgz: Destination path inside container

# ==============================================================================
# Extract and install Spark
# ==============================================================================
RUN mkdir -p $SPARK_HOME && \
    tar -xzf /tmp/spark.tgz -C $SPARK_HOME --strip-components=1 && \
    rm /tmp/spark.tgz

# What this does (line by line):
# - mkdir -p $SPARK_HOME:
#   - mkdir: Make directory
#   - -p: Create parent directories if needed, no error if exists
#   - Creates /opt/spark/
# - tar -xzf /tmp/spark.tgz -C $SPARK_HOME:
#   - Extracts directly into /opt/spark/ (not /opt/)
# - rm /tmp/spark.tgz:
#   - Deletes the .tgz file to save space (~250 MB)
# - && : Chains commands (only runs next if previous succeeded)

# ==============================================================================
# Create working directories
# ==============================================================================
RUN mkdir -p /opt/spark/work /opt/spark/logs

# What this does:
# - mkdir -p: Creates directories
#   - -p: Creates parent directories if needed, no error if exists
# - /opt/spark/work: Where Spark workers store temporary files
# - /opt/spark/logs: Where Spark writes log files

# ==============================================================================
# Expose network ports
# ==============================================================================
EXPOSE 7077 8080 8081 4040

# What this does:
# - EXPOSE: Documents which ports the container will use (doesn't actually open them)
# - 7077: Spark Master RPC port (workers connect here)
# - 8080: Master Web UI (view cluster status in browser)
# - 8081: Worker Web UI (view worker status in browser)
# - 4040: Application Web UI (view running job details)
# Note: These are just documentation; actual port mapping happens with 'docker run -p'

# ==============================================================================
# Set working directory
# ==============================================================================
WORKDIR $SPARK_HOME

# What this does:
# - WORKDIR: Sets the default directory when container starts
# - Any command you run in the container starts from /opt/spark

# ==============================================================================
# Default command
# ==============================================================================
CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]

# What this does:
# - CMD: Default command to run when container starts (can be overridden)
# - bin/spark-class: Spark wrapper script that launches Java programs
# - org.apache.spark.deploy.master.Master: Java class for Spark Master
# - This starts the container as a Spark Master by default
# - For workers, we'll override this with a different command

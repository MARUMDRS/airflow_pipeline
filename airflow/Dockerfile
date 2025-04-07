FROM apache/airflow:2.10.1-python3.10
# User root for access permission
USER root

# Update apt before installing necessary libs
RUN apt-get update
# Install necessary libs
RUN apt-get install -y apt-utils build-essential gcc curl

# Java folder inside tar-gz file
ENV JAVA_FOLDER jdk-17.0.12
# Root folder to place Java
ENV JVM_ROOT /usr/lib/jvm

# Tar.gz file name
ENV JAVA_PKG_NAME jdk-17.0.12_linux-x64_bin.tar.gz

# Java download URL
ENV JAVA_TAR_GZ_URL https://download.oracle.com/java/17/archive/$JAVA_PKG_NAME


# Update apt again before installing wget; Install wget; Remove the apt lists
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*    && \
# Clean apt; Autoremove any installation files
    apt-get clean                                                               && \
    apt-get autoremove                                                          && \
# Print "Downloading " URL
    echo Downloading $JAVA_TAR_GZ_URL                                           && \
# Download Java
    wget -q $JAVA_TAR_GZ_URL                                                    && \
# Unpack Java
    tar -xvf $JAVA_PKG_NAME                                                     && \
# Remove tar-gz file after unpacking
    rm $JAVA_PKG_NAME                                                           && \
# Make directory for Java
    mkdir -p /usr/lib/jvm                                                       && \
# Copy files to Java directory
    mv ./$JAVA_FOLDER $JVM_ROOT                                                 && \
# Select active Java and Javac versions
    update-alternatives --install /usr/bin/java java $JVM_ROOT/$JAVA_FOLDER/bin/java 1        && \
    update-alternatives --install /usr/bin/javac javac $JVM_ROOT/$JAVA_FOLDER/bin/javac 1     && \
# Print Java version
    java -version
# Add JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/jdk-17.0.12/
ENV PATH="/usr/lib/jvm/jdk-17.0.12/bin:${PATH}"

RUN apt-get update && apt-get install -y git-all

USER airflow
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

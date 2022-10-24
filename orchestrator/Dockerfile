FROM apache/airflow:2.4.2-python3.8

USER root

# Install OpenJDK-8
RUN echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list 
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk && \
	sudo apt-get install -y python3.8-dev python3-distutils

RUN apt-get install unzip -y && \
    apt-get autoremove -y

# Set JAVA_HOME
RUN export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64


COPY requirements.txt requirements.txt

USER airflow

RUN pip install --no-cache-dir -r requirements.txt
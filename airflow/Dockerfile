FROM apache/airflow:2.3.0

USER root

# Install OpenJDK-8
RUN echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list 
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk

RUN apt-get install unzip -y && \
    apt-get autoremove -y

# Set JAVA_HOME
RUN export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir pymongo==3.6.0
RUN pip install --no-cache-dir dnspython==1.13.0
RUN pip install --no-cache-dir apache-airflow-providers-mongo==2.3.3
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==3.0.0
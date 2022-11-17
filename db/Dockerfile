# Dockerizing Python and MongoDB
# Based on ubuntu:latest, installs MongoDB following the instructions from:
# http://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/
# INSTRUCTIONS:
# - Create the contianer:
#   > docker build -t ubuntu_pymongo .
# - Create a folder to share your project in your host with the container. Ex: ~/shared
# - Run the next command (need the route of the created shared folder), this command access to the bash of container:
#   > docker run -v /c/Users/Jhonny/Documents/vm_share/mongoDB/shared:/data/code -t -i -p 27019:27017 ubuntu_pymongo
# - To open another bash console run the command: 
#   > docker exec -it <id_contenedor> bash
# - Run the mongo database:
#   > mongod
# - To connect compass or another gui with mongo use the IP of docker: 192.168.99.100 and the port 27019, or another that you indicate in the command
# https://stackoverflow.com/questions/33558506/how-to-create-a-mongo-docker-image-with-default-collections-and-data
# https://stackoverflow.com/questions/43575295/how-to-import-data-to-mongodb-container-and-creating-an-image

FROM mongo:5.0.4

COPY . /code

# Installation:
# 1. Python
RUN apt-get update && apt-get install -y \
	software-properties-common \
	curl \
	python3.9 \
	python3.9-distutils \
	wget \
	nano \
&& add-apt-repository ppa:deadsnakes/ppa \
&& curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
&& python3.9 get-pip.py \
&& pip install pymongo

# Expose port #27017 from the container to the host
EXPOSE 27017

# Set /usr/bin/mongod as the dockerized entry-point application
#CMD ["mongod"]
FROM mcr.microsoft.com/mssql/server:2019-latest

USER root

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# Copy initialization scripts
COPY . /usr/src/app

# Grant permissions for the run-initialization script to be executable
RUN chmod +x /usr/src/app/run-initialization.sh

RUN sed -i 's/\r//g' /usr/src/app/run-initialization.sh

# Set environment variables, not to have to write them with docker run command
# Note: make sure that your password matches what is in the run-initialization script 
ENV SA_PASSWORD mssql1Ipw
ENV ACCEPT_EULA Y
ENV MSSQL_PID Express

# Expose port 1433 in case accesing from other container
EXPOSE 1433

# Run Microsoft SQl Server and initialization script (at the same time)
# Note: If you want to start MsSQL only (without initialization script) you can comment bellow line out, CMD entry from base image will be taken
CMD /bin/bash ./entrypoint.sh
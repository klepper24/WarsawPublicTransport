# WarsawPublicTransport

## Getting Started

All commands and scripts below are for Ubuntu, but they may be easily adopted for any other Linux.

1. Create folder GIT and clone the repository
2. Install Docker
3. Run `docker-compose up airflow-init` to initialize environment
4. Start all services with `docker-compose up -d` command
5. Open webserver which is available at http://localhost:8080
login: airflow 
password: airflow
6. Configure api_key variable:
<img width="864" alt="Variable" src="https://user-images.githubusercontent.com/25270608/199450117-026f5b6c-c605-4e93-9074-422d7c4ab95c.png">
<img width="877" alt="api_key" src="https://user-images.githubusercontent.com/25270608/199450270-f24368ad-0364-4e9c-b60f-c1eb4b5899fe.png">

In order to get api-key go to this page https://api.um.warszawa.pl/# -> Logowanie -> Rejestracja konta  and fill out the form

7. Configure connections 
 <img width="879" alt="Connection" src="https://user-images.githubusercontent.com/25270608/199455624-ec11c31f-f6ea-4054-b9b1-fb262127742d.png">
a) API connection:
<img width="852" alt="api_connection" src="https://user-images.githubusercontent.com/25270608/199455644-7c6678f6-ca6e-4572-9a50-2740c0cc5a32.PNG">
b) Spark connection:
<img width="883" alt="spark_connection" src="https://user-images.githubusercontent.com/25270608/199455662-17740f03-c6bc-4870-aa5b-a22e3cfb8193.PNG">

8. Turn on relevant DAGs

## Cleaning-up the environment
To stop and delete containers, delete volumes with database data and download images, run docker-compose down --volumes --rmi all

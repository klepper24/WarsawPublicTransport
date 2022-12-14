# WarsawPublicTransport

## Getting Started

1. Clone the repository with the following command `git clone https://github.com/klepper24/WarsawPublicTransport.git GIT`
1. Install Docker
1. Run `docker-compose up airflow-init` to initialize environment
1. Start all services with `docker-compose up -d` command
1. Open webserver which is available at [http://localhost:8080](http://localhost:8080)
   * `login: airflow `
   * `password: airflow`
1. Configure `api_key` variable:
   * In order to get `api_key` go to this page [https://api.um.warszawa.pl](https://api.um.warszawa.pl) -> Logowanie -> Rejestracja konta  and fill out the form
   * <img width="864" alt="Variable" src="https://user-images.githubusercontent.com/25270608/199450117-026f5b6c-c605-4e93-9074-422d7c4ab95c.png">
   * <img width="877" alt="api_key" src="https://user-images.githubusercontent.com/25270608/199450270-f24368ad-0364-4e9c-b60f-c1eb4b5899fe.png">
1. Configure connections 
   * <img width="879" alt="Connection" src="https://user-images.githubusercontent.com/25270608/199455624-ec11c31f-f6ea-4054-b9b1-fb262127742d.png">
   * API connection:
     - <img width="852" alt="api_connection" src="https://user-images.githubusercontent.com/25270608/199455644-7c6678f6-ca6e-4572-9a50-2740c0cc5a32.PNG">
   * Spark connection:
     - <img width="883" alt="spark_connection" src="https://user-images.githubusercontent.com/25270608/199455662-17740f03-c6bc-4870-aa5b-a22e3cfb8193.PNG">

1. Turn on relevant DAGs

## Local development

1. Create local virtual envirtonment
   ```bash
   python3.8 -m venv venv  
   source venv/bin/activate
   ```
1. Install poetry ([Instalation guide](https://python-poetry.org/docs/#installation))
   ``` bash 
   curl -sSL https://install.python-poetry.org | python3 -
   poetry install
   ```
1. Install dependencies locally
```bash
pip install cryptography==36.0.2
poetry install
```
## Cleaning-up the environment
To stop and delete containers, delete volumes with database data and download images, run `docker-compose down --volumes --rmi all`

## CodeQuality

```bash
tox -e flake8
tox -e pylint
```

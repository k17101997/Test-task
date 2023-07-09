# Installations

## 1. Docker
Follow the instructions in [Docker](https://www.docker.com/) homepage to instal Docker.

## 2. Linux operating system: Ubuntu

# Usage

## 1. Requirements
### 1.1 In root project folder, create folder and generate env file
```bash
mkdir -p ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
echo "_PIP_ADDITIONAL_REQUIREMENTS=pymongo" >> .env
```

### 1.2  first run command to init metadata for Airflow
```bash
docker-compse up airflow-init
```

### 1.2 Next, run below command to start all components in background (Airflow, Postgresql, Mongodb, Redis) 
```bash
docker-compose up -d
```

### 1.3 Check all components is started
```bash
docker ps
```

If the list container name is displayed with status healthy as below, all components is started
```bash
CONTAINER ID   IMAGE                  COMMAND                  CREATED       STATUS                      PORTS                      NAMES
831cfc03b7d6   apache/airflow:2.6.2   "/usr/bin/dumb-init …"   5 hours ago   Up 41 minutes (healthy)     8080/tcp                   test-airflow-worker-1
4b51ce2fcc19   apache/airflow:2.6.2   "/usr/bin/dumb-init …"   5 hours ago   Up 46 minutes (healthy)     8080/tcp                   test-airflow-triggerer-1
3c6a5ae3703c   apache/airflow:2.6.2   "/usr/bin/dumb-init …"   5 hours ago   Up 46 minutes (healthy)     8080/tcp                   test-airflow-scheduler-1
266beba2cbf5   apache/airflow:2.6.2   "/usr/bin/dumb-init …"   5 hours ago   Up 47 minutes (healthy)     0.0.0.0:8080->8080/tcp     test-airflow-webserver-1
d97c1373306f   postgres:13            "docker-entrypoint.s…"   5 hours ago   Up 47 minutes (healthy)     0.0.0.0:5432->5432/tcp     test-postgres-1
9012b2ee8945   redis:latest           "docker-entrypoint.s…"   5 hours ago   Up 46 minutes (healthy)     6379/tcp                   test-redis-1
929fefa030e8   mongo:latest           "docker-entrypoint.s…"   5 hours ago   Up 47 minutes (healthy)     0.0.0.0:27017->27017/tcp   test-mongo-1
```

### 1.4 Open browser on local machine [Airflow](http://locallhost:8080)
```
User/password: airflow/airflow
```

## 2. ETL Pipeline
### 2.1 SQL
In local Airflow homepage, search a dag name: <b>etl_sql</b> </br>
Next Unpause dag and press Trigger button to run the pipeline

### 2.2 NoSQL
In local Airflow homepage, search a dag name: <b>etl_nosql</b> </br>
Next Unpause dag and press Trigger button to run the pipeline

## 3. SQL
There are 3 sql file in [SQL Folder](sql)

### 3.1 Top 10 from_station, to_station has the most bike rentals -> [Top 10 stations has most bike rentals](sql/top_10_station_has_most_bike_rentals.sql)

Run the command below to execute sql script
```bash
docker cp sql/top_10_station_has_most_bike_rentals.sql test-postgres-1:/opt
docker exec -it test-postgres-1 psql airflow airflow -f /opt/top_10_station_has_most_bike_rentals.sql
```

First result is top 10 from station has the most bike retals </br>
The next result is top 10 to station has the most bike retal

### 3.2 How many new bike rentals on 2019-05-16 and the running totals until that day of eachfrom_station? -> [From station running totals with new bikes from 2019-05-16](sql/from_station_running_totals_with_new_bikes_from_2019_05_16.sql)

Run the command below to execute sql script
```bash
docker cp sql/from_station_running_totals_with_new_bikes_from_2019_05_16.sql test-postgres-1:/opt
docker exec -it test-postgres-1 psql airflow airflow -f /opt/from_station_running_totals_with_new_bikes_from_2019_05_16.sql
```

### 3.3 Calculate Day-over-Day (DoD), Month-over-Month (MoM) of bike rentals -> [Day-over-Day (DoD), Month-over-Month (MoM) of bike rentals](sql/dod_mom_bike_rentals.sql)
Run the command below to execute sql script
```bash
docker cp sql/dod_mom_bike_rentals.sql test-postgres-1:/opt
docker exec -it test-postgres-1 psql airflow airflow -f /opt/dod_mom_bike_rentals.sql
```

First result is DoD of bike rentals </br>
The next result is MoM of bike rentals


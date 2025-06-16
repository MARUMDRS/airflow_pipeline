# AutoSuggest Preprocessing Pipeline

## Overview

This repository is part of a group project in Databases Course of DSIT NKUA 2025. It contains a modular Apache Airflow-based ETL pipeline designed to preprocess data for training Autosuggest model. The pipeline is inspired by the structure of [tzerefos/airflow-seminar](https://github.com/tzerefos/airflow-seminar) and has been extended to crawl, process, and trace Jupyter Notebooks that use pandas from Github, extract transformation logic, and persist training samples.

The system leverages:

- **Redis** as the Celery backend/broker.
- **MongoDB** to persist repository metadata and preprocessing results.
- **Flower** to monitor task queues and Celery workers.
- **Docker Compose** to wire all services together for local deployment.

The overall architecture is illustrated below:

![Airflow Architecture](/images/Airflow_Architecture.png)

---

## Deployment

### Requirements

#### Hardware

- Memory: 16 GB RAM
- Processor: Intel(R) Core(TM) i5-6500 or greater / AMD Ryzen 5 3600 or greater
- Storage: 15 GB available space

#### Software

- [Docker][Docker-url] version >= 27.4.0
- [Docker Compose][Docker-compose-url] version >= 2.29.2

### Installation

1. Create a file named `.env.local` in the root of the repository with your Kaggle credentials:

```env
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key
````

2. Initialize the Airflow environment:

```bash
docker compose up airflow-init
```

3. Start the services with:

```bash
docker compose --profile flower up -d
```

---

## Accessing Services

Once the containers are running, access the services:

|      Service      |                        URL                       |
| :---------------: | :----------------------------------------------: |
|   Airflow Web UI  |  [http://localhost:8080](http://localhost:8080)  |
| Flower Monitoring |  [http://localhost:5555](http://localhost:5555)  |
|  Mongo Express UI | [http://localhost:28081](http://localhost:28081) |

---

## Setting Up Airflow Connection

1. Navigate to Airflow's web interface
2. Login: `username: airflow`, `password: airflow`
3. Go to `Admin > Connections`
4. Click **"Add"**
5. Use the following configuration:

| Key             | Value                     |
| --------------- | ------------------------- |
| Connection ID   | `mongo_default`           |
| Connection Type | `MongoDB`                 |
| Host            | `mongodb`                 |
| Port            | `27017`                   |
| Schema          | `github_crawl`            |
| Login/Password  | as defined in `.env` file |

---

## Pipeline Structure

The pipeline is made of **three Airflow DAGs**:

1. **`github_crawler_dag.py`**

   * Uses GitHub GraphQL API to search for repos that contain `.ipynb` notebooks using `pandas`
   * Clones the repo, checks for pandas usage, and inserts metadata into MongoDB

2. **`dataset_resolver_dag.py`**

   * Fetches unprocessed repos
   * Detects usage of datasets from local and Kaggle sources
   * Updates MongoDB with `dataset_found` status and `notebooks_with_dataset` list

3. **`notebook_replayer_dag.py`**

   * Replays each notebook line by line using a custom tracer
   * Captures pandas transformation logic (e.g., `merge`, `groupby`)
   * Saves training samples as `.csv` + `.json` in mounted directories

---

## Technologies Used

* [Docker][Docker-url]
* [Docker Compose][Docker-compose-url]
* [Airflow][Airflow-url]
* [Redis][Redis-url]
* [Flower][Flower-url]
* [MongoDB][Mongo-url]
* [Mongo Express UI][Mongo-express-repo]

---

## Credits

This project was forked and adapted from the great foundation provided by:
[tzerefos/airflow-seminar](https://github.com/tzerefos/airflow-seminar)

[Airflow-url]: https://airflow.apache.org/
[Docker-url]: https://docs.docker.com/
[Docker-compose-url]: https://docs.docker.com/compose/
[Flower-url]: https://flower.readthedocs.io/en/latest/
[Mongo-url]: https://www.mongodb.com/
[Redis-url]: https://redis.io/
[Mongo-express-repo]: https://github.com/mongo-express/mongo-express?tab=MIT-1-ov-file

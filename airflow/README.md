# Start Airflow standalone with Docker

- Create develop folder

    ```bash
    git clone https://github.com/uuboyscy/airflow-demo
    cd airflow-demo
    ```

- Start via Docker

    ```bash
    docker run -it -d \
    	--name airflow-server \
    	-p 8080:8080 \
    	-v $(PWD)/dags:/opt/airflow/dags \
    	-v $(PWD)/logs:/opt/airflow/logs \
    	-v $(PWD)/utils:/opt/airflow/utils \
    	-v $(PWD)/tasks:/opt/airflow/tasks \
    	-e PYTHONPATH=/opt/airflow \
    	apache/airflow:latest airflow standalone
    ```

- Create user

    ```bash
    docker exec -it airflow-server /bin/bash

    # Execute following command in container
    airflow users create \
        --username airflow \
        --firstname airflow \
        --password airflow \
        --lastname airflow \
        --role Admin \
        --email your_email@example.com

    mkdir -p /opt/airflow/utils
    touch /opt/airflow/utils/__init__.py
    touch /opt/airflow/tasks/__init__.py
    ```

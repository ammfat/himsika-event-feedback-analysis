# Project EFA

End-to-end [E]vent [F]eedback [A]nalytical systems from data pipeline, warehouse, to visualization dashboard.

An initiative to shape data-driven culture at the Association of Information System Students of Unsika ([Himsika](https:/himsika.unsika.ac.id)).

---

## How to

1. Setup environment variables.

    ```
    echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
    ```

1. Build and run the Airflow migration container.

    ```
    docker compose up --build airflow-init
    ```

1. Build and run the Airflow container.

    ```
    docker compose up --build
    ```

1. Open the Airflow web UI at [localhost:8080](http://localhost:8080).

1. Add GCP Connections in **Admin > Connections** by using the [service account](https://airflow.apache.org/docs/apache-airflow-providers-google/8.7.0/connections/gcp.html) credential file.
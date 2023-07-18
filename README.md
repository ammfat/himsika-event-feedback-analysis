# Project EFA

End-to-end [E]vent [F]eedback [A]nalytical systems from data pipeline, warehouse, to visualization dashboard.

An initiative project to build data-driven culture at the Association of Information System Students of Unsika ([Himsika](https://himsika.unsika.ac.id)).

---

## Publications

- [Medium](https://medium.com/mlearning-ai/how-i-use-data-engineering-to-start-the-data-driven-culture-in-a-student-organization-part-1-d146221a76a2)

- [Undergraduate Thesis](https://drive.google.com/file/d/13GqtH5ehW1sCZHYeBDOJ5bevsavWej_2/view?usp=sharing)

- [Dimensional Data Design for Event Feedback Data Warehouse](https://doi.org/10.31326/jisa.v6i1.1648)

## How to

1. Setup project directories.
    ```
    mkdir .creds/ .logs/ .plugins/
    ```

1. Setup environment variables.

    ```
    echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
    ```

1. Add Google Cloud/Application service account (SA) credential file in **creds** directory.

    ```
    mv some_location/the_sa_credential.json .creds/service_account.json
    ```

1. Build and run the Airflow migration container.

    ```
    docker compose up airflow-init
    ```

1. Build and run the Airflow container.

    ```
    docker compose up
    ```

1. Open the Airflow web UI at [localhost:8080](http://localhost:8080).

1. Add [GCP Connections]((https://airflow.apache.org/docs/apache-airflow-providers-google/8.7.0/connections/gcp.html)) in **Admin > Connections** by using the SA credential file.

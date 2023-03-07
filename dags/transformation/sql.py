class Query(
        project_id
        , bq_event_dataset_name
        , bq_raw_data_dataset_name
        , bq_dwh_dataset_name
    ):

    def __init__(
        self
        , bq_event_dataset_name
        , bq_raw_data_dataset_name
        , bq_dwh_dataset_name
    ):
        self.project_id = project_id
        self.bq_raw_data_dataset_name = bq_raw_data_dataset_name
        self.bq_event_dataset_name = bq_event_dataset_name
        self.bq_dwh_dataset_name = bq_dwh_dataset_name

        self.set_dim_events()
        self.set_dim_instances()
        self.set_dim_degree_programs()
        self.set_dim_professions()

    def set_dim_events(self):
        self.dim_events_history = f"""
            CREATE OR REPLACE TABLE
            `{self.project_id}.{bq_dwh_dataset_name}.dim_events_history` (
                id BYTES
                , event_name STRING
                , cabinet STRING
                , insert_date DATE
            )
            PARTITION BY
                DATE_TRUNC(insert_date, MONTH)
            AS (
            WITH cte_group_event AS (
                SELECT
                DISTINCT CASE
                    WHEN CONTAINS_SUBSTR(lower(event), 'talkshow silogy fest') THEN "Talkshow Silogy Fest"
                    ELSE INITCAP(event)
                END AS event_name
                , date AS insert_date
                , tahun_acara AS year
                FROM
                `{self.project_id}.{bq_event_dataset_name}.*`
            ), cte_join_cabinet AS (
                SELECT
                MD5(ev.event_name) AS id
                , ev.event_name AS event_name
                , ca.name AS cabinet
                , ev.insert_date
                FROM
                cte_group_event ev
                JOIN
                `{self.project_id}.{bq_raw_data_dataset_name}.cabinet` ca
                ON ev.year = ca.end_year
            )
            SELECT
                a.id
                , a.event_name
                , a.cabinet
                , a.insert_date
            FROM
                cte_join_cabinet a
            )
        """

        self.dim_events = f"""
        CREATE OR REPLACE TABLE
        `{self.project_id}.{bq_dwh_dataset_name}.dim_events`
        AS (
        WITH cte_min_max_insert_date AS (
            SELECT
            *
            , MIN(insert_date) OVER(PARTITION BY event_name) AS held_at
            , MAX(insert_date) OVER(PARTITION BY event_name) AS last_responded_feedback_at
            FROM
            `{self.project_id}.{bq_dwh_dataset_name}.dim_events_history`
        )
        SELECT
            * EXCEPT(insert_date)
        FROM
            cte_min_max_insert_date
        WHERE
            insert_date = held_at
        ORDER BY
            held_at DESC
        )
        """

    def set_dim_instances(self):
        self.dim_instances_history = f"""
        CREATE OR REPLACE TABLE
        `{self.project_id}.{bq_dwh_dataset_name}.dim_instances_history` (
            id BYTES
            , instance_name STRING
            , insert_date DATE
        )
        PARTITION BY
        DATE_TRUNC(insert_date, MONTH)
        AS (
        WITH cte_na AS (
            SELECT
            CASE
                WHEN instansi = 'Na' THEN 'NA' ELSE instansi
            END AS instance_name
            , date AS insert_date
            FROM
            `{self.project_id}.{bq_event_dataset_name}.*`
        )
        SELECT
            DISTINCT MD5(instance_name) AS id
            , instance_name
            , insert_date
        FROM
            cte_na
        )
        """

        self.dim_instances = f"""
        CREATE OR REPLACE TABLE
        `{self.project_id}.{bq_dwh_dataset_name}.dim_instances`
        AS (
        WITH cte_max_insert_date AS (
            SELECT
            *
            , MAX(insert_date) OVER(PARTITION BY instance_name) AS max_insert_date
            FROM
            `{self.project_id}.{bq_dwh_dataset_name}.dim_instances_history`
        )
        SELECT
            * EXCEPT(insert_date, max_insert_date)
            , insert_date AS last_modified_at
        FROM
            cte_max_insert_date
        WHERE
            insert_date = max_insert_date
        ORDER BY
            instance_name
        )
        """

    def set_degree_programs(self):
        self.dim_degree_programs_history = f"""
        CREATE OR REPLACE TABLE
        `{self.project_id}.{bq_dwh_dataset_name}.dim_degree_programs_history` (
            id BYTES
            , degree_program_name STRING
            , insert_date DATE
        )
        PARTITION BY
        DATE_TRUNC(insert_date, MONTH)
        AS (
        WITH cte_na AS (
            SELECT
            CASE
                WHEN program_studi = 'Na' THEN 'NA' ELSE program_studi
            END AS degree_program_name
            , date AS insert_date
            FROM
            `{self.project_id}.{bq_event_dataset_name}.*`
        )
        SELECT
            DISTINCT MD5(degree_program_name) AS id
            , degree_program_name
            , insert_date
        FROM
            cte_na
        )
        """

        self.dim_degree_programs = f"""
        CREATE OR REPLACE TABLE
        `{self.project_id}.{bq_dwh_dataset_name}.dim_degree_programs`
        AS (
        WITH cte_max_insert_date AS (
            SELECT
            *
            , MAX(insert_date) OVER(PARTITION BY degree_program_name) AS max_insert_date
            FROM
            `{self.project_id}.{bq_dwh_dataset_name}.dim_degree_programs_history`
        )
        SELECT
            * EXCEPT(insert_date, max_insert_date)
            , insert_date AS last_modified_at
        FROM
            cte_max_insert_date
        WHERE
            insert_date = max_insert_date
        ORDER BY
            degree_program_name
        )
        """

    def set_dim_professions(self):
        self.dim_professions_history = f"""
        CREATE OR REPLACE TABLE
        `{self.project_id}.{bq_dwh_dataset_name}.dim_professions`
        AS (
        WITH cte_max_insert_date AS (
            SELECT
            *
            , MAX(insert_date) OVER(PARTITION BY profession_name) AS max_insert_date
            FROM
            `{self.project_id}.{bq_dwh_dataset_name}.dim_professions_history`
        )
        SELECT
            * EXCEPT(insert_date, max_insert_date)
            , max_insert_date AS last_modified_at
        FROM
            cte_max_insert_date
        WHERE
            insert_date = max_insert_date
        ORDER BY
            profession_name
        )
        """

        self.dim_professions = f"""
            CREATE OR REPLACE TABLE
            `{self.project_id}.{bq_dwh_dataset_name}.dim_professions`
            AS (
            WITH cte_max_insert_date AS (
                SELECT
                *
                , MAX(insert_date) OVER(PARTITION BY profession_name) AS max_insert_date
                FROM
                `{self.project_id}.{bq_dwh_dataset_name}.dim_professions_history`
            )
            SELECT
                * EXCEPT(insert_date, max_insert_date)
                , max_insert_date AS last_modified_at
            FROM
                cte_max_insert_date
            WHERE
                insert_date = max_insert_date
            ORDER BY
                profession_name
            )
        """

    def set_fact_rates_by_responses(self):
        self.fact_rates_by_responses = f"""
        CREATE OR REPLACE TABLE
        `{self.project_id}.{bq_dwh_dataset_name}.fact_rates_by_responses`
        PARTITION BY
        DATE_TRUNC(timestamp, MONTH)
        AS (
        WITH cte_cleansing AS (
            SELECT
            timestamp
            , CASE
                WHEN CONTAINS_SUBSTR(lower(event), 'talkshow silogy fest') THEN "Talkshow Silogy Fest"
                ELSE INITCAP(event)
                END AS events
            , CASE
                WHEN instansi = 'Na' THEN 'NA' ELSE instansi
                END AS instances
            , CASE
                WHEN program_studi = 'Na' THEN 'NA' ELSE program_studi
                END AS degree_programs
            , tahun_angkatan_peserta AS student_year
            , pekerjaan AS professions
            , kepuasan_terhadap_pemateri AS speaker_rates
            , kepuasan_terhadap_manajemen AS management_rates
            , kepuasan_terhadap_keseluruhan_acara AS overall_event_rates
            , kepuasan_terhadap_durasi AS duration_rates
            , tingkat_kemenarikan_topik AS topic_rates
            , performa_mc AS mc_rates
            , performa_moderator AS moderator_rates
            , keikutsertaan_lanjutan AS revisit_expectation
            , kritik_dan_saran AS feedback_comments
            FROM
            `{self.project_id}.{bq_event_dataset_name}.*`
        ), cte_generate_id AS (
            SELECT
            * EXCEPT(events, instances, degree_programs, professions)
            , MD5(events) AS event_id
            , MD5(instances) AS instance_id
            , MD5(degree_programs) AS degree_program_id
            , MD5(professions) AS profession_id
            FROM
            cte_cleansing
        )
        SELECT
            timestamp
            , event_id
            , instance_id
            , degree_program_id
            , profession_id
            , student_year
            , revisit_expectation
            , feedback_comments
            , speaker_rates
            , management_rates
            , overall_event_rates
            , duration_rates
            , topic_rates
            , mc_rates
            , moderator_rates
        FROM 
            cte_generate_id
        )
        """

    def set_view_rates_by_responses(self):
        self.view_rates_by_responses = f"""
        CREATE OR REPLACE VIEW
        `{self.project_id}.{bq_dwh_dataset_name}.view_rates_by_responses`
        AS (
        SELECT
            feedbacks.timestamp
            , events.event_name
            , events.held_at
            , instances.instance_name
            , degree_programs.degree_program_name
            , professions.profession_name
            , feedbacks.feedback_comments
            , feedbacks.revisit_expectation
            , feedbacks.speaker_rates
            , feedbacks.management_rates
            , feedbacks.overall_event_rates
            , feedbacks.duration_rates
            , feedbacks.topic_rates
            , feedbacks.mc_rates
            , feedbacks.moderator_rates
            , AVG(feedbacks.speaker_rates) AS avg_speaker_rates
            , AVG(feedbacks.management_rates) AS avg_management_rates
            , AVG(feedbacks.overall_event_rates) AS avg_overall_event_rates
            , AVG(feedbacks.duration_rates) AS avg_duration_rates
            , AVG(feedbacks.topic_rates) AS avg_topic_rates
            , AVG(feedbacks.mc_rates) AS avg_mc_rates
            , AVG(feedbacks.moderator_rates) AS avg_moderator_rates
        FROM 
            `{self.project_id}.{bq_dwh_dataset_name}.fact_rates_by_responses` AS feedbacks
        JOIN
            `{self.project_id}.{bq_dwh_dataset_name}.dim_events` AS events
            ON feedbacks.event_id = events.id
        JOIN
            `{self.project_id}.{bq_dwh_dataset_name}.dim_instances` AS instances
            ON feedbacks.instance_id = instances.id
        JOIN
            `{self.project_id}.{bq_dwh_dataset_name}.dim_degree_programs` AS degree_programs
            ON feedbacks.degree_program_id = degree_programs.id
        JOIN
            `{self.project_id}.{bq_dwh_dataset_name}.dim_professions` AS professions
            ON feedbacks.profession_id = professions.id
        GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        ORDER BY
            1 DESC
        )
        """

    def get_sql_dim_events_history(self):
        return self.dim_events_history
    
    def get_sql_dim_events(self):
        return self.dim_events

    def get_sql_dim_instances_history(self):
        return self.dim_instances_history
    
    def get_sql_dim_instances(self):
        return self.dim_instances
    
    def get_sql_dim_degree_programs_history(self):
        return self.dim_degree_programs_history

    def get_sql_dim_degree_programs(self):
        return self.dim_degree_programs

    def get_sql_dim_professions(self):
        return self.dim_professions

    def get_sql_fact_rates_by_responses(self):
        return self.fact_rates_by_responses

    def get_sql_view_rates_by_responses(self):
        return self.view_rates_by_responses

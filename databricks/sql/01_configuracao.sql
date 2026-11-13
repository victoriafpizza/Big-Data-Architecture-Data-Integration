CREATE SCHEMA IF NOT EXISTS {{namespace}};
CREATE VOLUME IF NOT EXISTS {{volume}};

CREATE TABLE IF NOT EXISTS {{namespace}}.bronze_obstaculos (
    fonte STRING NOT NULL,
    data_logica DATE NOT NULL,
    extraido_em_utc STRING NOT NULL,
    airflow_run_id STRING NOT NULL,
    url_origem STRING NOT NULL,
    arquivo_original STRING NOT NULL,
    arquivo_metadados STRING NOT NULL,
    sha256_original STRING NOT NULL,
    arquivo_ingestao STRING NOT NULL,
    registro_indice BIGINT NOT NULL,
    entidade_id STRING NOT NULL,
    registro_json STRING NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS {{namespace}}.bronze_calcadas (
    fonte STRING NOT NULL,
    data_logica DATE NOT NULL,
    extraido_em_utc STRING NOT NULL,
    airflow_run_id STRING NOT NULL,
    url_origem STRING NOT NULL,
    arquivo_original STRING NOT NULL,
    arquivo_metadados STRING NOT NULL,
    sha256_original STRING NOT NULL,
    arquivo_ingestao STRING NOT NULL,
    registro_indice BIGINT NOT NULL,
    entidade_id STRING NOT NULL,
    registro_json STRING NOT NULL
) USING DELTA;

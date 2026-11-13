COPY INTO {{table}}
FROM (
    SELECT fonte, CAST(data_logica AS DATE) AS data_logica, extraido_em_utc,
           airflow_run_id, url_origem, arquivo_original, arquivo_metadados,
           sha256_original, _metadata.file_path AS arquivo_ingestao,
           CAST(registro_indice AS BIGINT) AS registro_indice, entidade_id, registro_json
    FROM '{{stage_dir}}'
)
FILEFORMAT = JSON
FILES = ('registros.jsonl')
FORMAT_OPTIONS ('mode' = 'FAILFAST', 'multiLine' = 'false')
COPY_OPTIONS ('force' = 'false')

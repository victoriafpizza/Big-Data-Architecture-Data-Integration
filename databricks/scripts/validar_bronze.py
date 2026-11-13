"""Executa COPY INTO e verifica o conteúdo e a linhagem da Bronze."""
import argparse

from common import (
    DATES, SOURCES, STAGE_FILE, Workspace, add_workspace_args, names, prepare_lot, render,
)

GROUP_FIELDS = (
    "fonte", "data_logica", "extraido_em_utc", "airflow_run_id", "url_origem",
    "arquivo_original", "arquivo_metadados", "sha256_original",
)


def inspect_table(workspace, table, lots, required):
    group = ", ".join(GROUP_FIELDS)
    rows = workspace.execute(f"""
SELECT {group}, arquivo_ingestao,
       count(*), count(DISTINCT registro_indice), min(registro_indice), max(registro_indice),
       count(DISTINCT entidade_id),
       count_if(NOT (entidade_id <=> get_json_object(registro_json, '$.id'))),
       sha2(concat_ws('', transform(
           array_sort(collect_list(named_struct('posicao', registro_indice,
                                                'digest', sha2(registro_json, 256)))),
           item -> item.digest)), 256)
FROM {table}
GROUP BY {group}, arquivo_ingestao
ORDER BY data_logica
""".strip())

    loaded = set()
    for row in rows:
        day = row[1]
        if day in loaded or day not in lots:
            raise ValueError(f"{table}: data ou arquivo duplicado/inesperado ({day})")
        lot = lots[day]
        expected_lineage = [lot["lineage"][field] for field in GROUP_FIELDS]
        suffix = f"/bronze_input/{lot['lineage']['fonte']}/data={day}/{STAGE_FILE}"
        if row[:8] != expected_lineage or not row[8] or not row[8].endswith(suffix):
            raise ValueError(f"{table} {day}: linhagem divergente")
        count = lot["records"]
        expected_metrics = [count, count, 1, count, len(lot["entities"]), 0]
        if list(map(int, row[9:15])) != expected_metrics or row[15] != lot["records_sha256"]:
            raise ValueError(f"{table} {day}: contagem, identidade ou conteúdo divergente")
        loaded.add(day)
    if not required <= loaded:
        raise ValueError(f"{table}: faltam datas esperadas ({', '.join(sorted(required - loaded))})")
    return loaded


def verify_totals(workspace, table, lots):
    expected = [
        sum(lot["records"] for lot in lots.values()),
        len(lots),
        len(set().union(*(lot["entities"] for lot in lots.values()))),
    ]
    rows = workspace.execute(f"""
SELECT count(*), count(DISTINCT arquivo_ingestao), count(DISTINCT entidade_id)
FROM {table}
""".strip())
    actual = list(map(int, rows[0]))
    if actual != expected:
        raise ValueError(f"{table}: totais divergentes, esperado {expected}, obtido {actual}")
    return dict(zip(("registros_fisicos", "arquivos_processados", "entidades_unicas"), actual))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_workspace_args(parser)
    parser.add_argument("--step", choices=("A", "B", "replay"), required=True)
    args = parser.parse_args()
    target = names(args)
    dates = DATES[:1] if args.step == "A" else DATES
    lots = {
        source: {day: prepare_lot(source, day, target["volume_root"]) for day in dates}
        for source in SOURCES
    }
    workspace = Workspace(args)
    workspace.require_managed_volume(target["volume_name"])
    for source_lots in lots.values():
        for lot in source_lots.values():
            for path, payload in lot["files"].items():
                workspace.verify_remote(path, payload)

    required_before = {"A": set(), "B": {DATES[0]}, "replay": set(dates)}[args.step]
    tables = {source: f"{target['namespace']}.`{table}`" for source, (_, table) in SOURCES.items()}
    for source, table in tables.items():
        loaded = inspect_table(workspace, table, lots[source], required_before)
        workspace.record("bronze_antes", step=args.step, table=table, dates=sorted(loaded))

    for source, table in tables.items():
        for lot in lots[source].values():
            workspace.execute(render("02_bronze.sql", table=table, stage_dir=lot["stage_dir"]))

    for source, table in tables.items():
        inspect_table(workspace, table, lots[source], set(dates))
        workspace.record("bronze_validada", step=args.step, table=table,
                         **verify_totals(workspace, table, lots[source]))


if __name__ == "__main__":
    main()

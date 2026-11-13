"""Lotes imutáveis, preparação JSON Lines e conexão com o Databricks."""
import hashlib
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
SQL = ROOT / "databricks" / "sql"
DATES = ("2026-11-11", "2026-11-12")
SOURCES = {
    "obstaculos": ("obstaculos", "bronze_obstaculos"),
    "geosampa_calcadas": ("features", "bronze_calcadas"),
}
STAGE_FILE = "registros.jsonl"

sys.path.insert(0, str(ROOT / "src"))
from ingestao import FILES, verify


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def prepare_lot(source, day, volume_root):
    os.environ.setdefault("INGEST_ROOT", str(ROOT / "chegada"))
    metadata = verify(source, day)
    folder = Path(os.environ["INGEST_ROOT"]) / source / f"data={day}"
    data_name = FILES[source]
    raw = (folder / data_name).read_bytes()
    metadata_bytes = (folder / "metadados.json").read_bytes()
    if sha256(raw) != metadata["sha256"] or json.loads(metadata_bytes) != metadata:
        raise ValueError(f"{source} {day}: arquivos mudaram durante a leitura")

    field, _ = SOURCES[source]
    raw_dir = f"{volume_root}/raw/{source}/data={day}"
    stage_dir = f"{volume_root}/bronze_input/{source}/data={day}"
    lineage = {
        "fonte": source,
        "data_logica": day,
        "extraido_em_utc": metadata["extraido_em_utc"],
        "airflow_run_id": metadata["airflow_run_id"],
        "url_origem": metadata["url"],
        "arquivo_original": f"{raw_dir}/{data_name}",
        "arquivo_metadados": f"{raw_dir}/metadados.json",
        "sha256_original": metadata["sha256"],
    }

    rows = json.loads(raw)[field]
    exact_rows = json.loads(raw, parse_float=Decimal)[field]
    lines, hashes, entities = [], [], set()
    for index, (row, exact) in enumerate(zip(rows, exact_rows, strict=True), start=1):
        record = json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        if json.loads(record, parse_float=Decimal) != exact:
            raise ValueError(f"{source} {day}: perda de precisão no registro {index}")
        entity = str(row["id"])
        entities.add(entity)
        hashes.append(sha256(record.encode("utf-8")))
        lines.append(json.dumps({
            **lineage, "registro_indice": index, "entidade_id": entity, "registro_json": record,
        }, ensure_ascii=False, separators=(",", ":")))

    return {
        "lineage": lineage,
        "stage_dir": stage_dir,
        "files": {
            lineage["arquivo_original"]: raw,
            lineage["arquivo_metadados"]: metadata_bytes,
            f"{stage_dir}/{STAGE_FILE}": ("\n".join(lines) + "\n").encode("utf-8"),
        },
        "entities": entities,
        "records": len(lines),
        "records_sha256": sha256("".join(hashes).encode("ascii")),
    }


def identifier(value):
    if not re.fullmatch(r"[a-z0-9_][a-z0-9_-]{0,127}", value):
        raise ValueError("Use identificadores em minúsculas com letras, números, _ ou -.")
    return value


def add_workspace_args(parser):
    parser.add_argument("--catalog")
    parser.add_argument("--schema", default="rota_livre_big_data")
    parser.add_argument("--volume", default="chegada_rota_livre")
    parser.add_argument("--warehouse", default=os.environ.get("DATABRICKS_WAREHOUSE_ID"))
    parser.add_argument("--profile", default="ROTA_LIVRE")
    parser.add_argument("--evidence", type=Path)


def names(args):
    catalog, schema, volume = map(identifier, (args.catalog or "", args.schema, args.volume))
    return {
        "namespace": f"`{catalog}`.`{schema}`",
        "volume": f"`{catalog}`.`{schema}`.`{volume}`",
        "volume_name": f"{catalog}.{schema}.{volume}",
        "volume_root": f"/Volumes/{catalog}/{schema}/{volume}",
    }


def render(filename, **values):
    template = (SQL / filename).read_text(encoding="utf-8-sig")
    return re.sub(r"\{\{(\w+)\}\}", lambda match: values[match[1]], template).strip()


class Workspace:
    def __init__(self, args):
        if not args.warehouse or not args.evidence:
            raise ValueError("Informe --warehouse e --evidence para execução remota.")
        from databricks.sdk import WorkspaceClient

        self.evidence = args.evidence
        self.warehouse = args.warehouse
        self.client = WorkspaceClient(profile=args.profile, auth_type="databricks-cli")
        self.client.current_user.me()
        self.client.warehouses.get(self.warehouse)
        self.record("workspace", host=urlsplit(self.client.config.host).hostname,
                    warehouse=self.warehouse, profile=args.profile)

    def record(self, event, **fields):
        entry = {"utc": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
        self.evidence.parent.mkdir(parents=True, exist_ok=True)
        with self.evidence.open("a", encoding="utf-8") as log:
            log.write(json.dumps(entry, ensure_ascii=False) + "\n")
        print(event, json.dumps(fields, ensure_ascii=False))

    def execute(self, sql):
        api = self.client.statement_execution
        response = api.execute_statement(statement=sql, warehouse_id=self.warehouse,
                                         wait_timeout="10s")
        statement_id = response.statement_id
        self.record("sql_submetido", id=statement_id, sql=sql)
        deadline = time.monotonic() + 600
        while response.status.state.value in {"PENDING", "RUNNING"}:
            if time.monotonic() >= deadline:
                api.cancel_execution(statement_id)
                self.record("sql_timeout", id=statement_id)
                raise TimeoutError(f"Timeout SQL. Consulte a instrução {statement_id}.")
            time.sleep(2)
            response = api.get_statement(statement_id)

        state = response.status.state.value
        if state != "SUCCEEDED":
            error = response.status.error
            self.record("sql_falhou", id=statement_id, state=state,
                        error_code=str(error.error_code) if error else None)
            raise RuntimeError(f"SQL {state}. Consulte a instrução {statement_id} no histórico.")

        if response.manifest and (response.manifest.truncated or
                                  (response.manifest.total_chunk_count or 0) > 1):
            raise RuntimeError(f"Resultado SQL inesperadamente paginado: {statement_id}")
        rows = (response.result.data_array or []) if response.result else []
        self.record("sql_concluido", id=statement_id, rows=rows)
        return rows

    def require_managed_volume(self, volume_name):
        volume = self.client.volumes.read(volume_name)
        if volume.volume_type.value != "MANAGED":
            raise ValueError("O Volume precisa ser gerenciado pelo Unity Catalog.")
        self.record("volume", name=volume_name, type=volume.volume_type.value)

    def read(self, path):
        with self.client.files.download(path).contents as stream:
            return stream.read()

    def verify_remote(self, path, payload):
        if self.read(path) != payload:
            raise ValueError(f"Arquivo remoto diverge do lote local: {path}")
        self.record("arquivo_verificado", path=path, size=len(payload), sha256=sha256(payload))

    def upload(self, path, payload):
        from databricks.sdk.errors import AlreadyExists, NotFound

        self.client.files.create_directory(path.rsplit("/", 1)[0])
        try:
            existing = self.read(path)
        except NotFound:
            try:
                self.client.files.upload(path, io.BytesIO(payload), overwrite=False)
            except AlreadyExists:
                pass  # Outro processo publicou. A leitura abaixo exige conteúdo idêntico.
            else:
                self.record("arquivo_enviado", path=path, size=len(payload), sha256=sha256(payload))
            self.verify_remote(path, payload)
        else:
            if existing != payload:
                raise ValueError(f"Arquivo remoto já existe com conteúdo diferente: {path}")
            self.record("arquivo_reutilizado", path=path, size=len(payload), sha256=sha256(payload))


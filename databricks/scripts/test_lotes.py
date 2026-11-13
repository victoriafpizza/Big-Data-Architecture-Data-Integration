"""Testes locais com os lotes reais, sem acesso ao Databricks."""
import json
import unittest
from decimal import Decimal

from common import DATES, SOURCES, STAGE_FILE, prepare_lot, render, sha256


class LotesLocaisTest(unittest.TestCase):
    def test_json_lines_preserva_registros_e_linhagem(self):
        for day in DATES:
            for source, (key, _) in SOURCES.items():
                with self.subTest(source=source, day=day):
                    lot = prepare_lot(source, day, "")
                    lineage = lot["lineage"]
                    files = lot["files"]
                    raw = files[lineage["arquivo_original"]]
                    self.assertEqual(sha256(raw), lineage["sha256_original"])
                    self.assertEqual(json.loads(files[lineage["arquivo_metadados"]])["data_logica"], day)
                    original = json.loads(raw, parse_float=Decimal)[key]
                    lines = files[f"{lot['stage_dir']}/{STAGE_FILE}"].splitlines()
                    self.assertEqual(len(lines), len(original))
                    for index, (line, expected) in enumerate(zip(lines, original, strict=True), 1):
                        row = json.loads(line)
                        self.assertEqual(row["registro_indice"], index)
                        self.assertEqual(row["entidade_id"], str(expected["id"]))
                        self.assertEqual(json.loads(row["registro_json"], parse_float=Decimal), expected)
                        self.assertEqual({name: row[name] for name in lineage}, lineage)
                    self.assertEqual(lot["files"], prepare_lot(source, day, "")["files"])

    def test_totais_fisicos_e_entidades_dos_snapshots(self):
        for source, expected in (("obstaculos", (4, 2)), ("geosampa_calcadas", (1806, 903))):
            with self.subTest(source=source):
                lots = [prepare_lot(source, day, "") for day in DATES]
                self.assertEqual(
                    (sum(lot["records"] for lot in lots), len(lots[0]["entities"] | lots[1]["entities"])),
                    expected,
                )
                self.assertEqual({lot["lineage"]["extraido_em_utc"][:10] for lot in lots},
                                 {"2026-11-12"})

    def test_sql_restringe_arquivo_e_desabilita_force(self):
        for source, (_, table) in SOURCES.items():
            for day in DATES:
                path = f"/Volumes/catalogo/rota_livre_big_data/chegada_rota_livre/bronze_input/{source}/data={day}"
                sql = render("02_bronze.sql", table=table, stage_dir=path)
                self.assertIn(f"FROM '{path}'", sql)
                self.assertIn("FILES = ('registros.jsonl')", sql)
                self.assertIn("COPY_OPTIONS ('force' = 'false')", sql)
                self.assertNotIn("{{", sql)


if __name__ == "__main__":
    unittest.main()

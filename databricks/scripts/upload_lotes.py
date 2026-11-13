"""Envia um lote verificado ou produz o inventário local sem conectar."""
import argparse
import json

from common import DATES, SOURCES, Workspace, add_workspace_args, names, prepare_lot, render, sha256


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_workspace_args(parser)
    parser.add_argument("--date", choices=DATES, required=True)
    parser.add_argument("--setup", action="store_true", help="Cria schema, volume e tabelas Delta.")
    parser.add_argument("--local-check", action="store_true")
    args = parser.parse_args()
    if args.local_check and args.setup:
        parser.error("--local-check não aceita --setup")
    target = {"volume_root": ""} if args.local_check else names(args)
    lots = [prepare_lot(source, args.date, target["volume_root"]) for source in SOURCES]

    if args.local_check:
        for lot in lots:
            print(json.dumps({
                **lot["lineage"], "registros": lot["records"],
                "entidades_unicas": len(lot["entities"]),
                "registros_sha256": lot["records_sha256"],
                "arquivos": [
                    {"caminho_relativo": path.lstrip("/"), "bytes": len(payload), "sha256": sha256(payload)}
                    for path, payload in lot["files"].items()
                ],
            }, ensure_ascii=False))
        return

    workspace = Workspace(args)
    if args.setup:
        sql = render("01_configuracao.sql", **target)
        for statement in sql.split(";"):
            if statement.strip():
                workspace.execute(statement.strip())
    workspace.require_managed_volume(target["volume_name"])
    for lot in lots:
        for path, payload in lot["files"].items():
            workspace.upload(path, payload)
        workspace.record("lote_disponivel", **lot["lineage"], registros=lot["records"])


if __name__ == "__main__":
    main()

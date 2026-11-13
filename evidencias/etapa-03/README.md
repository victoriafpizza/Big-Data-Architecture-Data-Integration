# Evidências da etapa 3

| Verificação | Resultado |
|---|---|
| Diretório | `C:\GSBigData\big-data`, entrega independente do DevOps. |
| Lotes locais | Duas datas lógicas, quatro arquivos de dados com SHA-256 confirmado e quatro metadados originais. As quatro extrações ocorreram em 12/11/2026. |
| Preparação local | JSON Lines determinístico com 2 obstáculos e 903 feições por lote. A leitura com Decimal verificou igualdade integral dos objetos, inclusive propriedades e geometrias. |
| Ambiente Windows | Nenhum Databricks CLI no PATH, nenhuma variável `DATABRICKS_*`, nenhum perfil `.databrickscfg` ou cache no diretório de usuário. O alias Python do Windows não executou. |
| SDK | Versão 0.166.0 instalada em ambiente descartável. Assinaturas para upload, execução SQL e consulta de Volume conferidas localmente. |
| Workspace, catálogo, schema e SQL Warehouse | Workspace autenticado indisponível. Capacidade e permissões da conta permanecem sem verificação. |
| Volume, uploads e Delta Bronze | Execução remota bloqueada pela autenticação. Sem resultados ou capturas remotas. |

- [Inventário local](inventario-local.jsonl) com data lógica, instante da coleta, origem, tamanhos e hashes do arquivo bruto, dos metadados e do JSON Lines.
- [Testes locais da etapa 3](testes-locais.txt) com três testes aprovados.
- [Contratos do SDK](sdk-local.txt) e [regressão da ingestão](regressao-ingestao.txt).
- [Instruções de autenticação e execução](../../databricks/README.md).

A contagem física local de ambas as datas é 4 obstáculos e 1.806 feições, com 2 e 903 IDs distintos, respectivamente. Ainda é necessário confirmar no Databricks o upload A, a primeira ingestão, o upload B, a segunda ingestão, a reexecução e os resultados das consultas.

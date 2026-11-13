# Databricks | Etapa 3

O fluxo lê os lotes imutáveis em `chegada/`, confere os metadados e envia os bytes originais e os metadados para um Volume gerenciado. Uma conversão determinística gera JSON Lines, com um objeto por obstáculo ou feição. `COPY INTO` carrega cada arquivo derivado em uma tabela Delta Bronze.

A integração foi preparada e testada localmente. A execução no Databricks depende de autenticação em um workspace Free Edition ou acadêmico com computação já autorizada. [Evidências da etapa](../evidencias/etapa-03/README.md).

## Layout e contagens locais

O destino planejado é `/Volumes/<catalog>/rota_livre_big_data/chegada_rota_livre/`.

```text
raw/<fonte>/data=YYYY-MM-DD/obstaculos.json ou calcadas.geojson
raw/<fonte>/data=YYYY-MM-DD/metadados.json
bronze_input/<fonte>/data=YYYY-MM-DD/registros.jsonl
```

As fontes são `obstaculos` e `geosampa_calcadas`. O lote A tem data lógica **2026-11-11** e o B, **2026-11-12**. As quatro extrações ocorreram em **12/11/2026**. Cada lote tem dois arquivos originais, dois metadados e dois arquivos derivados previstos para upload.

O JSON/GeoJSON original permanece byte a byte em `raw/`, incluindo o envelope. Na entrada Bronze, `registro_json` contém o objeto integral de `obstaculos[]` ou `features[]`. A conversão verifica a igualdade usando números decimais para detectar perda de precisão. Propriedades e geometria das feições são preservadas. Cada linha registra fonte, data lógica, timestamp da coleta, URL, execução Airflow, caminhos de origem, SHA-256 e posição de base 1. O SQL acrescenta o caminho observado do arquivo JSON Lines via `_metadata.file_path`.

| Fase | Obstáculos físicos / arquivos / entidades | Calçadas físicas / arquivos / entidades |
|---|---:|---:|
| Após A, esperado | 2 / 1 / 2 | 903 / 1 / 903 |
| Após A + B, esperado | 4 / 2 / 2 | 1.806 / 2 / 903 |
| Após reexecução, esperado | 4 / 2 / 2 | 1.806 / 2 / 903 |

Os dois snapshots têm os mesmos IDs por fonte. Aumento de linhas representa ingestão de mais um arquivo por fonte, sem evidência de novas entidades. O inventário usa dados reais locais. A tabela acima descreve o resultado esperado da execução remota.

## Autenticação e permissões

1. Entre no [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) ou no workspace acadêmico existente. Use a opção Free Edition se precisar criar conta.
2. Em **Catalog**, identifique um catálogo Unity Catalog de sua equipe. O script pode criar o schema `rota_livre_big_data` quando a conta tiver essa permissão. Em schema existente, use um nome autorizado com permissões para criar Volume e tabelas e para ler e gravar dados. Para a demonstração do primeiro lote, comece com tabelas Bronze vazias.
3. Em **SQL Warehouses**, selecione o warehouse Free Edition ou acadêmico já autorizado, confirme `CAN USE` e copie o ID em **Connection details** ou na URL da página do warehouse. Copie também a URL base HTTPS do workspace.
4. Instale o Databricks CLI moderno e Python 3.11 ou superior, caso estejam ausentes. Reinicie o PowerShell após instalar. [Instalação do CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install).

```powershell
winget install --id Databricks.DatabricksCLI --exact
# Se o Python 3.12 ainda não estiver instalado
winget install --id Python.Python.3.12 --exact
```

No PowerShell 7.4+, configure OAuth no navegador. O perfil e o cache ficam no diretório do usuário.

```powershell
# Execute na raiz deste repositório.
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$workspaceUrl = Read-Host 'URL base HTTPS do workspace'
databricks auth login --host $workspaceUrl --profile ROTA_LIVRE
databricks current-user me --profile ROTA_LIVRE | Out-Null
py -3.12 -m venv .venv
$python = '.\.venv\Scripts\python.exe'
& $python -m pip install -r databricks/requirements.txt
```

O SDK usa o OAuth do CLI. Mantenha o perfil e o cache fora do projeto. [OAuth U2M](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m?language=CLI).

## Execução sequencial e evidências

Na mesma sessão do PowerShell:

```powershell
$catalog = Read-Host 'Catálogo Unity Catalog autorizado'
$warehouse = Read-Host 'ID do SQL Warehouse autorizado'
$log = "evidencias/etapa-03/execucao-remota-$(Get-Date -Format yyyyMMdd-HHmmss).jsonl"
$target = @('--catalog', $catalog, '--warehouse', $warehouse, '--evidence', $log)

& $python databricks/scripts/upload_lotes.py @target --setup --date 2026-11-11
& $python databricks/scripts/validar_bronze.py @target --step A
& $python databricks/scripts/upload_lotes.py @target --date 2026-11-12
& $python databricks/scripts/validar_bronze.py @target --step B
& $python databricks/scripts/validar_bronze.py @target --step replay
```

`--setup` renderiza [01_configuracao.sql](sql/01_configuracao.sql), que cria o schema, o Volume gerenciado e as tabelas `bronze_obstaculos` e `bronze_calcadas`. Se a conta não puder provisionar os objetos, solicite ao administrador que execute o SQL de configuração no catálogo autorizado. Depois, omita `--setup` e use as opções `--schema` e `--volume` em todos os comandos para apontar para os nomes aprovados.

O upload reutiliza arquivos de conteúdo idêntico, compara todos os bytes depois do envio e interrompe a operação se um caminho já contiver dados divergentes. O validador confere novamente a existência e os bytes de cada arquivo remoto. A fase B inclui os mesmos comandos COPY da fase A mais os arquivos da segunda data. A fase `replay` repete os comandos da fase B. O [02_bronze.sql](sql/02_bronze.sql) usa caminhos fixos por data e fonte com `force=false`. A comparação SQL valida a linhagem por arquivo, posições, entidades e um hash agregado dos registros brutos em ordem. As contagens anteriores, SQL submetido, IDs de instrução, resultados e hashes verificados ficam no JSONL local.

Para obter capturas, abra **Catalog > catálogo > schema > Volumes > chegada_rota_livre**, expanda `raw` e registre as duas datas por fonte. Em **Query History**, abra as instruções pelos IDs do JSONL e registre as três fases. Mantenha capturas e resultados compartilháveis em `evidencias/etapa-03/` após revisar dados de conta e credenciais. [COPY INTO](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into) · [Metadados de arquivo](https://docs.databricks.com/aws/en/ingestion/file-metadata-column) · [Volumes](https://docs.databricks.com/aws/en/volumes/volume-files).

## Testes locais

Com Python e as dependências instaladas, é possível executar a preparação sem autenticação:

```powershell
& $python databricks/scripts/upload_lotes.py --local-check --date 2026-11-11
& $python databricks/scripts/upload_lotes.py --local-check --date 2026-11-12
& $python databricks/scripts/test_lotes.py -v
```

Na validação desta etapa, os três comandos foram executados com o Python 3.11 da imagem já disponível do Airflow, com os diretórios montados em leitura e rede desabilitada.

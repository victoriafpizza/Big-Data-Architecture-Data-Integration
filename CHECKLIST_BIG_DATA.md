# Checklist Big Data & Data Integration

Os critérios 1 a 6 seguem os briefings das etapas 1 a 3. Os demais organizam o trabalho previsto, sujeito à redação oficial da rubrica.

| Nº | Critério | Situação |
|---|---|---|
| 1 | Duas fontes pertinentes, incluindo fonte pública externa real | **Verificado.** API com dois registros estáticos do projeto e WFS oficial com 903 feições no recorte. A fonte interna é o retorno original do projeto e não comprova coleta em campo. |
| 2 | DAG diária no Airflow, extração das duas fontes e gravação datada em chegada/ | **Verificado.** Execuções agendada e manual concluídas, arquivos conferidos no Windows e reexecução comprovada. |
| 3 | Duas extrações paralelas e uma tarefa de verificação | **Verificado.** Sobreposição real de 606,481 ms, verificação após ambas e bloqueio por falha HTTP 404. [Evidências](evidencias/etapa-02/README.md) |
| 4 | Credenciais e parâmetros sensíveis fora do código | **Verificado.** Variáveis locais, proteção Git, mascaramento, Fernet, chave web e auditoria automatizada. [Evidências](evidencias/etapa-02/README.md) |
| 5 | Dois lotes de datas lógicas diferentes enviados ao Databricks Volume | **Parcial.** Lotes de 11/11 e 12/11 conferidos localmente, coletados em 12/11. Upload bloqueado por falta de autenticação. [Evidências](evidencias/etapa-03/README.md) |
| 6 | Bronze Delta com ingestão incremental e idempotente | **Parcial.** SQL COPY INTO preparado e conversão local testada. Criação das tabelas e execução SQL bloqueadas por falta de autenticação. [Evidências](evidencias/etapa-03/README.md) |
| 7 | Comprovar incrementalidade, idempotência e rastreabilidade no Databricks | Pendente de execução e aprovação na próxima etapa. |
| 8 | Criar a camada Prata | Pendente |
| 9 | Aplicar tipagem, limpeza e regras de qualidade na Prata | Pendente |
| 10 | Integrar as duas fontes na Prata | Pendente |
| 11 | Criar a camada Ouro | Pendente |
| 12 | Materializar indicadores analíticos na Ouro | Pendente |
| 13 | Executar e validar consultas SQL analíticas | Pendente |
| 14 | Construir o dashboard com os indicadores | Pendente |
| 15 | Organizar as evidências da cadeia completa | Pendente |
| 16 | Consolidar a entrega em PDF | Pendente |

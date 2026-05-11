# Olist Analytics — Plataforma de Data Engineering

> Pipeline de dados batch end-to-end sobre o maior dataset público de e-commerce brasileiro, construído com padrões de produção: ingestão idempotente, modelagem dimensional (Star Schema), transformações versionadas com dbt e visualização analítica via Metabase — 100% containerizado com Docker Compose.

<br>

## Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Stack](#stack)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Como Executar](#como-executar)
- [Modelo de Dados](#modelo-de-dados)
- [Modelos dbt](#modelos-dbt)
- [Decisões de Arquitetura](#decisões-de-arquitetura)
- [Qualidade de Dados](#qualidade-de-dados)
- [O que este projeto demonstra](#o-que-este-projeto-demonstra)

<br>

## Visão Geral

Este projeto implementa uma **plataforma de analytics batch** sobre o [Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) da Olist, com ~100k pedidos reais entre 2016 e 2018.

**Perguntas de negócio que o projeto responde:**

- Qual o funil de status dos pedidos e qual a taxa de cancelamento por período?
- Qual o tempo médio de entrega por estado do Brasil?
- Quais sellers têm o maior GMV e melhor NPS (review score)?
- Quais categorias de produto lideram em receita e volume de vendas?
- Qual a distribuição dos métodos de pagamento e taxa de parcelamento?

**Padrão de carga:** Full Refresh diário (Truncate & Load). A fonte é um dataset estático, logo não há delta confiável para ingestão incremental — esta decisão é intencional e documentada em [Decisões de Arquitetura](#decisões-de-arquitetura).

<br>

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Compose                               │
│                                                                     │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │                  PostgreSQL 17 (pgdatabase)                  │  │
│   │                                                              │  │
│   │   ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐  │  │
│   │   │  airflow_db  │  │   olist_db   │  │  metabase_db    │  │  │
│   │   │  (metadados  │  │  ┌────────┐  │  │  (metadados     │  │  │
│   │   │  do Airflow) │  │  │  raw   │  │  │  do Metabase)   │  │  │
│   │   └──────────────┘  │  ├────────┤  │  └─────────────────┘  │  │
│   │                     │  │staging │  │                        │  │
│   │                     │  ├────────┤  │                        │  │
│   │                     │  │ marts  │  │                        │  │
│   │                     │  └────────┘  │                        │  │
│   │                     └──────────────┘                        │  │
│   └──────────────────────────────────────────────────────────────┘  │
│                              ▲                                      │
│         ┌────────────────────┼─────────────────────┐               │
│         │                   │                      │               │
│   ┌─────┴──────┐     ┌──────┴──────┐      ┌───────┴──────┐        │
│   │   Airflow  │     │   dbt-core  │      │   Metabase   │        │
│   │  :8081     │────►│ (via Airflow│      │   :3000      │        │
│   │            │     │  BashOp.)   │      │              │        │
│   └─────┬──────┘     └─────────────┘      └──────────────┘        │
│         │                                                           │
│         │  ┌──────────────────────────────────────┐                │
│         └─►│  ./data:/tmp/data  (volume bind)     │                │
│            │  9 CSVs do Olist                     │                │
│            └──────────────────────────────────────┘                │
│                                                                     │
│   ┌──────────────┐                                                  │
│   │   DBeaver /  │  (ferramenta de inspeção do banco)               │
│   │  CloudBeaver │                                                  │
│   │    :8080     │                                                  │
│   └──────────────┘                                                  │
└─────────────────────────────────────────────────────────────────────┘
```

**Fluxo de dados:**

```
[CSVs do Olist]
      │
      ▼  (COPY via STDIN — PostgresHook)
[raw.*]  ← TRUNCATE & LOAD idempotente
      │
      ▼  (dbt run)
[staging.*]  ← cast de tipos, renaming, chaves surrogate (MD5)
      │
      ▼  (dbt run)
[intermediate.*]  ← regras de negócio, métricas derivadas
      │
      ▼  (dbt run)
[marts.*]  ← Star Schema — dims + fcts para consumo analítico
      │
      ▼
[Metabase]  ← dashboards de negócio
```

<br>

## Stack

| Camada              | Tecnologia             | Versão    | Papel                                       |
|---------------------|------------------------|-----------|---------------------------------------------|
| Orquestração        | Apache Airflow         | 2.x       | Agendamento, dependências, logs e retry      |
| Banco de Dados      | PostgreSQL             | 17        | Data warehouse local                        |
| Transformação       | dbt-core + dbt-postgres| 1.8.x     | Modelagem dimensional, testes, documentação |
| Visualização        | Metabase               | latest    | Dashboards analíticos de negócio            |
| Inspeção            | CloudBeaver (DBeaver)  | latest    | IDE SQL para exploração do banco            |
| Runtime             | Docker + Compose       | 24.x      | Isolamento e reprodutibilidade total        |
| Linguagem           | Python                 | 3.11      | Lógica de ingestão e orquestração           |

**Fonte de dados:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 9 CSVs, ~100k pedidos, dados reais de 2016–2018.

<br>

## Estrutura do Repositório

```
olist-analytics/
│
├── docker-compose.yml          # Orquestra todos os serviços
├── Dockerfile                  # Imagem customizada do Airflow com dependências
├── .env.example                # Template de variáveis de ambiente
├── requirements.txt            # Dependências Python
├── pyproject.toml
│
├── init-scripts/
│   └── init.sql                # Criação dos bancos isolados e schemas ao subir o Postgres
│                               # (airflow_db, metabase_db + schemas raw/staging/marts no olist_db)
│
├── data/                       # CSVs do Olist (bind mount: ./data → /tmp/data)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_sellers_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   └── product_category_name_translation.csv
│
├── dags/
│   └── olist_batch_ingestion.py  # DAG principal com Dynamic Task Mapping
│
├── dbt/
│   ├── dbt_project.yml           # Config global: materialization, paths
│   ├── packages.yml              # dbt-utils (surrogate_key, etc.)
│   ├── profiles.yml.example      # Template de conexão dbt → olist_db
│   │
│   └── models/
│       ├── staging/
│       │   └── olist/
│       │       ├── _olist_sources.yml        # Contrato de origem (schema raw)
│       │       ├── stg_olist_orders.sql
│       │       ├── stg_olist_order_items.sql
│       │       ├── stg_olist_customers.sql
│       │       ├── stg_olist_sellers.sql
│       │       ├── stg_olist_products.sql
│       │       ├── stg_olist_order_payments.sql
│       │       └── stg_olist_order_reviews.sql
│       │
│       ├── intermediate/
│       │   └── int_orders_metrics.sql        # Métricas derivadas com regras de negócio
│       │
│       └── marts/
│           └── vendas/
│               ├── dim_customers.sql
│               ├── dim_sellers.sql
│               ├── dim_products.sql
│               ├── dim_date.sql
│               └── fct_order_items.sql       # Tabela fato no grão do item
│
└── docs/
    └── images/                 # Prints do dashboard e lineage graph
```

<br>

## Como Executar

### Pré-requisitos

- Docker 24+ e Docker Compose V2
- 4 GB de RAM disponível
- Dataset Olist baixado do Kaggle e extraído em `./data/`

### 1. Clone o repositório

```bash
git clone https://github.com/luisfelipebp/olist-analytics.git
cd olist-analytics
```

### 2. Configure as variáveis de ambiente

```bash
cp .env.example .env
```

Edite o `.env`:

```env
# PostgreSQL
DB_USER=dataeng
DB_PASSWORD=dataeng123
DB_NAME=olist_db
DB_PORT=5432

# Metabase (banco separado para metadados do Metabase)
MB_DB_DBNAME=metabase_db
```

> O `init.sql` em `init-scripts/` criará automaticamente os bancos `airflow_db` e `metabase_db` na primeira inicialização do Postgres, além dos schemas `raw`, `staging` e `marts` dentro de `olist_db`.

### 3. Suba o ambiente

```bash
docker compose up -d
```

Na primeira execução, o Airflow precisa inicializar seu banco de metadados. Aguarde ~60s até todos os healthchecks passarem.

### 4. Crie o usuário do Airflow

```bash
docker exec -it airflow_olist bash

airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@olist.local \
  --password admin
```

### 5. Configure o dbt (perfil de conexão)

```bash
# Dentro do container do Airflow
cp /opt/airflow/dbt/profiles.yml.example ~/.dbt/profiles.yml

# Edite o arquivo com as credenciais do olist_db
# host: pgdatabase  (nome do serviço no Docker Compose)
# database: olist_db
```

### 6. Acesse as interfaces

| Serviço        | URL                    | Credenciais             |
|----------------|------------------------|-------------------------|
| Airflow        | http://localhost:8081  | admin / admin           |
| Metabase       | http://localhost:3000  | configuração inicial    |
| CloudBeaver    | http://localhost:8080  | configuração inicial    |
| PostgreSQL     | localhost:5432         | conforme `.env`         |

### 7. Coloque os dados no lugar e execute o pipeline

Garanta que os 9 CSVs do Olist estão em `./data/`. Então na Airflow UI:

1. Ative a DAG `olist_batch_ingestion`
2. Clique em **Trigger DAG** para rodar manualmente
3. Acompanhe as tasks de ingestão e a execução do dbt no log

### 8. Valide com dbt (opcional — para inspecionar os modelos)

```bash
docker exec -it airflow_olist bash

cd /opt/airflow/dbt

dbt debug          # testa conexão
dbt run            # executa todos os modelos
dbt test           # roda os testes de qualidade
dbt docs generate  # gera documentação
dbt docs serve --port 8082  # serve em localhost:8082
```

<br>

## Modelo de Dados

Este projeto implementa um **Star Schema** seguindo a Metodologia de Kimball. O grão da tabela fato é o **item do pedido** — a unidade mais atômica de análise que permite cruzar produto, seller, cliente e pagamento sem quebrar a cardinalidade.

```
                    ┌─────────────────┐
                    │  dim_customers  │
                    │─────────────────│
                    │ customer_sk  PK │
                    │ customer_id     │
                    │ city            │
                    │ state           │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌──────────────────┐
│   dim_sellers   │          │          │   dim_products   │
│─────────────────│          │          │──────────────────│
│ seller_sk    PK │          │          │ product_sk    PK │
│ seller_id       │          ▼          │ product_id       │
│ city            │  ┌───────────────┐  │ category_name    │
│ state           ├─►│fct_order_items│◄─┤ weight_g         │
└─────────────────┘  │───────────────│  │ dimensions_cm    │
                     │ order_item_sk │  └──────────────────┘
                     │ order_sk   FK │
                     │ customer_sk FK│
                     │ seller_sk  FK │
                     │ product_sk FK │
                     │ date_sk    FK │
                     │ price         │     ┌──────────────┐
                     │ freight_value │     │   dim_date   │
                     │ review_score  │◄────│──────────────│
                     │ delivery_days │     │ date_sk   PK │
                     └───────────────┘     │ date         │
                                          │ year         │
                                          │ month        │
                                          │ quarter      │
                                          │ day_of_week  │
                                          │ is_weekend   │
                                          └──────────────┘
```

**Por que o grão é o item e não o pedido?**

Um pedido pode conter múltiplos produtos de múltiplos sellers. Se a fato fosse no grão do pedido, seria impossível analisar receita por produto ou performance por seller sem quebrar a granularidade ou duplicar linhas. O item do pedido é o único grão que suporta todas as dimensões de análise sem ambiguidade.

<br>

## Modelos dbt

### Camada Staging (`materialized: view`)

Responsabilidade única: normalizar a camada raw. Sem regras de negócio.

| Modelo                     | Origem raw                    | O que faz                                              |
|----------------------------|-------------------------------|--------------------------------------------------------|
| `stg_olist_orders`         | `raw.olist_orders`            | Cast de timestamps, renaming de colunas                |
| `stg_olist_order_items`    | `raw.olist_order_items`       | Cast de `price` e `freight_value` para NUMERIC         |
| `stg_olist_customers`      | `raw.olist_customers`         | Padronização de campos de localização                  |
| `stg_olist_sellers`        | `raw.olist_sellers`           | Padronização de campos de localização                  |
| `stg_olist_products`       | `raw.olist_products`          | COALESCE em `category_name` para substituir NULLs      |
| `stg_olist_order_payments` | `raw.olist_order_payments`    | Cast numérico, mapeamento de tipos de pagamento        |
| `stg_olist_order_reviews`  | `raw.olist_order_reviews`     | Cast de scores e timestamps                            |

> **Staging nunca filtra linhas problemáticas.** Registros com inconsistências (ex: `approved_at > delivered_at`) passam para a camada intermediate — o staging apenas expõe o problema via testes. Isso preserva rastreabilidade e separa detecção de tratamento.

### Camada Intermediate (`materialized: view`)

Responsabilidade: regras de negócio e métricas derivadas que requerem lógica condicional.

| Modelo                  | O que faz                                                                           |
|-------------------------|-------------------------------------------------------------------------------------|
| `int_orders_metrics`    | Calcula `delivery_days` com precisão decimal (`EXTRACT(EPOCH FROM ...) / 86400.0`). Trata o problema de qualidade de datas (`approved_at > delivered_at`) retornando NULL em vez de valor negativo — sem mascarar a inconsistência da source. |

**Por que a métrica `delivery_days` não fica no staging?**

Calcular `delivery_days` no staging misturaria responsabilidades: staging normaliza, intermediate aplica regras. Colocar o cálculo no staging também esconderia o problema de qualidade dos 61 registros com `approved_at > delivered_at`, pois o resultado negativo seria silenciado sem rastreabilidade.

### Camada Marts (`materialized: table`)

Responsabilidade: dados prontos para consumo analítico no BI.

| Modelo             | Grão            | Métricas principais                                          |
|--------------------|-----------------|--------------------------------------------------------------|
| `dim_customers`    | 1 cliente único | city, state — contexto geográfico do comprador              |
| `dim_sellers`      | 1 seller único  | city, state — contexto geográfico do vendedor               |
| `dim_products`     | 1 produto único | category, weight_g, dimensions_cm                           |
| `dim_date`         | 1 data          | year, month, quarter, day_of_week, is_weekend               |
| `fct_order_items`  | 1 item de pedido| price, freight_value, review_score, delivery_days, order_status |

**Por que `table` e não `view` nos marts?**

Views recalculam o resultado a cada consulta. Para um analista abrindo um dashboard com joins entre fato e dimensões sobre 100k+ linhas, isso significa latência a cada clique. Materializar como table move o custo para o momento do `dbt run` (1x por dia), não para o momento da consulta do usuário final.

**Surrogate Keys via MD5 (dbt_utils.generate_surrogate_key)**

Autoincremento (`SERIAL`) é um anti-pattern em OLAP porque o ID depende da ordem de inserção. Se o pipeline rodar duas vezes, os IDs mudam — quebrando joins históricos. O MD5 gerado a partir da chave natural (`customer_id`, `seller_id`, etc.) é determinístico: o mesmo input sempre gera o mesmo hash, independente de quantas vezes o pipeline rodar.

<br>

## Decisões de Arquitetura

Esta seção documenta as principais decisões de design e os trade-offs considerados. É a seção mais importante para entender o projeto além do código.

---

### 1. Isolamento de bancos de dados por responsabilidade

**Decisão:** Três bancos distintos no mesmo servidor Postgres — `airflow_db` (metadados do Airflow), `olist_db` (data warehouse), `metabase_db` (metadados do Metabase).

**Por quê:** Misturar metadados de infraestrutura com dados de negócio cria acoplamento operacional. Se o banco do Airflow corromper, o DW continua intacto. Segue o princípio de separação de responsabilidades no nível de storage.

**Como funciona:** O `init.sql` em `init-scripts/` é executado automaticamente pelo `docker-entrypoint-initdb.d` do Postgres na primeira inicialização. Ele cria os três bancos e os schemas `raw`, `staging`, `marts` dentro do `olist_db`.

---

### 2. Volume compartilhado entre Airflow e Postgres

**Decisão:** `./data:/tmp/data` mapeado tanto no container do Airflow quanto no do Postgres.

**Por quê:** Elimina transferência de dados pela rede entre containers. O Airflow lê o CSV diretamente do disco e faz stream para o Postgres via `copy_expert(STDIN)`. Alternativa rejeitada: enviar o DataFrame inteiro pela conexão de rede, o que seria O(n) em memória RAM do worker.

---

### 3. `COPY` via `copy_expert(STDIN)` em vez de `df.to_sql()`

**Decisão:** `PostgresHook.get_conn()` + `cursor.copy_expert(COPY FROM STDIN)` para ingestão dos CSVs.

**Por quê:**

| Abordagem              | RAM usada            | Velocidade       | Log de linhas |
|------------------------|----------------------|------------------|---------------|
| `pandas.to_sql()`      | Todo o CSV em memória| Lenta (row-by-row Python) | Difícil      |
| `COPY FROM '/path'`    | Mínima (I/O disco)   | Rápida           | Não retorna   |
| `copy_expert(STDIN)`   | Stream (chunk)       | Rápida           | Retorna count |

O `copy_expert` com STDIN combina a performance do `COPY` nativo do Postgres com a flexibilidade do Python: o arquivo pode estar em qualquer lugar acessível ao worker (disco, S3, API), não precisa estar fisicamente no servidor do banco.

---

### 4. Full Refresh (Truncate & Load) em vez de Upsert na camada raw

**Decisão:** `TRUNCATE` + `COPY` atômicos (dentro de transação `BEGIN/COMMIT`) para cada carga.

**Por quê:** O dataset Olist é estático — sem delta confiável para identificar apenas registros novos. Um upsert (`ON CONFLICT`) na camada raw exigiria PKs garantidas na fonte, o que não existe neste dataset. O Full Refresh é a estratégia correta para fontes sem integridade de chave garantida.

**Atomicidade:** `TRUNCATE` e `COPY` dentro de `BEGIN/COMMIT` garantem que o banco nunca fique em estado inconsistente: ou a carga completa vai, ou o rollback preserva os dados anteriores.

**`TRUNCATE` vs `DELETE`:** `TRUNCATE` é DDL — desaloca páginas de dados no disco instantaneamente sem registrar linha por linha no WAL (Write-Ahead Log). `DELETE` é DML — registra cada deleção no WAL, consumindo CPU e I/O desnecessários para Full Refresh.

---

### 5. Dynamic Task Mapping com `.expand()`

**Decisão:** Uma única `PythonOperator` com `.expand(op_kwargs=[...])` para gerar tasks dinâmicas por arquivo CSV.

**Por quê:** Escalabilidade sem alteração de código. Se novos CSVs forem adicionados, o `glob.glob('/tmp/data/*.csv')` os detecta automaticamente no próximo run.

**Trade-off aceito:** Tasks dinâmicas aparecem agrupadas na Graph View do Airflow (colchetes `[]`). Para investigar falhas individuais, é necessário clicar no grupo e inspecionar cada sub-task manualmente.

---

### 6. `PythonOperator` + `PostgresHook` em vez de `PostgresOperator`

**Decisão:** Substituição do `PostgresOperator` por `PythonOperator` com `PostgresHook`.

**Por quê:** O `PostgresOperator` envia SQL ao banco e recebe apenas "OK" ou "Erro" — não retorna o número de linhas afetadas para o Python. Sem isso, não há como fazer log de auditoria (`Run de 2025-01-15 inseriu 99.441 linhas`). O `PostgresHook` expõe o cursor completo, permitindo `SELECT COUNT(*)` pós-carga e log estruturado.

---

### 7. O dbt não sabe que o Airflow existe

**Decisão:** O dbt conecta diretamente ao `olist_db` via `profiles.yml`. A integração com o Airflow é feita pelo `BashOperator` chamando `dbt run` no final da DAG.

**Por quê:** Separação de responsabilidades. O dbt é uma ferramenta de transformação — seu universo começa e termina dentro do banco analítico. O Airflow é o orquestrador — ele sabe a ordem de execução, mas não precisa saber como o dbt transforma os dados. Essa separação facilita testar o dbt independentemente da orquestração.

---

### 8. Sem Foreign Keys declaradas no banco

**Decisão:** As relações entre dimensões e fato existem semanticamente, mas não como constraints `FOREIGN KEY` no PostgreSQL.

**Por quê:** Em OLAP com cargas massivas em bulk, constraints de FK verificam integridade linha por linha durante a inserção — custo desnecessário. A responsabilidade de garantir referential integrity foi delegada ao dbt via testes de `relationships`. Isso segue a prática padrão de data warehouses analíticos.

<br>

## Qualidade de Dados

### Testes genéricos (schema.yml)

| Modelo                  | Coluna          | Testes aplicados             |
|-------------------------|-----------------|------------------------------|
| `stg_olist_orders`      | `order_id`      | `not_null`, `unique`         |
| `stg_olist_order_items` | `order_item_sk` | `not_null`, `unique`         |
| `stg_olist_customers`   | `customer_id`   | `not_null`, `unique`         |
| `stg_olist_sellers`     | `seller_id`     | `not_null`, `unique`         |
| `stg_olist_products`    | `product_id`    | `not_null`, `unique`         |
| `fct_order_items`       | `customer_sk`   | `relationships` → dim_customers |
| `fct_order_items`       | `seller_sk`     | `relationships` → dim_sellers   |

### Problema de qualidade identificado: inconsistência temporal

Durante a análise exploratória, foram identificadas **61 linhas** onde `order_approved_at > order_delivered_customer_date` — logicamente impossível (aprovação posterior à entrega).

```sql
-- Teste customizado que monitora o problema
SELECT order_id, order_approved_at, order_delivered_customer_date
FROM {{ ref('stg_olist_orders') }}
WHERE order_approved_at > order_delivered_customer_date
```

**Estratégia de tratamento:**

- O **teste existe e falha** quando os 61 registros estão presentes — exposição do problema, não ocultação.
- A **métrica `delivery_days`** é calculada na camada intermediate com `CASE WHEN` retornando `NULL` para esses registros — proteção da análise sem mascarar a inconsistência da fonte.
- Os dados originais **não são alterados** em nenhuma camada.

> **Princípio aplicado:** Testes detectam problemas. Transformações protegem métricas. Misturar os dois leva à perda de rastreabilidade.

<br>

## O que este projeto demonstra

Para recrutadores de Engenharia de Dados: as decisões abaixo refletem padrões de produção, não apenas execução de tutorial.

**Ingestão e infraestrutura**
- Containerização completa com Docker Compose (redes, volumes, healthchecks, `depends_on`)
- Separação de ambientes por responsabilidade (airflow_db ≠ olist_db ≠ metabase_db)
- Ingestão bulk com `COPY`/`STDIN` — sem `pandas.to_sql()` para cargas massivas
- Atomicidade transacional na carga (BEGIN/COMMIT com rollback automático)
- Pipeline idempotente: re-execução não gera dados duplicados ou inconsistentes

**Orquestração**
- Dynamic Task Mapping com `.expand()` — escalável sem alteração de código
- Auditoria por execução: log do número de linhas inseridas por tabela e por data
- Separação entre lógica de negócio (módulos Python) e orquestração (DAG)

**Transformação e modelagem**
- Star Schema seguindo a Metodologia de Kimball
- Grão da fato definido corretamente no item do pedido (não no pedido)
- Surrogate Keys determinísticas via MD5 — idempotência de chaves em recargas
- Camadas com responsabilidades distintas: staging → intermediate → marts
- `delivery_days` calculado em segundos e convertido para dias decimais (`EXTRACT(EPOCH)`)
- `dim_date` para facilitar análises temporais no BI

**Qualidade de dados**
- Testes genéricos (`not_null`, `unique`, `relationships`) via dbt
- Testes customizados detectando inconsistências reais da fonte
- Separação entre detecção do problema (teste) e proteção da métrica (transformação)

**Documentação**
- Decisões de arquitetura documentadas com trade-offs explícitos
- Lineage graph completo via `dbt docs`

<br>

---

**Dataset:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) · Licença CC BY-NC-SA 4.0
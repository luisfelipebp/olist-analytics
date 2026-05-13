# Olist Analytics — V2: Streaming com Redpanda

> Evolução da [V1 (batch)](../README.md): adição de uma camada de ingestão orientada a eventos com **Redpanda** (Kafka-compatible), simulador de pedidos em Python e um consumer que persiste eventos em tempo real no schema `streaming` do PostgreSQL — sem ZooKeeper, sem Schema Registry, sem overhead desnecessário.

<br>

## O que mudou da V1 para a V2

| Componente             | V1                          | V2                                           |
|------------------------|-----------------------------|----------------------------------------------|
| Ingestão               | Batch CSV → PostgreSQL      | Batch CSV **+** stream de eventos → PostgreSQL |
| Novos serviços         | —                           | Redpanda (broker) + Redpanda Console         |
| Novo schema            | `raw`, `staging`, `marts`   | + `streaming`                                |
| Nova tabela            | —                           | `streaming.olist_events`                     |
| Novo código Python     | —                           | `producer.py` (producer) + `consumer.py` |

A arquitetura batch da V1 **não foi alterada**. O streaming é uma camada adicional, não um substituto.

<br>

## Arquitetura V2

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            Docker Compose                                │
│                                                                          │
│  CAMINHO BATCH (inalterado da V1)                                        │
│  ─────────────────────────────────────────────────────────────────────   │
│  [CSVs Olist] → Airflow DAG → raw.* → dbt → staging/marts → Metabase    │
│                                                                          │
│  CAMINHO STREAMING (novo na V2)                                          │
│  ─────────────────────────────────────────────────────────────────────   │
│                                                                          │
│  ┌──────────────────┐    publish     ┌────────────────────────────────┐  │
│  │produer           │ ─────────────► │         Redpanda               │  │
│  │ (Python)         │                │  topic: olist.order_events     │  │
│  │                  │                │                                │  │
│  │ Lê pedidos do    │                │  ┌──────────────────────────┐  │  │
│  │ PostgreSQL e os  │                │  │   Redpanda Console :8082 │  │  │
│  │ "replaya" como   │                │  │   (UI de inspeção)       │  │  │
│  │ eventos JSON     │                │  └──────────────────────────┘  │  │
│  └──────────────────┘                └──────────────┬─────────────────┘  │
│                                                     │ consume            │
│                                                     ▼                    │
│                                        ┌────────────────────┐            │
│                                        │    consumer.py     │            │
│                                        │                    │            │
│                                        │  desserializa JSON │            │
│                                        │  INSERT idempotente│            │
│                                        └─────────┬──────────┘            │
│                                                  │                       │
│                                                  ▼                       │
│                                   ┌──────────────────────────┐           │
│                                   │       PostgreSQL          │           │
│                                   │  schema: streaming        │           │
│                                   │  tabela: olist_events     │           │
│                                   └──────────────────────────┘           │
└──────────────────────────────────────────────────────────────────────────┘
```

**Fluxo de eventos:**

```
[fct_order_items / stg_olist_orders]  ← fonte dos pedidos históricos
           │
           ▼  (producer.py lê e "replaya")
   [evento JSON por pedido]
           │
           ▼  (kafka-python producer)
   [Redpanda — topic: olist.order_events]
           │
           ▼  (consumer.py — loop contínuo)
   [streaming.olist_events]  ← INSERT ON CONFLICT DO NOTHING (idempotente)
```

<br>

## Novos Componentes

### Redpanda

[Redpanda](https://redpanda.com/) é um broker de mensagens **100% compatível com o protocolo Kafka**, escrito em C++. A escolha sobre o Kafka Apache foi intencional:

- **Sem ZooKeeper:** o Redpanda é auto-suficiente (arquitetura Raft interna). O Kafka tradicional exige um cluster ZooKeeper separado — complexidade desnecessária para um ambiente local de desenvolvimento.
- **Sem Schema Registry:** o schema dos eventos é validado pelo consumer via código Python, não por um serviço externo. Suficiente para este estágio.
- **Console UI embutida:** interface web para inspecionar topics, offsets e mensagens sem ferramentas externas.

### Event Simulator (Producer)

Script Python que lê pedidos do PostgreSQL (da camada `staging` ou `marts`) e os republica como eventos JSON no Redpanda, simulando um sistema de produção em tempo real.

**Schema do evento:**

```json
{
  "event_id": "uuid-v4-gerado",
  "event_type": "order_placed | order_shipped | order_delivered",
  "order_id": "e481f51cbdc54678b7cc49136f2d6af7",
  "customer_id": "9ef432eb6251297304e76186b10a928d",
  "seller_id": "48436dade18ac8b2bce089ec2a041202",
  "product_id": "87285b34884572647811a353c7ac498a",
  "total_value": 299.90,
  "event_timestamp": "2018-03-08T12:34:56.000Z"
}
```

O simulador gera **2 a 3 eventos por pedido** (placed → shipped → delivered), com `time.sleep(0.1)` entre publicações para simular chegada gradual — não um dump instantâneo de todos os dados.

### Consumer

Script Python que roda em loop contínuo (`poll()`), lê eventos do topic `olist.order_events` e persiste em `streaming.olist_events` com `INSERT ... ON CONFLICT (event_id) DO NOTHING`.

O campo `event_id` (UUID gerado pelo simulator) garante idempotência: se o consumer reiniciar e reprocessar mensagens já consumidas (comportamento esperado no modelo *at-least-once*), não há duplicatas.

<br>

## Stack adicional (V2)

| Componente      | Tecnologia       | Versão  | Papel                                         |
|-----------------|------------------|---------|-----------------------------------------------|
| Broker          | Redpanda         | latest  | Substituto Kafka-compatible sem ZooKeeper      |
| Console         | Redpanda Console | latest  | UI para inspeção de topics e offsets          |
| Producer        | kafka-python     | 2.0.x   | Publicação de eventos JSON no topic           |
| Consumer        | kafka-python     | 2.0.x   | Consumo e persistência em `streaming.*`       |

<br>

## Estrutura de Arquivos (novos na V2)

```
olist-analytics/
│
├── docker-compose.yml          # + serviços redpanda e redpanda-console
│
├── streaming/
│   ├── producer.py      # Producer: lê pedidos e publica eventos
│   └── consumer.py             # Consumer: persiste eventos no PostgreSQL
│
├── init-scripts/
│   └── init.sql                # + CREATE SCHEMA IF NOT EXISTS streaming;
│                               # + CREATE TABLE streaming.olist_events
│
└── dbt/
    └── models/
        └── staging/
            └── olist/
                └── stg_streaming_events.sql  # (opcional) expõe eventos no lineage
```

<br>

## Schema da Tabela de Eventos

```sql
CREATE TABLE IF NOT EXISTS streaming.olist_events (
    event_id          UUID          PRIMARY KEY,
    event_type        VARCHAR(50)   NOT NULL,
    order_id          VARCHAR       NOT NULL,
    customer_id       VARCHAR,
    seller_id         VARCHAR,
    product_id        VARCHAR,
    total_value       NUMERIC(10,2),
    event_timestamp   TIMESTAMP     NOT NULL,
    ingested_at       TIMESTAMP     DEFAULT NOW(),
    kafka_offset      BIGINT,
    kafka_partition   INT
);
```

**Por que `kafka_offset` e `kafka_partition`?**

Guardar offset e partition permite rastreabilidade completa: dado qualquer registro em `olist_events`, é possível localizar exatamente qual mensagem no Redpanda o gerou. Em casos de reprocessamento ou debug, você busca a mensagem original pelo offset — não precisa confiar apenas no dado já transformado.

<br>

## Como Executar (V2)

### 1. Atualize o docker-compose.yml

Adicione os serviços Redpanda e Console ao `docker-compose.yml` existente:

```yaml
redpanda:
  image: redpandadata/redpanda:latest
  container_name: redpanda_olist
  command:
    - redpanda
    - start
    - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
    - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
    - --overprovisioned
    - --smp 1
    - --memory 512M
    - --reserve-memory 0M
    - --node-id 0
    - --check=false
  ports:
    - "9092:9092"
    - "19092:19092"
  networks:
    - default

redpanda-console:
  image: redpandadata/console:latest
  container_name: redpanda_console_olist
  depends_on:
    - redpanda
  ports:
    - "8082:8080"
  environment:
    - KAFKA_BROKERS=redpanda:9092
  networks:
    - default
```

### 2. Suba o ambiente completo

```bash
docker compose up -d
```

### 3. Crie o topic no Redpanda

```bash
# Via CLI dentro do container
docker exec -it redpanda_olist \
  rpk topic create olist.order_events \
  --partitions 1 \
  --replicas 1

# Verifique
docker exec -it redpanda_olist rpk topic list
```

Ou crie pela UI em **http://localhost:8082** → Topics → Create Topic.

### 4. Atualize o schema do PostgreSQL

Execute no `olist_db` (via CloudBeaver em :8080 ou psql):

```sql
CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE IF NOT EXISTS streaming.olist_events (
    event_id          UUID          PRIMARY KEY,
    event_type        VARCHAR(50)   NOT NULL,
    order_id          VARCHAR       NOT NULL,
    customer_id       VARCHAR,
    seller_id         VARCHAR,
    product_id        VARCHAR,
    total_value       NUMERIC(10,2),
    event_timestamp   TIMESTAMP     NOT NULL,
    ingested_at       TIMESTAMP     DEFAULT NOW(),
    kafka_offset      BIGINT,
    kafka_partition   INT
);
```

### 5. Rode o consumer (em background)

```bash
# Terminal 1 — consumer aguardando eventos
python streaming/consumer.py
```

O consumer fica em loop com `poll(timeout_ms=1000)`. Enquanto não há eventos, permanece ocioso. Ao receber eventos, persiste e commita o offset.

### 6. Rode o simulador (producer)

```bash
# Terminal 2 — simulador publicando eventos
python streaming/producer.py
```

Você verá no terminal do consumer os eventos sendo recebidos e persistidos em `streaming.olist_events`.

### 7. Valide no Redpanda Console

Acesse **http://localhost:8082**:
- **Topics → olist.order_events:** veja as mensagens, offsets e timestamps
- **Consumer Groups:** inspecione o lag do seu consumer

### 8. Valide no PostgreSQL

```sql
-- Quantos eventos foram persistidos?
SELECT COUNT(*) FROM streaming.olist_events;

-- Distribuição por tipo de evento
SELECT event_type, COUNT(*) as total
FROM streaming.olist_events
GROUP BY event_type
ORDER BY total DESC;

-- Últimos 10 eventos recebidos
SELECT event_id, event_type, order_id, total_value, ingested_at
FROM streaming.olist_events
ORDER BY ingested_at DESC
LIMIT 10;
```

<br>

## Decisões de Arquitetura

### Por que `streaming.olist_events` e não `bronze.order_events`?

A nomenclatura `streaming` reflete a **origem dos dados**, não a camada de qualidade. O schema comunica imediatamente ao leitor que esses registros vêm de um stream de eventos — não de um batch CSV. Isso torna o modelo de dados auto-documentado: `raw` = CSVs, `streaming` = eventos, `staging/marts` = transformações dbt.

Nomear como `bronze` seguiria a convenção Medallion Architecture (Bronze/Silver/Gold), que é igualmente válida — mas exigiria nomear `raw` como Bronze e `staging` como Silver, o que não foi a convenção adotada na V1. Manter `streaming` preserva consistência com as decisões anteriores do projeto.

---

### Por que *at-least-once* e não *exactly-once*?

*Exactly-once* no Kafka exige transações distribuídas entre producer e consumer — configuração complexa e com overhead de performance significativo. Para este projeto, *at-least-once* com idempotência no consumer (`ON CONFLICT DO NOTHING` via `event_id` UUID) entrega a mesma garantia de resultado com muito menos complexidade.

Em produção: a escolha entre *at-least-once* + idempotência vs *exactly-once* depende do custo de reprocessamento e da criticidade dos dados. Para analytics, *at-least-once* + idempotência é o padrão mais comum.

---

### Por que não integrar `streaming.olist_events` aos marts dbt?

Na V2, os eventos são ingeridos e monitorados — a integração analítica fica para uma iteração futura. Juntar dados de streaming com dados batch em um mart exige uma decisão de modelagem não-trivial: como reconciliar granularidades diferentes? Como tratar eventos tardios? Fazer isso prematuramente geraria um mart frágil. A separação intencional em `streaming.*` permite evoluir sem quebrar a V1.

---

### Por que o consumer roda fora do Airflow?

O Airflow é um orquestrador de tasks com início e fim definidos. Um consumer de streaming é um processo contínuo sem fim natural — o modelo mental é diferente. Colocar o consumer dentro do Airflow exigiria um operador com `time_out` arbitrário ou um sensor com polling, o que emularia mal o comportamento de um consumer real. Para esta escala, um script Python em loop é a escolha correta e honesta.

<br>

## Conceitos de Mensageria Demonstrados

| Conceito            | Onde aparece no projeto                                               |
|---------------------|-----------------------------------------------------------------------|
| **Topic**           | `olist.order_events` — canal nomeado de publicação                   |
| **Partition**       | 1 partition (suficiente para volume local)                           |
| **Offset**          | Salvo em `kafka_offset` — rastreabilidade de cada mensagem           |
| **Consumer Group**  | Consumer registrado em grupo — permite escalar horizontalmente       |
| **At-least-once**   | Offset commitado após INSERT — reprocessamento sem duplicatas        |
| **Producer**        | `producer.py` — serializa e publica JSON                      |
| **Consumer**        | `consumer.py` — desserializa, persiste e commita offset              |

<br>

---

**← [README V1 — Pipeline Batch](../README.md)**
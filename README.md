# wow-analytics

Proof of Concept regarding using Blizzard API, especially Auction House, to test analytics components.

## Architecture

### Proof of Concept

Based on [JupyterLab](https://jupyter.org/) notebooks in order to:

- store Blizzard AH data into [Apache Parquet](https://parquet.apache.org/) files
- load those Parquet files into [DuckDB](https://duckdb.org/) for data exploration

```mermaid
architecture-beta

group blizzard(cloud)[Battle Net]
service bli_api(database)[API] in blizzard

group jupyterlab(cloud)[JupyterLab]
service puller(server)[pull_wow_ah_parquet] in jupyterlab
service tmp_store(disk)[Parquet files] in jupyterlab
service loader(server)[load_ah_data] in jupyterlab
service duck(database)[DuckDB] in jupyterlab


puller:L --> R:bli_api
puller:R --> L:tmp_store
loader:L --> R:tmp_store
loader:T -- B:duck
```

### Production target

For a larger scale deployment:

- Use a real-time analytics store such as [ClickHouse](https://clickhouse.com/)
- Visualize data from dashboards in [Metabase](https://www.metabase.com/)

```mermaid
architecture-beta

group blizzard(cloud)[Battle Net]
service bli_api(database)[API] in blizzard

group wowanal(cloud)[WoW Analytics]
service feeder(server)[Feeder] in wowanal
service store(database)[Clickhouse] in wowanal
service dashboard(server)[Metabase] in wowanal

feeder:L --> R:bli_api
feeder:R --> L:store
dashboard:L --> R:store
```

## Requirements

In order to use this project you will need:

- Install [Mise](https://mise.jdx.dev/getting-started.html)
- Create an API Access from [Battle.net Developer portal](https://community.developer.battle.net/access/clients):
    - You will get a set of OAuth credentials: **Client ID** & **Client Secret**

## How to use

You can use the following commands to set up your local environment:

```shell
# Install required tools
mise install

# Set up Python environment
mise run setup

# Provide secret credentials
cat > .env << EOF
BLIZZARD_API_CLIENT_ID=YOUR_CLIENT_ID
BLIZZARD_API_CLIENT_SECRET=YOUR_CLIENT_SECRET
BLIZZARD_API_REGION=eu
EOF
```

# Viro

Viro explores real CDC wastewater activity for COVID-19, influenza A, and RSV through a small Java application on local Kubernetes.

A publisher sends a frozen CDC extract to a Kafka topic with three partitions. Three consumers store their partitions in local DuckDB databases. A browser UI runs one parameterized SQL query across the workers and can query exported Parquet files. Prometheus collects service metrics.

## Status

The Java build and container image succeeded, and the public repository uses `master` as its default branch. The local kind deployment is in progress; its two-CPU Docker engine has repeatedly timed out Kubernetes control-plane requests while pulling and unpacking images. Kafka and consumer readiness plus end-to-end query validation are still pending. See [verification results](docs/results.md) for the observed status. Proposed targets are not benchmark results.

## Run locally

Requirements: Java 21, Maven, Docker, kind, kubectl, and Python 3. The commands use a dedicated cluster named `viro` and the `kind-viro` context.

```sh
python3 scripts/fetch-data.py
mvn -q test package
docker build -t viro:local .
scripts/cluster.sh create
scripts/cluster.sh deploy
```

Open separate terminals for the two local interfaces:

```sh
scripts/cluster.sh gateway
scripts/cluster.sh metrics
```

Open [Viro](http://localhost:8080) and [Prometheus](http://localhost:9090). Wait for ingestion to finish, run the query, flush the data, then run the same query in Parquet mode.

## One useful query

Count observations by state, pathogen, and activity category. Filter by state, anonymous site, pathogen, reporting-week range, and activity category.

The UI shows the SQL template. It does not accept arbitrary SQL. A row is a site/pathogen/week observation, not an infected person or a unique community.

## Documents

- [Design](docs/design.md)
- [CDC data contract](docs/data-contract.md)
- [Build steps and parallel work](docs/build-plan.md)
- [Metrics and validation](docs/validation.md)
- [Scaling and saturation experiment](docs/scaling-experiment.md)
- [Actual results](docs/results.md)

## Source and limits

Use [CDC dataset atcp-73re](https://data.cdc.gov/Public-Health-Surveillance/CDC-Wastewater-Viral-Activity-Level-for-SARS-CoV-2/atcp-73re). The reproducible extract contains up to 400 recent rows per pathogen and explicitly excludes WastewaterSCAN-sourced records because CDC publishes additional terms for that source. Downloaded data stays outside Git.

This is a frozen-data demonstration. CDC can revise historical values; automatic refresh is not implemented. One broker and one host provide no host-loss redundancy. Consumers have fixed assignments, so automatic rebalancing and horizontal scaling are future work.

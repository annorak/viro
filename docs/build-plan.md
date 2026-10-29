# Build plan and parallel tasks

The reduced target is a running Kubernetes demonstration with one query. The earlier general SQL language and dataset-generation machinery are deferred. Time boxes guide prioritization; passing checks determine completion.

## Independent tracks

| Track | Can start immediately | Depends on |
| --- | --- | --- |
| Java service | Worker storage, fixed query, gateway, UI, metrics | Agreed JSON and HTTP contracts |
| Kubernetes | Cluster, Kafka, storage, Prometheus, app manifests | Local container engine |
| Source and publisher | CDC fetch script, frozen extract, Kafka publisher | Verified source fields |
| Documentation | Design, source contract, validation plan | Agreed reduced scope |

Service packaging waits for both Java and publisher code. App deployment waits for the image and cluster. Ingestion waits for Kafka and workers. End-to-end checks wait for ingestion.

## First five minutes

1. Create the standalone repository directory.
2. Verify the source schema and download 400 rows per pathogen.
3. Start the dedicated kind cluster and broker.
4. Agree on environment variables and worker HTTP endpoints.
5. Record source policy, row count, and checksum.

Exit: a validated source file exists and infrastructure startup is in progress.

## Build the runnable path

1. Package one Java 21 artifact with producer, worker, and gateway roles.
2. Create the three-partition topic from the publisher.
3. Give each worker its fixed partition and persistent database.
4. Persist observations and next offset in one transaction.
5. Implement the single parameterized grouped-count query.
6. Merge worker results in the gateway and fail on missing workers.
7. Serve the small UI and expose metrics.

Exit: the artifact compiles, focused tests pass, and the container image builds.

## Deploy and verify

1. Load the image into kind.
2. Deploy the three workers and gateway.
3. Supply the source extract through a ConfigMap.
4. Run the publisher Job and wait for ingestion.
5. Confirm the query counts every distinct source row.
6. Verify all three pathogen filters.
7. Flush all workers and compare Parquet with database results.
8. Restart a worker and repeat the count.
9. Verify Prometheus scrape targets and service metrics.
10. Save observed results, then commit and push the application and docs.

Exit: a browser can query real data on local Kubernetes; database and Parquet agree after flush; metrics are observable; restart preserves the result.

## Remaining time

Measure one finite replay and query latency under concurrent traffic. If time permits, compare transaction batch sizes with equivalent isolated state. Do not claim a saturation point or scaling gain from an incomplete test.

The current fixed assignment uses three workers. A valid one/two/three-worker scaling experiment requires explicit ownership remapping and storage handling. That is a separate follow-up, not an achieved feature.

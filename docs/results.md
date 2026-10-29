# Verification results

Status: build and image creation succeeded. The local kind deployment is incomplete because the two-CPU Docker engine repeatedly timed out its Kubernetes control plane while unpacking the Kafka and Prometheus images. Do not treat the deployment as validated yet.

## Checks completed on October 29, 2026

- `mvn -q test package` passed. Project-local Maven settings point directly to Maven Central and bypass host Oracle mirrors.
- `docker build -t viro:local .` succeeded. The image is `linux/amd64` and was imported into kind.
- The CDC extract has 1,200 rows (400 per pathogen) and is present in the local `data/` directory. The extract is intentionally excluded from Git; `scripts/fetch-data.py` recreates it.
- `Annorak/viro` is public, and GitHub reports `master` as the default branch.
- The Kafka and Viro worker StatefulSets, gateway, producer Job, and Prometheus deployment were accepted by Kubernetes.
- Prometheus reached Ready. A port-forwarded gateway returned the UI with HTTP 200. A filtered `/api/query` returned HTTP 503 because workers are unavailable. Prometheus `up` reports its own target as `1`; the gateway and three worker targets are `0` because they are not ready. Kafka started but is not Ready and has restarted once. Worker claims remain Pending after local-path provisioning timeouts, so worker pods have not started. Producer job attempts exited before Kafka became available. No end-to-end data query or application metric scrape has been verified.
- Kubernetes repeatedly logged API-server TLS timeouts and scheduler restarts while images were being unpacked. The Docker engine currently exposes two CPUs to kind.

## Remaining validation

- [ ] Confirm Kafka and all three worker pods are Ready.
- [ ] Confirm the producer Job completes and Kafka has exactly three topic partitions.
- [ ] Reconcile source row count against the sum of the three local databases.
- [ ] Verify a filtered database query and compare it with the Parquet snapshot query.
- [ ] Scrape a worker metric from Prometheus.
- [ ] Restart a worker and verify its data and stored offset recover.
- [ ] Measure the load limit and record the first failure and recovery behavior.
- [ ] Run the one-, two-, and three-worker experiments in [the scaling plan](scaling-experiment.md).

Do not report an end-to-end success, measured saturation point, or throughput improvement until the corresponding evidence is recorded here.

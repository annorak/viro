# Implement the scaling experiment

The current application has three fixed workers. Worker ordinal zero owns partition zero, ordinal one owns partition one, and ordinal two owns partition two. Each worker stores its own database, and the gateway lists all three workers. This layout does not support a correct one- or two-worker comparison.

Keep the Kafka topic at three partitions. Store each partition's DuckDB database, offset, and Parquet exports under a stable partition path such as `/data/partition-0/`. For the single-node kind cluster, mount one persistent local volume at `/data`. Allow one active writer per partition database.

## Add configurable partition ownership

1. Replace the worker's single partition setting with a validated list of assigned partitions.
2. Give each partition its own database connection and next-offset metadata.
3. Assign the configured partition set to the Kafka consumer.
4. Route each record to its partition database. Save the record and next Kafka offset in one transaction.
5. On restart, seek every assigned partition to its own saved next offset.
6. Reject out-of-range, duplicate, or missing partition assignments before the worker becomes ready.
7. Configure the gateway with the active worker list and verify that the workers cover partitions zero, one, and two exactly once.
8. Return an unavailable response when any required worker fails. Never return a successful partial aggregate.

Use these maps for the worker-count comparison:

| Workers | Worker 0 | Worker 1 | Worker 2 |
| ---: | --- | --- | --- |
| 1 | 0, 1, 2 | stopped | stopped |
| 2 | 0, 2 | 1 | stopped |
| 3 | 0 | 1 | 2 |

Use Kafka manual assignment for the controlled runs. Stop all workers before changing the map. Preserve the Kafka topic and partition-keyed files across each restart. The active worker count must never leave a partition unowned or give a partition to two workers.

## Verify each worker count

1. Add tests for duplicate, missing, and out-of-range assignments.
2. Verify that a worker can consume two assigned partitions and save both offsets with their records.
3. Restart a worker after a database commit. Confirm it resumes from each partition's saved offset.
4. Run the same frozen CDC input with one, two, and three workers.
5. Compare all site, pathogen, and week keys and values across the three results.
6. Stop a required worker. Confirm the gateway fails the query instead of returning partial results.

Do not begin load runs until every worker count passes the same reconciliation check.

## Prepare comparable load runs

1. Freeze the CDC extract and record its checksum, row count, pathogen counts, and retrieval time.
2. Hold the topic layout, input, producer rate, query mix, batch size, container limits, and Docker limits constant.
3. Give each run an identifier and isolated partition database paths. Preserve the frozen input and the shared Kafka history.
4. Set the replay count so the highest offered rate lasts at least 60 seconds. The current 1,200-row extract is too small for a sustained test in one pass.
5. Generate queries at a fixed open-loop rate. Count completed, rejected, and timed-out requests.
6. Track replay messages separately from distinct CDC observations. Repeated source rows exercise processing and database upserts, but they do not represent new observations.
7. Start each run from the same storage state. Reset only that run's test data.

## Find the worker limit

1. Run the same offered producer rate and query mix with one worker for at least 60 seconds.
2. Record published and durable messages per second, lag for each partition, publish-to-durable latency, query latency and errors, CPU, memory, and disk use.
3. Stop the workers, apply the two-worker map, and repeat the run.
4. Stop the workers, apply the three-worker map, and repeat the run.
5. Compare the results at equal offered load. Report the CPU and memory allocated to each run.
6. Increase the offered producer rate in steps. Mark saturation at the first sustained backlog growth, latency-target failure, resource exhaustion, or error increase.
7. If the finite input ends before the observation window, report finite-replay throughput instead of sustained capacity.

Kafka's three partitions limit this topic to three useful consumer owners. A fourth worker cannot increase this topic's parallelism.

## Compare one improvement

1. Restore the same input, worker map, query mix, and storage state.
2. Run transactions with batch size one.
3. Repeat with a bounded batch size of 100.
4. Compare durable throughput, ingestion latency, query latency, lag, and resource use.
5. Keep the batch change only if it improves a measured user or operator outcome without missing or extra observations.

## Record evidence

Write results in [verification results](results.md). Include the commit, image digest, CDC checksum, worker count, partition map, offered rate, query rate, run duration, batch size, resource limits, throughput, per-partition lag, latency percentiles, errors, recovery time, and exact source reconciliation.

Record unrun checks as pending. Do not claim a scaling gain, saturation point, or broker-failure recovery until the corresponding run succeeds. The local setup has one Kafka broker and one Kubernetes node, so it does not test broker or host redundancy.

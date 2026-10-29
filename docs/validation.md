# Metrics and validation

## Required evidence

Verify the actual deployed system before marking the demonstration complete:

- Kafka reports exactly three partitions.
- All three workers consume and persist records.
- The unfiltered database query matches the source's distinct-row count.
- Each pathogen filter matches the source manifest.
- Parquet and database results match after ingestion drains and all exports succeed.
- Replaying the extract does not increase distinct rows.
- A worker restart preserves its records and next offset.
- The gateway fails visibly when a required worker is unavailable.
- Prometheus successfully scrapes worker and gateway metrics.

## Measurements

Measure acknowledged producer records and publication duration, persisted observations, duplicate replay, partition lag, query requests and duration, errors, and export count and duration.

Use only bounded metric labels such as partition, mode, and outcome. Do not place site IDs, arbitrary filters, or query text in labels.

Report a latency percentile only if histogram buckets or raw samples support it. A sum/count metric supports a mean, not p95. Record source event dates separately from ingestion timestamps.

Record CPU, memory, container restarts, the shared Docker allocation, software versions, batch settings, and corpus checksum when comparing runs.

## Initial targets

At 100 records per second and five queries per second, the proposed targets are durable-ingestion p95 below two seconds and query p95 below 500 ms. These are unverified until measured.

Correctness requires exact source-to-storage reconciliation with zero missing or extra natural keys. A matching total alone is not sufficient proof of all record values.

## Short demonstration

1. Load the frozen extract once and save producer completion output.
2. Poll worker status until backlog drains.
3. Compare aggregate results with a reference computed from the same extract.
4. Apply each filter and test an empty match.
5. Flush to Parquet and compare the same grouped results.
6. Replay the source and verify stable distinct counts.
7. Restart a worker and verify recovery.
8. Capture metrics and inspect error counters.

## Load and improvement experiments

For a capacity experiment, hold corpus, query mix, resource limits, and initial state constant. Use an open-loop generator and include rejected and timed-out work.

Increase load until sustained backlog growth, latency-target failure, or resource errors occur. Record a sufficiently long observation window. If the corpus ends first, report finite-replay throughput rather than sustained capacity.

Repeated writes skipped as duplicates are not fresh-ingestion throughput. Compare batch size one with a bounded batch only using equivalent initial storage and source data. Record query latency alongside ingestion throughput.

Publish only observed improvements. Document unrun scaling, saturation, and failure checks as remaining work.

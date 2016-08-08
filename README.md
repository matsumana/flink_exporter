[Prometheus](https://prometheus.io/) exporter for [Apache Flink](https://flink.apache.org/).

# export metrics

- Overview
  - flink_overview_taskmanagers
  - flink_overview_slots_total
  - flink_overview_slots_available
  - flink_overview_jobs_running
  - flink_overview_jobs_finished
  - flink_overview_jobs_cancelled
  - flink_overview_jobs_failed

- Job status
  - flink_job_status_created
  - flink_job_status_running
  - flink_job_status_failing
  - flink_job_status_failed
  - flink_job_status_cancelling
  - flink_job_status_canceled
  - flink_job_status_finished
  - flink_job_status_restarting

- Read/Write Bytes & Records
  - flink_read_bytes
  - flink_read_records
  - flink_write_bytes
  - flink_write_records
  - flink_read_bytes_total
  - flink_read_records_total
  - flink_write_bytes_total
  - flink_write_records_total

- Checkpoint
  - flink_checkpoint_count
  - flink_checkpoint_duration
  - flink_checkpoint_size

# Command Line Options

Name     | Description | Default
---------|-------------|----
port | exporter's port number | 9160
log-level | Set Logging level | info
flink-job-manager-url | flink job manager url | http://localhost:8081/

---

# Prepare

## install gox

Please read the [documentation](https://github.com/mitchellh/gox)

## install glide

Please read the [documentation](https://github.com/Masterminds/glide)

## install ghr

Please read the [documentation](https://github.com/tcnksm/ghr)

# Install the dependencies

```
$ glide install
```

# Build

```
# for Mac
$ make build-mac

# for Linux
$ make build-linux
```

# Run app

```
$ releases/darwin/amd64/flink_exporter <options>
```

# Release to GitHub

```
$ make release-upload
```

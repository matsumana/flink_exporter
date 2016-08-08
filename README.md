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

- Checkpoint
  - flink_checkpoint_count_min
  - flink_checkpoint_count_max
  - flink_checkpoint_count_avg
  - flink_checkpoint_duration_min
  - flink_checkpoint_duration_max
  - flink_checkpoint_duration_avg
  - flink_checkpoint_size_min
  - flink_checkpoint_size_max
  - flink_checkpoint_size_avg

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

# Install the dependencies

```
$ glide install
```

# Build

```
# for Mac
$ make mac

# for Linux
$ make linux
```

# Run app

```
$ ./flink_exporter_xxx_xxx <options>
```

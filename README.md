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
  - flink_job_status_running

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

- Exceptions
  - flink_exception_count

# Command Line Options

Name     | Description | Default | e.g.
---------|-------------|----|---
port | exporter's port number | 9160 |
log-level | Set Logging level | info |
flink-job-manager-url | flink job manager url | | http://host.domain.com:8081/
yarn-resource-manager-url | flink job manager url | | http://host.domain.com:8088/ws/v1/cluster/apps?applicationTypes=Apache%20Flink&states=RUNNING

## Options check

Specify either `fink-job-manager-url` or `yarn-resource-manager-url`. Can't specify both.

---

# Prepare

## install the dependency tools

```
$ make install-depends
```

install below:

- [glide](https://github.com/Masterminds/glide)
- [gox](https://github.com/mitchellh/gox)
- [ghr](https://github.com/tcnksm/ghr)

## Install the dependency libraries

```
$ glide install
```

# Build

## Build with Docker

```
$ make build-with-docker
```

## Build without Docker

```
# for Mac
$ make build-mac

# for Linux
$ make build-linux
```

# Run app

```
# for Mac
$ releases/darwin/amd64/flink_exporter <options>

# for Linux
$ releases/linux/amd64/flink_exporter <options>
```

# Release to GitHub

```
$ make release-upload
```

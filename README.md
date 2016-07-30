[Prometheus](https://prometheus.io/) exporter for [Apache Flink](https://flink.apache.org/).

# Command Line Options

Name     | Description | Default
---------|-------------|----
port | exporter's port number | 9160
log-level | Set Logging level | info
interval | Interval to fetch metrics from the endpoint in second | 60
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

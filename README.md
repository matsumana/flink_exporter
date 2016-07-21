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
$ gox --osarch "darwin/amd64 linux/amd64"
```

# Run app

```
$ ./flink_exporter_xxx_xxx --flink-job-manager-url=http://localhost:8081/ --interval=15
```

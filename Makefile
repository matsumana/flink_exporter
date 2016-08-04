VERSION=$(patsubst "%",%,$(lastword $(shell grep 'version = ' main.go)))
GOX_OPTS="{{.Dir}}_{{.OS}}_{{.Arch}}_v$(VERSION)"

all: mac linux

mac:
	gox --osarch "darwin/amd64" --output $(GOX_OPTS)

linux:
	gox --osarch "linux/amd64"  --output $(GOX_OPTS)

.PHONY : clean
clean:
	rm -rf ./flink_exporter_*

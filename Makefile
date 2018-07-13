VERSION=$(patsubst "%",%,$(lastword $(shell grep "version\s*=\s" main.go)))
RELEASE_DIR=releases
ARTIFACTS_DIR=$(RELEASE_DIR)/artifacts/$(VERSION)
GOX_OPTS="$(RELEASE_DIR)/{{.OS}}/{{.Arch}}/{{.Dir}}"
GITHUB_USERNAME=matsumana
BUILD_GOLANG_VERSION=1.10.3

$(ARTIFACTS_DIR):
	@mkdir -p $(ARTIFACTS_DIR)

.PHONY : install-depends
install-depends:
	go get github.com/Masterminds/glide
	go get github.com/mitchellh/gox
	go get github.com/tcnksm/ghr

.PHONY : fmt
fmt:
	go fmt ./...

build-with-docker:
	docker run --rm -v "$(PWD)":/go/src/github.com/matsumana/flink_exporter -w /go/src/github.com/matsumana/flink_exporter golang:$(BUILD_GOLANG_VERSION) bash -c 'make install-depends && glide install && make build-all'

build-all: clean build-mac build-linux

build-mac: fmt
	gox --osarch "darwin/amd64" --output $(GOX_OPTS)

build-linux: fmt
	gox --osarch "linux/amd64"  --output $(GOX_OPTS)

release-targz: $(ARTIFACTS_DIR)
	tar cvfz $(ARTIFACTS_DIR)/flink_exporter_$(GOOS)_$(GOARCH)_$(VERSION).tgz -C $(RELEASE_DIR)/$(GOOS)/$(GOARCH) flink_exporter

release-all: build-with-docker release-mac release-linux

release-mac: build-mac
	@$(MAKE) release-targz GOOS=darwin GOARCH=amd64

release-linux: build-linux
	@$(MAKE) release-targz GOOS=linux GOARCH=amd64

release-github-token:
	if [ ! -f "./github_token" ]; then echo 'file github_token is required'; exit 1 ; fi

release-upload: release-all release-github-token
	ghr -u $(GITHUB_USERNAME) -t $(shell cat github_token) --draft --replace $(VERSION) $(ARTIFACTS_DIR)

.PHONY : clean
clean:
	rm -rf $(RELEASE_DIR)/*/*
	rm -rf $(ARTIFACTS_DIR)/*

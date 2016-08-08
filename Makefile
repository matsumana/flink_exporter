VERSION=$(patsubst "%",%,$(lastword $(shell grep "version\s*=\s" main.go)))
RELEASE_DIR=releases
ARTIFACTS_DIR=$(RELEASE_DIR)/artifacts/$(VERSION)
GOX_OPTS="$(RELEASE_DIR)/{{.OS}}/{{.Arch}}/{{.Dir}}"
GITHUB_USERNAME=matsumana

$(ARTIFACTS_DIR):
	@mkdir -p $(ARTIFACTS_DIR)

.PHONY : fmt
fmt:
	go fmt ./...

build-mac:
	gox --osarch "darwin/amd64" --output $(GOX_OPTS)

build-linux:
	gox --osarch "linux/amd64"  --output $(GOX_OPTS)

release-targz: $(ARTIFACTS_DIR)
	tar cvfz $(ARTIFACTS_DIR)/flink_exporter_$(GOOS)_$(GOARCH)_$(VERSION).tgz -C $(RELEASE_DIR)/$(GOOS)/$(GOARCH) flink_exporter

release-mac: build-mac
	@$(MAKE) release-targz GOOS=darwin GOARCH=amd64

release-linux: build-linux
	@$(MAKE) release-targz GOOS=linux GOARCH=amd64

release-github-token:
	@echo "file `github_token` is required"

release-upload: release-mac release-linux release-github-token
	ghr -u $(GITHUB_USERNAME) -t $(shell cat github_token) --draft --replace $(VERSION) $(ARTIFACTS_DIR)

.PHONY : clean
clean:
	rm -rf $(RELEASE_DIR)/*/*
	rm -rf $(ARTIFACTS_DIR)/*

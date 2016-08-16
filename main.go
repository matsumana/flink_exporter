package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/matsumana/flink_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/urfave/cli.v2"
	"net/http"
	"os"
)

const (
	version   = "1.0.0"
	endpoint  = "/metrics"
	namespace = "flink"
)

type appOpts struct {
	Name    string
	Version string
	Flags   []cli.Flag
}

func main() {
	opts := &appOpts{
		Name:    "flink_exporter",
		Version: version,
	}
	opts.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "log-level",
			Usage: "Set Logging level",
			Value: "info",
		},
		&cli.IntFlag{
			Name:  "port",
			Usage: "The port number used to expose metrics via http",
			Value: 9160,
		},
		&cli.StringFlag{
			Name:  "flink-job-manager-url",
			Usage: "flink job manager url",
		},
		&cli.StringFlag{
			Name:  "yarn-resource-manager-url",
			Usage: "YARN ResourceManager url",
		},
	}
	log.Debugf("opts = %v", opts)

	err := newApp(opts).Run(os.Args)
	if err != nil {
		os.Exit(1)
	}
}

func newApp(opts *appOpts) *cli.App {
	return &cli.App{
		Name:    opts.Name,
		Version: opts.Version,
		Usage:   "Prometheus exporter for Apache Flink",
		Flags:   opts.Flags,
		Action:  action,
	}
}

func action(c *cli.Context) error {

	setupLogging(c)
	checkArgs(c)

	flinkJobManagerUrl := c.String("flink-job-manager-url")
	yarnResourceManagerUrl := c.String("yarn-resource-manager-url")

	// register exporter
	exporter := exporter.NewExporter(flinkJobManagerUrl, yarnResourceManagerUrl, namespace)
	prometheus.MustRegister(exporter)

	// http listen and serve
	port := c.Int("port")
	log.Debugf("port = %v", port)

	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add("Location", endpoint)
		w.WriteHeader(http.StatusMovedPermanently)
	})
	http.Handle(endpoint, prometheus.Handler())
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		log.Fatal(err)
	}

	return nil
}

func checkArgs(c *cli.Context) {
	flinkJobManagerUrl := c.String("flink-job-manager-url")
	yarnResourceManagerUrl := c.String("yarn-resource-manager-url")

	log.Debugf("flink-job-manager-url = %v", flinkJobManagerUrl)
	log.Debugf("yarn-resource-manager-url = %v", yarnResourceManagerUrl)

	if flinkJobManagerUrl == "" && yarnResourceManagerUrl == "" {
		log.Fatal("Specify either fink-job-manager-url or yarn-resource-manager-url. Can't specify both.")
	}

	if flinkJobManagerUrl != "" && yarnResourceManagerUrl != "" {
		log.Fatal("Specify either fink-job-manager-url or yarn-resource-manager-url. Can't specify both.")
	}
}

func setupLogging(c *cli.Context) {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	levelString := c.String("log-level")
	level, err := log.ParseLevel(levelString)
	if err != nil {
		log.Fatalf("could not set log level to '%s';err:<%s>", levelString, err)
	}
	log.SetLevel(level)
}

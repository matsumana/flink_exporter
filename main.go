package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/kawamuray/prometheus-exporter-harness/harness"
	c "github.com/matsumana/flink_exporter/collector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli"
)

const (
	version = "0.0.1"
)

var (
	flinkJobManagerUrl string
)

type collector struct{}

func main() {
	opts := harness.NewExporterOpts("flink_exporter", version)
	opts.Init = initExporter
	opts.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "flink-job-manager-url",
			Usage: "flink job manager url",
			Value: "http://localhost:8081/",
		},
	}

	harness.Main(opts)
}

func initExporter(c *cli.Context, reg *harness.MetricRegistry) (harness.Collector, error) {
	flinkJobManagerUrl = c.String("flink-job-manager-url")
	log.Debug(flinkJobManagerUrl)

	reg.Register("flink_overview_taskmanagers", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_taskmanagers",
		Help: "is quantity of flink taskmanagers",
	}))
	reg.Register("flink_overview_slots_total", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_slots_total",
		Help: "is quantity of flink slots-total",
	}))
	reg.Register("flink_overview_slots_available", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_slots_available",
		Help: "is quantity of flink slots-available",
	}))
	reg.Register("flink_overview_jobs_running", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_jobs_running",
		Help: "is quantity of flink jobs-running",
	}))
	reg.Register("flink_overview_jobs_finished", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_jobs_finished",
		Help: "is quantity of flink jobs-finished",
	}))
	reg.Register("flink_overview_jobs_cancelled", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_jobs_cancelled",
		Help: "is quantity of flink jobs-cancelled",
	}))
	reg.Register("flink_overview_jobs_failed", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_jobs_failed",
		Help: "is quantity of flink jobs-failed",
	}))

	reg.Register("flink_write_records", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_write_records",
		Help: "is quantity of flink write records",
	}))

	return &collector{}, nil
}

func (col *collector) Collect(reg *harness.MetricRegistry) {
	o := c.Overview{}
	overview := o.GetOverview(flinkJobManagerUrl)
	reg.Get("flink_overview_taskmanagers").(prometheus.Gauge).Set(float64(overview.TaskManagers))
	reg.Get("flink_overview_slots_total").(prometheus.Gauge).Set(float64(overview.SlotsTotal))
	reg.Get("flink_overview_slots_available").(prometheus.Gauge).Set(float64(overview.SlotsAvailable))
	reg.Get("flink_overview_jobs_running").(prometheus.Gauge).Set(float64(overview.JobsRunning))
	reg.Get("flink_overview_jobs_finished").(prometheus.Gauge).Set(float64(overview.JobsFinished))
	reg.Get("flink_overview_jobs_cancelled").(prometheus.Gauge).Set(float64(overview.JobsCancelled))
	reg.Get("flink_overview_jobs_failed").(prometheus.Gauge).Set(float64(overview.JobsFailed))

	j := c.Job{}
	writeRecords := j.GetWriteRecords(flinkJobManagerUrl)
	reg.Get("flink_write_records").(prometheus.Gauge).Set(float64(writeRecords))
}

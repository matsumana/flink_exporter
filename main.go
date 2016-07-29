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

	// overview
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

	// Read/Write
	reg.Register("flink_write_records", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_write_records",
		Help: "is quantity of flink write records",
	}))

	// checkpoint
	reg.Register("flink_checkpoint_count", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_count",
		Help: "is quantity of flink checkpoint count",
	}))
	reg.Register("flink_checkpoint_duration_min", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_duration_min",
		Help: "is quantity of flink checkpoint duration min",
	}))
	reg.Register("flink_checkpoint_duration_max", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_duration_max",
		Help: "is quantity of flink checkpoint duration max",
	}))
	reg.Register("flink_checkpoint_duration_avg", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_duration_avg",
		Help: "is quantity of flink checkpoint duration avg",
	}))
	reg.Register("flink_checkpoint_size_min", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_size_min",
		Help: "is quantity of flink checkpoint size min",
	}))
	reg.Register("flink_checkpoint_size_max", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_size_max",
		Help: "is quantity of flink checkpoint size max",
	}))
	reg.Register("flink_checkpoint_size_avg", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_size_avg",
		Help: "is quantity of flink checkpoint size avg",
	}))

	return &collector{}, nil
}

func (col *collector) Collect(reg *harness.MetricRegistry) {
	// overview
	o := c.Overview{}
	overview := o.GetMetrics(flinkJobManagerUrl)
	reg.Get("flink_overview_taskmanagers").(prometheus.Gauge).Set(float64(overview.TaskManagers))
	reg.Get("flink_overview_slots_total").(prometheus.Gauge).Set(float64(overview.SlotsTotal))
	reg.Get("flink_overview_slots_available").(prometheus.Gauge).Set(float64(overview.SlotsAvailable))
	reg.Get("flink_overview_jobs_running").(prometheus.Gauge).Set(float64(overview.JobsRunning))
	reg.Get("flink_overview_jobs_finished").(prometheus.Gauge).Set(float64(overview.JobsFinished))
	reg.Get("flink_overview_jobs_cancelled").(prometheus.Gauge).Set(float64(overview.JobsCancelled))
	reg.Get("flink_overview_jobs_failed").(prometheus.Gauge).Set(float64(overview.JobsFailed))

	// Read/Write
	j := c.Job{}
	writeRecords, checkpoint := j.GetMetrics(flinkJobManagerUrl)
	reg.Get("flink_write_records").(prometheus.Gauge).Set(float64(writeRecords))

	// checkpoint
	reg.Get("flink_checkpoint_count").(prometheus.Gauge).Set(float64(checkpoint.Count))
	reg.Get("flink_checkpoint_duration_min").(prometheus.Gauge).Set(float64(checkpoint.DurationMin))
	reg.Get("flink_checkpoint_duration_max").(prometheus.Gauge).Set(float64(checkpoint.DurationMax))
	reg.Get("flink_checkpoint_duration_avg").(prometheus.Gauge).Set(float64(checkpoint.DurationAvg))
	reg.Get("flink_checkpoint_size_min").(prometheus.Gauge).Set(float64(checkpoint.SizeMin))
	reg.Get("flink_checkpoint_size_max").(prometheus.Gauge).Set(float64(checkpoint.SizeMax))
	reg.Get("flink_checkpoint_size_avg").(prometheus.Gauge).Set(float64(checkpoint.SizeAvg))
}

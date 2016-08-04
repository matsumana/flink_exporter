package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/kawamuray/prometheus-exporter-harness/harness"
	c "github.com/matsumana/flink_exporter/collector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli"
)

const (
	version = "0.1.0"
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
		Help: "flink overview taskmanagers",
	}))
	reg.Register("flink_overview_slots_total", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_slots_total",
		Help: "flink overview slots-total",
	}))
	reg.Register("flink_overview_slots_available", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_slots_available",
		Help: "flink overview slots-available",
	}))
	reg.Register("flink_overview_jobs_running", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_jobs_running",
		Help: "flink overview jobs-running",
	}))
	reg.Register("flink_overview_jobs_finished", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_jobs_finished",
		Help: "flink overview jobs-finished",
	}))
	reg.Register("flink_overview_jobs_cancelled", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_jobs_cancelled",
		Help: "flink overview jobs-cancelled",
	}))
	reg.Register("flink_overview_jobs_failed", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_overview_jobs_failed",
		Help: "flink overview jobs-failed",
	}))

	// job status
	reg.Register("flink_job_status_created", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_job_status_created",
		Help: "flink job status created",
	}))
	reg.Register("flink_job_status_running", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_job_status_running",
		Help: "flink job status running",
	}))
	reg.Register("flink_job_status_failing", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_job_status_failing",
		Help: "flink job status failing",
	}))
	reg.Register("flink_job_status_failed", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_job_status_failed",
		Help: "flink job status failed",
	}))
	reg.Register("flink_job_status_cancelling", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_job_status_cancelling",
		Help: "flink job status cancelling",
	}))
	reg.Register("flink_job_status_canceled", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_job_status_canceled",
		Help: "flink job status canceled",
	}))
	reg.Register("flink_job_status_finished", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_job_status_finished",
		Help: "flink job status finished",
	}))
	reg.Register("flink_job_status_restarting", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_job_status_restarting",
		Help: "flink job status restarting",
	}))

	// Read/Write
	reg.Register("flink_read_bytes", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_read_bytes",
		Help: "flink read bytes",
	}))
	reg.Register("flink_read_records", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_read_records",
		Help: "flink read records",
	}))
	reg.Register("flink_write_bytes", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_write_bytes",
		Help: "flink write bytes",
	}))
	reg.Register("flink_write_records", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_write_records",
		Help: "flink write records",
	}))

	// checkpoint
	reg.Register("flink_checkpoint_count_avg", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_count_avg",
		Help: "flink checkpoint count avg",
	}))
	reg.Register("flink_checkpoint_count_min", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_count_min",
		Help: "flink checkpoint count min",
	}))
	reg.Register("flink_checkpoint_count_max", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_count_max",
		Help: "flink checkpoint count max",
	}))
	reg.Register("flink_checkpoint_duration_min", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_duration_min",
		Help: "flink checkpoint duration min",
	}))
	reg.Register("flink_checkpoint_duration_max", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_duration_max",
		Help: "flink checkpoint duration max",
	}))
	reg.Register("flink_checkpoint_duration_avg", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_duration_avg",
		Help: "flink checkpoint duration avg",
	}))
	reg.Register("flink_checkpoint_size_min", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_size_min",
		Help: "flink checkpoint size min",
	}))
	reg.Register("flink_checkpoint_size_max", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_size_max",
		Help: "flink checkpoint size max",
	}))
	reg.Register("flink_checkpoint_size_avg", prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flink_checkpoint_size_avg",
		Help: "flink checkpoint size avg",
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

	j := c.Job{}
	readWriteMertics, checkpoint, jobStatus := j.GetMetrics(flinkJobManagerUrl)

	// job status
	reg.Get("flink_job_status_created").(prometheus.Gauge).Set(float64(jobStatus.Created))
	reg.Get("flink_job_status_running").(prometheus.Gauge).Set(float64(jobStatus.Running))
	reg.Get("flink_job_status_failing").(prometheus.Gauge).Set(float64(jobStatus.Failing))
	reg.Get("flink_job_status_failed").(prometheus.Gauge).Set(float64(jobStatus.Failed))
	reg.Get("flink_job_status_cancelling").(prometheus.Gauge).Set(float64(jobStatus.Cancelling))
	reg.Get("flink_job_status_canceled").(prometheus.Gauge).Set(float64(jobStatus.Canceled))
	reg.Get("flink_job_status_finished").(prometheus.Gauge).Set(float64(jobStatus.Finished))
	reg.Get("flink_job_status_restarting").(prometheus.Gauge).Set(float64(jobStatus.Restarting))

	// Read/Write
	reg.Get("flink_read_bytes").(prometheus.Gauge).Set(float64(readWriteMertics.ReadBytes))
	reg.Get("flink_read_records").(prometheus.Gauge).Set(float64(readWriteMertics.ReadRecords))
	reg.Get("flink_write_bytes").(prometheus.Gauge).Set(float64(readWriteMertics.WriteBytes))
	reg.Get("flink_write_records").(prometheus.Gauge).Set(float64(readWriteMertics.WriteRecords))

	// checkpoint
	reg.Get("flink_checkpoint_count_min").(prometheus.Gauge).Set(float64(checkpoint.CountMin))
	reg.Get("flink_checkpoint_count_max").(prometheus.Gauge).Set(float64(checkpoint.CountMax))
	reg.Get("flink_checkpoint_count_avg").(prometheus.Gauge).Set(float64(checkpoint.CountAvg))
	reg.Get("flink_checkpoint_duration_min").(prometheus.Gauge).Set(float64(checkpoint.DurationMin))
	reg.Get("flink_checkpoint_duration_max").(prometheus.Gauge).Set(float64(checkpoint.DurationMax))
	reg.Get("flink_checkpoint_duration_avg").(prometheus.Gauge).Set(float64(checkpoint.DurationAvg))
	reg.Get("flink_checkpoint_size_min").(prometheus.Gauge).Set(float64(checkpoint.SizeMin))
	reg.Get("flink_checkpoint_size_max").(prometheus.Gauge).Set(float64(checkpoint.SizeMax))
	reg.Get("flink_checkpoint_size_avg").(prometheus.Gauge).Set(float64(checkpoint.SizeAvg))
}

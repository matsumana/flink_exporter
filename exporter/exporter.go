package exporter

import (
	log "github.com/Sirupsen/logrus"
	"github.com/matsumana/flink_exporter/collector"
	"github.com/prometheus/client_golang/prometheus"
)

type Exporter struct {
	gauges             map[string]prometheus.Gauge
	gaugeVecs          map[string]*prometheus.GaugeVec
	flinkJobManagerUrl string
}

func NewExporter(flinkJobManagerUrl string, namespace string) *Exporter {
	gauges := make(map[string]prometheus.Gauge)
	gaugeVecs := make(map[string]*prometheus.GaugeVec)

	// overview
	gauges["overview_taskmanagers"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "overview_taskmanagers",
		Help:      "overview_taskmanagers"})
	gauges["overview_slots_total"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "overview_slots_total",
		Help:      "overview_slots_total"})
	gauges["overview_slots_available"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "overview_slots_available",
		Help:      "overview_slots_available"})
	gauges["overview_jobs_running"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "overview_jobs_running",
		Help:      "overview_jobs_running"})
	gauges["overview_jobs_finished"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "overview_jobs_finished",
		Help:      "overview_jobs_finished"})
	gauges["overview_jobs_cancelled"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "overview_jobs_cancelled",
		Help:      "overview_jobs_cancelled"})
	gauges["overview_jobs_failed"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "overview_jobs_failed",
		Help:      "overview_jobs_failed"})

	// job status
	gaugeVecs["job_status_created"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_created",
		Help:      "job_status_created"},
		[]string{"jobName"})
	gaugeVecs["job_status_running"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_running",
		Help:      "job_status_running"},
		[]string{"jobName"})
	gaugeVecs["job_status_failing"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_failing",
		Help:      "job_status_failing"},
		[]string{"jobName"})
	gaugeVecs["job_status_failed"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_failed",
		Help:      "job_status_failed"},
		[]string{"jobName"})
	gaugeVecs["job_status_cancelling"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_cancelling",
		Help:      "job_status_cancelling"},
		[]string{"jobName"})
	gaugeVecs["job_status_canceled"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_canceled",
		Help:      "job_status_canceled"},
		[]string{"jobName"})
	gaugeVecs["job_status_finished"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_finished",
		Help:      "job_status_finished"},
		[]string{"jobName"})
	gaugeVecs["job_status_restarting"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_restarting",
		Help:      "job_status_restarting"},
		[]string{"jobName"})

	// Read/Write
	gaugeVecs["read_bytes"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "read_bytes",
		Help:      "read_bytes"},
		[]string{"jobName"})
	gaugeVecs["read_records"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "read_records",
		Help:      "read_records"},
		[]string{"jobName"})
	gaugeVecs["write_bytes"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "write_bytes",
		Help:      "write_bytes"},
		[]string{"jobName"})
	gaugeVecs["write_records"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "write_records",
		Help:      "write_records"},
		[]string{"jobName"})

	// checkpoint
	gaugeVecs["checkpoint_count_avg"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_count_avg",
		Help:      "checkpoint_count_avg"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_count_min"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_count_min",
		Help:      "checkpoint_count_min"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_count_max"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_count_max",
		Help:      "checkpoint_count_max"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_duration_min"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_duration_min",
		Help:      "checkpoint_duration_min"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_duration_max"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_duration_max",
		Help:      "checkpoint_duration_max"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_duration_avg"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_duration_avg",
		Help:      "checkpoint_duration_avg"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_size_min"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_size_min",
		Help:      "checkpoint_size_min"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_size_max"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_size_max",
		Help:      "checkpoint_size_max"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_size_avg"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_size_avg",
		Help:      "checkpoint_size_avg"},
		[]string{"jobName"})

	return &Exporter{
		gauges:             gauges,
		gaugeVecs:          gaugeVecs,
		flinkJobManagerUrl: flinkJobManagerUrl,
	}
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, value := range e.gauges {
		value.Describe(ch)
	}
	for _, value := range e.gaugeVecs {
		value.Describe(ch)
	}
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	log.Debugf("%v", e)

	e.collectGauge()
	e.collectGaugeVec()

	for _, value := range e.gauges {
		value.Collect(ch)
	}
	for _, value := range e.gaugeVecs {
		value.Collect(ch)
	}
}

func (e *Exporter) collectGauge() {
	// overview
	o := collector.Overview{}
	overview := o.GetMetrics(e.flinkJobManagerUrl)
	e.gauges["overview_taskmanagers"].Set(float64(overview.TaskManagers))
	e.gauges["overview_slots_total"].Set(float64(overview.SlotsTotal))
	e.gauges["overview_slots_available"].Set(float64(overview.SlotsAvailable))
	e.gauges["overview_jobs_running"].Set(float64(overview.JobsRunning))
	e.gauges["overview_jobs_finished"].Set(float64(overview.JobsFinished))
	e.gauges["overview_jobs_cancelled"].Set(float64(overview.JobsCancelled))
	e.gauges["overview_jobs_failed"].Set(float64(overview.JobsFailed))
}

func (e *Exporter) collectGaugeVec() {
	j := collector.Job{}
	readWriteMertics, checkpoint, jobStatus := j.GetMetrics(e.flinkJobManagerUrl)

	// TODO fix
	jobName := "foo"

	// job status
	e.gaugeVecs["job_status_created"].WithLabelValues(jobName).Set(float64(jobStatus.Created))
	e.gaugeVecs["job_status_running"].WithLabelValues(jobName).Set(float64(jobStatus.Running))
	e.gaugeVecs["job_status_failing"].WithLabelValues(jobName).Set(float64(jobStatus.Failing))
	e.gaugeVecs["job_status_failed"].WithLabelValues(jobName).Set(float64(jobStatus.Failed))
	e.gaugeVecs["job_status_cancelling"].WithLabelValues(jobName).Set(float64(jobStatus.Cancelling))
	e.gaugeVecs["job_status_canceled"].WithLabelValues(jobName).Set(float64(jobStatus.Canceled))
	e.gaugeVecs["job_status_finished"].WithLabelValues(jobName).Set(float64(jobStatus.Finished))
	e.gaugeVecs["job_status_restarting"].WithLabelValues(jobName).Set(float64(jobStatus.Restarting))

	// Read/Write
	e.gaugeVecs["read_bytes"].WithLabelValues(jobName).Set(float64(readWriteMertics.ReadBytes))
	e.gaugeVecs["read_records"].WithLabelValues(jobName).Set(float64(readWriteMertics.ReadRecords))
	e.gaugeVecs["write_bytes"].WithLabelValues(jobName).Set(float64(readWriteMertics.WriteBytes))
	e.gaugeVecs["write_records"].WithLabelValues(jobName).Set(float64(readWriteMertics.WriteRecords))

	// checkpoint
	e.gaugeVecs["checkpoint_count_min"].WithLabelValues(jobName).Set(float64(checkpoint.CountMin))
	e.gaugeVecs["checkpoint_count_max"].WithLabelValues(jobName).Set(float64(checkpoint.CountMax))
	e.gaugeVecs["checkpoint_count_avg"].WithLabelValues(jobName).Set(float64(checkpoint.CountAvg))
	e.gaugeVecs["checkpoint_duration_min"].WithLabelValues(jobName).Set(float64(checkpoint.DurationMin))
	e.gaugeVecs["checkpoint_duration_max"].WithLabelValues(jobName).Set(float64(checkpoint.DurationMax))
	e.gaugeVecs["checkpoint_duration_avg"].WithLabelValues(jobName).Set(float64(checkpoint.DurationAvg))
	e.gaugeVecs["checkpoint_size_min"].WithLabelValues(jobName).Set(float64(checkpoint.SizeMin))
	e.gaugeVecs["checkpoint_size_max"].WithLabelValues(jobName).Set(float64(checkpoint.SizeMax))
	e.gaugeVecs["checkpoint_size_avg"].WithLabelValues(jobName).Set(float64(checkpoint.SizeAvg))
}

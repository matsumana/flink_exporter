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
	gaugeVecs["checkpoint_count"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_count",
		Help:      "checkpoint_count"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_duration"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_duration",
		Help:      "checkpoint_duration"},
		[]string{"jobName"})
	gaugeVecs["checkpoint_size"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "checkpoint_size",
		Help:      "checkpoint_size"},
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
	jobStatuses, readWrites, checkpoints := j.GetMetrics(e.flinkJobManagerUrl)

	// job status
	for _, value := range jobStatuses {
		e.gaugeVecs["job_status_created"].WithLabelValues(value.JobName).Set(float64(value.Created))
		e.gaugeVecs["job_status_running"].WithLabelValues(value.JobName).Set(float64(value.Running))
		e.gaugeVecs["job_status_failing"].WithLabelValues(value.JobName).Set(float64(value.Failing))
		e.gaugeVecs["job_status_failed"].WithLabelValues(value.JobName).Set(float64(value.Failed))
		e.gaugeVecs["job_status_cancelling"].WithLabelValues(value.JobName).Set(float64(value.Cancelling))
		e.gaugeVecs["job_status_canceled"].WithLabelValues(value.JobName).Set(float64(value.Canceled))
		e.gaugeVecs["job_status_finished"].WithLabelValues(value.JobName).Set(float64(value.Finished))
		e.gaugeVecs["job_status_restarting"].WithLabelValues(value.JobName).Set(float64(value.Restarting))
	}

	// Read/Write
	for _, value := range readWrites {
		e.gaugeVecs["read_bytes"].WithLabelValues(value.JobName).Set(float64(value.ReadBytes))
		e.gaugeVecs["read_records"].WithLabelValues(value.JobName).Set(float64(value.ReadRecords))
		e.gaugeVecs["write_bytes"].WithLabelValues(value.JobName).Set(float64(value.WriteBytes))
		e.gaugeVecs["write_records"].WithLabelValues(value.JobName).Set(float64(value.WriteRecords))
	}

	// checkpoint
	for _, value := range checkpoints {
		e.gaugeVecs["checkpoint_count"].WithLabelValues(value.JobName).Set(float64(value.Count))
		e.gaugeVecs["checkpoint_duration"].WithLabelValues(value.JobName).Set(float64(value.Duration))
		e.gaugeVecs["checkpoint_size"].WithLabelValues(value.JobName).Set(float64(value.Size))
	}
}

package exporter

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"
	"github.com/matsumana/flink_exporter/collector"
	"github.com/matsumana/flink_exporter/util"
	"github.com/prometheus/client_golang/prometheus"
)

type Exporter struct {
	gauges                 map[string]prometheus.Gauge
	gaugeVecs              map[string]*prometheus.GaugeVec
	flinkJobManagerUrl     string
	yarnResourceManagerUrl string
}

func NewExporter(flinkJobManagerUrl string, yarnResourceManagerUrl string, namespace string) *Exporter {
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
	gaugeVecs["job_status_running"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_status_running",
		Help:      "job_status_running"},
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

	// Read/Write total
	gauges["read_bytes_total"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "read_bytes_total",
		Help:      "read_bytes_total"})
	gauges["read_records_total"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "read_records_total",
		Help:      "read_records_total"})
	gauges["write_bytes_total"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "write_bytes_total",
		Help:      "write_bytes_total"})
	gauges["write_records_total"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "write_records_total",
		Help:      "write_records_total"})

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

	// exceptions
	gaugeVecs["exception_count"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "exception_count",
		Help:      "exception_count"},
		[]string{"jobName"})

	return &Exporter{
		gauges:                 gauges,
		gaugeVecs:              gaugeVecs,
		flinkJobManagerUrl:     flinkJobManagerUrl,
		yarnResourceManagerUrl: yarnResourceManagerUrl,
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

func (e *Exporter) getFlinkJobManagerUrl() []string {
	if e.flinkJobManagerUrl != "" && e.yarnResourceManagerUrl == "" {
		return []string{e.flinkJobManagerUrl}
	} else {
		return e.getFlinkJobManagerUrlFromYarn(e.yarnResourceManagerUrl)
	}
}

func (e *Exporter) getFlinkJobManagerUrlFromYarn(yarnResourceManagerUrl string) []string {
	httpClient := util.HttpClient{}
	jsonStr, err := httpClient.Get(yarnResourceManagerUrl)
	if err != nil {
		log.Errorf("HttpClient.Get = %v", err)
		return []string{}
	}

	// parse
	js, err := simpleJson.NewJson([]byte(jsonStr))
	if err != nil {
		log.Errorf("simpleJson.NewJson = %v", err)
		return []string{}
	}

	// apps
	var apps []interface{}
	apps, err = js.Get("apps").Get("app").Array()
	if err != nil {
		log.Errorf("js.Get 'apps' = %v", err)
		return []string{}
	}
	log.Debugf("apps = %v", apps)

	var trackingUrls []string
	for _, app := range apps {
		if a, ok := app.(map[string]interface{}); ok {
			if trackingUrl, found := a["trackingUrl"]; found {
				trackingUrls = append(trackingUrls, fmt.Sprint(trackingUrl))
			}
		}
	}

	log.Debugf("trackingUrls = %v", trackingUrls)

	return trackingUrls
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

	flinkJobManagerUrls := e.getFlinkJobManagerUrl()
	channel := make(chan collector.Overview)
	for _, flinkJobManagerUrl := range flinkJobManagerUrls {
		go func(flinkJobManagerUrl string) {
			channel <- o.GetMetrics(flinkJobManagerUrl)
		}(flinkJobManagerUrl)
	}

	// receive from all channels
	overview := collector.Overview{}
	for i := 0; i < len(flinkJobManagerUrls); i++ {
		ov := <-channel
		overview.TaskManagers += ov.TaskManagers
		overview.SlotsTotal += ov.SlotsTotal
		overview.SlotsAvailable += ov.SlotsAvailable
		overview.JobsRunning += ov.JobsRunning
		overview.JobsFinished += ov.JobsFinished
		overview.JobsCancelled += ov.JobsCancelled
		overview.JobsFailed += ov.JobsFailed
	}

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

	flinkJobManagerUrls := e.getFlinkJobManagerUrl()
	log.Debugf("flinkJobManagerUrls=%v", flinkJobManagerUrls)

	channel := make(chan collector.JobMetrics)
	for _, flinkJobManagerUrl := range flinkJobManagerUrls {
		go func(flinkJobManagerUrl string) {
			channel <- j.GetMetrics(flinkJobManagerUrl)
		}(flinkJobManagerUrl)
	}

	readWriteTotalMertics := collector.ReadWriteTotalMertics{}
	log.Debugf("flinkJobManagerUrls=%v", flinkJobManagerUrls)
	for i := 0; i < len(flinkJobManagerUrls); i++ {
		jobMetrics := <-channel

		// job status
		for _, value := range jobMetrics.JobStatusMetrics {
			e.gaugeVecs["job_status_running"].WithLabelValues(value.JobName).Set(float64(value.Running))
		}

		// Read/Write
		for _, value := range jobMetrics.ReadWriteTotalMertics.Details {
			e.gaugeVecs["read_bytes"].WithLabelValues(value.JobName).Set(float64(value.ReadBytes))
			e.gaugeVecs["read_records"].WithLabelValues(value.JobName).Set(float64(value.ReadRecords))
			e.gaugeVecs["write_bytes"].WithLabelValues(value.JobName).Set(float64(value.WriteBytes))
			e.gaugeVecs["write_records"].WithLabelValues(value.JobName).Set(float64(value.WriteRecords))
		}

		// Read/Write total
		readWriteTotalMertics.ReadBytesTotal += jobMetrics.ReadWriteTotalMertics.ReadBytesTotal
		readWriteTotalMertics.ReadRecordsTotal += jobMetrics.ReadWriteTotalMertics.ReadRecordsTotal
		readWriteTotalMertics.WriteBytesTotal += jobMetrics.ReadWriteTotalMertics.WriteBytesTotal
		readWriteTotalMertics.WriteRecordsTotal += jobMetrics.ReadWriteTotalMertics.WriteRecordsTotal

		// checkpoint
		for _, value := range jobMetrics.CheckpointMetrics {
			e.gaugeVecs["checkpoint_count"].WithLabelValues(value.JobName).Set(float64(value.Count))
			e.gaugeVecs["checkpoint_duration"].WithLabelValues(value.JobName).Set(float64(value.Duration))
			e.gaugeVecs["checkpoint_size"].WithLabelValues(value.JobName).Set(float64(value.Size))
		}

		// exceptions
		for _, value := range jobMetrics.ExceptionMetrics {
			log.Debugf("exceptions=%v", value)
			e.gaugeVecs["exception_count"].WithLabelValues(value.JobName).Set(float64(value.Count))
		}
	}

	// Read/Write total
	e.gauges["read_bytes_total"].Set(float64(readWriteTotalMertics.ReadBytesTotal))
	e.gauges["read_records_total"].Set(float64(readWriteTotalMertics.ReadRecordsTotal))
	e.gauges["write_bytes_total"].Set(float64(readWriteTotalMertics.WriteBytesTotal))
	e.gauges["write_records_total"].Set(float64(readWriteTotalMertics.WriteRecordsTotal))
}

package main

import (
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"
	"github.com/kawamuray/prometheus-exporter-harness/harness"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	version = "0.0.1"
)

var (
	flinkJobManagerUrl string
)

type collector struct{}

type Overview struct {
	TaskManagers   int
	SlotsTotal     int
	SlotsAvailable int
	JobsRunning    int
	JobsFinished   int
	JobsCancelled  int
	JobsFailed     int
	FlinkVersion   string
}

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

	return &collector{}, nil
}

func (col *collector) Collect(reg *harness.MetricRegistry) {
	overview := getOverview()

	reg.Get("flink_overview_taskmanagers").(prometheus.Gauge).Set(float64(overview.TaskManagers))
	reg.Get("flink_overview_slots_total").(prometheus.Gauge).Set(float64(overview.SlotsTotal))
	reg.Get("flink_overview_slots_available").(prometheus.Gauge).Set(float64(overview.SlotsAvailable))
	reg.Get("flink_overview_jobs_running").(prometheus.Gauge).Set(float64(overview.JobsRunning))
	reg.Get("flink_overview_jobs_finished").(prometheus.Gauge).Set(float64(overview.JobsFinished))
	reg.Get("flink_overview_jobs_cancelled").(prometheus.Gauge).Set(float64(overview.JobsCancelled))
	reg.Get("flink_overview_jobs_failed").(prometheus.Gauge).Set(float64(overview.JobsFailed))
}

func getOverview() Overview {
	url := strings.Trim(flinkJobManagerUrl, "/") + "/overview"
	log.Debug(url)

	overview := Overview{
		TaskManagers:   -1,
		SlotsTotal:     -1,
		SlotsAvailable: -1,
		JobsRunning:    -1,
		JobsFinished:   -1,
		JobsCancelled:  -1,
		JobsFailed:     -1,
	}

	response, err := http.Get(url)
	if err != nil {
		log.Errorf("http.Get = %v", err)
		return overview
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("ioutil.ReadAll = %v", err)
		return overview
	}
	if response.StatusCode != 200 {
		log.Errorf("response.StatusCode = %v", response.StatusCode)
		return overview
	}

	jsonStr := string(body)
	log.Debug(jsonStr)

	// parse
	js, err := simpleJson.NewJson([]byte(jsonStr))
	if err != nil {
		log.Errorf("simpleJson.NewJson = %v", err)
		return overview
	}

	// taskmanagers
	overview.TaskManagers, err = js.Get("taskmanagers").Int()
	if err != nil {
		log.Errorf("js.Get 'taskmanagers' = %v", err)
		return overview
	}
	log.Debugf("overview.TaskManagers = %v", overview.TaskManagers)

	// slots-total
	overview.SlotsTotal, err = js.Get("slots-total").Int()
	if err != nil {
		log.Errorf("js.Get 'slots-total' = %v", err)
		return overview
	}
	log.Debugf("overview.SlotsTotal = %v", overview.SlotsTotal)

	// slots-available
	overview.SlotsAvailable, err = js.Get("slots-available").Int()
	if err != nil {
		log.Errorf("js.Get 'slots-available' = %v", err)
		return overview
	}
	log.Debugf("overview.SlotsAvailable = %v", overview.SlotsAvailable)

	// jobs-running
	overview.JobsRunning, err = js.Get("jobs-running").Int()
	if err != nil {
		log.Errorf("js.Get 'jobs-running' = %v", err)
		return overview
	}
	log.Debugf("overview.JobsRunning = %v", overview.JobsRunning)

	// jobs-finished
	overview.JobsFinished, err = js.Get("jobs-finished").Int()
	if err != nil {
		log.Errorf("js.Get 'jobs-finished' = %v", err)
		return overview
	}
	log.Debugf("overview.JobsFinished = %v", overview.JobsFinished)

	// jobs-cancelled
	overview.JobsCancelled, err = js.Get("jobs-cancelled").Int()
	if err != nil {
		log.Errorf("js.Get 'jobs-cancelled' = %v", err)
		return overview
	}
	log.Debugf("overview.JobsCancelled = %v", overview.JobsCancelled)

	// jobs-failed
	overview.JobsFailed, err = js.Get("jobs-failed").Int()
	if err != nil {
		log.Errorf("js.Get 'jobs-failed' = %v", err)
		return overview
	}
	log.Debugf("overview.JobsFailed = %v", overview.JobsFailed)

	return overview
}

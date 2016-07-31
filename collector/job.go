package collector

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"
	"strconv"
	"strings"
)

type Job struct{}

type ReadWriteMertics struct {
	ReadBytes    int64
	WriteBytes   int64
	ReadRecords  int64
	WriteRecords int64
}

type CheckpointMetrics struct {
	Count       int64
	DurationMin int
	DurationMax int
	DurationAvg int
	SizeMin     int64
	SizeMax     int64
	SizeAvg     int64
}

type checkpoint struct {
	Count    int64
	Duration int
	Size     int64
}

func (j *Job) GetMetrics(flinkJobManagerUrl string) (ReadWriteMertics, CheckpointMetrics) {
	jobs := j.getJobs(flinkJobManagerUrl)
	readWrite := j.getReadWrite(flinkJobManagerUrl, jobs)
	checkpoint := j.getCheckpoints(flinkJobManagerUrl, jobs)
	return readWrite, checkpoint
}

func (j *Job) getJobs(flinkJobManagerUrl string) []string {
	url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs"
	httpClient := HttpClient{}
	jsonStr, err := httpClient.Get(url)
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

	// jobs
	var jobs []string
	jobs, err = js.Get("jobs-running").StringArray()
	if err != nil {
		log.Errorf("js.Get 'jobs-running' = %v", err)
		return []string{}
	}
	log.Debugf("jobs = %v", jobs)

	return jobs
}

func (j *Job) getReadWrite(flinkJobManagerUrl string, jobs []string) ReadWriteMertics {
	readWrite := ReadWriteMertics{}
	for _, job := range jobs {
		url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job
		httpClient := HttpClient{}
		jsonStr, err := httpClient.Get(url)
		if err != nil {
			log.Errorf("HttpClient.Get = %v", err)
			return ReadWriteMertics{}
		}

		// parse
		js, err := simpleJson.NewJson([]byte(jsonStr))
		if err != nil {
			log.Errorf("simpleJson.NewJson = %v", err)
			return ReadWriteMertics{}
		}

		// vertices
		var vertices []interface{}
		vertices, err = js.Get("vertices").Array()
		if err != nil {
			log.Errorf("js.Get 'vertices' = %v", err)
			return ReadWriteMertics{}
		}
		log.Debugf("vertices = %v", vertices)

		for _, vertice := range vertices {
			v, _ := vertice.(map[string]interface{})
			log.Debugf("metrics = %v", v["metrics"])

			// only start with 'Source'
			if strings.HasPrefix(fmt.Sprint(v["name"]), "Source") {
				metrics, _ := v["metrics"].(map[string]interface{})
				record := ReadWriteMertics{}
				record.ReadBytes, _ = strconv.ParseInt(fmt.Sprint(metrics["read-bytes"]), 10, 64)
				record.WriteBytes, _ = strconv.ParseInt(fmt.Sprint(metrics["write-bytes"]), 10, 64)
				record.ReadRecords, _ = strconv.ParseInt(fmt.Sprint(metrics["read-records"]), 10, 64)
				record.WriteRecords, _ = strconv.ParseInt(fmt.Sprint(metrics["write-records"]), 10, 64)

				readWrite.ReadBytes += record.ReadBytes
				readWrite.ReadRecords += record.ReadRecords
				readWrite.WriteBytes += record.WriteBytes
				readWrite.WriteRecords += record.WriteRecords
			}
		}
	}

	log.Debugf("readWrite = %v", readWrite)

	return readWrite
}

func (j *Job) getCheckpoints(flinkJobManagerUrl string, jobs []string) CheckpointMetrics {
	checkpoints := []checkpoint{}
	httpClient := HttpClient{}
	for _, job := range jobs {
		url := strings.Trim(flinkJobManagerUrl, "/") + "/jobs/" + job + "/checkpoints"
		jsonStr, err := httpClient.Get(url)
		if err != nil {
			log.Errorf("HttpClient.Get = %v", err)
			return CheckpointMetrics{}
		}

		// not exists checkpoint info
		if jsonStr == "{}" {
			continue
		}

		// parse
		js, err := simpleJson.NewJson([]byte(jsonStr))
		if err != nil {
			log.Errorf("simpleJson.NewJson = %v", err)
			return CheckpointMetrics{}
		}

		checkpoint := checkpoint{
			Count:    -1,
			Duration: -1,
			Size:     -1,
		}

		// count
		checkpoint.Count, err = js.Get("count").Int64()
		if err != nil {
			log.Errorf("js.Get 'count' = %v", err)
			return CheckpointMetrics{}
		}
		log.Debugf("count = %v", checkpoint.Count)

		// history
		var histories []interface{}
		histories, err = js.Get("history").Array()
		if err != nil {
			log.Errorf("js.Get 'history' = %v", err)
			return CheckpointMetrics{}
		}
		log.Debugf("history = %v", histories)

		if len(histories) > 0 {
			history, _ := histories[0].(map[string]interface{})
			checkpoint.Duration, _ = strconv.Atoi(fmt.Sprint(history["duration"]))
			checkpoint.Size, _ = strconv.ParseInt(fmt.Sprint(history["size"]), 10, 64)

			log.Debugf("checkpoint = %v", checkpoint)

			checkpoints = append(checkpoints, checkpoint)
		}
	}

	log.Debugf("checkpoints = %v", checkpoints)

	cp := CheckpointMetrics{
		Count:       -1,
		DurationMin: -1,
		DurationMax: -1,
		DurationAvg: -1,
		SizeMin:     -1,
		SizeMax:     -1,
		SizeAvg:     -1,
	}

	if len(checkpoints) > 0 {
		// avg
		var sumCount int64
		var sumDuration int
		var sumSize int64
		for _, checkpoint := range checkpoints {
			sumCount += checkpoint.Count
			sumDuration += checkpoint.Duration
			sumSize += checkpoint.Size
		}
		log.Debugf("len(checkpoints) = %v", int64(len(checkpoints)))
		log.Debugf("sumCount = %v", sumCount)
		log.Debugf("sumDuration = %v", sumDuration)
		log.Debugf("sumSize = %v", sumSize)

		cp.Count = sumCount
		cp.DurationAvg = sumDuration / len(checkpoints)
		cp.SizeAvg = sumSize / int64(len(checkpoints))

		latest := checkpoints[0]

		// min
		countMin := latest.Count
		durationMin := latest.Duration
		sizeMin := latest.Size
		for _, checkpoint := range checkpoints {
			// smaller?
			if checkpoint.Count < countMin {
				countMin = checkpoint.Count
			}
			if checkpoint.Duration < durationMin {
				durationMin = checkpoint.Duration
			}
			if checkpoint.Size < sizeMin {
				sizeMin = checkpoint.Size
			}
		}
		cp.DurationMin = durationMin
		cp.SizeMin = sizeMin

		// max
		countMax := latest.Count
		durationMax := latest.Duration
		sizeMax := latest.Size
		for _, checkpoint := range checkpoints {
			// bigger?
			if checkpoint.Count > countMax {
				countMax = checkpoint.Count
			}
			if checkpoint.Duration > durationMax {
				durationMax = checkpoint.Duration
			}
			if checkpoint.Size > sizeMax {
				sizeMax = checkpoint.Size
			}
		}
		cp.DurationMax = durationMax
		cp.SizeMax = sizeMax
	}

	log.Debugf("checkpoint = %v", cp)

	return cp
}

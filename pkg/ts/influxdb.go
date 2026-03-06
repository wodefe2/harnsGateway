package ts

import (
	"encoding/json"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"k8s.io/klog/v2"
	"time"
)

type TsManager struct {
	service http.Service
	client  influxdb2.Client
}

func NewTsManager(influxdbUrl, influxdbToken string) *TsManager {
	service := http.NewService(influxdbUrl, influxdbToken, http.DefaultOptions())
	client := influxdb2.NewClientWithOptions(influxdbUrl, influxdbToken, influxdb2.DefaultOptions().SetLogLevel(3))

	s := &TsManager{
		service: service,
		client:  client,
	}
	return s
}

func (s *TsManager) SaveOrUpdateTimeSeries(points []*write.Point) error {
	start := time.Now()
	// end := time.Now().AddDate(0, 0, 5)
	// start := end.AddDate(0, -5, 1)
	//
	// deleteAPI := s.client.DeleteAPI()
	// err := deleteAPI.DeleteWithName(context.Background(), "main", "data-raw", start, end, "_measurement=device_data_electric_meter")
	// if err != nil {
	// 	fmt.Println(err)
	// }

	if len(points) == 0 {
		return nil
	}
	klog.V(2).InfoS("SaveOrUpdateTimeSeries started", "points", len(points))
	if klog.V(5).Enabled() {
		klog.Infof("SaveOrUpdateTimeSeries points:\n%s", pointsForLog(points))
	}

	writeApi := api.NewWriteAPI("main", "data-raw", s.service, write.DefaultOptions().SetBatchSize(uint(len(points))).SetUseGZip(true))
	errCh := writeApi.Errors()
	go func() {
		for err := range errCh {
			klog.V(2).InfoS("Failed to write data into influxdb", "err", err.Error())
		}
	}()
	for _, p := range points {
		writeApi.WritePoint(p)
	}
	closeStart := time.Now()
	klog.V(2).InfoS("SaveOrUpdateTimeSeries closing WriteAPI", "points", len(points))
	writeApi.Close()
	closeElapsed := time.Since(closeStart)
	totalElapsed := time.Since(start)
	if closeElapsed >= 3*time.Second {
		klog.V(1).InfoS("SaveOrUpdateTimeSeries WriteAPI.Close is slow", "points", len(points), "closeElapsedMs", closeElapsed.Milliseconds(), "totalElapsedMs", totalElapsed.Milliseconds())
	} else {
		klog.V(2).InfoS("SaveOrUpdateTimeSeries finished", "points", len(points), "closeElapsedMs", closeElapsed.Milliseconds(), "totalElapsedMs", totalElapsed.Milliseconds())
	}
	return nil
}

type pointForLog struct {
	Index       int                    `json:"index"`
	Measurement string                 `json:"measurement,omitempty"`
	Time        string                 `json:"time,omitempty"`
	Tags        map[string]string      `json:"tags,omitempty"`
	Fields      map[string]interface{} `json:"fields,omitempty"`
}

func pointsForLog(points []*write.Point) string {
	formatted := make([]pointForLog, 0, len(points))
	for i, p := range points {
		item := pointForLog{
			Index: i,
		}
		if p == nil {
			formatted = append(formatted, item)
			continue
		}

		item.Measurement = p.Name()
		if !p.Time().IsZero() {
			item.Time = p.Time().Format(time.RFC3339Nano)
		}

		tags := p.TagList()
		if len(tags) > 0 {
			item.Tags = make(map[string]string, len(tags))
			for _, tag := range tags {
				if tag == nil {
					continue
				}
				item.Tags[tag.Key] = tag.Value
			}
		}

		fields := p.FieldList()
		if len(fields) > 0 {
			item.Fields = make(map[string]interface{}, len(fields))
			for _, field := range fields {
				if field == nil {
					continue
				}
				item.Fields[field.Key] = field.Value
			}
		}

		formatted = append(formatted, item)
	}

	data, err := json.MarshalIndent(formatted, "", "  ")
	if err != nil {
		klog.ErrorS(err, "Failed to format points for log")
		return "[]"
	}
	return string(data)
}

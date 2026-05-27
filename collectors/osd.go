package collectors

import (
	"encoding/json"
	"fmt"
	"log"
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// OSDCollector displays statistics about OSD in the ceph cluster.
type OSDCollector struct {
	conn Conn

	// CommitLatency displays in seconds how long it takes for an operation to be applied to disk
	CommitLatency *prometheus.GaugeVec

	// ApplyLatency displays in seconds how long it takes to get applied to the backing filesystem
	ApplyLatency *prometheus.GaugeVec
}

// NewOSDCollector creates an instance of the OSDCollector
func NewOSDCollector(conn Conn, cluster string) *OSDCollector {
	labels := make(prometheus.Labels)

	return &OSDCollector{
		conn: conn,

		CommitLatency: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_perf_commit_latency_seconds",
				Help:        "OSD Perf Commit Latency",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		ApplyLatency: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_perf_apply_latency_seconds",
				Help:        "OSD Perf Apply Latency",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),
	}
}

func (o *OSDCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.CommitLatency,
		o.ApplyLatency,
	}
}

type cephPerfStat struct {
	PerfInfo []struct {
		ID    json.Number `json:"id"`
		Stats struct {
			CommitLatency json.Number `json:"commit_latency_ms"`
			ApplyLatency  json.Number `json:"apply_latency_ms"`
		} `json:"perf_stats"`
	} `json:"osd_perf_infos"`
}

func (o *OSDCollector) collectOSDPerf() error {
	osdPerfCmd := o.cephOSDPerfCommand()
	buf, _, err := o.conn.MonCommand(osdPerfCmd)
	if err != nil {
		log.Println("[ERROR] Unable to collect data from ceph osd perf", err)
		return err
	}

	osdPerf := &cephPerfStat{}
	if err := json.Unmarshal(buf, osdPerf); err != nil {
		return err
	}

	for _, perfStat := range osdPerf.PerfInfo {
		osdID, err := perfStat.ID.Int64()
		if err != nil {
			return err
		}
		osdName := fmt.Sprintf("osd.%v", osdID)

		commitLatency, err := perfStat.Stats.CommitLatency.Float64()
		if err != nil {
			return err
		}
		o.CommitLatency.WithLabelValues(osdName).Set(commitLatency / 1e3)

		applyLatency, err := perfStat.Stats.ApplyLatency.Float64()
		if err != nil {
			return err
		}
		o.ApplyLatency.WithLabelValues(osdName).Set(applyLatency / 1e3)
	}

	return nil
}

func (o *OSDCollector) cephOSDPerfCommand() []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd perf",
		"format": "json",
	})
	if err != nil {
		panic(err)
	}
	return cmd
}

// Describe sends the descriptors
func (o *OSDCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics
func (o *OSDCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		// 只采集 OSD 延迟指标
		if err := o.collectOSDPerf(); err != nil {
			log.Println("failed collecting osd perf stats:", err)
		}

		// 推送指标
		for _, metric := range o.collectorList() {
			metric.Collect(ch)
		}

		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		log.Println("[WARN] OSD collector timed out after 8s, skipping this scrape")
		return
	}
}

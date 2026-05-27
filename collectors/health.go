package collectors

import (
	"bufio"
	"bytes"
	"encoding/json"
	_ "fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	recoveryIORateRegex    = regexp.MustCompile(`(\d+) (\w{2})/s`)
	recoveryIOKeysRegex    = regexp.MustCompile(`(\d+) keys/s`)
	recoveryIOObjectsRegex = regexp.MustCompile(`(\d+) objects/s`)
	clientIOReadRegex      = regexp.MustCompile(`(\d+) ([kKmMgG][bB])/s rd`)
	clientIOWriteRegex     = regexp.MustCompile(`(\d+) ([kKmMgG][bB])/s wr`)
	clientIOReadOpsRegex   = regexp.MustCompile(`(\d+) op/s rd`)
	clientIOWriteOpsRegex  = regexp.MustCompile(`(\d+) op/s wr`)

	// Older versions of Ceph, hammer (v0.94) and below, support this format.
	clientIOOpsRegex = regexp.MustCompile(`(\d+) op/s[^ \w]*$`)
)

// ClusterHealthCollector collects information about the health of an overall cluster.
type ClusterHealthCollector struct {
	conn Conn

	HealthStatus prometheus.Gauge
	TotalPGs     prometheus.Gauge

	OSDsUp prometheus.Gauge
	OSDsIn prometheus.Gauge
	OSDsNum prometheus.Gauge

	RecoveryIORate prometheus.Gauge
	RecoveryIOKeys prometheus.Gauge
	RecoveryIOObjects prometheus.Gauge

	ClientIORead prometheus.Gauge
	ClientIOWrite prometheus.Gauge
	ClientIOOps prometheus.Gauge
	ClientIOReadOps prometheus.Gauge
	ClientIOWriteOps prometheus.Gauge
}

const (
	CephHealthOK   = "HEALTH_OK"
	CephHealthWarn = "HEALTH_WARN"
	CephHealthErr  = "HEALTH_ERR"
)

func NewClusterHealthCollector(conn Conn, cluster string) *ClusterHealthCollector {
	labels := make(prometheus.Labels)

	return &ClusterHealthCollector{
		conn: conn,

		HealthStatus: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "health_status",
				Help:        "Health status of Cluster, err:2, warn:1, ok:0",
				ConstLabels: labels,
			},
		),
		TotalPGs: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "active_pgs",
				Help:        "Total no. of PGs in the cluster",
				ConstLabels: labels,
			},
		),
		OSDsUp: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osds_up",
				Help:        "Count of OSDs that are in UP state",
				ConstLabels: labels,
			},
		),
		OSDsIn: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osds_in",
				Help:        "Count of OSDs that are in IN state",
				ConstLabels: labels,
			},
		),
		OSDsNum: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osds",
				Help:        "Count of total OSDs in the cluster",
				ConstLabels: labels,
			},
		),
		RecoveryIORate: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "recovery_io_bytes",
				Help:        "Rate of bytes being recovered in cluster per second",
				ConstLabels: labels,
			},
		),
		RecoveryIOKeys: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "recovery_io_keys",
				Help:        "Rate of keys being recovered in cluster per second",
				ConstLabels: labels,
			},
		),
		RecoveryIOObjects: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "recovery_io_objects",
				Help:        "Rate of objects being recovered in cluster per second",
				ConstLabels: labels,
			},
		),
		ClientIORead: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "client_io_read_bytes",
				Help:        "Rate of bytes being read by all clients per second",
				ConstLabels: labels,
			},
		),
		ClientIOWrite: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "client_io_write_bytes",
				Help:        "Rate of bytes being written by all clients per second",
				ConstLabels: labels,
			},
		),
		ClientIOOps: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "client_io_ops",
				Help:        "Total client ops on the cluster measured per second",
				ConstLabels: labels,
			},
		),
		ClientIOReadOps: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "client_io_read_ops",
				Help:        "Client read I/O ops per second",
				ConstLabels: labels,
			},
		),
		ClientIOWriteOps: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "client_io_write_ops",
				Help:        "Client write I/O ops per second",
				ConstLabels: labels,
			},
		),
	}
}

func (c *ClusterHealthCollector) metricsList() []prometheus.Metric {
	return []prometheus.Metric{
		c.HealthStatus,
		c.TotalPGs,
		c.OSDsUp,
		c.OSDsIn,
		c.OSDsNum,
		c.RecoveryIORate,
		c.RecoveryIOKeys,
		c.RecoveryIOObjects,
		c.ClientIORead,
		c.ClientIOWrite,
		c.ClientIOOps,
		c.ClientIOReadOps,
		c.ClientIOWriteOps,
	}
}

type cephHealthStats struct {
	Health struct {
		OverallStatus string `json:"overall_status"`
	} `json:"health"`
	OSDMap struct {
		OSDMap struct {
			NumOSDs        json.Number `json:"num_osds"`
			NumUpOSDs      json.Number `json:"num_up_osds"`
			NumInOSDs      json.Number `json:"num_in_osds"`
		} `json:"osdmap"`
	} `json:"osdmap"`
	PGMap struct {
		NumPGs json.Number `json:"num_pgs"`
	} `json:"pgmap"`
}

func (c *ClusterHealthCollector) collect() error {
	cmd := c.cephJSONUsage()
	buf, _, err := c.conn.MonCommand(cmd)
	if err != nil {
		return err
	}

	stats := &cephHealthStats{}
	if err := json.Unmarshal(buf, stats); err != nil {
		return err
	}

	for _, metric := range c.metricsList() {
		if gauge, ok := metric.(prometheus.Gauge); ok {
			gauge.Set(0)
		}
	}

	switch stats.Health.OverallStatus {
	case CephHealthOK:
		c.HealthStatus.Set(0)
	case CephHealthWarn:
		c.HealthStatus.Set(1)
	case CephHealthErr:
		c.HealthStatus.Set(2)
	default:
		c.HealthStatus.Set(2)
	}

	osdsUp, err := stats.OSDMap.OSDMap.NumUpOSDs.Float64()
	if err == nil {
		c.OSDsUp.Set(osdsUp)
	}

	osdsIn, err := stats.OSDMap.OSDMap.NumInOSDs.Float64()
	if err == nil {
		c.OSDsIn.Set(osdsIn)
	}

	osdsNum, err := stats.OSDMap.OSDMap.NumOSDs.Float64()
	if err == nil {
		c.OSDsNum.Set(osdsNum)
	}

	totalPGs, err := stats.PGMap.NumPGs.Float64()
	if err == nil {
		c.TotalPGs.Set(totalPGs)
	}

	return nil
}

type format string

const (
	jsonFormat  format = "json"
	plainFormat format = "plain"
)

func (c *ClusterHealthCollector) cephPlainUsage() []byte {
	return c.cephUsageCommand(plainFormat)
}

func (c *ClusterHealthCollector) cephJSONUsage() []byte {
	return c.cephUsageCommand(jsonFormat)
}

func (c *ClusterHealthCollector) cephUsageCommand(f format) []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "status",
		"format": f,
	})
	if err != nil {
		panic(err)
	}
	return cmd
}

func (c *ClusterHealthCollector) collectRecoveryClientIO() error {
	cmd := c.cephPlainUsage()
	buf, _, err := c.conn.MonCommand(cmd)
	if err != nil {
		return err
	}

	sc := bufio.NewScanner(bytes.NewReader(buf))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())

		switch {
		case strings.HasPrefix(line, "recovery io"):
			fallthrough
		case strings.HasPrefix(line, "recovery:"):
			if err := c.collectRecoveryIO(line); err != nil {
				return err
			}
		case strings.HasPrefix(line, "client io"):
			fallthrough
		case strings.HasPrefix(line, "client:"):
			if err := c.collectClientIO(line); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *ClusterHealthCollector) collectClientIO(clientStr string) error {
	matched := clientIOReadRegex.FindStringSubmatch(clientStr)
	if len(matched) == 3 {
		v, err := strconv.Atoi(matched[1])
		if err == nil {
			switch strings.ToLower(matched[2]) {
			case "gb":
				v *= 1e9
			case "mb":
				v *= 1e6
			case "kb":
				v *= 1e3
			}
			c.ClientIORead.Set(float64(v))
		}
	}

	matched = clientIOWriteRegex.FindStringSubmatch(clientStr)
	if len(matched) == 3 {
		v, err := strconv.Atoi(matched[1])
		if err == nil {
			switch strings.ToLower(matched[2]) {
			case "gb":
				v *= 1e9
			case "mb":
				v *= 1e6
			case "kb":
				v *= 1e3
			}
			c.ClientIOWrite.Set(float64(v))
		}
	}

	var clientIOOps float64
	matched = clientIOOpsRegex.FindStringSubmatch(clientStr)
	if len(matched) == 2 {
		v, _ := strconv.Atoi(matched[1])
		clientIOOps = float64(v)
	}

	var clientIOReadOps, clientIOWriteOps float64
	matched = clientIOReadOpsRegex.FindStringSubmatch(clientStr)
	if len(matched) == 2 {
		v, _ := strconv.Atoi(matched[1])
		clientIOReadOps = float64(v)
		c.ClientIOReadOps.Set(clientIOReadOps)
	}

	matched = clientIOWriteOpsRegex.FindStringSubmatch(clientStr)
	if len(matched) == 2 {
		v, _ := strconv.Atoi(matched[1])
		clientIOWriteOps = float64(v)
		c.ClientIOWriteOps.Set(clientIOWriteOps)
	}

	if clientIOOps == 0 {
		clientIOOps = clientIOReadOps + clientIOWriteOps
	}
	c.ClientIOOps.Set(clientIOOps)

	return nil
}

func (c *ClusterHealthCollector) collectRecoveryIO(recoveryStr string) error {
	matched := recoveryIORateRegex.FindStringSubmatch(recoveryStr)
	if len(matched) == 3 {
		v, err := strconv.Atoi(matched[1])
		if err == nil {
			switch strings.ToLower(matched[2]) {
			case "gb":
				v *= 1e9
			case "mb":
				v *= 1e6
			case "kb":
				v *= 1e3
			}
			c.RecoveryIORate.Set(float64(v))
		}
	}

	matched = recoveryIOKeysRegex.FindStringSubmatch(recoveryStr)
	if len(matched) == 2 {
		v, _ := strconv.Atoi(matched[1])
		c.RecoveryIOKeys.Set(float64(v))
	}

	matched = recoveryIOObjectsRegex.FindStringSubmatch(recoveryStr)
	if len(matched) == 2 {
		v, _ := strconv.Atoi(matched[1])
		c.RecoveryIOObjects.Set(float64(v))
	}
	return nil
}

func (c *ClusterHealthCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metricsList() {
		ch <- metric.Desc()
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
// It requires the caller to handle synchronization.
func (c *ClusterHealthCollector) Collect(ch chan<- prometheus.Metric) {
	// 8 秒超时保护
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		if err := c.collect(); err != nil {
			log.Println("failed collecting cluster health metrics:", err)
		}

		if err := c.collectRecoveryClientIO(); err != nil {
			log.Println("failed collecting cluster recovery/client io:", err)
		}

		for _, metric := range c.metricsList() {
			ch <- metric
		}

		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		log.Println("[WARN] ClusterHealth collector timed out after 8s, skipping this scrape")
		return
	}
}

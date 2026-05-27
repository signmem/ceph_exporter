//   Copyright 2022 DigitalOcean
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package ceph

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	recoveryIORateRegex         = regexp.MustCompile(`(\d+) (\w{2})/s`)
	recoveryIOKeysRegex         = regexp.MustCompile(`(\d+) keys/s`)
	clientReadBytesPerSecRegex  = regexp.MustCompile(`(\d+) ([kKmMgG][bB])/s rd`)
	clientWriteBytesPerSecRegex = regexp.MustCompile(`(\d+) ([kKmMgG][bB])/s wr`)
	clientIOReadOpsRegex        = regexp.MustCompile(`(\d+) op/s rd`)
	clientIOWriteOpsRegex       = regexp.MustCompile(`(\d+) op/s wr`)

	// Older versions of Ceph, hammer (v0.94) and below, support this format.
	clientIOOpsRegex = regexp.MustCompile(`(\d+) op/s[^ \w]*$`)
)

// ClusterHealthCollector collects information about the health of an overall cluster.
// It surfaces changes in the ceph parameters unlike data usage that ClusterUsageCollector
// does.
type ClusterHealthCollector struct {
	conn   Conn
	logger *logrus.Logger

	// healthChecksMap stores warnings and their criticality
	healthChecksMap map[string]int

	// HealthStatus shows the overall health status of a given cluster.
	HealthStatus *prometheus.Desc

	// HealthStatusInterpreter shows the overall health status of a given
	// cluster, with a breakdown of the HEALTH_WARN status into two groups
	// based on criticality.
	HealthStatusInterpreter prometheus.Gauge

	// ActivePGs shows the no. of PGs the cluster is actively serving data
	// from.
	ActivePGs *prometheus.Desc

	// SlowOps depicts no. of total slow ops in the cluster
	SlowOps *prometheus.Desc

	// Objects show the total no. of RADOS objects that are currently allocated
	Objects *prometheus.Desc

	// OSDsUp show the no. of OSDs that are in the UP state and are able to serve requests.
	OSDsUp *prometheus.Desc

	// OSDsIn shows the no. of OSDs that are marked as IN in the cluster.
	OSDsIn *prometheus.Desc

	// OSDsNum shows the no. of total OSDs the cluster has.
	OSDsNum *prometheus.Desc

	// RecoveryIORate shows the i/o rate at which the cluster is performing its ongoing
	// recovery at.
	RecoveryIORate *prometheus.Desc

	// RecoveryIOKeys shows the rate of rados keys recovery.
	RecoveryIOKeys *prometheus.Desc

	// ClientReadBytesPerSec shows the total client read i/o on the cluster.
	ClientReadBytesPerSec *prometheus.Desc

	// ClientWriteBytesPerSec shows the total client write i/o on the cluster.
	ClientWriteBytesPerSec *prometheus.Desc

	// ClientIOOps shows the rate of total operations conducted by all clients on the cluster.
	ClientIOOps *prometheus.Desc

	// ClientIOReadOps shows the rate of total read operations conducted by all clients on the cluster.
	ClientIOReadOps *prometheus.Desc

	// ClientIOWriteOps shows the rate of total write operations conducted by all clients on the cluster.
	ClientIOWriteOps *prometheus.Desc

	// MgrsActive shows the number of active mgrs, can be either 0 or 1.
	MgrsActive *prometheus.Desc

	// MgrsNum shows the total number of mgrs, including standbys.
	MgrsNum *prometheus.Desc
}

const (
	// CephHealthOK denotes the status of ceph cluster when healthy.
	CephHealthOK = "HEALTH_OK"

	// CephHealthWarn denotes the status of ceph cluster when unhealthy but recovering.
	CephHealthWarn = "HEALTH_WARN"

	// CephHealthErr denotes the status of ceph cluster when unhealthy but usually needs
	// manual intervention.
	CephHealthErr = "HEALTH_ERR"
)

// NewClusterHealthCollector creates a new instance of ClusterHealthCollector to collect health
// metrics on.
func NewClusterHealthCollector(exporter *Exporter) *ClusterHealthCollector {
	labels := make(prometheus.Labels)

	collector := &ClusterHealthCollector{
		conn:   exporter.Conn,
		logger: exporter.Logger,

		healthChecksMap: map[string]int{
			"AUTH_BAD_CAPS":                        2,
			"BLUEFS_AVAILABLE_SPACE":               1,
			"BLUEFS_LOW_SPACE":                     1,
			"BLUEFS_SPILLOVER":                     1,
			"BLUESTORE_DISK_SIZE_MISMATCH":         1,
			"BLUESTORE_FRAGMENTATION":              1,
			"BLUESTORE_LEGACY_STATFS":              1,
			"BLUESTORE_NO_COMPRESSION":             1,
			"BLUESTORE_NO_PER_POOL_MAP":            1,
			"CACHE_POOL_NEAR_FULL":                 1,
			"CACHE_POOL_NO_HIT_SET":                1,
			"DEVICE_HEALTH":                        1,
			"DEVICE_HEALTH_IN_USE":                 2,
			"DEVICE_HEALTH_TOOMANY":                2,
			"LARGE_OMAP_OBJECTS":                   1,
			"MANY_OBJECTS_PER_PG":                  1,
			"MGR_DOWN":                             2,
			"MGR_MODULE_DEPENDENCY":                1,
			"MGR_MODULE_ERROR":                     2,
			"MON_CLOCK_SKEW":                       2,
			"MON_DISK_BIG":                         1,
			"MON_DISK_CRIT":                        2,
			"MON_DISK_LOW":                         2,
			"MON_DOWN":                             2,
			"MON_MSGR2_NOT_ENABLED":                2,
			"OBJECT_MISPLACED":                     1,
			"OBJECT_UNFOUND":                       2,
			"OLD_CRUSH_STRAW_CALC_VERSION":         1,
			"OLD_CRUSH_TUNABLES":                   2,
			"OSDMAP_FLAGS":                         1,
			"OSD_BACKFILLFULL":                     2,
			"OSD_CHASSIS_DOWN":                     1,
			"OSD_DATACENTER_DOWN":                  1,
			"OSD_DOWN":                             1,
			"OSD_FLAGS":                            1,
			"OSD_FULL":                             2,
			"OSD_HOST_DOWN":                        1,
			"OSD_NEARFULL":                         2,
			"OSD_NO_DOWN_OUT_INTERVAL":             2,
			"OSD_NO_SORTBITWISE":                   2,
			"OSD_ORPHAN":                           2,
			"OSD_OSD_DOWN":                         1,
			"OSD_OUT_OF_ORDER_FULL":                2,
			"OSD_PDU_DOWN":                         1,
			"OSD_POD_DOWN":                         1,
			"OSD_RACK_DOWN":                        1,
			"OSD_REGION_DOWN":                      1,
			"OSD_ROOM_DOWN":                        1,
			"OSD_ROOT_DOWN":                        1,
			"OSD_ROW_DOWN":                         1,
			"OSD_SCRUB_ERRORS":                     2,
			"PG_AVAILABILITY":                      1,
			"PG_BACKFILL_FULL":                     2,
			"PG_DAMAGED":                           2,
			"PG_DEGRADED":                          1,
			"PG_NOT_DEEP_SCRUBBED":                 1,
			"PG_NOT_SCRUBBED":                      1,
			"PG_RECOVERY_FULL":                     2,
			"PG_SLOW_SNAP_TRIMMING":                1,
			"POOL_APP_NOT_ENABLED":                 2,
			"POOL_FULL":                            2,
			"POOL_NEAR_FULL":                       2,
			"POOL_TARGET_SIZE_BYTES_OVERCOMMITTED": 1,
			"POOL_TARGET_SIZE_RATIO_OVERCOMMITTED": 1,
			"POOL_TOO_FEW_PGS":                     1,
			"POOL_TOO_MANY_PGS":                    1,
			"TELEMETRY_CHANGED":                    1,
			"TOO_FEW_OSDS":                         1,
			"TOO_FEW_PGS":                          1,
			"TOO_MANY_PGS":                         1,
		},

		HealthStatus: prometheus.NewDesc(fmt.Sprintf("%s_health_status", cephNamespace), "Health status of Cluster, can vary only between 3 states (err:2, warn:1, ok:0)", nil, labels),
		HealthStatusInterpreter: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "health_status_interp",
				Help:        "Health status of Cluster, can vary only between 4 states (err:3, critical_warn:2, soft_warn:1, ok:0)",
				ConstLabels: labels,
			},
		),
		ActivePGs: prometheus.NewDesc(fmt.Sprintf("%s_pg_active", cephNamespace), "No. of active PGs in the cluster", nil, labels),
		SlowOps:   prometheus.NewDesc(fmt.Sprintf("%s_slow_requests", cephNamespace), "No. of slow requests/slow ops", nil, labels),
		Objects:   prometheus.NewDesc(fmt.Sprintf("%s_cluster_objects", cephNamespace), "No. of rados objects within the cluster", nil, labels),

		OSDsUp:  prometheus.NewDesc(fmt.Sprintf("%s_osd_up", cephNamespace), "Count of OSDs that are in UP state", nil, labels),
		OSDsIn:  prometheus.NewDesc(fmt.Sprintf("%s_osd_in", cephNamespace), "Count of OSDs that are in IN state and available to serve requests", nil, labels),
		OSDsNum: prometheus.NewDesc(fmt.Sprintf("%s_osd_metadata", cephNamespace), "Count of total OSDs in the cluster", nil, labels),

		RecoveryIORate:        prometheus.NewDesc(fmt.Sprintf("%s_osd_recovery_bytes", cephNamespace), "Rate of bytes being recovered in cluster per second", nil, labels),
		RecoveryIOKeys:         prometheus.NewDesc(fmt.Sprintf("%s_recovery_io_keys", cephNamespace), "Rate of keys being recovered in cluster per second", nil, labels),
		ClientReadBytesPerSec:  prometheus.NewDesc(fmt.Sprintf("%s_osd_op_r_out_bytes", cephNamespace), "Rate of bytes being read by all clients per second", nil, labels),
		ClientWriteBytesPerSec: prometheus.NewDesc(fmt.Sprintf("%s_osd_op_w_in_bytes", cephNamespace), "Rate of bytes being written by all clients per second", nil, labels),
		ClientIOOps:            prometheus.NewDesc(fmt.Sprintf("%s_client_io_ops", cephNamespace), "Total client ops on the cluster measured per second", nil, labels),
		ClientIOReadOps:        prometheus.NewDesc(fmt.Sprintf("%s_osd_op_r", cephNamespace), "Total client read I/O ops on the cluster measured per second", nil, labels),
		ClientIOWriteOps:       prometheus.NewDesc(fmt.Sprintf("%s_osd_op_w", cephNamespace), "Total client write I/O ops on the cluster measured per second", nil, labels),

		MgrsActive: prometheus.NewDesc(fmt.Sprintf("%s_mgrs_active", cephNamespace), "Count of active mgrs, can be either 0 or 1", nil, labels),
		MgrsNum:    prometheus.NewDesc(fmt.Sprintf("%s_mgr_metadata", cephNamespace), "Total number of mgrs, including standbys", nil, labels),
	}

	return collector
}

// collectorsList represents legacy gauges before the migration to constmetrics
func (c *ClusterHealthCollector) collectorsList() []prometheus.Collector {
	return []prometheus.Collector{
		c.HealthStatusInterpreter,
	}
}

func (c *ClusterHealthCollector) descriptorList() []*prometheus.Desc {
	return []*prometheus.Desc{
		c.HealthStatus,
		c.HealthStatusInterpreter.Desc(),
		c.ActivePGs,
		c.SlowOps,
		c.Objects,
		c.OSDsUp,
		c.OSDsIn,
		c.OSDsNum,
		c.RecoveryIORate,
		c.RecoveryIOKeys,
		c.ClientReadBytesPerSec,
		c.ClientWriteBytesPerSec,
		c.ClientIOOps,
		c.ClientIOReadOps,
		c.ClientIOWriteOps,
		c.MgrsActive,
		c.MgrsNum,
	}
}

type osdMap struct {
	NumOSDs   float64 `json:"num_osds"`
	NumUpOSDs float64 `json:"num_up_osds"`
	NumInOSDs float64 `json:"num_in_osds"`
}

type cephHealthStats struct {
	Health struct {
		Summary []struct {
			Severity string `json:"severity"`
			Summary  string `json:"summary"`
		} `json:"summary"`
		Status string `json:"status"`
		Checks map[string]struct {
			Severity string `json:"severity"`
			Summary  struct {
				Message string `json:"message"`
			} `json:"summary"`
		} `json:"checks"`
	} `json:"health"`
	OSDMap map[string]interface{} `json:"osdmap"`
	PGMap  struct {
		NumPGs                  float64 `json:"num_pgs"`
		TotalObjects            float64 `json:"num_objects"`
		WriteOpPerSec           float64 `json:"write_op_per_sec"`
		ReadOpPerSec            float64 `json:"read_op_per_sec"`
		WriteBytePerSec         float64 `json:"write_bytes_sec"`
		ReadBytePerSec          float64 `json:"read_bytes_sec"`
		RecoveringBytePerSec    float64 `json:"recovering_bytes_per_sec"`
		RecoveringKeysPerSec    float64 `json:"recovering_keys_per_sec"`
		PGsByState              []struct {
			Count  float64 `json:"count"`
			States string  `json:"state_name"`
		} `json:"pgs_by_state"`
	} `json:"pgmap"`
	MgrMap struct {
		Available   bool `json:"available"`
		NumStandBys int  `json:"num_standbys"`
		ActiveName  string `json:"active_name"`
		StandBys    []struct {
			Name string `json:"name"`
		} `json:"standbys"`
	} `json:"mgrmap"`
}

func (c *ClusterHealthCollector) collect(ch chan<- prometheus.Metric, version *Version) error {
	cmd := c.cephUsageCommand(jsonFormat)
	buf, _, err := c.conn.MonCommand(cmd)
	if err != nil {
		c.logger.WithError(err).WithField(
			"args", string(cmd),
		).Error("error executing mon command")
		return err
	}

	stats := &cephHealthStats{}
	if err := json.Unmarshal(buf, stats); err != nil {
		return err
	}

	switch stats.Health.Status {
	case CephHealthOK:
		ch <- prometheus.MustNewConstMetric(c.HealthStatus, prometheus.GaugeValue, float64(0))
		c.HealthStatusInterpreter.Set(float64(0))
	case CephHealthWarn:
		ch <- prometheus.MustNewConstMetric(c.HealthStatus, prometheus.GaugeValue, float64(1))
		c.HealthStatusInterpreter.Set(float64(2))
	case CephHealthErr:
		ch <- prometheus.MustNewConstMetric(c.HealthStatus, prometheus.GaugeValue, float64(2))
		c.HealthStatusInterpreter.Set(float64(3))
	}

	var slowOpsRegexNautilus = regexp.MustCompile(`([\d]+) slow ops, oldest one blocked for ([\d]+) sec`)
	var mapEmpty = len(c.healthChecksMap) == 0

	for _, s := range stats.Health.Summary {
		matched := slowOpsRegexNautilus.FindStringSubmatch(s.Summary)
		if len(matched) == 3 {
			v, err := strconv.Atoi(matched[1])
			if err != nil {
				return err
			}
			ch <- prometheus.MustNewConstMetric(c.SlowOps, prometheus.GaugeValue, float64(v))
		}
	}

	for k, check := range stats.Health.Checks {
		if k == "SLOW_OPS" {
			matched := slowOpsRegexNautilus.FindStringSubmatch(check.Summary.Message)
			if len(matched) == 3 {
				v, err := strconv.Atoi(matched[1])
				if err != nil {
					return err
				}
				ch <- prometheus.MustNewConstMetric(c.SlowOps, prometheus.GaugeValue, float64(v))
			}
		}

		if !mapEmpty {
			if val, present := c.healthChecksMap[k]; present {
				c.HealthStatusInterpreter.Set(float64(val))
			}
		}
	}

	var activePGs float64
	for _, p := range stats.PGMap.PGsByState {
		stateArray := strings.Split(p.States, "+")
		for _, state := range stateArray {
			if state == "active" {
				activePGs += p.Count
			}
		}
	}
	ch <- prometheus.MustNewConstMetric(c.ActivePGs, prometheus.GaugeValue, activePGs)

	ch <- prometheus.MustNewConstMetric(c.ClientReadBytesPerSec, prometheus.GaugeValue, stats.PGMap.ReadBytePerSec)
	ch <- prometheus.MustNewConstMetric(c.ClientWriteBytesPerSec, prometheus.GaugeValue, stats.PGMap.WriteBytePerSec)
	ch <- prometheus.MustNewConstMetric(c.ClientIOOps, prometheus.GaugeValue, stats.PGMap.ReadOpPerSec+stats.PGMap.WriteOpPerSec)
	ch <- prometheus.MustNewConstMetric(c.ClientIOReadOps, prometheus.GaugeValue, stats.PGMap.ReadOpPerSec)
	ch <- prometheus.MustNewConstMetric(c.ClientIOWriteOps, prometheus.GaugeValue, stats.PGMap.WriteOpPerSec)
	ch <- prometheus.MustNewConstMetric(c.RecoveryIOKeys, prometheus.GaugeValue, stats.PGMap.RecoveringKeysPerSec)
	ch <- prometheus.MustNewConstMetric(c.RecoveryIORate, prometheus.GaugeValue, stats.PGMap.RecoveringBytePerSec)

	var actualOsdMap osdMap
	if version.IsAtLeast(Octopus) {
		if stats.OSDMap != nil {
			actualOsdMap = osdMap{
				NumOSDs:   stats.OSDMap["num_osds"].(float64),
				NumUpOSDs: stats.OSDMap["num_up_osds"].(float64),
				NumInOSDs: stats.OSDMap["num_in_osds"].(float64),
			}
		}
	} else {
		if stats.OSDMap != nil {
			innerMap := stats.OSDMap["osdmap"].(map[string]interface{})
			actualOsdMap = osdMap{
				NumOSDs:   innerMap["num_osds"].(float64),
				NumUpOSDs: innerMap["num_up_osds"].(float64),
				NumInOSDs: innerMap["num_in_osds"].(float64),
			}
		}
	}

	ch <- prometheus.MustNewConstMetric(c.OSDsUp, prometheus.GaugeValue, actualOsdMap.NumUpOSDs)
	ch <- prometheus.MustNewConstMetric(c.OSDsIn, prometheus.GaugeValue, actualOsdMap.NumInOSDs)
	ch <- prometheus.MustNewConstMetric(c.OSDsNum, prometheus.GaugeValue, actualOsdMap.NumOSDs)

	ch <- prometheus.MustNewConstMetric(c.Objects, prometheus.GaugeValue, stats.PGMap.TotalObjects)

	activeMgr := 0
	standByMgrs := 0
	if version.IsAtLeast(Octopus) {
		if stats.MgrMap.Available {
			activeMgr = 1
		}
		standByMgrs = stats.MgrMap.NumStandBys
	} else {
		if len(stats.MgrMap.ActiveName) > 0 {
			activeMgr = 1
		}
		standByMgrs = len(stats.MgrMap.StandBys)
	}

	ch <- prometheus.MustNewConstMetric(c.MgrsActive, prometheus.GaugeValue, float64(activeMgr))
	ch <- prometheus.MustNewConstMetric(c.MgrsNum, prometheus.GaugeValue, float64(activeMgr+standByMgrs))

	return nil
}

type format string

const (
	jsonFormat  format = "json"
	plainFormat format = "plain"
)

func (c *ClusterHealthCollector) cephUsageCommand(f format) []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "status",
		"format": f,
	})
	if err != nil {
		c.logger.WithError(err).Panic("error marshalling ceph status")
	}
	return cmd
}

func (c *ClusterHealthCollector) collectRecoveryClientIO(ch chan<- prometheus.Metric) error {
	cmd := c.cephUsageCommand(plainFormat)
	buf, _, err := c.conn.MonCommand(cmd)
	if err != nil {
		c.logger.WithError(err).WithField(
			"args", string(cmd),
		).Error("error executing mon command")
		return err
	}

	sc := bufio.NewScanner(bytes.NewReader(buf))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "cluster:" {
			return nil
		}

		switch {
		case strings.HasPrefix(line, "recovery io"):
			if err := c.collectRecoveryIO(line, ch); err != nil {
				return err
			}
		case strings.HasPrefix(line, "recovery:"):
			if err := c.collectRecoveryIO(line, ch); err != nil {
				return err
			}
		case strings.HasPrefix(line, "client io"):
			if err := c.collectClientIO(line, ch); err != nil {
				return err
			}
		case strings.HasPrefix(line, "client:"):
			if err := c.collectClientIO(line, ch); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *ClusterHealthCollector) collectClientIO(clientStr string, ch chan<- prometheus.Metric) error {
	matched := clientReadBytesPerSecRegex.FindStringSubmatch(clientStr)
	if len(matched) == 3 {
		v, err := strconv.Atoi(matched[1])
		if err != nil {
			return err
		}

		switch strings.ToLower(matched[2]) {
		case "gb":
			v = v * 1e9
		case "mb":
			v = v * 1e6
		case "kb":
			v = v * 1e3
		}
		ch <- prometheus.MustNewConstMetric(c.ClientReadBytesPerSec, prometheus.GaugeValue, float64(v))
	}

	matched = clientWriteBytesPerSecRegex.FindStringSubmatch(clientStr)
	if len(matched) == 3 {
		v, err := strconv.Atoi(matched[1])
		if err != nil {
			return err
		}

		switch strings.ToLower(matched[2]) {
		case "gb":
			v = v * 1e9
		case "mb":
			v = v * 1e6
		case "kb":
			v = v * 1e3
		}
		ch <- prometheus.MustNewConstMetric(c.ClientWriteBytesPerSec, prometheus.GaugeValue, float64(v))
	}

	var clientIOOps float64
	matched = clientIOOpsRegex.FindStringSubmatch(clientStr)
	if len(matched) == 2 {
		v, err := strconv.Atoi(matched[1])
		if err != nil {
			return err
		}
		clientIOOps = float64(v)
	}

	var ClientIOReadOps, ClientIOWriteOps float64
	matched = clientIOReadOpsRegex.FindStringSubmatch(clientStr)
	if len(matched) == 2 {
		v, err := strconv.Atoi(matched[1])
		if err != nil {
			return err
		}
		ClientIOReadOps = float64(v)
		ch <- prometheus.MustNewConstMetric(c.ClientIOReadOps, prometheus.GaugeValue, ClientIOReadOps)
	}

	matched = clientIOWriteOpsRegex.FindStringSubmatch(clientStr)
	if len(matched) == 2 {
		v, err := strconv.Atoi(matched[1])
		if err != nil {
			return err
		}
		ClientIOWriteOps = float64(v)
		ch <- prometheus.MustNewConstMetric(c.ClientIOWriteOps, prometheus.GaugeValue, ClientIOWriteOps)
	}

	if clientIOOps == 0 {
		clientIOOps = ClientIOReadOps + ClientIOWriteOps
	}
	ch <- prometheus.MustNewConstMetric(c.ClientIOOps, prometheus.GaugeValue, clientIOOps)

	return nil
}

func (c *ClusterHealthCollector) collectRecoveryIO(recoveryStr string, ch chan<- prometheus.Metric) error {
	matched := recoveryIORateRegex.FindStringSubmatch(recoveryStr)
	if len(matched) == 3 {
		v, err := strconv.Atoi(matched[1])
		if err != nil {
			return err
		}

		switch strings.ToLower(matched[2]) {
		case "gb":
			v = v * 1e9
		case "mb":
			v = v * 1e6
		case "kb":
			v = v * 1e3
		}
		ch <- prometheus.MustNewConstMetric(c.RecoveryIORate, prometheus.GaugeValue, float64(v))
	}

	matched = recoveryIOKeysRegex.FindStringSubmatch(recoveryStr)
	if len(matched) == 2 {
		v, err := strconv.Atoi(matched[1])
		if err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(c.RecoveryIOKeys, prometheus.GaugeValue, float64(v))
	}
	return nil
}

// Describe sends all the descriptions of individual metrics of ClusterHealthCollector
// to the provided prometheus channel.
func (c *ClusterHealthCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.descriptorList() {
		ch <- metric
	}
}

func (c *ClusterHealthCollector) Collect(ch chan<- prometheus.Metric, version *Version) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			c.logger.Debug("collecting cluster health metrics")
			if err := c.collect(ch, version); err != nil {
				c.logger.WithError(err).Error("error collecting cluster health metrics")
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			c.logger.Debug("collecting cluster recovery/client I/O metrics")
			if err := c.collectRecoveryClientIO(ch); err != nil {
				c.logger.WithError(err).Error("error collecting cluster recovery/client I/O metrics")
			}
		}()

		wg.Wait()
		for _, metric := range c.collectorsList() {
			metric.Collect(ch)
		}

		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		c.logger.Warn("⚠️ ClusterHealth collector timed out after 8s, skipping this scrape")
		return
	}
}

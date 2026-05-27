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
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"context"
	_ "strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	osdLabelFormat = "osd.%v"
)

// OSDCollector displays statistics about OSD in the Ceph cluster.
type OSDCollector struct {
	conn   Conn
	logger *logrus.Logger

	// osdLabelsCache holds a cache of osd labels
	osdLabelsCache map[int64]*cephOSDLabel

	// CommitLatency displays in seconds how long it takes for an operation to be applied to disk
	CommitLatency *prometheus.GaugeVec

	// ApplyLatency displays in seconds how long it takes to get applied to the backing filesystem
	ApplyLatency *prometheus.GaugeVec

	// OSDDownDesc displays OSDs present in the cluster in "down" state
	OSDDownDesc *prometheus.Desc

	// PGObjectsRecoveredDesc displays total number of objects recovered in a PG
	PGObjectsRecoveredDesc *prometheus.Desc
}

// NewOSDCollector creates an instance of the OSDCollector
func NewOSDCollector(exporter *Exporter) *OSDCollector {
	labels := make(prometheus.Labels)
	osdLabels := []string{"ceph_daemon"}

	o := &OSDCollector{
		conn:   exporter.Conn,
		logger: exporter.Logger,

		osdLabelsCache: make(map[int64]*cephOSDLabel),

		CommitLatency: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_commit_latency_ms",
				Help:        "OSD Perf Commit Latency",
				ConstLabels: labels,
			},
			osdLabels,
		),

		ApplyLatency: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_apply_latency_ms",
				Help:        "OSD Perf Apply Latency",
				ConstLabels: labels,
			},
			osdLabels,
		),

		OSDDownDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_osd_down", cephNamespace),
			"Number of OSDs down in the cluster",
			append([]string{"status"}, osdLabels...),
			labels,
		),

		PGObjectsRecoveredDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_pg_objects_recovered", cephNamespace),
			"Number of objects recovered in a PG",
			[]string{"pgid"},
			labels,
		),
	}

	return o
}

func (o *OSDCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.CommitLatency,
		o.ApplyLatency,
	}
}

type cephOSDDF struct {
	OSDNodes []struct {
		Name string `json:"name"`
	} `json:"nodes"`

	Summary struct {
		TotalKB      json.Number `json:"total_kb"`
		TotalUsedKB  json.Number `json:"total_kb_used"`
		TotalAvailKB json.Number `json:"total_kb_avail"`
		AverageUtil  json.Number `json:"average_utilization"`
	} `json:"summary"`
}

type CephOSDPerfStat struct {
	PerfInfo []struct {
		ID    json.Number `json:"id"`
		Stats struct {
			CommitLatency json.Number `json:"commit_latency_ms"`
			ApplyLatency  json.Number `json:"apply_latency_ms"`
		} `json:"perf_stats"`
	} `json:"osd_perf_infos"`
}

type cephOSDDump struct {
	OSDs []struct {
		OSD   json.Number `json:"osd"`
		Up    json.Number `json:"up"`
		In    json.Number `json:"in"`
		State []string    `json:"state"`
	} `json:"osds"`

	PgUpmapItems []struct {
		PgID     string `json:"pgid"`
		Mappings []struct {
			From int `json:"from"`
			To   int `json:"to"`
		} `json:"mappings"`
	} `json:"pg_upmap_items"`

	FullRatio         json.Number `json:"full_ratio"`
	NearFullRatio     json.Number `json:"nearfull_ratio"`
	BackfillFullRatio json.Number `json:"backfillfull_ratio"`
}

type cephOSDTree struct {
	Nodes []struct {
		ID          int64   `json:"id"`
		Name        string  `json:"name"`
		Type        string  `json:"type"`
		Status      string  `json:"status"`
		Class       string  `json:"device_class"`
		CrushWeight float64 `json:"crush_weight"`
		Children    []int64 `json:"children"`
	} `json:"nodes"`
	Stray []struct {
		ID          int64   `json:"id"`
		Name        string  `json:"name"`
		Type        string  `json:"type"`
		Status      string  `json:"status"`
		CrushWeight float64 `json:"crush_weight"`
		Children    []int   `json:"children"`
	} `json:"stray"`
}

type osdNode struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Status string `json:"status"`
}

type cephOSDTreeDown struct {
	Nodes []osdNode `json:"nodes"`
	Stray []osdNode `json:"stray"`
}

type cephPGDumpBrief struct {
	PGStats []struct {
		PGID          string `json:"pgid"`
		ActingPrimary int64  `json:"acting_primary"`
		Acting        []int  `json:"acting"`
		State         string `json:"state"`
	} `json:"pg_stats"`
}

type cephOSDLabel struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Status      string  `json:"status"`
	DeviceClass string  `json:"device_class"`
	CrushWeight float64 `json:"crush_weight"`
	Root        string  `json:"root"`
	Rack        string  `json:"rack"`
	Host        string  `json:"host"`
	parent      int64   // parent id when building tables
}

func (o *OSDCollector) collectOSDDF() error {
	args := o.cephOSDDFCommand()
	buf, _, err := o.conn.MgrCommand(args)
	if err != nil {
		o.logger.WithError(err).WithField(
			"args", string(bytes.Join(args, []byte(","))),
		).Error("error executing mgr command")
		return err
	}

	buf = bytes.Replace(buf, []byte("-nan"), []byte("0"), -1)
	osdDF := &cephOSDDF{}
	if err := json.Unmarshal(buf, osdDF); err != nil {
		return err
	}

	return nil
}

func (o *OSDCollector) collectOSDPerf() error {
	o.logger.Debug("osd perf r")

	args := o.cephOSDPerfCommand()
	buf, _, err := o.conn.MgrCommand(args)
	if err != nil {
		o.logger.WithError(err).WithField(
			"args", string(bytes.Join(args, []byte(","))),
		).Error("error executing mon command")
		return err
	}

	o.logger.Debugf("osd perf raw output: %s", string(buf))
	osdPerf := &CephOSDPerfStat{}
	if err := json.Unmarshal(buf, osdPerf); err != nil {
		return err
	}

	for _, perfStat := range osdPerf.PerfInfo {
		osdID, err := perfStat.ID.Int64()
		if err != nil {
			return err
		}
		osdName := fmt.Sprintf(osdLabelFormat, osdID)

		commitLatency, err := perfStat.Stats.CommitLatency.Float64()
		if err != nil {
			return err
		}
		o.CommitLatency.WithLabelValues(osdName).Set(commitLatency)

		applyLatency, err := perfStat.Stats.ApplyLatency.Float64()
		if err != nil {
			return err
		}
		o.ApplyLatency.WithLabelValues(osdName).Set(applyLatency)
	}

	return nil
}

func buildOSDLabels(data []byte) (map[int64]*cephOSDLabel, error) {
	nodeList := &cephOSDTree{}
	if err := json.Unmarshal(data, nodeList); err != nil {
		return nil, err
	}

	nodeMap := make(map[int64]*cephOSDLabel)
	for _, node := range nodeList.Nodes {
		label := cephOSDLabel{
			ID:          node.ID,
			Name:        node.Name,
			Type:        node.Type,
			Status:      node.Status,
			DeviceClass: node.Class,
			CrushWeight: node.CrushWeight,
			parent:      math.MaxInt64,
		}
		nodeMap[node.ID] = &label
	}

	for _, node := range nodeList.Nodes {
		for _, child := range node.Children {
			if label, ok := nodeMap[child]; ok {
				label.parent = node.ID
			}
		}
	}

	var findParent func(from *cephOSDLabel, kind string) (*cephOSDLabel, bool)
	findParent = func(from *cephOSDLabel, kind string) (*cephOSDLabel, bool) {
		if parent, ok := nodeMap[from.parent]; ok {
			if parent.Type == kind {
				return parent, true
			}
			return findParent(parent, kind)
		}
		return nil, false
	}

	for k := range nodeMap {
		osdLabel := nodeMap[k]
		if host, ok := findParent(osdLabel, "host"); ok {
			osdLabel.Host = host.Name
		}
		if rack, ok := findParent(osdLabel, "rack"); ok {
			osdLabel.Rack = rack.Name
		}
		if root, ok := findParent(osdLabel, "root"); ok {
			osdLabel.Root = root.Name
		}
	}

	for k := range nodeMap {
		osdLabel := nodeMap[k]
		if osdLabel.Type != "osd" {
			delete(nodeMap, k)
		}
	}
	return nodeMap, nil
}

func (o *OSDCollector) buildOSDLabelCache() error {
	cmd := o.cephOSDTreeCommand()
	data, _, err := o.conn.MonCommand(cmd)
	if err != nil {
		o.logger.WithError(err).WithField(
			"args", string(cmd),
		).Error("error executing mon command")
		return err
	}

	cache, err := buildOSDLabels(data)
	if err != nil {
		return err
	}
	o.osdLabelsCache = cache
	return nil
}

func (o *OSDCollector) getOSDLabelFromID(id int64) *cephOSDLabel {
	if label, ok := o.osdLabelsCache[id]; ok {
		return label
	}
	return &cephOSDLabel{}
}

func (o *OSDCollector) getOSDLabelFromName(osdid string) *cephOSDLabel {
	var id int64
	c, err := fmt.Sscanf(osdid, "osd.%d", &id)
	if err != nil || c != 1 {
		return &cephOSDLabel{}
	}
	return o.getOSDLabelFromID(id)
}

func (o *OSDCollector) collectOSDTreeDown(ch chan<- prometheus.Metric) error {
	cmd := o.cephOSDTreeCommand("down")
	buff, _, err := o.conn.MonCommand(cmd)
	if err != nil {
		o.logger.WithError(err).WithField(
			"args", string(cmd),
		).Error("error executing mon command")
		return err
	}

	osdDown := &cephOSDTreeDown{}
	if err := json.Unmarshal(buff, osdDown); err != nil {
		return err
	}

	downItems := append(osdDown.Nodes, osdDown.Stray...)
	for _, downItem := range downItems {
		if downItem.Type != "osd" {
			continue
		}
		osdName := downItem.Name
		ch <- prometheus.MustNewConstMetric(o.OSDDownDesc, prometheus.GaugeValue, 1,
			downItem.Status,
			osdName)
	}

	return nil
}

func (o *OSDCollector) collectOSDDump() error {
	cmd := o.cephOSDDump()
	buff, _, err := o.conn.MonCommand(cmd)
	if err != nil {
		o.logger.WithError(err).WithField(
			"args", string(cmd),
		).Error("error executing mon command")
		return err
	}

	osdDump := cephOSDDump{}
	if err := json.Unmarshal(buff, &osdDump); err != nil {
		return err
	}
	return nil
}

func (o *OSDCollector) performPGDumpBrief() (*cephPGDumpBrief, error) {
	args := o.cephPGDumpCommand()
	buf, _, err := o.conn.MgrCommand(args)
	if err != nil {
		o.logger.WithError(err).WithField(
			"args", string(bytes.Join(args, []byte(","))),
		).Error("error executing mgr command")
		return nil, err
	}

	var pgDumpBrief cephPGDumpBrief
	if err := json.Unmarshal(buf, &pgDumpBrief); err == nil {
		return &pgDumpBrief, nil
	}

	var pgStats []struct {
		PGID          string `json:"pgid"`
		ActingPrimary int64  `json:"acting_primary"`
		Acting        []int  `json:"acting"`
		State         string `json:"state"`
	}
	if err := json.Unmarshal(buf, &pgStats); err == nil {
		pgDumpBrief.PGStats = pgStats
		return &pgDumpBrief, nil
	}

	return &cephPGDumpBrief{PGStats: []struct {
		PGID          string `json:"pgid"`
		ActingPrimary int64  `json:"acting_primary"`
		Acting        []int  `json:"acting"`
		State         string `json:"state"`
	}{}}, nil
}

func (o *OSDCollector) cephOSDDump() []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd dump",
		"format": jsonFormat,
	})
	if err != nil {
		o.logger.WithError(err).Panic("error marshalling ceph osd dump")
	}
	return cmd
}

func (o *OSDCollector) cephOSDDFCommand() [][]byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd df",
		"format": jsonFormat,
	})
	if err != nil {
		o.logger.WithError(err).Panic("error marshalling ceph osd df")
	}
	return [][]byte{cmd}
}

func (o *OSDCollector) cephOSDPerfCommand() [][]byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd perf",
		"format": jsonFormat,
	})
	if err != nil {
		o.logger.WithError(err).Panic("error marshalling ceph osd perf")
	}
	return [][]byte{cmd}
}

func (o *OSDCollector) cephOSDTreeCommand(states ...string) []byte {
	req := map[string]interface{}{
		"prefix": "osd tree",
		"format": jsonFormat,
	}
	if len(states) > 0 {
		req["states"] = states
	}
	cmd, err := json.Marshal(req)
	if err != nil {
		o.logger.WithError(err).Panic("error marshalling ceph osd tree")
	}
	return cmd
}

func (o *OSDCollector) cephPGDumpCommand() [][]byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix":       "pg dump",
		"dumpcontents": []string{"pgs_brief"},
		"format":       jsonFormat,
	})
	if err != nil {
		o.logger.WithError(err).Panic("error marshalling ceph pg dump")
	}
	return [][]byte{cmd}
}

// Describe sends metric descriptors
func (o *OSDCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	ch <- o.OSDDownDesc
	ch <- o.PGObjectsRecoveredDesc
}

// Collect collects metrics
func (o *OSDCollector) Collect(ch chan<- prometheus.Metric, version *Version) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	o.CommitLatency.Reset()
	o.ApplyLatency.Reset()

	done := make(chan struct{})

	go func() {
		o.buildOSDLabelCache()

		localWg := &sync.WaitGroup{}

		localWg.Add(1)
		go func() {
			defer localWg.Done()
			if err := o.collectOSDPerf(); err != nil {
				o.logger.WithError(err).Error("error collecting OSD perf metrics")
			}
		}()

		localWg.Add(1)
		go func() {
			defer localWg.Done()
			if err := o.collectOSDDump(); err != nil {
				o.logger.WithError(err).Error("error collecting OSD dump metrics")
			}
		}()

		localWg.Add(1)
		go func() {
			defer localWg.Done()
			if err := o.collectOSDDF(); err != nil {
				o.logger.WithError(err).Error("error collecting OSD df metrics")
			}
		}()

		localWg.Add(1)
		go func() {
			defer localWg.Done()
			if err := o.collectOSDTreeDown(ch); err != nil {
				o.logger.WithError(err).Error("error collecting OSD tree down metrics")
			}
		}()

		localWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		o.logger.Warn("⚠️ OSD collector timed out after 8s, skipping this scrape")
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}

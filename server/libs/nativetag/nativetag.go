/*
 * Copyright (c) 2025 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nativetag

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/deepflowio/deepflow/server/libs/ckdb"
	"github.com/deepflowio/deepflow/server/libs/logger"
)

var log = logger.MustGetLogger("nativetag")

type NativeTagTable uint8

const (
	APPLICATION_LOG NativeTagTable = iota
	EVENT_EVENT
	EVENT_PERF_EVENT
	L7_FLOW_LOG
	DEEPFLOW_ADMIN
	DEEPFLOW_TENANT
	EXT_METRICS
	PROFILE

	MAX_NATIVE_TAG_TABLE
)

var NativeTagDatabaseNames = [MAX_NATIVE_TAG_TABLE]string{
	APPLICATION_LOG:  "application_log",
	EVENT_EVENT:      "event",
	EVENT_PERF_EVENT: "event",
	L7_FLOW_LOG:      "flow_log",
	DEEPFLOW_ADMIN:   "deepflow_admin",
	DEEPFLOW_TENANT:  "deepflow_tenant",
	EXT_METRICS:      "ext_metrics",
	PROFILE:          "profile",
}

var NativeTagTableNames = [MAX_NATIVE_TAG_TABLE]string{
	APPLICATION_LOG:  "log",
	EVENT_EVENT:      "event",
	EVENT_PERF_EVENT: "perf_event",
	L7_FLOW_LOG:      "l7_flow_log",
	DEEPFLOW_ADMIN:   "deepflow_server",
	DEEPFLOW_TENANT:  "deepflow_collector",
	EXT_METRICS:      "metrics",
	PROFILE:          "in_process",
}

func (table NativeTagTable) Database() string {
	return NativeTagDatabaseNames[table]
}

func (table NativeTagTable) Table() string {
	return NativeTagTableNames[table]
}

func (table NativeTagTable) LocalTable() string {
	return table.Table() + "_local"
}

var NativeTags [ckdb.MAX_ORG_ID + 1][MAX_NATIVE_TAG_TABLE]*NativeTag

type NativeTag struct {
	Version        uint32
	AttributeNames []string
	ColumnNames    []string
	ColumnTypes    []ckdb.ColumnType // ckdb.String, ckdb.Int64, ckdb.Float64
}

func UpdateNativeTag(orgId uint16, table NativeTagTable, nativeTag *NativeTag) {
	oldVersion := uint32(0)
	oldNativeTag := NativeTags[orgId][table]
	if oldNativeTag != nil {
		oldVersion = oldNativeTag.Version

	}
	newNativeTag := *nativeTag
	newNativeTag.Version = oldVersion + 1

	NativeTags[orgId][table] = &newNativeTag
}

func CKAddNativeTag(conn *sql.DB, orgId uint16, table NativeTagTable, nativeTag *NativeTag) error {
	for i, columnName := range nativeTag.ColumnNames {
		sql := fmt.Sprintf("ALTER TABLE %s.`%s` ADD COLUMN %s %s",
			ckdb.OrgDatabasePrefix(orgId)+table.Database(), table.Table(), columnName, nativeTag.ColumnTypes[i])
		log.Infof("add native tag: %s", sql)
		_, err := conn.Exec(sql)
		if err != nil {
			// if it has already been added, you need to skip the error
			if strings.Contains(err.Error(), "column with this name already exists") {
				log.Infof("db: %s, table: %s error: %s", table.Database(), table.Table(), err)
				continue
			}
			return err
		}
	}
	return nil
}

func GetAllNativeTags() [ckdb.MAX_ORG_ID + 1][MAX_NATIVE_TAG_TABLE]*NativeTag {
	return NativeTags
}

func GetNativeTags(orgId uint16, table NativeTagTable) *NativeTag {
	return NativeTags[orgId][table]
}

func GetTableNativeTagsVersion(orgId uint16, table NativeTagTable) uint32 {
	nativeTag := NativeTags[orgId][table]
	if nativeTag == nil {
		return 0
	}
	return nativeTag.Version
}

func GetTableNativeTagsColumnBlock(orgId uint16, table NativeTagTable) *NativeTagsBlock {
	nativeTag := NativeTags[orgId][table]
	if nativeTag == nil {
		return nil
	}
	return nativeTag.NewColumnBlock()
}

type NativeTagsBlock struct {
	TagNames, StringColumnNames []string
	ColTags                     []proto.ColStr

	IntMetricsNames, IntColumnNames []string
	ColIntMetrics                   []proto.ColInt64

	FloatMetricsNames, FloatColumnNames []string
	ColFloatMetrics                     []proto.ColFloat64
}

func (b *NativeTagsBlock) Reset() {
	for i := range b.ColTags {
		b.ColTags[i].Reset()
	}
	for i := range b.ColIntMetrics {
		b.ColIntMetrics[i].Reset()
	}
	for i := range b.ColFloatMetrics {
		b.ColFloatMetrics[i].Reset()
	}
}

func (b *NativeTagsBlock) ToInput(input proto.Input) proto.Input {
	if len(b.TagNames) != len(b.ColTags) ||
		len(b.IntMetricsNames) != len(b.ColIntMetrics) ||
		len(b.FloatMetricsNames) != len(b.ColFloatMetrics) {
		log.Warningf("invalid native block length: %d %d, %d %d, %d %d",
			len(b.TagNames), len(b.ColTags), len(b.IntMetricsNames), len(b.ColIntMetrics), len(b.FloatMetricsNames), len(b.ColFloatMetrics))
		return input
	}
	for i := range b.ColTags {
		input = append(input, proto.InputColumn{Name: b.StringColumnNames[i], Data: &b.ColTags[i]})
	}
	for i := range b.ColIntMetrics {
		input = append(input, proto.InputColumn{Name: b.IntColumnNames[i], Data: &b.ColIntMetrics[i]})
	}
	for i := range b.ColFloatMetrics {
		input = append(input, proto.InputColumn{Name: b.FloatColumnNames[i], Data: &b.ColFloatMetrics[i]})
	}
	return input
}

func IndexOf(slice []string, str string) int {
	for i, v := range slice {
		if v == str {
			return i
		}
	}
	return -1
}

func (b *NativeTagsBlock) AppendToColumnBlock(attributeNames, attributeValues, metricsNames []string, metricsValues []float64) {
	for i, name := range b.TagNames {
		if index := IndexOf(attributeNames, name); index >= 0 {
			b.ColTags[i].Append(attributeValues[index])
		} else {
			b.ColTags[i].Append("")
		}
	}
	for i, name := range b.IntMetricsNames {
		if index := IndexOf(attributeNames, name); index >= 0 {
			valueInt64, _ := strconv.ParseInt(attributeValues[index], 10, 64)
			b.ColIntMetrics[i].Append(valueInt64)
		} else if index := IndexOf(metricsNames, name); index >= 0 {
			valueInt64 := int64(metricsValues[index])
			b.ColIntMetrics[i].Append(valueInt64)
		} else {
			b.ColIntMetrics[i].Append(0)
		}
	}

	for i, name := range b.FloatMetricsNames {
		if index := IndexOf(attributeNames, name); index >= 0 {
			valueFloat64, _ := strconv.ParseFloat(attributeValues[index], 64)
			b.ColFloatMetrics[i].Append(valueFloat64)
		} else if index := IndexOf(metricsNames, name); index >= 0 {
			b.ColFloatMetrics[i].Append(metricsValues[index])
		} else {
			b.ColFloatMetrics[i].Append(0)
		}
	}
}

func (t *NativeTag) NewColumnBlock() *NativeTagsBlock {
	block := &NativeTagsBlock{}
	for i, name := range t.AttributeNames {
		switch t.ColumnTypes[i] {
		case ckdb.String:
			block.TagNames = append(block.TagNames, name)
			block.StringColumnNames = append(block.StringColumnNames, t.ColumnNames[i])
			block.ColTags = append(block.ColTags, proto.ColStr{})
		case ckdb.Int64:
			block.IntMetricsNames = append(block.IntMetricsNames, name)
			block.IntColumnNames = append(block.IntColumnNames, t.ColumnNames[i])
			block.ColIntMetrics = append(block.ColIntMetrics, proto.ColInt64{})
		case ckdb.Float64:
			block.FloatMetricsNames = append(block.FloatMetricsNames, name)
			block.FloatColumnNames = append(block.FloatColumnNames, t.ColumnNames[i])
			block.ColFloatMetrics = append(block.ColFloatMetrics, proto.ColFloat64{})
		}
	}
	return block
}

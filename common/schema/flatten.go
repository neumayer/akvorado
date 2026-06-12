// SPDX-FileCopyrightText: 2026 Free Mobile
// SPDX-License-Identifier: AGPL-3.0-only

package schema

import (
	"encoding/json"
	"net/netip"

	"github.com/ClickHouse/ch-go/proto"
)

// MarshalFlowJSON serializes the current (not-yet-finalized) flow as a flat
// JSON object. Unlike json.Marshal(bf), which only sees the fixed struct
// fields, this also reads the dynamic columns set during decoding (SrcPort,
// DstPort, Bytes, Packets, Proto, …) directly from the ClickHouse batch. It
// must therefore live inside this package, as bf.batch is unexported.
//
// It is meant to be called from the outlet worker before FinalizeAndSend, so
// the fixed fields are still on the struct and the per-flow dynamic columns are
// the last appended value of each set column.
func (bf *FlowMessage) MarshalFlowJSON() ([]byte, error) {
	m := make(map[string]any, 24)

	// Fixed fields (still on the struct at this point).
	m["TimeReceived"] = bf.TimeReceived
	if bf.SamplingRate != 0 {
		m["SamplingRate"] = bf.SamplingRate
	}
	if bf.ExporterAddress.IsValid() {
		m["ExporterAddress"] = bf.ExporterAddress.Unmap().String()
	}
	if bf.SrcAddr.IsValid() {
		m["SrcAddr"] = bf.SrcAddr.Unmap().String()
	}
	if bf.DstAddr.IsValid() {
		m["DstAddr"] = bf.DstAddr.Unmap().String()
	}
	if bf.NextHop.IsValid() {
		m["NextHop"] = bf.NextHop.Unmap().String()
	}
	if bf.InIf != 0 {
		m["InIf"] = bf.InIf
	}
	if bf.OutIf != 0 {
		m["OutIf"] = bf.OutIf
	}
	if bf.SrcVlan != 0 {
		m["SrcVlan"] = bf.SrcVlan
	}
	if bf.DstVlan != 0 {
		m["DstVlan"] = bf.DstVlan
	}
	if bf.SrcAS != 0 {
		m["SrcAS"] = bf.SrcAS
	}
	if bf.DstAS != 0 {
		m["DstAS"] = bf.DstAS
	}

	// Dynamic columns populated during decode/enrich (ports, bytes, …).
	for _, column := range bf.schema.Columns() {
		if column.Disabled || !bf.batch.columnSet.Test(uint(column.Key)) {
			continue
		}
		col := bf.batch.columns[column.Key]
		if col == nil {
			continue
		}
		if v, ok := lastColumnValue(col); ok {
			m[column.Name] = v
		}
	}

	return json.Marshal(m)
}

// lastColumnValue reads the most recently appended value of a ClickHouse
// column. Scalar types only; arrays are skipped (not needed for (IP, port)
// detection). Mirrors the type switches in Undo/appendDefaultValues.
func lastColumnValue(c proto.Column) (any, bool) {
	switch col := c.(type) {
	case *proto.ColUInt64:
		if n := len(*col); n > 0 {
			return (*col)[n-1], true
		}
	case *proto.ColUInt32:
		if n := len(*col); n > 0 {
			return (*col)[n-1], true
		}
	case *proto.ColUInt16:
		if n := len(*col); n > 0 {
			return (*col)[n-1], true
		}
	case *proto.ColUInt8:
		if n := len(*col); n > 0 {
			return (*col)[n-1], true
		}
	case *proto.ColEnum8:
		if n := len(*col); n > 0 {
			return uint8((*col)[n-1]), true
		}
	case *proto.ColIPv6:
		if n := len(*col); n > 0 {
			return netip.AddrFrom16([16]byte((*col)[n-1])).Unmap().String(), true
		}
	case *proto.ColDateTime:
		if n := len(col.Data); n > 0 {
			return uint32(col.Data[n-1]), true
		}
	case *proto.ColLowCardinality[string]:
		if n := len(col.Values); n > 0 {
			return col.Values[n-1], true
		}
	case *proto.ColLowCardinality[proto.IPv6]:
		if n := len(col.Values); n > 0 {
			return netip.AddrFrom16([16]byte(col.Values[n-1])).Unmap().String(), true
		}
	}
	return nil, false
}

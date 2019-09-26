package zerodoc

import (
	"strconv"

	"gitlab.x.lan/yunshan/droplet-libs/app"
	"gitlab.x.lan/yunshan/droplet-libs/codec"
)

type FlowMeter struct {
	SumFlowCount          uint64 `db:"sum_flow_count"`
	SumNewFlowCount       uint64 `db:"sum_new_flow_count"`
	SumClosedFlowCount    uint64 `db:"sum_closed_flow_count"`
	SumPacketTx           uint64 `db:"sum_packet_tx"`
	SumPacketRx           uint64 `db:"sum_packet_rx"`
	SumBitTx              uint64 `db:"sum_bit_tx"`
	SumBitRx              uint64 `db:"sum_bit_rx"`
	SumFlowDuration       uint64 `db:"sum_flow_duration"`        // ms 废弃
	SumClosedFlowDuration uint64 `db:"sum_closed_flow_duration"` // ms 废弃
}

func (m *FlowMeter) SortKey() uint64 {
	return m.SumPacketTx + m.SumPacketRx
}

func (m *FlowMeter) Encode(encoder *codec.SimpleEncoder) {
	encoder.WriteVarintU64(m.SumFlowCount)
	encoder.WriteVarintU64(m.SumNewFlowCount)
	encoder.WriteVarintU64(m.SumClosedFlowCount)
	encoder.WriteVarintU64(m.SumPacketTx)
	encoder.WriteVarintU64(m.SumPacketRx)
	encoder.WriteVarintU64(m.SumBitTx)
	encoder.WriteVarintU64(m.SumBitRx)
	encoder.WriteVarintU64(m.SumFlowDuration)
	encoder.WriteVarintU64(m.SumClosedFlowDuration)
}

func (m *FlowMeter) Decode(decoder *codec.SimpleDecoder) {
	m.SumFlowCount = decoder.ReadVarintU64()
	m.SumNewFlowCount = decoder.ReadVarintU64()
	m.SumClosedFlowCount = decoder.ReadVarintU64()
	m.SumPacketTx = decoder.ReadVarintU64()
	m.SumPacketRx = decoder.ReadVarintU64()
	m.SumBitTx = decoder.ReadVarintU64()
	m.SumBitRx = decoder.ReadVarintU64()
	m.SumFlowDuration = decoder.ReadVarintU64()
	m.SumClosedFlowDuration = decoder.ReadVarintU64()
}

func (m *FlowMeter) ConcurrentMerge(other app.Meter) {
	if pm, ok := other.(*FlowMeter); ok {
		m.SumFlowCount += pm.SumFlowCount
		m.SumNewFlowCount += pm.SumNewFlowCount
		m.SumClosedFlowCount += pm.SumClosedFlowCount
		m.SumPacketTx += pm.SumPacketTx
		m.SumPacketRx += pm.SumPacketRx
		m.SumBitTx += pm.SumBitTx
		m.SumBitRx += pm.SumBitRx
		m.SumFlowDuration += pm.SumFlowDuration
		m.SumClosedFlowDuration += pm.SumClosedFlowDuration
	}
}

func (m *FlowMeter) SequentialMerge(other app.Meter) { // other为后一个时间的统计量
	if pm, ok := other.(*FlowMeter); ok {
		m.SumFlowCount = m.SumClosedFlowCount + pm.SumFlowCount
		m.SumNewFlowCount += pm.SumNewFlowCount
		m.SumClosedFlowCount += pm.SumClosedFlowCount
		m.SumPacketTx += pm.SumPacketTx
		m.SumPacketRx += pm.SumPacketRx
		m.SumBitTx += pm.SumBitTx
		m.SumBitRx += pm.SumBitRx
		m.SumFlowDuration += m.SumClosedFlowDuration + pm.SumFlowDuration
		m.SumClosedFlowDuration += pm.SumClosedFlowDuration
	}
}

func (m *FlowMeter) ToKVString() string {
	buffer := make([]byte, MAX_STRING_LENGTH)
	size := m.MarshalTo(buffer)
	return string(buffer[:size])
}

func (m *FlowMeter) MarshalTo(b []byte) int {
	offset := 0

	offset += copy(b[offset:], "sum_flow_count=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumFlowCount, 10))
	offset += copy(b[offset:], "i,sum_new_flow_count=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumNewFlowCount, 10))
	offset += copy(b[offset:], "i,sum_closed_flow_count=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumClosedFlowCount, 10))
	offset += copy(b[offset:], "i,sum_packet_tx=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumPacketTx, 10))
	offset += copy(b[offset:], "i,sum_packet_rx=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumPacketRx, 10))
	offset += copy(b[offset:], "i,sum_packet=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumPacketTx+m.SumPacketRx, 10))
	offset += copy(b[offset:], "i,sum_bit_tx=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumBitTx, 10))
	offset += copy(b[offset:], "i,sum_bit_rx=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumBitRx, 10))
	offset += copy(b[offset:], "i,sum_bit=")
	offset += copy(b[offset:], strconv.FormatUint(m.SumBitTx+m.SumBitRx, 10))
	b[offset] = 'i'
	offset++

	return offset
}

func (m *FlowMeter) Fill(ids []uint8, values []interface{}) {
	for i, id := range ids {
		if id <= _METER_INVALID_ || id >= _METER_MAX_ID_ || values[i] == nil {
			continue
		}
		switch id {
		case _METER_SUM_FLOW_COUNT:
			m.SumFlowCount = uint64(values[i].(int64))
		case _METER_SUM_NEW_FLOW_COUNT:
			m.SumNewFlowCount = uint64(values[i].(int64))
		case _METER_SUM_CLOSED_FLOW_COUNT:
			m.SumClosedFlowCount = uint64(values[i].(int64))
		case _METER_SUM_PACKET_TX:
			m.SumPacketTx = uint64(values[i].(int64))
		case _METER_SUM_PACKET_RX:
			m.SumPacketRx = uint64(values[i].(int64))
		case _METER_SUM_BIT_TX:
			m.SumBitTx = uint64(values[i].(int64))
		case _METER_SUM_BIT_RX:
			m.SumBitRx = uint64(values[i].(int64))
		default:
			log.Warningf("unsupport meter id=%d", id)
		}
	}
}

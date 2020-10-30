package zerodoc

import (
	"encoding/binary"
	"math"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/google/gopacket/layers"

	"gitlab.x.lan/yunshan/droplet-libs/codec"
	"gitlab.x.lan/yunshan/droplet-libs/datatype"
)

func TestHasEdgeTagField(t *testing.T) {
	c := IPPath
	if !c.HasEdgeTagField() {
		t.Error("Edge Tag处理不正确")
	}
	c = IP
	if c.HasEdgeTagField() {
		t.Error("Edge Tag处理不正确")
	}
}

func TestInt16Unmarshal(t *testing.T) {
	for i := math.MinInt16; i <= math.MaxInt16; i++ {
		v, _ := unmarshalUint16WithSpecialID(marshalUint16WithSpecialID(int16(i)))
		if int16(i) != v {
			t.Errorf("序列化反序列化[%d]不正确", i)
		}
	}
}

func TestNegativeID(t *testing.T) {
	f := Field{L3EpcID: datatype.EPC_FROM_DEEPFLOW, GroupID: datatype.GROUP_INTERNET}
	if f.NewTag(L3EpcID|GroupID).ToKVString() != ",group_id=-2,l3_epc_id=-1" {
		t.Error("int16值处理得不正确")
	}
	f = Field{L3EpcID: 32767, GroupID: -3}
	if f.NewTag(L3EpcID|GroupID).ToKVString() != ",group_id=65533,l3_epc_id=32767" {
		t.Error("int16值处理得不正确")
	}
}

func TestCloneTagWithIPv6Fields(t *testing.T) {
	var ip [net.IPv6len]byte
	for i := range ip {
		ip[i] = byte(i)
	}
	tagOrigin := AcquireTag()
	tagOrigin.Field = AcquireField()
	tagOrigin.IsIPv6 = 1
	tagOrigin.IP6 = ip[:net.IPv6len]
	tagCloned := CloneTag(tagOrigin)
	if !tagCloned.IP6.Equal(tagOrigin.IP6) {
		t.Error("CloneTag产生的tag和原tag字段不一致")
	}
	tagCloned.IP6[0] = 255
	if tagCloned.IP6.Equal(tagOrigin.IP6) {
		t.Error("CloneTag产生的tag和原tag共享了字段")
	}
}

type Strings []string

func (s Strings) Len() int           { return len(s) }
func (s Strings) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Strings) Less(i, j int) bool { return s[i] < s[j] }

func parseTagkeys(b []byte) []string {
	str := string(b)
	ret := make([]string, 0)
	splits := strings.Split(str, ",")
	for _, s := range splits {
		if index := strings.Index(s, "="); index != -1 {
			ret = append(ret, s[:index])
		}
	}
	return ret
}

func TestMarshallToInfluxdb(t *testing.T) {
	b := make([]byte, 1024)
	f := Field{}
	tag := &Tag{&f, 0, ""}
	tag.GlobalThreadID = 112
	tag.Code = ^(GroupIDPath | HostIDPath | IPPath | L3DevicePath | L3EpcIDPath | RegionIDPath | SubnetIDPath | PodNodeIDPath | AZIDPath | PodGroupIDPath | PodNSIDPath | PodIDPath | PodClusterIDPath)

	l := tag.MarshalTo(b)
	strs := parseTagkeys(b[:l])
	cloneStrs := make([]string, 0)
	cloneStrs = append(cloneStrs, strs...)
	sort.Sort(Strings(strs))
	if strs[0] < "_id" {
		t.Error("tag的keys在打包时, 所有key都必须小于'_id', 当前最小的key是:", strs[0])
	}

	for i, s := range cloneStrs {
		if s != strs[i] {
			t.Error("tag的keys在打包时, 没有按字典顺序排序在:", s)
		}
	}

	tag.Code = ^(GroupID | HostID | IP | L3Device | L3EpcID | RegionID | SubnetID | PodNodeID | AZID | PodGroupID | PodNSID | PodID | PodClusterID)
	l = tag.MarshalTo(b)
	strs = parseTagkeys(b[:l])
	cloneStrs = cloneStrs[:0]
	cloneStrs = append(cloneStrs, strs...)
	sort.Sort(Strings(strs))
	if strs[0] < "_id" {
		t.Error("tag的keys在打包时, 所有key都必须小于'_id', 当前最小的key是:", strs[0])
	}

	for i, s := range cloneStrs {
		if s != strs[i] {
			t.Error("tag的keys在打包时, 没有按字典顺序排序在:", s)
		}
	}
}

func checkTagAndMiniTagEqual(tag *Tag, miniTag *MiniTag) bool {
	reversed := false
	if miniTag.Code&Direction != 0 && miniTag.HasEdgeTagField() || tag.Code&TAPSide != 0 {
		if miniTag.Code&Direction == 0 || !miniTag.HasEdgeTagField() {
			return false
		}
		if tag.Code&TAPSide == 0 {
			return false
		}
		if tag.Code & ^TAPSide != miniTag.Code & ^Direction {
			return false
		}
		reversed = tag.TAPSide == Server
	} else if tag.Code != miniTag.Code {
		return false
	}
	if tag.GlobalThreadID != miniTag.GlobalThreadID {
		return false
	}

	srcIP, dstIP := miniTag.IP(), miniTag.IP1()
	srcEpc, dstEpc := miniTag.L3EpcID, miniTag.L3EpcID1
	if reversed {
		srcIP, dstIP = dstIP, srcIP
		srcEpc, dstEpc = dstEpc, srcEpc
	}

	code := tag.Code

	if code&IP != 0 {
		if tag.IsIPv6 != 0 && miniTag.IsIPv6 != 0 {
			if !tag.IP6.Equal(srcIP) {
				return false
			}
		} else if tag.IsIPv6 == 0 && miniTag.IsIPv6 == 0 {
			if tag.IP != binary.BigEndian.Uint32(srcIP) {
				return false
			}
		} else {
			return false
		}
	}
	if code&L3EpcID != 0 {
		if tag.L3EpcID != srcEpc {
			return false
		}
	}

	if code&IPPath != 0 {
		if tag.IsIPv6 != 0 && miniTag.IsIPv6 != 0 {
			if !tag.IP6.Equal(srcIP) {
				return false
			}
			if !tag.IP61.Equal(dstIP) {
				return false
			}
		} else if tag.IsIPv6 == 0 && miniTag.IsIPv6 == 0 {
			if tag.IP != binary.BigEndian.Uint32(srcIP) {
				return false
			}
			if tag.IP1 != binary.BigEndian.Uint32(dstIP) {
				return false
			}
		} else {
			return false
		}
	}
	if code&L3EpcIDPath != 0 {
		if tag.L3EpcID != srcEpc {
			return false
		}
		if tag.L3EpcID1 != dstEpc {
			return false
		}
	}

	if code&ACLGID != 0 {
		if tag.ACLGID != miniTag.ACLGID {
			return false
		}
	}
	if code&Protocol != 0 {
		if tag.Protocol != miniTag.Protocol {
			return false
		}
	}
	if code&ServerPort != 0 {
		if tag.ServerPort != miniTag.ServerPort {
			return false
		}
	}
	if code&VTAPID != 0 {
		if tag.VTAPID != miniTag.VTAPID {
			return false
		}
	}
	if code&TAPType != 0 {
		if tag.TAPType != miniTag.TAPType {
			return false
		}
	}
	if code&TAPSide != 0 {
		if !(tag.TAPSide == Client && miniTag.Direction == ClientToServer || tag.TAPSide == Server && miniTag.Direction == ServerToClient) {
			return false
		}
	}

	if code&TagType != 0 {
		if tag.TagType != miniTag.TagType {
			return false
		}
	}
	if code&TagValue != 0 {
		if tag.TagValue != miniTag.TagValue {
			return false
		}
	}
	return true
}

func TestEncodeMiniTag(t *testing.T) {
	encoder := &codec.SimpleEncoder{}
	decoder := &codec.SimpleDecoder{}

	sideMiniTag := &MiniTag{
		MiniField: &MiniField{
			IsIPv6:     0,
			rawIP:      [net.IPv6len]byte{10, 11, 1, 123},
			L3EpcID:    15,
			VTAPID:     24,
			Protocol:   layers.IPProtocolTCP,
			ServerPort: 5324,
			Direction:  ClientToServer,
			TAPType:    ToR,
			ACLGID:     16,
			TagType:    TAG_TYPE_TCP_FLAG,
			TagValue:   18,
		},
		Code: IP | L3EpcID | VTAPID | Protocol | ServerPort | Direction | TAPType | ACLGID | TagType | TagValue,
	}

	sideMiniTag.Encode(encoder)
	decoder.Init(encoder.Bytes())

	sideTag := &Tag{Field: &Field{}}
	sideTag.Decode(decoder)

	if !checkTagAndMiniTagEqual(sideTag, sideMiniTag) {
		t.Error("mini tag and tag mismatch")
		t.Error("tag:     ", sideTag)
		t.Error("mini tag:", sideMiniTag)
	}

	encoder.Reset()

	edgeMiniTag := &MiniTag{
		MiniField: &MiniField{
			IsIPv6:     1,
			rawIP:      [net.IPv6len]byte{0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
			rawIP1:     [net.IPv6len]byte{0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
			L3EpcID:    15,
			L3EpcID1:   16,
			VTAPID:     24,
			Protocol:   layers.IPProtocolTCP,
			ServerPort: 5324,
			Direction:  ClientToServer,
			TAPType:    ToR,
			ACLGID:     16,
			TagType:    TAG_TYPE_TCP_FLAG,
			TagValue:   18,
		},
		Code: IPPath | L3EpcIDPath | VTAPID | Protocol | ServerPort | Direction | TAPType | ACLGID | TagType | TagValue,
	}

	edgeMiniTag.Encode(encoder)
	decoder.Init(encoder.Bytes())

	edgeTag := &Tag{Field: &Field{}}
	edgeTag.Decode(decoder)

	if !checkTagAndMiniTagEqual(edgeTag, edgeMiniTag) {
		t.Error("mini tag and tag mismatch")
		t.Error("tag:     ", edgeTag)
		t.Error("mini tag:", edgeMiniTag)
	}

	encoder.Reset()
	edgeMiniTag.Direction = ServerToClient

	edgeMiniTag.Encode(encoder)
	decoder.Init(encoder.Bytes())

	edgeTag = &Tag{Field: &Field{}}
	edgeTag.Decode(decoder)

	if !checkTagAndMiniTagEqual(edgeTag, edgeMiniTag) {
		t.Error("mini tag and tag mismatch")
		t.Error("tag:     ", edgeTag)
		t.Error("mini tag:", edgeMiniTag)
	}
}

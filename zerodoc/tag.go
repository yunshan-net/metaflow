package zerodoc

import (
	"strconv"
	"strings"

	"fmt"

	"github.com/google/gopacket/layers"
	"gitlab.x.lan/yunshan/droplet-libs/app"
	"gitlab.x.lan/yunshan/droplet-libs/codec"
	"gitlab.x.lan/yunshan/droplet-libs/pool"
	"gitlab.x.lan/yunshan/droplet-libs/utils"
)

type Code uint64

const (
	IP Code = 0x1 << iota
	_       // 1 << 1, MAC
	GroupID
	L2EpcID
	L3EpcID
	L2Device // 1 << 5
	L3Device
	Host
)

const (
	IPPath Code = 0x10000 << iota // 1 << 16
	_                             // 1 << 17, MACPath
	GroupIDPath
	L2EpcIDPath
	L3EpcIDPath
	L2DevicePath // 1 << 21
	L3DevicePath
	HostPath
)

const (
	Direction Code = 0x100000000 << iota // 1 << 32
	ACLGID
	VLANID
	Protocol
	ServerPort
	_ // 1 << 37
	_ // 1 << 38, VTAP
	TAPType
	SubnetID
	_ // 1 << 41, ACLID
	ACLDirection
	Scope
)

const (
	Country Code = 1 << 63
	Region  Code = 1 << 62
	ISPCode Code = 1 << 61

	CodeIndexBits uint32 = 6 // 修改此值需更新zero
	MaxCodeIndex  uint32 = (1 << CodeIndexBits) - 1
	CodeIndices   Code   = Code(MaxCodeIndex) << 48 // 1<<48 ~ 1<<53: code index
)

func IndexToCode(i uint32) Code {
	if i > MaxCodeIndex {
		panic(fmt.Sprintf("目前支持的最大CodeIndex为%d", MaxCodeIndex))
	}
	return Code(i) << 48
}

func CodeToIndex(c Code) uint32 {
	return uint32(c>>48) & MaxCodeIndex
}

func (c Code) HasEdgeTagField() bool {
	return c&0xffff0000 != 0
}

func (c Code) HasL2TagField() bool {
	// FIXME: GroupID、GroupIDPath待定
	return c&(L2EpcID|L2Device|Host|L2EpcIDPath|L2DevicePath|HostPath|VLANID|SubnetID) != 0
}

func (c Code) RemoveIndex() Code {
	return c &^ CodeIndices
}

// 从不同EndPoint获取的网包字段组成Field，是否可能重复。
// 注意，不能判断从同样的EndPoint获取的网包字段组成Field可能重复。
func (c Code) PossibleDuplicate() bool {
	return c&(CodeIndices|GroupID|L2EpcID|L3EpcID|Host|GroupIDPath|L2EpcIDPath|L3EpcIDPath|HostPath|ACLGID|VLANID|Protocol|TAPType|SubnetID|ACLDirection|Country|Region|ISPCode) == c
}

// 是否全部取自网包的对称字段（非源、目的字段）
func (c Code) IsSymmetric() bool {
	return c&(CodeIndices|ACLGID|VLANID|Protocol|TAPType|SubnetID|ACLDirection) == c
}

type DeviceType uint8

const (
	_ DeviceType = iota
	VMDevice
	_
	ThirdPartyDevice // 3
	_
	VGatewayDevice // 5
	HostDevice
	NetworkDevice
	FloatingIPDevice
	DHCPDevice
)

type DirectionEnum uint8

const (
	_ DirectionEnum = iota
	ClientToServer
	ServerToClient
)

type TAPTypeEnum uint8

const (
	OvS TAPTypeEnum = iota
	ISP
	Spine
	ToR
)

type ACLDirectionEnum uint8

const (
	ACL_FORWARD ACLDirectionEnum = 1 << iota
	ACL_BACKWARD
)

type ScopeEnum uint8

const (
	SCOPE_ALL ScopeEnum = iota
	SCOPE_IN_EPC
	SCOPE_OUT_EPC
	SCOPE_IN_SUBNET
	SCOPE_OUT_SUBNET
)

func (s ScopeEnum) String() string {
	switch s {
	case SCOPE_ALL:
		return "all"
	case SCOPE_IN_EPC:
		return "in_epc"
	case SCOPE_OUT_EPC:
		return "out_epc"
	case SCOPE_IN_SUBNET:
		return "in_subnet"
	case SCOPE_OUT_SUBNET:
		return "out_subnet"
	default:
		panic("invalid scope type")
	}
}

type Field struct {
	// 注意字节对齐！

	IP           uint32
	GroupID      int16
	L2EpcID      int16
	L3EpcID      int16
	L2DeviceID   uint16
	L3DeviceID   uint16
	L2DeviceType DeviceType
	L3DeviceType DeviceType
	Host         uint32

	IP1           uint32
	GroupID1      int16
	L2EpcID1      int16
	L3EpcID1      int16
	L2DeviceID1   uint16
	L3DeviceID1   uint16
	L2DeviceType1 DeviceType
	L3DeviceType1 DeviceType
	Host1         uint32

	ACLGID       uint16
	VLANID       uint16
	Direction    DirectionEnum
	Protocol     layers.IPProtocol
	ServerPort   uint16
	SubnetID     uint16
	TAPType      TAPTypeEnum
	ACLDirection ACLDirectionEnum
	Scope        ScopeEnum

	Country string
	Region  string
	ISP     string
}

type Tag struct {
	*Field
	Code
	id string
}

func (t *Tag) ToKVString() string {
	var buf strings.Builder
	// 在InfluxDB的line protocol中，tag紧跟在measurement name之后，总会以逗号开头

	// 1<<0 ~ 1<<6
	if t.Code&IP != 0 {
		buf.WriteString(",ip=")
		buf.WriteString(utils.IpFromUint32(t.IP).String())
	}
	if t.Code&GroupID != 0 {
		buf.WriteString(",group_id=")
		buf.WriteString(strconv.FormatInt(int64(t.GroupID), 10))
	}
	if t.Code&L2EpcID != 0 {
		buf.WriteString(",l2_epc_id=")
		buf.WriteString(strconv.FormatInt(int64(t.L2EpcID), 10))
	}
	if t.Code&L3EpcID != 0 {
		buf.WriteString(",l3_epc_id=")
		buf.WriteString(strconv.FormatInt(int64(t.L3EpcID), 10))
	}
	if t.Code&L2Device != 0 {
		buf.WriteString(",l2_device_id=")
		buf.WriteString(strconv.FormatUint(uint64(t.L2DeviceID), 10))
		buf.WriteString(",l2_device_type=")
		buf.WriteString(strconv.FormatUint(uint64(t.L2DeviceType), 10))
	}
	if t.Code&L3Device != 0 {
		buf.WriteString(",l3_device_id=")
		buf.WriteString(strconv.FormatUint(uint64(t.L3DeviceID), 10))
		buf.WriteString(",l3_device_type=")
		buf.WriteString(strconv.FormatUint(uint64(t.L3DeviceType), 10))
	}
	if t.Code&Host != 0 {
		buf.WriteString(",host=")
		buf.WriteString(utils.IpFromUint32(t.Host).String())
	}

	// 1<<16 ~ 1<<22
	if t.Code&IPPath != 0 {
		buf.WriteString(",ip_0=")
		buf.WriteString(utils.IpFromUint32(t.IP).String())
		buf.WriteString(",ip_1=")
		buf.WriteString(utils.IpFromUint32(t.IP1).String())
	}
	if t.Code&GroupIDPath != 0 {
		buf.WriteString(",group_id_0=")
		buf.WriteString(strconv.FormatInt(int64(t.GroupID), 10))
		buf.WriteString(",group_id_1=")
		buf.WriteString(strconv.FormatInt(int64(t.GroupID1), 10))
	}
	if t.Code&L2EpcIDPath != 0 {
		buf.WriteString(",l2_epc_id_0=")
		buf.WriteString(strconv.FormatInt(int64(t.L2EpcID), 10))
		buf.WriteString(",l2_epc_id_1=")
		buf.WriteString(strconv.FormatInt(int64(t.L2EpcID1), 10))
	}
	if t.Code&L3EpcIDPath != 0 {
		buf.WriteString(",l3_epc_id_0=")
		buf.WriteString(strconv.FormatInt(int64(t.L3EpcID), 10))
		buf.WriteString(",l3_epc_id_1=")
		buf.WriteString(strconv.FormatInt(int64(t.L3EpcID1), 10))
	}
	if t.Code&L2DevicePath != 0 {
		buf.WriteString(",l2_device_id_0=")
		buf.WriteString(strconv.FormatUint(uint64(t.L2DeviceID), 10))
		buf.WriteString(",l2_device_id_1=")
		buf.WriteString(strconv.FormatUint(uint64(t.L2DeviceID1), 10))
		buf.WriteString(",l2_device_type_0=")
		buf.WriteString(strconv.FormatUint(uint64(t.L2DeviceType), 10))
		buf.WriteString(",l2_device_type_1=")
		buf.WriteString(strconv.FormatUint(uint64(t.L2DeviceType1), 10))
	}
	if t.Code&L3DevicePath != 0 {
		buf.WriteString(",l3_device_id_0=")
		buf.WriteString(strconv.FormatUint(uint64(t.L3DeviceID), 10))
		buf.WriteString(",l3_device_id_1=")
		buf.WriteString(strconv.FormatUint(uint64(t.L3DeviceID1), 10))
		buf.WriteString(",l3_device_type_0=")
		buf.WriteString(strconv.FormatUint(uint64(t.L3DeviceType), 10))
		buf.WriteString(",l3_device_type_1=")
		buf.WriteString(strconv.FormatUint(uint64(t.L3DeviceType1), 10))
	}
	if t.Code&HostPath != 0 {
		buf.WriteString(",host_0=")
		buf.WriteString(utils.IpFromUint32(t.Host).String())
		buf.WriteString(",host_1=")
		buf.WriteString(utils.IpFromUint32(t.Host1).String())
	}

	// 1<<32 ~ 1<<48
	if t.Code&Direction != 0 {
		switch t.Direction {
		case ClientToServer:
			buf.WriteString(",direction=c2s")
		case ServerToClient:
			buf.WriteString(",direction=s2c")
		}
	}
	if t.Code&ACLGID != 0 {
		buf.WriteString(",acl_gid=")
		buf.WriteString(strconv.FormatUint(uint64(t.ACLGID), 10))
	}
	if t.Code&VLANID != 0 {
		buf.WriteString(",vlan_id=")
		buf.WriteString(strconv.FormatUint(uint64(t.VLANID), 10))
	}
	if t.Code&Protocol != 0 {
		buf.WriteString(",protocol=")
		buf.WriteString(strconv.FormatUint(uint64(t.Protocol), 10))
	}
	if t.Code&ServerPort != 0 {
		buf.WriteString(",server_port=")
		buf.WriteString(strconv.FormatUint(uint64(t.ServerPort), 10))
	}
	if t.Code&TAPType != 0 {
		buf.WriteString(",tap_type=")
		buf.WriteString(strconv.FormatUint(uint64(t.TAPType), 10))
	}
	if t.Code&SubnetID != 0 {
		buf.WriteString(",subnet_id=")
		buf.WriteString(strconv.FormatUint(uint64(t.SubnetID), 10))
	}
	if t.Code&ACLDirection != 0 {
		switch t.ACLDirection {
		case ACL_FORWARD:
			buf.WriteString(",acl_direction=fwd")
		case ACL_BACKWARD:
			buf.WriteString(",acl_direction=bwd")
		}
	}
	if t.Code&Scope != 0 {
		buf.WriteString(",scope=")
		buf.WriteString(t.Scope.String())
	}

	if t.Code&Country != 0 {
		buf.WriteString(",country=")
		buf.WriteString(t.Country)
	}
	if t.Code&Region != 0 {
		buf.WriteString(",region=")
		buf.WriteString(t.Region)
	}
	if t.Code&ISPCode != 0 {
		buf.WriteString(",isp=")
		buf.WriteString(t.ISP)
	}

	return buf.String()
}

func (t *Tag) String() string {
	var buf strings.Builder
	buf.WriteString("fields:")
	buf.WriteString(t.ToKVString())
	buf.WriteString(" code:")
	buf.WriteString(fmt.Sprintf("x%016x", t.Code))
	return buf.String()
}

func (t *Tag) Decode(decoder *codec.SimpleDecoder) {
	offset := decoder.Offset()

	t.Code = Code(decoder.ReadU64())

	if t.Code&IP != 0 {
		t.IP = decoder.ReadU32()
	}
	if t.Code&GroupID != 0 {
		t.GroupID = int16(decoder.ReadU16())
	}
	if t.Code&L2EpcID != 0 {
		t.L2EpcID = int16(decoder.ReadU16())
	}
	if t.Code&L3EpcID != 0 {
		t.L3EpcID = int16(decoder.ReadU16())
	}
	if t.Code&L2Device != 0 {
		t.L2DeviceID = uint16(decoder.ReadU16())
		t.L2DeviceType = DeviceType(decoder.ReadU8())
	}
	if t.Code&L3Device != 0 {
		t.L3DeviceID = uint16(decoder.ReadU16())
		t.L3DeviceType = DeviceType(decoder.ReadU8())
	}
	if t.Code&Host != 0 {
		t.Host = decoder.ReadU32()
	}

	if t.Code&IPPath != 0 {
		t.IP = decoder.ReadU32()
		t.IP1 = decoder.ReadU32()
	}
	if t.Code&GroupIDPath != 0 {
		t.GroupID = int16(decoder.ReadU16())
		t.GroupID1 = int16(decoder.ReadU16())
	}
	if t.Code&L2EpcIDPath != 0 {
		t.L2EpcID = int16(decoder.ReadU16())
		t.L2EpcID1 = int16(decoder.ReadU16())
	}
	if t.Code&L3EpcIDPath != 0 {
		t.L3EpcID = int16(decoder.ReadU16())
		t.L3EpcID1 = int16(decoder.ReadU16())
	}
	if t.Code&L2DevicePath != 0 {
		t.L2DeviceID = uint16(decoder.ReadU16())
		t.L2DeviceType = DeviceType(decoder.ReadU8())
		t.L2DeviceID1 = uint16(decoder.ReadU16())
		t.L2DeviceType1 = DeviceType(decoder.ReadU8())
	}
	if t.Code&L3DevicePath != 0 {
		t.L3DeviceID = uint16(decoder.ReadU16())
		t.L3DeviceType = DeviceType(decoder.ReadU8())
		t.L3DeviceID1 = uint16(decoder.ReadU16())
		t.L3DeviceType1 = DeviceType(decoder.ReadU8())
	}
	if t.Code&HostPath != 0 {
		t.Host = decoder.ReadU32()
		t.Host1 = decoder.ReadU32()
	}

	if t.Code&Direction != 0 {
		t.Direction = DirectionEnum(decoder.ReadU8())
	}
	if t.Code&ACLGID != 0 {
		t.ACLGID = decoder.ReadU16()
	}
	if t.Code&VLANID != 0 {
		t.VLANID = decoder.ReadU16()
	}
	if t.Code&Protocol != 0 {
		t.Protocol = layers.IPProtocol(decoder.ReadU8())
	}
	if t.Code&ServerPort != 0 {
		t.ServerPort = decoder.ReadU16()
	}
	if t.Code&TAPType != 0 {
		t.TAPType = TAPTypeEnum(decoder.ReadU8())
	}
	if t.Code&SubnetID != 0 {
		t.SubnetID = decoder.ReadU16()
	}
	if t.Code&ACLDirection != 0 {
		t.ACLDirection = ACLDirectionEnum(decoder.ReadU8())
	}
	if t.Code&Scope != 0 {
		t.Scope = ScopeEnum(decoder.ReadU8())
	}

	if t.Code&Country != 0 {
		t.Country = decoder.ReadString255()
	}
	if t.Code&Region != 0 {
		t.Region = decoder.ReadString255()
	}
	if t.Code&ISPCode != 0 {
		t.ISP = decoder.ReadString255()
	}

	if !decoder.Failed() {
		t.id = string(decoder.Bytes()[offset:decoder.Offset()]) // Encode内容就是它的id
	}
}

func (t *Tag) Encode(encoder *codec.SimpleEncoder) {
	if t.id != "" {
		encoder.WriteRawString(t.id) // ID就是序列化bytes，避免重复计算
		return
	}

	encoder.WriteU64(uint64(t.Code))

	if t.Code&IP != 0 {
		encoder.WriteU32(t.IP)
	}
	if t.Code&GroupID != 0 {
		encoder.WriteU16(uint16(t.GroupID))
	}
	if t.Code&L2EpcID != 0 {
		encoder.WriteU16(uint16(t.L2EpcID))
	}
	if t.Code&L3EpcID != 0 {
		encoder.WriteU16(uint16(t.L3EpcID))
	}
	if t.Code&L2Device != 0 {
		encoder.WriteU16(t.L2DeviceID)
		encoder.WriteU8(uint8(t.L2DeviceType))
	}
	if t.Code&L3Device != 0 {
		encoder.WriteU16(t.L3DeviceID)
		encoder.WriteU8(uint8(t.L3DeviceType))
	}
	if t.Code&Host != 0 {
		encoder.WriteU32(t.Host)
	}

	if t.Code&IPPath != 0 {
		encoder.WriteU32(t.IP)
		encoder.WriteU32(t.IP1)
	}
	if t.Code&GroupIDPath != 0 {
		encoder.WriteU16(uint16(t.GroupID))
		encoder.WriteU16(uint16(t.GroupID1))
	}
	if t.Code&L2EpcIDPath != 0 {
		encoder.WriteU16(uint16(t.L2EpcID))
		encoder.WriteU16(uint16(t.L2EpcID1))
	}
	if t.Code&L3EpcIDPath != 0 {
		encoder.WriteU16(uint16(t.L3EpcID))
		encoder.WriteU16(uint16(t.L3EpcID1))
	}
	if t.Code&L2DevicePath != 0 {
		encoder.WriteU16(t.L2DeviceID)
		encoder.WriteU8(uint8(t.L2DeviceType))
		encoder.WriteU16(t.L2DeviceID1)
		encoder.WriteU8(uint8(t.L2DeviceType1))
	}
	if t.Code&L3DevicePath != 0 {
		encoder.WriteU16(t.L3DeviceID)
		encoder.WriteU8(uint8(t.L3DeviceType))
		encoder.WriteU16(t.L3DeviceID1)
		encoder.WriteU8(uint8(t.L3DeviceType1))
	}
	if t.Code&HostPath != 0 {
		encoder.WriteU32(t.Host)
		encoder.WriteU32(t.Host1)
	}

	if t.Code&Direction != 0 {
		encoder.WriteU8(uint8(t.Direction))
	}
	if t.Code&ACLGID != 0 {
		encoder.WriteU16(t.ACLGID)
	}
	if t.Code&VLANID != 0 {
		encoder.WriteU16(t.VLANID)
	}
	if t.Code&Protocol != 0 {
		encoder.WriteU8(uint8(t.Protocol))
	}
	if t.Code&ServerPort != 0 {
		encoder.WriteU16(t.ServerPort)
	}
	if t.Code&TAPType != 0 {
		encoder.WriteU8(uint8(t.TAPType))
	}
	if t.Code&SubnetID != 0 {
		encoder.WriteU16(t.SubnetID)
	}
	if t.Code&ACLDirection != 0 {
		encoder.WriteU8(uint8(t.ACLDirection))
	}
	if t.Code&Scope != 0 {
		encoder.WriteU8(uint8(t.Scope))
	}

	if t.Code&Country != 0 {
		encoder.WriteString255(t.Country)
	}
	if t.Code&Region != 0 {
		encoder.WriteString255(t.Region)
	}
	if t.Code&ISPCode != 0 {
		encoder.WriteString255(t.ISP)
	}
}

func (t *Tag) GetID(encoder *codec.SimpleEncoder) string {
	if t.id == "" {
		encoder.Reset()
		t.Encode(encoder)
		t.id = encoder.String()
	}
	return t.id
}

func (t *Tag) SetID(id string) {
	t.id = id
}

func isFastCode(code Code) bool {
	// 认为所有只包含这四个Code子集的Tag能使用FashID
	return (code & ^(CodeIndices | ACLGID | IP | L3EpcID | TAPType)) == 0
}

// GetFastID 返回uint64的ID，0代表该tag的code不在fast ID的范围内
func (t *Tag) GetFastID() uint64 {
	if !isFastCode(t.Code) {
		return 0
	}
	var id uint64
	// 14b ACLGID + 32b IP + 16b L3EpcID + 2b TAPType
	//
	// 当code不存在的时候，有以下条件使得不同code的tag不会产生相同的fast ID：
	//   1. TAPType 0已经弃用，不会冲突
	//   2. L3EpcID 0是不存在的
	//   3. IP 255.255.255.255不用
	//   4. ACLGID 0不存在
	if t.Code&TAPType != 0 {
		id |= uint64(t.TAPType & 0x3)
	}
	if t.Code&L3EpcID != 0 {
		id |= uint64(uint16(t.L3EpcID)&0xFFFF) << 2
	}
	if t.Code&IP != 0 {
		id |= uint64(t.IP) << 18
	} else {
		id |= uint64(0xFFFFFFFF) << 18
	}
	if t.Code&ACLGID != 0 {
		id |= uint64(t.ACLGID&0x3FFF) << 50
	}
	return id
}

func (t *Tag) GetCode() uint64 {
	return uint64(t.Code)
}

func (t *Tag) HasVariedField() bool {
	return t.Code&(ServerPort|Region|Country|ISPCode) != 0 || t.HasEdgeTagField()
}

var databaseSuffix = [...]string{
	"",              // 000
	"acl",           // 001
	"edge",          // 010
	"acl_edge",      // 011
	"port",          // 100
	"acl_port",      // 101
	"edge_port",     // 110
	"acl_edge_port", // 111
}

func (t *Tag) DatabaseSuffix() string {
	code := 0
	if t.Code&ACLGID != 0 {
		code |= 0x1
	}
	if t.Code.HasEdgeTagField() {
		code |= 0x2
	}
	if t.Code&ServerPort != 0 {
		code |= 0x4
	}
	return databaseSuffix[code]
}

var fieldPool = pool.NewLockFreePool(func() interface{} {
	return &Field{}
})

func AcquireField() *Field {
	return fieldPool.Get().(*Field)
}

func ReleaseField(field *Field) {
	if field == nil {
		return
	}
	*field = Field{}
	fieldPool.Put(field)
}

func CloneField(field *Field) *Field {
	newField := AcquireField()
	*newField = *field
	return newField
}

var tagPool = pool.NewLockFreePool(func() interface{} {
	return &Tag{}
})

func AcquireTag() *Tag {
	return tagPool.Get().(*Tag)
}

// ReleaseTag 需要释放Tag拥有的Field
func ReleaseTag(tag *Tag) {
	if tag == nil {
		return
	}
	if tag.Field != nil {
		ReleaseField(tag.Field)
	}
	*tag = Tag{}
	tagPool.Put(tag)
}

// CloneTag 需要复制Tag拥有的Field
func CloneTag(tag *Tag) *Tag {
	newTag := AcquireTag()
	newTag.Field = CloneField(tag.Field)
	newTag.Code = tag.Code
	newTag.id = tag.id
	return newTag
}

func (t *Tag) Clone() app.Tag {
	return CloneTag(t)
}

func (t *Tag) Release() {
	ReleaseTag(t)
}

func (f *Field) NewTag(c Code) *Tag {
	tag := AcquireTag()
	tag.Field = CloneField(f)
	tag.Code = c
	tag.id = ""
	return tag
}

func (f *Field) FillTag(c Code, tag *Tag) {
	if tag.Field == nil {
		tag.Field = CloneField(f)
	} else {
		*tag.Field = *f
	}
	tag.Code = c
	tag.id = ""
}

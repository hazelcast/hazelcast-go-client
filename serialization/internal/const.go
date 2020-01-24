package internal

const (
	ByteSizeInBytes    = 1
	BoolSizeInBytes    = 1
	Uint8SizeInBytes   = 1
	Int16SizeInBytes   = 2
	Uint16SizeInBytes  = 2
	Int32SizeInBytes   = 4
	Float32SizeInBytes = 4
	Float64SizeInBytes = 8
	Int64SizeInBytes   = 8

	Version            = 0
	BeginFlag          = 0x80
	EndFlag            = 0x40
	BeginEndFlag       = BeginFlag | EndFlag
	ListenerFlag       = 0x01

	PayloadOffset = 18
	SizeOffset    = 0

	FrameLengthFieldOffset   = 0
	VersionFieldOffset       = FrameLengthFieldOffset + Int32SizeInBytes
	FlagsFieldOffset         = VersionFieldOffset + ByteSizeInBytes
	TypeFieldOffset          = FlagsFieldOffset + ByteSizeInBytes
	CorrelationIDFieldOffset = TypeFieldOffset + Int16SizeInBytes
	PartitionIDFieldOffset   = CorrelationIDFieldOffset + Int64SizeInBytes
	DataOffsetFieldOffset    = PartitionIDFieldOffset + Int32SizeInBytes
	HeaderSize               = DataOffsetFieldOffset + Int16SizeInBytes

	NilArrayLength = -1
)
package protocol

const (
	QUEUE_OFFER               = 0x0301
	QUEUE_PUT                 = 0x0302
	QUEUE_SIZE                = 0x0303
	QUEUE_REMOVE              = 0x0304
	QUEUE_POLL                = 0x0305
	QUEUE_TAKE                = 0x0306
	QUEUE_PEEK                = 0x0307
	QUEUE_ITERATOR            = 0x0308
	QUEUE_DRAINTO             = 0x0309
	QUEUE_DRAINTOMAXSIZE      = 0x030a
	QUEUE_CONTAINS            = 0x030b
	QUEUE_CONTAINSALL         = 0x030c
	QUEUE_COMPAREANDREMOVEALL = 0x030d
	QUEUE_COMPAREANDRETAINALL = 0x030e
	QUEUE_CLEAR               = 0x030f
	QUEUE_ADDALL              = 0x0310
	QUEUE_ADDLISTENER         = 0x0311
	QUEUE_REMOVELISTENER      = 0x0312
	QUEUE_REMAININGCAPACITY   = 0x0313
	QUEUE_ISEMPTY             = 0x0314
)

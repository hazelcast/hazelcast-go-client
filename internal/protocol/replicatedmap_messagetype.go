package protocol

const (
	REPLICATEDMAP_PUT                                = 0x0e01
	REPLICATEDMAP_SIZE                               = 0x0e02
	REPLICATEDMAP_ISEMPTY                            = 0x0e03
	REPLICATEDMAP_CONTAINSKEY                        = 0x0e04
	REPLICATEDMAP_CONTAINSVALUE                      = 0x0e05
	REPLICATEDMAP_GET                                = 0x0e06
	REPLICATEDMAP_REMOVE                             = 0x0e07
	REPLICATEDMAP_PUTALL                             = 0x0e08
	REPLICATEDMAP_CLEAR                              = 0x0e09
	REPLICATEDMAP_ADDENTRYLISTENERTOKEYWITHPREDICATE = 0x0e0a
	REPLICATEDMAP_ADDENTRYLISTENERWITHPREDICATE      = 0x0e0b
	REPLICATEDMAP_ADDENTRYLISTENERTOKEY              = 0x0e0c
	REPLICATEDMAP_ADDENTRYLISTENER                   = 0x0e0d
	REPLICATEDMAP_REMOVEENTRYLISTENER                = 0x0e0e
	REPLICATEDMAP_KEYSET                             = 0x0e0f
	REPLICATEDMAP_VALUES                             = 0x0e10
	REPLICATEDMAP_ENTRYSET                           = 0x0e11
	REPLICATEDMAP_ADDNEARCACHEENTRYLISTENER          = 0x0e12
)

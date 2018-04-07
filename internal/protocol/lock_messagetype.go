package protocol

const (
	LOCK_ISLOCKED                = 0x0701
	LOCK_ISLOCKEDBYCURRENTTHREAD = 0x0702
	LOCK_GETLOCKCOUNT            = 0x0703
	LOCK_GETREMAININGLEASETIME   = 0x0704
	LOCK_LOCK                    = 0x0705
	LOCK_UNLOCK                  = 0x0706
	LOCK_FORCEUNLOCK             = 0x0707
	LOCK_TRYLOCK                 = 0x0708
)

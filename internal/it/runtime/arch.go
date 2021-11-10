package runtime

import "runtime"

func Is32BitArch() bool {
	switch runtime.GOARCH {
	case "386", "arm", "mips", "mipsle":
		return true
	}
	return false
}

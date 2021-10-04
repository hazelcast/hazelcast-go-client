package runtime

import "runtime"

func Is32BitArch() bool {
	archs := []string{"386", "arm", "mips", "mipsle"}
	for _, arch := range archs {
		if runtime.GOARCH == arch {
			return true
		}
	}
	return false
}

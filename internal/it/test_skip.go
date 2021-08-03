package it

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal"
)

const (
	skipHzVersion     = "hz"
	skipClientVersion = "ver"
	skipOS            = "os"
	skipEnterprise    = "enterprise"
	skipOSS           = "oss"
	enterpriseKey     = "HAZELCAST_ENTERPRISE_KEY"
)

// SkipIf can be used to skip a test case based on comma-separated conditions.
// The following conditions may be set:
// "hz": Hazelcast version
// "ver": go client version
// "os": value of runtime.GOOS
// "enterprise"/"oss": presence of enterprise key environment variable
// Example: SkipIf(t, "ver > 1.1, hz = 5")
func SkipIf(t *testing.T, conditionString string) {
	skip, err := skipIf(conditionString)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if skip {
		t.Skip()
	}
}

func skipIf(conditionString string) (bool, error) {
	conditions := strings.Split(conditionString, ",")
	for _, condition := range conditions {
		condition = strings.Trim(condition, " ")
		switch parts := strings.Split(condition, " "); {
		case parts[0] == skipHzVersion:
			if err := validateLength(parts, 3, condition, "hz = 4.0"); err != nil {
				return true, err
			}
			skip, err := checkVersion(parts[2], parts[1], HzVersion())
			if err != nil {
				return true, err
			}
			if skip {
				return true, nil
			}
		case parts[0] == skipClientVersion:
			if err := validateLength(parts, 3, condition, "ver = 4.0"); err != nil {
				return true, err
			}
			skip, err := checkVersion(parts[2], parts[1], internal.ClientVersion)
			if err != nil {
				return true, err
			}
			if skip {
				return true, nil
			}
		case parts[0] == skipOS:
			if err := validateLength(parts, 3, condition, "os = windows"); err != nil {
				return true, err
			}
			skip, err := checkOS(parts[1], parts[2])
			if err != nil {
				return true, err
			}
			if skip {
				return true, nil
			}
		case parts[0] == skipEnterprise:
			if err := validateLength(parts, 1, condition, "enterprise"); err != nil {
				return true, err
			}
			skip := checkEnterprise(false)
			if skip {
				return true, nil
			}
		case parts[0] == "!"+skipEnterprise:
			if err := validateLength(parts, 1, condition, "!enterprise"); err != nil {
				return true, err
			}
			skip := checkEnterprise(true)
			if skip {
				return true, nil
			}
		case parts[0] == skipOSS:
			if err := validateLength(parts, 1, condition, "oss"); err != nil {
				return true, err
			}
			skip := checkEnterprise(true)
			if skip {
				return skip, nil
			}
		case parts[0] == "!"+skipOSS:
			if err := validateLength(parts, 1, condition, "!oss"); err != nil {
				return true, err
			}
			skip := checkEnterprise(false)
			if skip {
				return true, nil
			}
		default:
			return true, fmt.Errorf("Unexpected test skip constant \"%s\" in %s", parts[0], condition)
		}
	}
	return false, nil
}

func validateLength(parts []string, expected int, condition, example string) error {
	if len(parts) != expected {
		return fmt.Errorf("Unexpected format for %s, example of expected condition: \"%s\" ", condition, example)
	}
	return nil
}

func checkVersion(given, operator, actual string) (bool, error) {
	isGivenGreater, err := compareVersions(given, actual)
	if err != nil {
		return true, err
	}

	switch operator {
	case "=":
		if isGivenGreater == nil {
			return true, nil
		}
		return false, nil
	case "!=":
		if isGivenGreater != nil {
			return true, nil
		}
		return false, nil
	case ">":
		if isGivenGreater != nil && !*isGivenGreater {
			return true, nil
		}
		return false, nil
	case ">=":
		if isGivenGreater != nil && *isGivenGreater {
			return false, nil
		}
		return true, nil
	case "<":
		if isGivenGreater != nil && *isGivenGreater {
			return true, nil
		}
		return false, nil
	case "<=":
		if isGivenGreater != nil && !*isGivenGreater {
			return false, nil
		}
		return true, nil
	default:
		return true, fmt.Errorf("Unexpected test skip operator \"%s\" to compare versions", operator)
	}
}

func compareVersions(given, actual string) (*bool, error) {
	// versionNumbers describe the numbers of the present version
	actualVersions := strings.Split(actual, ".")
	// checkNumbers describe the numbers received to test for
	givenVersions := strings.Split(given, ".")

	min := len(givenVersions)
	if min > len(actualVersions) {
		min = len(actualVersions)
	}

	for i := 0; i < min; i++ {
		givenNumber, err := strconv.Atoi(givenVersions[i])
		if err != nil {
			return nil, fmt.Errorf("Could not parse version number (to integer): %d", givenNumber)
		}
		actualNumber, err := strconv.Atoi(actualVersions[i])
		if err != nil {
			return nil, fmt.Errorf("Could not parse version number (to integer): %d", givenNumber)
		}

		if givenNumber > actualNumber {
			res := true
			return &res, nil
		}
		if actualNumber > givenNumber {
			res := false
			return &res, nil
		}
	}
	return nil, nil
}

func checkOS(operator, value string) (bool, error) {
	switch operator {
	case "=":
		if runtime.GOOS == value {
			return true, nil
		}
	case "!=":
		if runtime.GOOS != value {
			return true, nil
		}
	default:
		return true, fmt.Errorf("Unexpected test skip operator \"%s\" in \"%s\" condition", operator, skipOS)
	}
	return false, nil
}

func checkEnterprise(expected bool) bool {
	_, actual := os.LookupEnv(enterpriseKey)
	if actual == expected {
		return true
	}
	return false
}

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
func SkipIf(t *testing.T, conditions string) {
	if skip := canSkip(conditions); skip {
		t.Skipf("Skipping test since: %s", conditions)
	}
}

func canSkip(conditionString string) bool {
	conditions := strings.Split(conditionString, ",")
	for _, condition := range conditions {
		condition = strings.Trim(condition, " ")
		if checkCondition(condition) {
			return true
		}
	}
	return false
}

func checkCondition(condition string) bool {
	switch parts := strings.Split(condition, " "); parts[0] {
	case skipHzVersion:
		validateLength(parts, 3, condition, "hz = 4.0")
		return checkVersion(parts[2], parts[1], HzVersion())
	case skipClientVersion:
		validateLength(parts, 3, condition, "ver = 4.0")
		return checkVersion(parts[2], parts[1], internal.ClientVersion)
	case skipOS:
		validateLength(parts, 3, condition, "os = windows")
		return checkOS(parts[1], parts[2])
	case skipEnterprise:
		validateLength(parts, 1, condition, "enterprise")
		return enterprise()
	case "!" + skipEnterprise:
		validateLength(parts, 1, condition, "!enterprise")
		return !enterprise()
	case skipOSS:
		validateLength(parts, 1, condition, "oss")
		return !enterprise()
	case "!" + skipOSS:
		validateLength(parts, 1, condition, "!oss")
		return enterprise()
	default:
		panic(fmt.Errorf("unexpected test skip constant \"%s\" in %s", parts[0], condition))
	}
}

func validateLength(parts []string, expected int, condition, example string) {
	if len(parts) != expected {
		panic(fmt.Errorf("unexpected format for %s, example of expected condition: \"%s\" ", condition, example))
	}
}

func checkVersion(given, operator, actual string) bool {
	greater := compareVersions(given, actual)
	switch operator {
	case "=":
		return greater == 0
	case "!=":
		return greater != 0
	case ">":
		return greater < 0
	case ">=":
		return greater <= 0
	case "<":
		return greater > 0
	case "<=":
		return greater >= 0
	default:
		panic(fmt.Errorf("unexpected test skip operator \"%s\" to compare versions", operator))
	}
}

func compareVersions(given, actual string) int {
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
			panic(fmt.Errorf("could not parse version number (to integer): %d", givenNumber))
		}
		actualNumber, err := strconv.Atoi(actualVersions[i])
		if err != nil {
			panic(fmt.Errorf("could not parse version number (to integer): %d", actualNumber))
		}

		if givenNumber > actualNumber {
			return 1
		}
		if actualNumber > givenNumber {
			return -1
		}
	}
	return 0
}

func checkOS(operator, value string) bool {
	switch operator {
	case "=":
		return runtime.GOOS == value
	case "!=":
		return runtime.GOOS != value
	default:
		panic(fmt.Errorf("unexpected test skip operator \"%s\" in \"%s\" condition", operator, skipOS))
	}
}

func enterprise() bool {
	_, enterprise := os.LookupEnv(enterpriseKey)
	return enterprise
}

package stats

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricsDictionary(t *testing.T) {
	d := newMetricsDictionary()
	assert.Equal(t, 0, d.Len())
	id0 := d.DictionaryID("foo")
	assert.Equal(t, 0, id0)
	assert.Equal(t, []wordID{{Word: "foo", ID: 0}}, d.Words())
	id1 := d.DictionaryID("abacus")
	assert.Equal(t, 1, id1)
	assert.Equal(t, []wordID{{Word: "abacus", ID: 1}, {Word: "foo", ID: 0}}, d.Words())
	id2 := d.DictionaryID("abacus")
	assert.Equal(t, 1, id2)
	assert.Equal(t, []wordID{{Word: "abacus", ID: 1}, {Word: "foo", ID: 0}}, d.Words())
}

func TestMetricsDictionary_InvalidWord(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatalf("should have paniced")
		}
	}()
	wb := make([]byte, maxWordLen+1)
	for i := 0; i < len(wb); i++ {
		wb[i] = 'c'
	}
	w := string(wb)
	d := newMetricsDictionary()
	d.DictionaryID(w)
}

func TestNewMetricCompressor(t *testing.T) {
	targetBin, err := ioutil.ReadFile("testdata/metrics_blob.bin")
	if err != nil {
		t.Fatal(err)
	}
	mc := NewMetricCompressor()
	md1 := metricDescriptor{
		Prefix:             "prefix",
		Metric:             "deltaMetric1",
		Discriminator:      "ds",
		DiscriminatorValue: "dsName1",
		HasUnit:            true,
		Unit:               metricUnitCount,
	}
	mc.AddLong(md1, 42)
	md2 := metricDescriptor{
		Prefix:             "prefix",
		Metric:             "deltaMetric2",
		Discriminator:      "ds",
		DiscriminatorValue: "dsName1",
		HasUnit:            true,
		Unit:               metricUnitCount,
	}
	mc.AddDouble(md2, -4.2)
	md3 := metricDescriptor{
		Prefix:  makeStr('a', 254),
		Metric:  "longPrefixMetric",
		HasUnit: true,
		Unit:    metricUnitBytes,
	}
	mc.AddLong(md3, 2147483647)
	bin := mc.GenerateBlob()
	ioutil.WriteFile("D:/temp/blob1.bin", bin, 666)
	assert.Equal(t, targetBin, bin)
}

func makeStr(char byte, count int) string {
	b := make([]byte, count)
	for i := 0; i < count; i++ {
		b[i] = char
	}
	return string(b)
}

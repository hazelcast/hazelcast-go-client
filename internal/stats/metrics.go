package stats

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"math"
	"sort"
)

const (
	maxWordLen                        = 255
	initialBufferSizeMetrics          = 2 << 11 // 4kB
	initialBufferSizeDictionary       = 2 << 10 // 2kB
	initialBufferSizeDescription      = 2 << 8  // 512B
	sizeFactorNumerator               = 11
	sizeFactorDenominator             = 10
	maskPrefix                   byte = 1 << 0
	maskMetric                   byte = 1 << 1
	maskDiscriminator            byte = 1 << 2
	maskDiscriminatorValue       byte = 1 << 3
	maskUnit                     byte = 1 << 4
	maskExcludedTargets          byte = 1 << 5
	maskTagCount                 byte = 1 << 6
	binaryFormatVersion               = 1
	nullDictionaryID                  = -1
	nullUnit                     byte = 0xFF
	sizeVersion                       = 2
	sizeDictionaryBlob                = 4
	sizeCountMetrics                  = 4
)

type MetricsCompressor struct {
	dict         *metricsDictionary
	lastMD       *metricDescriptor
	metricBuf    *bytes.Buffer
	dictBuf      *bytes.Buffer
	metricsCount int
	buf          []byte
}

func NewMetricCompressor() *MetricsCompressor {
	mc := &MetricsCompressor{
		buf: make([]byte, 8),
	}
	mc.Reset()
	return mc
}

func (mc *MetricsCompressor) AddLong(d metricDescriptor, value int64) {
	mc.writeDescriptor(d)
	mc.writeByte(mc.metricBuf, byte(valueTypeLong))
	mc.writeLong(mc.metricBuf, value)
}

func (mc *MetricsCompressor) AddDouble(d metricDescriptor, value float64) {
	mc.writeDescriptor(d)
	mc.writeByte(mc.metricBuf, byte(valueTypeDouble))
	mc.writeDouble(mc.metricBuf, value)
}

func (mc *MetricsCompressor) GenerateBlob() []byte {
	mc.writeDict()
	compressedDict := mc.compress(mc.dictBuf)
	compressedMetrics := mc.compress(mc.metricBuf)
	blobSize := sizeVersion + sizeDictionaryBlob + len(compressedDict) + sizeCountMetrics + len(compressedMetrics)
	blob := make([]byte, blobSize)
	pos := 0
	blob[pos] = 0
	pos++
	blob[pos] = binaryFormatVersion
	pos++
	binary.BigEndian.PutUint32(blob[pos:], uint32(len(compressedDict)))
	pos += 4
	pos += copy(blob[pos:], compressedDict)
	binary.BigEndian.PutUint32(blob[pos:], uint32(mc.metricsCount))
	pos += 4
	pos += copy(blob[pos:], compressedMetrics)
	return blob
}

func (mc *MetricsCompressor) Reset() {
	mc.dict = newMetricsDictionary()
	mc.metricBuf = &bytes.Buffer{}
	mc.dictBuf = &bytes.Buffer{}
	mc.metricsCount = 0
	mc.lastMD = nil
}

func (mc *MetricsCompressor) writeDescriptor(d metricDescriptor) {
	mask := d.calculateMask(mc.lastMD)
	mc.writeByte(mc.metricBuf, mask)
	if mask&maskPrefix == 0 {
		mc.writeInt(mc.metricBuf, mc.dict.DictionaryID(d.Prefix))
	}
	if mask&maskMetric == 0 {
		mc.writeInt(mc.metricBuf, mc.dict.DictionaryID(d.Metric))
	}
	if mask&maskDiscriminator == 0 {
		mc.writeInt(mc.metricBuf, mc.dict.DictionaryID(d.Discriminator))
	}
	if mask&maskDiscriminatorValue == 0 {
		mc.writeInt(mc.metricBuf, mc.dict.DictionaryID(d.DiscriminatorValue))
	}
	if mask&maskUnit == 0 {
		if d.HasUnit {
			mc.writeByte(mc.metricBuf, byte(d.Unit))
		} else {
			mc.writeByte(mc.metricBuf, nullUnit)
		}
	}
	// Include excludedTargets and tags bytes for compatibility purposes.
	if mask&maskExcludedTargets == 0 {
		mc.writeByte(mc.metricBuf, 0)
	}
	if mask&maskTagCount == 0 {
		mc.writeByte(mc.metricBuf, 0)
	}
	mc.metricsCount++
	mc.lastMD = &d
}

func (mc MetricsCompressor) compress(buf *bytes.Buffer) []byte {
	var b bytes.Buffer
	w, err := zlib.NewWriterLevel(&b, zlib.BestSpeed)
	if err != nil {
		panic(err)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		panic(err)
	}
	w.Close()
	return b.Bytes()
}

func (mc MetricsCompressor) writeByte(buf *bytes.Buffer, b byte) {
	buf.WriteByte(b)
}

func (mc MetricsCompressor) writeInt(buf *bytes.Buffer, i int32) {
	binary.BigEndian.PutUint32(mc.buf, uint32(i))
	buf.Write(mc.buf[:4])
}

func (mc MetricsCompressor) writeLong(buf *bytes.Buffer, l int64) {
	binary.BigEndian.PutUint64(mc.buf, uint64(l))
	buf.Write(mc.buf[:8])
}

func (mc MetricsCompressor) writeDouble(buf *bytes.Buffer, f float64) {
	binary.BigEndian.PutUint64(mc.buf, math.Float64bits(f))
	buf.Write(mc.buf[:8])
}

func (mc MetricsCompressor) writeDict() {
	words := mc.dict.Words()
	mc.writeInt(mc.dictBuf, int32(len(words)))
	lastText := ""
	for _, word := range words {
		text := word.Word
		textLen := len(text)
		maxCommonLen := len(lastText)
		if textLen < maxCommonLen {
			maxCommonLen = textLen
		}
		var commonLen int
		for commonLen = 0; commonLen < maxCommonLen && lastText[commonLen] == text[commonLen]; commonLen++ {
			// pass
		}
		diffLen := textLen - commonLen
		mc.writeInt(mc.dictBuf, word.ID)
		mc.writeByte(mc.dictBuf, byte(commonLen))
		mc.writeByte(mc.dictBuf, byte(diffLen))
		for i := commonLen; i < textLen; i++ {
			mc.dictBuf.WriteByte(0)
			mc.dictBuf.WriteByte(text[i])
		}
		lastText = text
	}
}

type metricsDictionary struct {
	wordIndex map[string]int
	words     []wordID
}

func newMetricsDictionary() *metricsDictionary {
	md := &metricsDictionary{
		wordIndex: map[string]int{},
	}
	return md
}

func (md *metricsDictionary) DictionaryID(word string) int32 {
	if len(word) > maxWordLen {
		panic("metrics descriptor name is too long")
	}
	if word == "" {
		return nullDictionaryID
	}
	if index, ok := md.wordIndex[word]; ok {
		return md.words[index].ID
	}
	n := int32(len(md.words))
	md.words = append(md.words, wordID{Word: word, ID: n})
	return n
}

func (md *metricsDictionary) Words() []wordID {
	sort.Sort(md)
	return md.words
}

func (md metricsDictionary) Len() int {
	return len(md.words)
}

func (md metricsDictionary) Less(i, j int) bool {
	return md.words[i].Word < md.words[j].Word
}

func (md metricsDictionary) Swap(i, j int) {
	md.words[i], md.words[j] = md.words[j], md.words[i]
	md.wordIndex[md.words[i].Word] = i
	md.wordIndex[md.words[j].Word] = j
}

type wordID struct {
	Word string
	ID   int32
}

type metricDescriptor struct {
	Prefix             string
	Metric             string
	Discriminator      string
	DiscriminatorValue string
	HasUnit            bool
	Unit               metricUnit
}

func (md metricDescriptor) calculateMask(lastMD *metricDescriptor) (mask byte) {
	if lastMD == nil {
		return
	}
	if md.Prefix == lastMD.Prefix {
		mask |= maskPrefix
	}
	if md.Metric == lastMD.Metric {
		mask |= maskMetric
	}
	if md.Discriminator == lastMD.Discriminator {
		mask |= maskDiscriminator
	}
	if md.DiscriminatorValue == lastMD.DiscriminatorValue {
		mask |= maskDiscriminatorValue
	}
	if md.Unit == lastMD.Unit {
		mask |= maskUnit
	}
	// include excludedTargets and tags bits for compatibility purposes
	mask |= maskExcludedTargets
	mask |= maskTagCount
	return
}

type metricUnit byte

const (
	// Size, counter, represented in bytes
	metricUnitBytes metricUnit = 0
	metricUnitMS    metricUnit = 1
	metricPercent   metricUnit = 3
	metricUnitCount metricUnit = 4
)

type valueType byte

const (
	valueTypeLong   valueType = 0
	valueTypeDouble valueType = 1
)

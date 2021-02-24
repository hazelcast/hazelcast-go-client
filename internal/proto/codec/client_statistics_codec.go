// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x000C00
	ClientStatisticsCodecRequestMessageType = int32(3072)
	// hex: 0x000C01
	ClientStatisticsCodecResponseMessageType = int32(3073)

	ClientStatisticsCodecRequestTimestampOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientStatisticsCodecRequestInitialFrameSize = ClientStatisticsCodecRequestTimestampOffset + proto.LongSizeInBytes
)

// The statistics is composed of three parameters.
//
// The first paramteter is the timestamp taken when the statistics collected.
//
// The second parameter, the clientAttribute is a String that is composed of key=value pairs separated by ','. The
// following characters ('=' '.' ',' '\') should be escaped.
//
// Please note that if any client implementation can not provide the value for statistics, the corresponding key, value
// pair will not be presented in the statistics string. Only the ones, that the client can provide will be added.
//
// The third parameter, metrics is a compressed byte array containing all metrics recorded by the client.
//
// The metrics are composed of the following fields:
//   - string:                 prefix
//   - string:                 metric
//   - string:                 discriminator
//   - string:                 discriminatorValue
//   - enum:                   unit [BYTES,MS,PERCENT,COUNT,BOOLEAN,ENUM]
//   - set of enum:            excluded targets [MANAGEMENT_CENTER,JMX,DIAGNOSTICS]
//   - set of <string,string>: tags associated with the metric
//
// The used compression algorithm is the same that is used inside the IMDG clients and members for storing the metrics blob
// in-memory. The algorithm uses a dictionary based delta compression further deflated by using ZLIB compression.
//
// The byte array has the following layout:
//
// +---------------------------------+--------------------+
// | Compressor version              |   2 bytes (short)  |
// +---------------------------------+--------------------+
// | Size of dictionary blob         |   4 bytes (int)    |
// +---------------------------------+--------------------+
// | ZLIB compressed dictionary blob |   variable size    |
// +---------------------------------+--------------------+
// | Number of metrics in the blob   |   4 bytes (int)    |
// +---------------------------------+--------------------+
// | ZLIB compressed metrics blob    |   variable size    |
// +---------------------------------+--------------------+
//
// ==========
// THE HEADER
// ==========
//
// Compressor version:      the version currently in use is 1.
// Size of dictionary blob: the size of the ZLIB compressed blob as it is constructed as follows.
//
// ===================
// THE DICTIONARY BLOB
// ===================
//
// The dictionary is built from the string fields of the metric and assigns an int dictionary id to every string in the metrics
// in the blob. The dictionary is serialized to the dictionary blob sorted by the strings using the following layout.
//
// +------------------------------------------------+--------------------+
// | Number of dictionary entries                   |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | Dictionary id                                  |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | Number of chars shared with previous entry     |   1 unsigned byte  |
// +------------------------------------------------+--------------------+
// | Number of chars not shared with previous entry |   1 unsigned byte  |
// +------------------------------------------------+--------------------+
// | The different characters                       |   variable size    |
// +------------------------------------------------+--------------------+
// | Dictionary id                                  |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | ...                                            |   ...              |
// +------------------------------------------------+--------------------+
//
// Let's say we have the following dictionary:
//   - <42,"gc.minorCount">
//   - <43,"gc.minorTime">
//
// It is then serialized as follows:
// +------------------------------------------------+--------------------+
// | 2 (size of the dictionary)                     |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | 42                                             |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | 0                                              |   1 unsigned byte  |
// +------------------------------------------------+--------------------+
// | 13                                             |   1 unsigned byte  |
// +------------------------------------------------+--------------------+
// | "gc.minorCount"                                |   13 bytes         |
// +------------------------------------------------+--------------------+
// | 43                                             |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | 8                                              |   1 unsigned byte  |
// +------------------------------------------------+--------------------+
// | 4                                              |   1 unsigned byte  |
// +------------------------------------------------+--------------------+
// | "Time"                                         |   13 bytes         |
// +------------------------------------------------+--------------------+
//
// The dictionary blob constructed this way is then gets ZLIB compressed.
//
// ===============
// THE METRIC BLOB
// ===============
//
// The compressed dictionary blob is followed by the number of metrics
// (int) present in the metrics blob.
//
// The number of metrics is followed by the compressed metrics blob with
// the following layout:
//
// +------------------------------------------------+--------------------+
// | Metrics mask                                   |   1 byte           |
// +------------------------------------------------+--------------------+
// | (*) Dictionary id of prefix                    |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | (*) Dictionary id of metric                    |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | (*) Dictionary id of discriminator             |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | (*) Dictionary id of discriminatorValue        |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | (*) Enum ordinal of the unit                   |   1 byte           |
// +------------------------------------------------+--------------------+
// | (*) Excluded targets bitset                    |   1 byte           |
// +------------------------------------------------+--------------------+
// | (*) Number of tags                             |   1 unsigned byte  |
// +------------------------------------------------+--------------------+
// | (**) Dictionary id of the tag 1                |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | (**) Dictionary id of the value of tag 1       |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | (**) Dictionary id of the tag 2                |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | (**) Dictionary id of the value of tag 2       |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | ...                                            |   ...              |
// +------------------------------------------------+--------------------+
// | Metrics mask                                   |   1 byte           |
// +------------------------------------------------+--------------------+
// | (*) Dictionary id of prefix                    |   4 bytes (int)    |
// +------------------------------------------------+--------------------+
// | ...                                            |   ...              |
// +------------------------------------------------+--------------------+
//
// The metrics mask shows which fields are the same in the current and the
// previous metric. The following masks are used to construct the metrics
// mask.
//
// MASK_PREFIX              = 0b00000001;
// MASK_METRIC              = 0b00000010;
// MASK_DISCRIMINATOR       = 0b00000100;
// MASK_DISCRIMINATOR_VALUE = 0b00001000;
// MASK_UNIT                = 0b00010000;
// MASK_EXCLUDED_TARGETS    = 0b00100000;
// MASK_TAG_COUNT           = 0b01000000;
//
// If a bit representing a field is set, the given field marked above with (*)
// is not written to blob and the last value for that field should be taken
// during deserialization.
//
// Since the number of tags are not limited, all tags and their values
// marked with (**) are written even if the tag set is the same as in the
// previous metric.
//
// The metrics blob constructed this way is then gets ZLIB compressed.

func EncodeClientStatisticsRequest(timestamp int64, clientAttributes string, metricsBlob []byte) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ClientStatisticsCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, ClientStatisticsCodecRequestTimestampOffset, timestamp)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientStatisticsCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, clientAttributes)
	EncodeByteArray(clientMessage, metricsBlob)

	return clientMessage
}

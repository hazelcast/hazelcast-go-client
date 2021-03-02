package config

// UniqueKeyTransformation Defines an assortment of transformations which can be applied to unique key values.
type UniqueKeyTransformation int32

const (
	// Object Extracted unique key value is interpreted as an object value.
	//	Non-negative unique ID is assigned to every distinct object value.
	Object UniqueKeyTransformation = iota
	// Long Extracted unique key value is interpreted as a whole integer value of byte, short, int or long type.
	//    The extracted value is up casted to long (if necessary) and unique non-negative ID is assigned
	//    to every distinct value.
	Long
	// Raw Extracted unique key value is interpreted as a whole integer value of byte, short, int or long type.
	//    The extracted value is up casted to long (if necessary) and the resulting value is used directly as an ID.
	Raw
)

// BitmapIndexOptions configures indexing options specific to bitmap indexes.
type BitmapIndexOptions struct {

	// uniqueKey Unique key attribute configured in this index config. Defaults to __key.
	//	The unique key attribute is used as a source of values which uniquely identify each entry being inserted into an index.
	uniqueKey string

	// uniqueKeyTransformation Unique key transformation configured in this index.
	//	The transformation is applied to every value extracted from unique key attribute.
	//	Defaults to OBJECT. Available values are OBJECT, LONG, and RAW
	uniqueKeyTransformation UniqueKeyTransformation
}

// NewBitmapIndexOptions create a new BitmapIndexOptions
func NewBitmapIndexOptions(uniqueKey string, uniqueKeyTransformation int32) *BitmapIndexOptions {
	return &BitmapIndexOptions{uniqueKey: uniqueKey, uniqueKeyTransformation: UniqueKeyTransformation(uniqueKeyTransformation)}
}

func (bitmapIndexOptions BitmapIndexOptions) UniqueKey() string {
	return bitmapIndexOptions.uniqueKey
}

func (bitmapIndexOptions BitmapIndexOptions) UniqueKeyTransformation() int32 {
	return int32(bitmapIndexOptions.uniqueKeyTransformation)
}

// IndexType Type of the index
type IndexType int32

const (
	// Sorted index can be used with equality and range predicates.
	Sorted IndexType = iota
	// Hash index can be used with equality predicates.
	Hash
	// Bitmap index can be used with equality predicates.
	Bitmap
)

type IndexConfig struct {
	name               string
	_type              IndexType
	attributes         []string
	bitmapIndexOptions *BitmapIndexOptions
}

func NewIndexConfig(name string, _type int32, attributes []string, bitmapIndexOptions *BitmapIndexOptions) IndexConfig {
	return IndexConfig{name: name, _type: IndexType(_type), attributes: attributes, bitmapIndexOptions: bitmapIndexOptions}
}

func (indexConfig IndexConfig) Name() string {
	return indexConfig.name
}

func (indexConfig IndexConfig) Type() int32 {
	return int32(indexConfig._type)
}

func (indexConfig IndexConfig) Attributes() []string {
	return indexConfig.attributes
}

func (indexConfig IndexConfig) BitmapIndexOptions() *BitmapIndexOptions {
	return indexConfig.bitmapIndexOptions
}

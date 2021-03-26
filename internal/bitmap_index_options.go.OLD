package internal

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
type BitmapIndexOptions interface {
	UniqueKey() string
	UniqueKeyTransformation() int32
}

type bitmapIndexOptions struct {

	// uniqueKey Unique key attribute configured in this index config. Defaults to __key.
	//	The unique key attribute is used as a source of values which uniquely identify each entry being inserted into an index.
	uniqueKey string

	// uniqueKeyTransformation Unique key transformation configured in this index.
	//	The transformation is applied to every value extracted from unique key attribute.
	//	Defaults to OBJECT. Available values are OBJECT, LONG, and RAW
	uniqueKeyTransformation UniqueKeyTransformation
}

// NewBitmapIndexOptions create a new BitmapIndexOptions
func NewBitmapIndexOptions(uniqueKey string, uniqueKeyTransformation int32) *bitmapIndexOptions {
	return &bitmapIndexOptions{uniqueKey: uniqueKey, uniqueKeyTransformation: UniqueKeyTransformation(uniqueKeyTransformation)}
}

func (bitmapIndexOptions bitmapIndexOptions) UniqueKey() string {
	return bitmapIndexOptions.uniqueKey
}

func (bitmapIndexOptions bitmapIndexOptions) UniqueKeyTransformation() int32 {
	return int32(bitmapIndexOptions.uniqueKeyTransformation)
}

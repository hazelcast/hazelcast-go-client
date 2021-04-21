package types

type IndexType int32

const (
	IndexTypeSorted = int32(0)
	IndexTypeHash   = int32(1)
	IndexTypeBitmap = int32(2)
)

const (
	UniqueKeyTransformationObject = int32(0)
	UniqueKeyTransformationLong   = int32(1)
	UniqueKeyTransformationRaw    = int32(2)
)

type IndexConfig struct {
	Name               string
	Type               int32
	Attributes         []string
	BitmapIndexOptions BitmapIndexOptions
}

type BitmapIndexOptions struct {
	UniqueKey               string
	UniqueKeyTransformation int32
}

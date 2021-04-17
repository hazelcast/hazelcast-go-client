package types

type IndexType int32

const (
	indexTypeSorted IndexType = 0
	indexTypeHash   IndexType = 1
	indexTypeBitmap IndexType = 2
)

type UniqueKeyTransformation int32

const (
	uniqueKeyTransformationObject UniqueKeyTransformation = 0
	uniqueKeyTransformationLong   UniqueKeyTransformation = 1
	uniqueKeyTransformationRaw    UniqueKeyTransformation = 2
)

type IndexConfig struct {
	name                    string
	indexType               IndexType
	attrs                   []string
	uniqueKey               string
	uniqueKeyTransformation UniqueKeyTransformation
}

// NewIndexConfig is a private function.
// It may be removed or changed anytime.
func NewIndexConfig(name string, indexType int32, attrs []string, bitmapIndexOptions BitmapIndexOptions) IndexConfig {
	return IndexConfig{
		name:                    name,
		indexType:               IndexType(indexType),
		attrs:                   attrs,
		uniqueKey:               bitmapIndexOptions.uniqueKey,
		uniqueKeyTransformation: bitmapIndexOptions.uniqueKeyTransformation,
	}
}

func (c *IndexConfig) Name() string {
	return c.name
}

func (c *IndexConfig) Type() IndexType {
	return c.indexType
}

func (c *IndexConfig) Attributes() []string {
	newAttrs := make([]string, 0, len(c.attrs))
	copy(newAttrs, c.attrs)
	return newAttrs
}

func (c *IndexConfig) BitmapIndexOptions() BitmapIndexOptions {
	return BitmapIndexOptions{
		uniqueKey:               c.uniqueKey,
		uniqueKeyTransformation: c.uniqueKeyTransformation,
	}
}

type IndexConfigBuilder struct {
	indexConfig *IndexConfig
}

func (c IndexConfigBuilder) Config() IndexConfig {
	return *c.indexConfig
}

func NewIndexConfigBuilder() *IndexConfigBuilder {
	return &IndexConfigBuilder{indexConfig: &IndexConfig{}}
}

func (c *IndexConfigBuilder) SetName(name string) *IndexConfigBuilder {
	c.indexConfig.name = name
	return c
}

func (c *IndexConfigBuilder) SetTypeSorted() *IndexConfigBuilder {
	c.indexConfig.indexType = indexTypeSorted
	return c
}

func (c *IndexConfigBuilder) SetTypeHash() *IndexConfigBuilder {
	c.indexConfig.indexType = indexTypeHash
	return c
}

func (c *IndexConfigBuilder) SetTypeBitmap() *IndexConfigBuilder {
	c.indexConfig.indexType = indexTypeBitmap
	return c
}

func (c *IndexConfigBuilder) SetAttrs(attrs ...string) *IndexConfigBuilder {
	newAttrs := make([]string, 0, len(attrs))
	copy(newAttrs, attrs)
	c.indexConfig.attrs = newAttrs
	return c
}

func (c *IndexConfigBuilder) SetUniqueKey(key string) *IndexConfigBuilder {
	c.indexConfig.uniqueKey = key
	return c
}

func (c *IndexConfigBuilder) SetUniqueKeyTransformationObject() *IndexConfigBuilder {
	c.indexConfig.uniqueKeyTransformation = uniqueKeyTransformationObject
	return c
}

func (c *IndexConfigBuilder) SetUniqueKeyTransformationLong() *IndexConfigBuilder {
	c.indexConfig.uniqueKeyTransformation = uniqueKeyTransformationLong
	return c
}

func (c *IndexConfigBuilder) SetUniqueKeyTransformationRaw() *IndexConfigBuilder {
	c.indexConfig.uniqueKeyTransformation = uniqueKeyTransformationRaw
	return c
}

type BitmapIndexOptions struct {
	uniqueKey               string
	uniqueKeyTransformation UniqueKeyTransformation
	isDefault               bool
}

func defaultBitmapIndexOptions() BitmapIndexOptions {
	return BitmapIndexOptions{
		isDefault: true,
	}
}

func (b BitmapIndexOptions) UniqueKey() string {
	return b.uniqueKey
}

func (b BitmapIndexOptions) UniqueKeyTransformation() int32 {
	return int32(b.uniqueKeyTransformation)
}

func (b BitmapIndexOptions) IsDefault() bool {
	return b.isDefault
}

// NewBitmapIndexOptions is a private function.
// It may be removed or changed anytime.
func NewBitmapIndexOptions(uniqueKey string, uniqueKeyTransformation int32) BitmapIndexOptions {
	return BitmapIndexOptions{
		uniqueKey:               uniqueKey,
		uniqueKeyTransformation: UniqueKeyTransformation(uniqueKeyTransformation),
	}
}

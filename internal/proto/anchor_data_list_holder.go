package proto

type AnchorDataListHolder struct {
	anchorPageList []int32
	anchorDataList []Pair
}

func NewAnchorDataListHolder(anchorPageList []int32, anchorDataList []Pair) AnchorDataListHolder {
	return AnchorDataListHolder{anchorPageList, anchorDataList}
}

func (a AnchorDataListHolder) AnchorPageList() []int32 {
	return a.anchorPageList
}

func (a AnchorDataListHolder) AnchorDataList() []Pair {
	return a.anchorDataList
}

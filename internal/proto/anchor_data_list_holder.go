package proto

type AnchorDataListHolder struct {
	anchorPageList []int32
	anchorDataList []Pair
}

func NewAnchorDataListHolder(anchorPageList []int32, anchorDataList []Pair) AnchorDataListHolder {
	return AnchorDataListHolder{anchorPageList, anchorDataList}
}

func (a AnchorDataListHolder) GetAnchorPageList() []int32 {
	return a.anchorPageList
}

func (a AnchorDataListHolder) GetAnchorDataList() []Pair {
	return a.anchorDataList
}

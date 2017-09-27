package internal

type ListProxy struct{
	partitionSpecificProxy
}

func (ilist *ListProxy) Destroy(){}
func (ilist *ListProxy) GetName() string{return ""}
func (ilist *ListProxy) GetPartitionKey() string{return ""}
func (ilist *ListProxy) GetServiceName() string{return ""}





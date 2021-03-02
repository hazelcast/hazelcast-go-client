package object

type DataReader interface {
	ReadString(dataOut *string) error
}

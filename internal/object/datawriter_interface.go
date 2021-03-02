package object

type DataWriter interface {
	WriteString(data string) error
}

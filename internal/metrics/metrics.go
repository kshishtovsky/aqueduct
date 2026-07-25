package metrics

var (
	FramesParsed     uint64
	FramesSerialized uint64
	ParseErrors      uint64
)

func IncFramesParsed() {
	FramesParsed++
}

func IncFramesSerialized() {
	FramesSerialized++
}

func IncParseErrors() {
	ParseErrors++
}

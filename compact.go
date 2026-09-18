package ursadb

type Compact string

func (c Compact) Valid() bool {
	return c == All || c == Smart
}

const (
	All   Compact = "all"
	Smart Compact = "smart"
)

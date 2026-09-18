package ursadb

// Type represents the various index types.
// It is recommended to use GRAM3, TEXT4, WIDE8 and, if space permits, HASH4 index types.
type Type string

func (t Type) Valid() bool {
	return t == GRAM3 ||
		t == TEXT4 ||
		t == HASH4 ||
		t == WIDE8
}

const (
	// GRAM3 is a classic trigrams.
	// GRAM3 is the most basic and important index, it should be used in all situations.
	//
	// https://cert-polska.github.io/ursadb/indextypes.html#gram3-index
	GRAM3 Type = "gram3"
	// TEXT4 behave like 4grams for text, but can't query anything else.
	// TEXT4 and WIDE8 indexes are very useful for textual data.
	// They don’t need a lot of space and improve results significantly, so they are almost always a good idea.
	//
	// https://cert-polska.github.io/ursadb/indextypes.html#text4-index
	TEXT4 Type = "text4"
	// WIDE8 behave like 4grams for utf16 ascii text, but can’t query anything else.
	// TEXT4 and WIDE8 indexes are very useful for textual data.
	// They don’t need a lot of space and improve results significantly, so they are almost always a good idea.
	//
	// https://cert-polska.github.io/ursadb/indextypes.html#wide8
	WIDE8 Type = "wide8"
	// HASH4 is a 4grams packed into three bytes (with collisions).
	// HASH4 is the most complicated type.
	// It’s not strictly necessary, and ursadb will work without it smoothly.
	// But if you have enough disk space and can afford extra processing time, it’ll reduce the number of false positives (and, in turn, make ursadb faster).
	//
	// https://cert-polska.github.io/ursadb/indextypes.html#hash4
	HASH4 Type = "hash4"
)

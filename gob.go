package main

import (
	"encoding/gob"
	"io"
	_ "net/http/pprof"

	"golang.org/x/crypto/blake2b"
)

type CompactGenome struct {
	Name     string
	Variants []tileVariantID
}

type LibraryEntry struct {
	TagSet         [][]byte
	CompactGenomes []CompactGenome
	TileVariants   []struct {
		Tag      tagID
		Blake2b  [blake2b.Size]byte
		Sequence []byte
	}
}

func ReadCompactGenomes(rdr io.Reader) ([]CompactGenome, error) {
	dec := gob.NewDecoder(rdr)
	var ret []CompactGenome
	for {
		var ent LibraryEntry
		err := dec.Decode(&ent)
		if err == io.EOF {
			return ret, nil
		} else if err != nil {
			return nil, err
		}
		ret = append(ret, ent.CompactGenomes...)
	}
}

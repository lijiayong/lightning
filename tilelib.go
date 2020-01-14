package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"io"
	"log"
	"sync"
)

type tileVariantID int32 // 1-based

type tileLibRef struct {
	tag     tagID
	variant tileVariantID
}

type tileSeq map[string][]tileLibRef

type tileLibrary struct {
	taglib  *tagLibrary
	variant [][][md5.Size]byte
	// count [][]int
	// seq map[[md5.Size]byte][]byte

	mtx sync.Mutex
}

func (tilelib *tileLibrary) TileFasta(filelabel string, rdr io.Reader) (tileSeq, error) {
	ret := tileSeq{}
	var wg sync.WaitGroup
	flush := func(seqlabel string, fasta []byte) {
		defer wg.Done()
		var path []tileLibRef
		if len(fasta) == 0 {
			return
		}
		tilestart := -1        // position in fasta of tile that ends here
		tiletagid := tagID(-1) // tag id starting tile that ends here
		tilelib.taglib.FindAll(fasta, func(id tagID, pos int) {
			if tilestart >= 0 {
				path = append(path, tilelib.getRef(tiletagid, fasta[tilestart:pos]))
			}
			tilestart = pos
			tiletagid = id
		})
		if tiletagid >= 0 {
			path = append(path, tilelib.getRef(tiletagid, fasta[tilestart:]))
		}
		ret[seqlabel] = path
		log.Printf("%s %s tiled with path len %d", filelabel, seqlabel, len(path))
	}
	var fasta []byte
	var seqlabel string
	scanner := bufio.NewScanner(rdr)
	for scanner.Scan() {
		buf := scanner.Bytes()
		if len(buf) == 0 || buf[0] == '>' {
			wg.Add(1)
			go flush(seqlabel, fasta)
			fasta = nil
			seqlabel = string(buf[1:])
		} else {
			fasta = append(fasta, bytes.ToLower(buf)...)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	wg.Add(1)
	go flush(seqlabel, fasta)
	wg.Wait()
	return ret, nil
}

// Return a tileLibRef for a tile with the given tag and sequence,
// adding the sequence to the library if needed.
func (tilelib *tileLibrary) getRef(tag tagID, seq []byte) tileLibRef {
	tilelib.mtx.Lock()
	defer tilelib.mtx.Unlock()
	// if tilelib.seq == nil {
	// 	tilelib.seq = map[[md5.Size]byte][]byte{}
	// }
	if len(tilelib.variant) <= int(tag) {
		tilelib.variant = append(tilelib.variant, make([][][md5.Size]byte, int(tag)-len(tilelib.variant)+1)...)
	}
	seqhash := md5.Sum(seq)
	for i, varhash := range tilelib.variant[tag] {
		if varhash == seqhash {
			return tileLibRef{tag: tag, variant: tileVariantID(i + 1)}
		}
	}
	tilelib.variant[tag] = append(tilelib.variant[tag], seqhash)
	// tilelib.seq[seqhash] = append([]byte(nil), seq...)
	return tileLibRef{tag: tag, variant: tileVariantID(len(tilelib.variant[tag]))}
}

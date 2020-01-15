package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"sync"

	"golang.org/x/crypto/blake2b"
)

type tileVariantID int32 // 1-based

type tileLibRef struct {
	tag     tagID
	variant tileVariantID
}

type tileSeq map[string][]tileLibRef

type tileLibrary struct {
	taglib  *tagLibrary
	variant [][][blake2b.Size]byte
	// count [][]int
	// seq map[[blake2b.Size]byte][]byte

	mtx sync.Mutex
}

func (tilelib *tileLibrary) TileFasta(filelabel string, rdr io.Reader) (tileSeq, error) {
	ret := tileSeq{}
	type jobT struct {
		label string
		fasta []byte
	}
	todo := make(chan jobT)
	scanner := bufio.NewScanner(rdr)
	go func() {
		defer close(todo)
		var fasta []byte
		var seqlabel string
		for scanner.Scan() {
			buf := scanner.Bytes()
			if len(buf) == 0 || buf[0] == '>' {
				todo <- jobT{seqlabel, fasta}
				seqlabel, fasta = string(buf[1:]), nil
				log.Printf("%s %s reading fasta", filelabel, seqlabel)
			} else {
				fasta = append(fasta, bytes.ToLower(buf)...)
			}
		}
		todo <- jobT{seqlabel, fasta}
	}()
	for job := range todo {
		if len(job.fasta) == 0 {
			continue
		}
		log.Printf("%s %s tiling", filelabel, job.label)
		var path []tileLibRef
		tilestart := -1        // position in fasta of tile that ends here
		tiletagid := tagID(-1) // tag id starting tile that ends here
		tilelib.taglib.FindAll(job.fasta, func(id tagID, pos int) {
			if tilestart >= 0 {
				path = append(path, tilelib.getRef(tiletagid, job.fasta[tilestart:pos]))
			}
			tilestart = pos
			tiletagid = id
		})
		if tiletagid >= 0 {
			path = append(path, tilelib.getRef(tiletagid, job.fasta[tilestart:]))
		}
		ret[job.label] = path
		log.Printf("%s %s tiled with path len %d", filelabel, job.label, len(path))
	}
	return ret, scanner.Err()
}

// Return a tileLibRef for a tile with the given tag and sequence,
// adding the sequence to the library if needed.
func (tilelib *tileLibrary) getRef(tag tagID, seq []byte) tileLibRef {
	tilelib.mtx.Lock()
	defer tilelib.mtx.Unlock()
	// if tilelib.seq == nil {
	// 	tilelib.seq = map[[blake2b.Size]byte][]byte{}
	// }
	if len(tilelib.variant) <= int(tag) {
		tilelib.variant = append(tilelib.variant, make([][][blake2b.Size]byte, int(tag)-len(tilelib.variant)+1)...)
	}
	hash, err := blake2b.New(32, nil)
	if err != nil {
		panic(err)
	}
	_, err = hash.Write(seq)
	if err != nil {
		panic(err)
	}
	var seqhash [blake2b.Size]byte
	copy(seqhash[:], hash.Sum(nil))
	for i, varhash := range tilelib.variant[tag] {
		if varhash == seqhash {
			return tileLibRef{tag: tag, variant: tileVariantID(i + 1)}
		}
	}
	tilelib.variant[tag] = append(tilelib.variant[tag], seqhash)
	// tilelib.seq[seqhash] = append([]byte(nil), seq...)
	return tileLibRef{tag: tag, variant: tileVariantID(len(tilelib.variant[tag]))}
}

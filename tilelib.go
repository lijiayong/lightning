package main

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
)

type tileVariantID uint16 // 1-based

type tileLibRef struct {
	tag     tagID
	variant tileVariantID
}

type tileSeq map[string][]tileLibRef

func (tseq tileSeq) Variants() []tileVariantID {
	maxtag := 0
	for _, refs := range tseq {
		for _, ref := range refs {
			if maxtag < int(ref.tag) {
				maxtag = int(ref.tag)
			}
		}
	}
	vars := make([]tileVariantID, maxtag+1)
	for _, refs := range tseq {
		for _, ref := range refs {
			vars[int(ref.tag)] = ref.variant
		}
	}
	return vars
}

type tileLibrary struct {
	taglib  *tagLibrary
	variant [][][blake2b.Size256]byte
	// count [][]int
	// seq map[[blake2b.Size]byte][]byte
	variants int

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
				log.Debugf("%s %s reading fasta", filelabel, seqlabel)
			} else {
				fasta = append(fasta, bytes.ToLower(buf)...)
			}
		}
		todo <- jobT{seqlabel, fasta}
	}()
	path := make([]tileLibRef, 2000000)
	totalPathLen := 0
	skippedSequences := 0
	for job := range todo {
		if len(job.fasta) == 0 {
			continue
		} else if strings.Contains(job.label, "_") {
			skippedSequences++
			continue
		}
		log.Debugf("%s %s tiling", filelabel, job.label)
		path = path[:0]
		tilestart := -1        // position in fasta of tile that ends here
		tiletagid := tagID(-1) // tag id starting tile that ends here
		tilelib.taglib.FindAll(job.fasta, func(id tagID, pos, taglen int) {
			if tilestart >= 0 {
				path = append(path, tilelib.getRef(tiletagid, job.fasta[tilestart:pos+taglen]))
			}
			tilestart = pos
			tiletagid = id
		})
		if tiletagid >= 0 {
			path = append(path, tilelib.getRef(tiletagid, job.fasta[tilestart:]))
		}
		pathcopy := make([]tileLibRef, len(path))
		copy(pathcopy, path)
		ret[job.label] = pathcopy
		log.Debugf("%s %s tiled with path len %d", filelabel, job.label, len(path))
		totalPathLen += len(path)
	}
	log.Printf("%s tiled with total path len %d in %d sequences (skipped %d sequences with '_' in name)", filelabel, totalPathLen, len(ret), skippedSequences)
	return ret, scanner.Err()
}

func (tilelib *tileLibrary) Len() int {
	tilelib.mtx.Lock()
	defer tilelib.mtx.Unlock()
	return tilelib.variants
}

// Return a tileLibRef for a tile with the given tag and sequence,
// adding the sequence to the library if needed.
func (tilelib *tileLibrary) getRef(tag tagID, seq []byte) tileLibRef {
	for _, b := range seq {
		if b != 'a' && b != 'c' && b != 'g' && b != 't' {
			// return "tile not found" if seq has any
			// no-calls
			return tileLibRef{tag: tag}
		}
	}
	tilelib.mtx.Lock()
	defer tilelib.mtx.Unlock()
	// if tilelib.seq == nil {
	// 	tilelib.seq = map[[blake2b.Size]byte][]byte{}
	// }
	if tilelib.variant == nil {
		tilelib.variant = make([][][blake2b.Size256]byte, tilelib.taglib.Len())
	}
	seqhash := blake2b.Sum256(seq)
	for i, varhash := range tilelib.variant[tag] {
		if varhash == seqhash {
			return tileLibRef{tag: tag, variant: tileVariantID(i + 1)}
		}
	}
	tilelib.variants++
	tilelib.variant[tag] = append(tilelib.variant[tag], seqhash)
	// tilelib.seq[seqhash] = append([]byte(nil), seq...)
	return tileLibRef{tag: tag, variant: tileVariantID(len(tilelib.variant[tag]))}
}

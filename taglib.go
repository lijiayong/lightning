package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

const tagmapKeySize = 24

type tagmapKey [tagmapKeySize]byte

type tagID int32

type tagInfo struct {
	id     tagID // 0-based position in input tagset
	tagseq []byte
}

type tagLibrary struct {
	tagmap map[tagmapKey]tagInfo
	keylen int
}

func (taglib *tagLibrary) Load(rdr io.Reader) error {
	var seqs [][]byte
	scanner := bufio.NewScanner(rdr)
	for scanner.Scan() {
		data := scanner.Bytes()
		if len(data) > 0 && data[0] == '>' {
		} else {
			seqs = append(seqs, append([]byte(nil), data...))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return taglib.setTags(seqs)
}

type tagMatch struct {
	id  tagID
	pos int
}

func (taglib *tagLibrary) FindAll(buf []byte, fn func(id tagID, pos int)) {
	var key tagmapKey
	for i := 0; i <= len(buf)-taglib.keylen; i++ {
		copy(key[:taglib.keylen], buf[i:])
		if taginfo, ok := taglib.tagmap[key]; !ok {
			continue
		} else if len(taginfo.tagseq) > taglib.keylen && (len(buf) < i+len(taginfo.tagseq) || !bytes.Equal(taginfo.tagseq, buf[i:i+len(taginfo.tagseq)])) {
			// key portion matches, but not the entire tag
			continue
		} else {
			fn(taginfo.id, i)
		}
	}
}

func (taglib *tagLibrary) Len() int {
	return len(taglib.tagmap)
}

func (taglib *tagLibrary) setTags(tags [][]byte) error {
	taglib.keylen = tagmapKeySize
	for _, t := range tags {
		if l := len(t); taglib.keylen > l {
			taglib.keylen = l
		}
	}
	taglib.tagmap = map[tagmapKey]tagInfo{}
	for i, t := range tags {
		t = bytes.ToLower(t)
		var key tagmapKey
		copy(key[:], t[:taglib.keylen])
		if _, ok := taglib.tagmap[key]; ok {
			return fmt.Errorf("first %d bytes of tag %d (%s) are not unique", taglib.keylen, i, key)
		}
		taglib.tagmap[key] = tagInfo{tagID(i), t}
	}
	return nil
}

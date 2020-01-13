package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/check.v1"
)

func Test(t *testing.T) { check.TestingT(t) }

type taglibSuite struct{}

var _ = check.Suite(&taglibSuite{})

func (s *taglibSuite) TestFindAllTinyData(c *check.C) {
	pr, pw, err := os.Pipe()
	c.Assert(err, check.IsNil)
	go func() {
		defer pw.Close()
		fmt.Fprintf(pw, `>0000.00
ggagaactgtgctccgccttcaga
acacatgctagcgcgtcggggtgg
gactctagcagagtggccagccac
`)
	}()
	var taglib tagLibrary
	err = taglib.Load(pr)
	c.Assert(err, check.IsNil)
	haystack := []byte(`ggagaactgtgctccgccttcagaccccccccccccccccccccacacatgctagcgcgtcggggtgggggggggggggggggggggggggggactctagcagagtggccagccac`)
	var matches []tagMatch
	taglib.FindAll(haystack, func(id tagID, pos int) {
		matches = append(matches, tagMatch{id, pos})
	})
	c.Check(matches, check.DeepEquals, []tagMatch{{0, 0}, {1, 44}, {2, 92}})
}

func (s *taglibSuite) TestFindAllRealisticSize(c *check.C) {
	start := time.Now()
	acgt := []byte{'a', 'c', 'g', 't'}
	haystack := make([]byte, 25000000) // ~1/2 smallest human chromosome
	c.Logf("@%v haystack", time.Since(start))
	rand.Read(haystack)
	for i := range haystack {
		haystack[i] = acgt[int(haystack[i]&3)]
	}

	tagcount := 12500
	tagsize := 24
	var tags []string
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		w := bufio.NewWriter(pw)
		defer w.Flush()
		used := map[string]bool{}
		fmt.Fprint(w, ">000\n")
		for i := 0; len(tags) < tagcount; i += (len(haystack) - tagsize) / tagcount {
			i := i
			tag := haystack[i : i+tagsize]
			for used[string(tag)] {
				i++
				tag = haystack[i : i+tagsize]
			}
			used[string(tag)] = true
			tags = append(tags, strings.ToLower(string(tag)))
			w.Write(tag)
			w.Write([]byte{'\n'})
		}
	}()
	c.Logf("@%v build library", time.Since(start))
	var taglib tagLibrary
	err := taglib.Load(pr)
	c.Assert(err, check.IsNil)
	c.Logf("@%v find tags in input", time.Since(start))
	var matches []tagMatch
	taglib.FindAll(haystack, func(id tagID, pos int) {
		matches = append(matches, tagMatch{id, pos})
	})
	c.Logf("@%v done", time.Since(start))
	c.Check(matches[0], check.Equals, tagMatch{0, 0})
	c.Check(matches[1].id, check.Equals, tagID(1))
}

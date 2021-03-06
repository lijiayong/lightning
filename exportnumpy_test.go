package main

import (
	"bytes"
	"os"

	"github.com/kshedden/gonpy"
	"gopkg.in/check.v1"
)

type exportSuite struct{}

var _ = check.Suite(&exportSuite{})

func (s *exportSuite) TestFastaToNumpy(c *check.C) {
	var buffer bytes.Buffer
	exited := (&importer{}).RunCommand("import", []string{"-local=true", "-tag-library", "testdata/tags", "-ref", "testdata/ref", "testdata/a.1.fasta"}, &bytes.Buffer{}, &buffer, os.Stderr)
	c.Assert(exited, check.Equals, 0)
	var output bytes.Buffer
	exited = (&exportNumpy{}).RunCommand("export-numpy", []string{"-local=true"}, &buffer, &output, os.Stderr)
	c.Check(exited, check.Equals, 0)
	npy, err := gonpy.NewReader(&output)
	c.Assert(err, check.IsNil)
	variants, err := npy.GetUint16()
	c.Assert(err, check.IsNil)
	for i := 0; i < 4; i += 2 {
		if variants[i] == 1 {
			c.Check(variants[i+1], check.Equals, uint16(2), check.Commentf("i=%d, v=%v", i, variants))
		} else {
			c.Check(variants[i], check.Equals, uint16(2), check.Commentf("i=%d, v=%v", i, variants))
		}
	}
	for i := 4; i < 9; i += 2 {
		c.Check(variants[i], check.Equals, uint16(1), check.Commentf("i=%d, v=%v", i, variants))
	}
}

func sortUints(variants []uint16) {
	for i := 0; i < len(variants); i += 2 {
		if variants[i] > variants[i+1] {
			for j := 0; j < len(variants); j++ {
				variants[j], variants[j+1] = variants[j+1], variants[j]
			}
			return
		}
	}
}

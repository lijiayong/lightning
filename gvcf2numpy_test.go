package main

import (
	"bytes"
	"os"

	"github.com/kshedden/gonpy"
	"gopkg.in/check.v1"
)

type gvcf2numpySuite struct{}

var _ = check.Suite(&gvcf2numpySuite{})

func (s *gvcf2numpySuite) TestFastaToNumpy(c *check.C) {
	var stdout bytes.Buffer
	var cmd gvcf2numpy
	exited := cmd.RunCommand("gvcf2numpy", []string{"-tag-library", "testdata/tags", "-ref", "testdata/ref", "testdata/a.1.fasta"}, &bytes.Buffer{}, &stdout, os.Stderr)
	c.Check(exited, check.Equals, 0)
	npy, err := gonpy.NewReader(&stdout)
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

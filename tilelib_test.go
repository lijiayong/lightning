package main

import (
	"bytes"

	"gopkg.in/check.v1"
)

type tilelibSuite struct{}

var _ = check.Suite(&tilelibSuite{})

func (s *tilelibSuite) TestSkipOOO(c *check.C) {
	var taglib tagLibrary
	err := taglib.Load(bytes.NewBufferString(`>0000.00
ggagaactgtgctccgccttcaga
acacatgctagcgcgtcggggtgg
gactctagcagagtggccagccac
cctcccgagccgagccacccgtca
gttattaataataacttatcatca
`))
	c.Assert(err, check.IsNil)

	// tags appear in seq: 4, 0, 2 (but skipOOO is false)
	tilelib := &tileLibrary{taglib: &taglib, skipOOO: false}
	tseq, err := tilelib.TileFasta("test-label", bytes.NewBufferString(`>test-seq
gttattaataataacttatcatca
ggggggggggggggggggggggg
ggagaactgtgctccgccttcaga
cccccccccccccccccccc
gactctagcagagtggccagccac
`))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{4, 1}, {0, 1}, {2, 1}}})

	// tags appear in seq: 0, 1, 2 -> don't skip
	tilelib = &tileLibrary{taglib: &taglib, skipOOO: true}
	tseq, err = tilelib.TileFasta("test-label", bytes.NewBufferString(`>test-seq
ggagaactgtgctccgccttcaga
cccccccccccccccccccc
acacatgctagcgcgtcggggtgg
ggggggggggggggggggggggg
gactctagcagagtggccagccac
`))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}, {2, 1}}})

	// tags appear in seq: 2, 3, 4 -> don't skip
	tilelib = &tileLibrary{taglib: &taglib, skipOOO: true}
	tseq, err = tilelib.TileFasta("test-label", bytes.NewBufferString(`>test-seq
gactctagcagagtggccagccac
cccccccccccccccccccc
cctcccgagccgagccacccgtca
ggggggggggggggggggggggg
gttattaataataacttatcatca
`))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{2, 1}, {3, 1}, {4, 1}}})

	// tags appear in seq: 4, 0, 2 -> skip 4
	tilelib = &tileLibrary{taglib: &taglib, skipOOO: true}
	tseq, err = tilelib.TileFasta("test-label", bytes.NewBufferString(`>test-seq
gttattaataataacttatcatca
cccccccccccccccccccc
ggagaactgtgctccgccttcaga
ggggggggggggggggggggggg
gactctagcagagtggccagccac
`))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {2, 1}}})

	// tags appear in seq: 0, 2, 1 -> skip 2
	tilelib = &tileLibrary{taglib: &taglib, skipOOO: true}
	tseq, err = tilelib.TileFasta("test-label", bytes.NewBufferString(`>test-seq
ggagaactgtgctccgccttcaga
cccccccccccccccccccc
gactctagcagagtggccagccac
ggggggggggggggggggggggg
acacatgctagcgcgtcggggtgg
`))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}}})

	// tags appear in seq: 0, 1, 1, 2 -> skip second tag1
	tilelib = &tileLibrary{taglib: &taglib, skipOOO: true}
	tseq, err = tilelib.TileFasta("test-label", bytes.NewBufferString(`>test-seq
ggagaactgtgctccgccttcaga
cccccccccccccccccccc
acacatgctagcgcgtcggggtgg
ggggggggggggggggggggggg
acacatgctagcgcgtcggggtgg
ggggggggggggggggggggggg
gactctagcagagtggccagccac
`))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}, {2, 1}}})

	// tags appear in seq: 0, 1, 3 -> don't skip
	tilelib = &tileLibrary{taglib: &taglib, skipOOO: true}
	tseq, err = tilelib.TileFasta("test-label", bytes.NewBufferString(`>test-seq
ggagaactgtgctccgccttcaga
cccccccccccccccccccc
acacatgctagcgcgtcggggtgg
ggggggggggggggggggggggg
cctcccgagccgagccacccgtca
`))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}, {3, 1}}})
}

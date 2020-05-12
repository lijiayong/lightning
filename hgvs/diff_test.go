package hgvs

import (
	"testing"

	"gopkg.in/check.v1"
)

func Test(t *testing.T) { check.TestingT(t) }

type diffSuite struct{}

var _ = check.Suite(&diffSuite{})

func (s *diffSuite) TestDiff(c *check.C) {
	for _, trial := range []struct {
		a      string
		b      string
		expect []string
	}{
		{
			a:      "aaaaaaaaaa",
			b:      "aaaaCaaaaa",
			expect: []string{"5a>C"},
		},
		{
			a:      "aaaacGcaaa",
			b:      "aaaaccaaa",
			expect: []string{"6del"},
		},
		{
			a:      "aaaacGGcaaa",
			b:      "aaaaccaaa",
			expect: []string{"6_7del"},
		},
		{
			a:      "aaaac",
			b:      "aaaa",
			expect: []string{"5del"},
		},
		{
			a:      "aaaa",
			b:      "aaCaa",
			expect: []string{"2_3insC"},
		},
		{
			a:      "aaGGGtt",
			b:      "aaCCCtt",
			expect: []string{"3_5delinsCCC"},
		},
		{
			a:      "aa",
			b:      "aaCCC",
			expect: []string{"2_3insCCC"},
		},
		{
			a:      "aaGGttAAtttt",
			b:      "aaCCttttttC",
			expect: []string{"3_4delinsCC", "7_8del", "12_13insC"},
		},
	} {
		c.Log(trial)
		var vars []string
		for _, v := range Diff(trial.a, trial.b) {
			vars = append(vars, v.String())
		}
		c.Check(vars, check.DeepEquals, trial.expect)
	}
}

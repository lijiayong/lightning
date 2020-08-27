package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/arvados/lightning/hgvs"
)

type diffFasta struct{}

func (cmd *diffFasta) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	offset := flags.Int("offset", 0, "coordinate offset")
	timeout := flags.Duration("timeout", 0, "timeout (examples: \"1s\", \"1ms\")")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}
	if len(flags.Args()) != 2 {
		err = fmt.Errorf("usage: %s [options] a.fasta b.fasta", prog)
		return 2
	}

	var fasta [2][]byte
	errs := make(chan error, 2)
	for idx, fnm := range flags.Args() {
		idx, fnm := idx, fnm
		go func() {
			f, err := os.Open(fnm)
			if err != nil {
				errs <- err
				return
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			scanner.Buffer(nil, 640*1024*1024)
			for scanner.Scan() {
				buf := scanner.Bytes()
				if len(buf) > 0 && buf[0] != '>' {
					fasta[idx] = append(fasta[idx], bytes.ToUpper(buf)...)
				}
			}
			errs <- scanner.Err()
		}()
	}
	for range flags.Args() {
		if err = <-errs; err != nil {
			return 1
		}
	}

	afasta := string(fasta[0])
	bfasta := string(fasta[1])
	variants, timedOut := hgvs.Diff(afasta, bfasta, *timeout)
	if *offset != 0 {
		for i := range variants {
			variants[i].Position += *offset
		}
	}
	switch len(variants) {
	case 0:
		fmt.Fprintf(stdout, "=,")
	default:
		var hgvsannos, vcfs []string
		var vcfPosition int
		var vcfRef, vcfNew string
		for _, v := range variants {
			hgvsannos = append(hgvsannos, v.String())
			originalPosition := v.Position - *offset
			if (len(v.Ref) == 0 || len(v.New) == 0) && originalPosition > 1 {
				vcfPosition = v.Position - 1
				vcfRef = fmt.Sprintf("%s%s", string(afasta[originalPosition-2]), v.Ref)
				vcfNew = fmt.Sprintf("%s%s", string(afasta[originalPosition-2]), v.New)
			} else {
				vcfPosition, vcfRef, vcfNew = v.Position, v.Ref, v.New
			}
			vcfs = append(vcfs, fmt.Sprintf("%d|%s|%s", vcfPosition, vcfRef, vcfNew))
		}
		hgvsanno := strings.Join(hgvsannos, ";")
		vcf := strings.Join(vcfs, ";")
		switch len(variants) {
		case 1:
			fmt.Fprintf(stdout, "%s,%s", hgvsanno, vcf)
		default:
			fmt.Fprintf(stdout, "[%s],%s", hgvsanno, vcf)
		}
	}
	if timedOut {
		fmt.Fprintf(stdout, ",timedOut\n")
	} else {
		fmt.Fprintf(stdout, "\n")
	}
	return 0
}

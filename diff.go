package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/curii/lightning/hgvs"
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
	sequence := flags.String("sequence", "chr1", "sequence label")
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

	variants := hgvs.Diff(string(fasta[0]), string(fasta[1]))
	if *offset != 0 {
		for i := range variants {
			variants[i].Position += *offset
		}
	}
	for _, v := range variants {
		fmt.Fprintf(stdout, "%s:g.%s\t%s\t%d\t%s\t%s\n", *sequence, v.String(), *sequence, v.Position, v.Ref, v.New)
	}
	return 0
}

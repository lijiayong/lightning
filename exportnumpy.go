package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/kshedden/gonpy"
)

type exportNumpy struct {
	output io.Writer
}

func (cmd *exportNumpy) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}
	cmd.output = stdout

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	cgs, err := ReadCompactGenomes(stdin)
	if err != nil {
		return 1
	}
	cols := 0
	for _, cg := range cgs {
		if cols < len(cg.Variants) {
			cols = len(cg.Variants)
		}
	}
	rows := len(cgs)
	out := make([]uint16, rows*cols)
	for row, cg := range cgs {
		for i, v := range cg.Variants {
			out[row*cols+i] = uint16(v)
		}
	}
	w := bufio.NewWriter(cmd.output)
	npw, err := gonpy.NewWriter(nopCloser{w})
	if err != nil {
		return 1
	}
	npw.Shape = []int{rows, cols}
	npw.WriteUint16(out)
	err = w.Flush()
	if err != nil {
		return 1
	}
	return 0
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"git.arvados.org/arvados.git/sdk/go/arvados"
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
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	inputFilename := flags.String("i", "", "input `file`")
	outputFilename := flags.String("o", "", "output `file`")
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

	if !*runlocal {
		if *outputFilename != "" {
			err = errors.New("cannot specify output file in container mode: not implemented")
			return 1
		}
		runner := arvadosContainerRunner{
			Name:        "lightning export-numpy",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         64000000000,
			VCPUs:       2,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"export-numpy", "-local=true", "-i", *inputFilename, "-o", "/mnt/output/library.npy"}
		err = runner.Run()
		if err != nil {
			return 1
		}
		return 0
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

	var output io.WriteCloser
	if *outputFilename == "" {
		output = nopCloser{cmd.output}
	} else {
		output, err = os.OpenFile(*outputFilename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer output.Close()
	}
	bufw := bufio.NewWriter(output)
	npw, err := gonpy.NewWriter(nopCloser{bufw})
	if err != nil {
		return 1
	}
	npw.Shape = []int{rows, cols}
	npw.WriteUint16(out)
	err = bufw.Flush()
	if err != nil {
		return 1
	}
	err = output.Close()
	if err != nil {
		return 1
	}
	return 0
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

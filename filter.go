package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
)

type filterer struct {
	output io.Writer
}

func (cmd *filterer) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	maxvariants := flags.Int("max-variants", -1, "drop tiles with more than `N` variants")
	mincoverage := flags.Float64("min-coverage", 1, "drop tiles with coverage less than `P` across all haplotypes (0 < P ≤ 1)")
	maxtag := flags.Int("max-tag", -1, "drop tiles with tag ID > `N`")
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

	log.Print("reading")
	cgs, err := ReadCompactGenomes(stdin)
	if err != nil {
		return 1
	}
	log.Printf("reading done, %d genomes", len(cgs))

	log.Print("filtering")
	ntags := 0
	for _, cg := range cgs {
		if ntags < len(cg.Variants)/2 {
			ntags = len(cg.Variants) / 2
		}
		if *maxvariants < 0 {
			continue
		}
		maxVariantID := tileVariantID(*maxVariants)
		for idx, variant := range cg.Variants {
			if variant > maxVariantID {
				for _, cg := range cgs {
					if len(cg.Variants) > idx {
						cg.Variants[idx & ^1] = 0
						cg.Variants[idx|1] = 0
					}
				}
			}
		}
	}

	if *maxtag >= 0 && ntags > *maxtag {
		ntags = *maxtag
		for i, cg := range cgs {
			if len(cg.Variants) > *maxtag*2 {
				cgs[i].Variants = cg.Variants[:*maxtag*2]
			}
		}
	}

	if *mincoverage < 1 {
		mincov := int(*mincoverage * float64(len(cgs)*2))
		cov := make([]int, ntags)
		for _, cg := range cgs {
			for idx, variant := range cg.Variants {
				if variant > 0 {
					cov[idx>>1]++
				}
			}
		}
		for tag, c := range cov {
			if c < mincov {
				for _, cg := range cgs {
					if len(cg.Variants) > tag*2 {
						cg.Variants[tag*2] = 0
						cg.Variants[tag*2+1] = 0
					}
				}
			}
		}
	}

	log.Print("filtering done")

	w := bufio.NewWriter(cmd.output)
	enc := gob.NewEncoder(w)
	log.Print("writing")
	err = enc.Encode(LibraryEntry{
		CompactGenomes: cgs,
	})
	if err != nil {
		return 1
	}
	log.Print("writing done")
	err = w.Flush()
	if err != nil {
		return 1
	}
	return 0
}

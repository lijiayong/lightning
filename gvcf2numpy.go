package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
)

type gvcf2numpy struct {
	tagLibraryFile string
	refFile        string
	output         io.Writer
	outputMtx      sync.Mutex
}

func (cmd *gvcf2numpy) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cmd.tagLibraryFile, "tag-library", "", "tag library fasta `file`")
	flags.StringVar(&cmd.refFile, "ref", "", "reference fasta `file`")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	} else if cmd.refFile == "" || cmd.tagLibraryFile == "" {
		fmt.Fprintln(os.Stderr, "cannot run without -tag-library and -ref arguments")
		return 2
	} else if flags.NArg() == 0 {
		flags.Usage()
		return 2
	}
	cmd.output = stdout

	log.Printf("tag library %s load starting", cmd.tagLibraryFile)
	f, err := os.Open(cmd.tagLibraryFile)
	if err != nil {
		return 1
	}
	var rdr io.ReadCloser = f
	if strings.HasSuffix(cmd.tagLibraryFile, ".gz") {
		rdr, err = gzip.NewReader(f)
		if err != nil {
			err = fmt.Errorf("%s: gzip: %s", cmd.tagLibraryFile, err)
			return 1
		}
	}
	var taglib tagLibrary
	err = taglib.Load(rdr)
	if err != nil {
		return 1
	}
	if taglib.Len() < 1 {
		err = fmt.Errorf("cannot tile: tag library is empty")
		return 1
	}
	log.Printf("tag library %s load done", cmd.tagLibraryFile)

	tilelib := tileLibrary{taglib: &taglib}
	err = cmd.tileGVCFs(&tilelib, flags.Args())
	if err != nil {
		return 1
	}
	return 0
}

func (cmd *gvcf2numpy) tileGVCFs(tilelib *tileLibrary, infiles []string) error {
	limit := make(chan bool, runtime.NumCPU())
	errs := make(chan error)
	var wg sync.WaitGroup
	for _, infile := range infiles {
		for phase := 0; phase < 2; phase++ {
			wg.Add(1)
			go func(infile string, phase int) {
				defer wg.Done()
				limit <- true
				defer func() { <-limit }()
				log.Printf("%s phase %d starting", infile, phase+1)
				defer log.Printf("%s phase %d done", infile, phase+1)
				tseq, err := cmd.tileGVCF(tilelib, infile, phase)
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				cmd.printVariants(fmt.Sprintf("%s phase %d", infile, phase+1), tseq)
			}(infile, phase)
		}
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	if err := <-errs; err != nil {
		return err
	}
	return nil
}

func (cmd *gvcf2numpy) printVariants(label string, tseq map[string][]tileLibRef) {
	maxtag := tagID(-1)
	for _, path := range tseq {
		for _, tvar := range path {
			if maxtag < tvar.tag {
				maxtag = tvar.tag
			}
		}
	}
	variant := make([]tileVariantID, maxtag+1)
	for _, path := range tseq {
		for _, tvar := range path {
			variant[tvar.tag] = tvar.variant
		}
	}

	{
		excerpt := variant
		if len(excerpt) > 100 {
			excerpt = excerpt[:100]
		}
		log.Printf("%q %v\n", label, excerpt)
	}
	cmd.outputMtx.Lock()
	defer cmd.outputMtx.Unlock()
	fmt.Fprintf(cmd.output, "%q %v\n", label, variant)
}

func (cmd *gvcf2numpy) tileGVCF(tilelib *tileLibrary, infile string, phase int) (tileseq tileSeq, err error) {
	args := []string{"bcftools", "consensus", "--fasta-ref", cmd.refFile, "-H", fmt.Sprint(phase + 1), infile}
	indexsuffix := ".tbi"
	if _, err := os.Stat(infile + ".csi"); err == nil {
		indexsuffix = ".csi"
	}
	if out, err := exec.Command("docker", "image", "ls", "-q", "lightning-runtime").Output(); err == nil && len(out) > 0 {
		args = append([]string{
			"docker", "run", "--rm",
			"--log-driver=none",
			"--volume=" + infile + ":" + infile + ":ro",
			"--volume=" + infile + indexsuffix + ":" + infile + indexsuffix + ":ro",
			"--volume=" + cmd.refFile + ":" + cmd.refFile + ":ro",
			"lightning-runtime",
		}, args...)
	}
	consensus := exec.Command(args[0], args[1:]...)
	consensus.Stderr = os.Stderr
	stdout, err := consensus.StdoutPipe()
	if err != nil {
		return
	}
	err = consensus.Start()
	if err != nil {
		return
	}
	tileseq, err = tilelib.TileFasta(fmt.Sprintf("%s phase %d", infile, phase+1), stdout)
	if err != nil {
		return
	}
	err = stdout.Close()
	if err != nil {
		return
	}
	err = consensus.Wait()
	if err != nil {
		err = fmt.Errorf("%s phase %d: bcftools: %s", infile, phase, err)
		return
	}
	return
}

package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kshedden/gonpy"
)

type gvcf2numpy struct {
	tagLibraryFile string
	refFile        string
	output         io.Writer
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

	infiles, err := listVCFFiles(flags.Args())
	if err != nil {
		return 1
	}

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
	tseqs, err := cmd.tileGVCFs(&tilelib, infiles)
	if err != nil {
		return 1
	}
	err = cmd.printVariants(tseqs)
	if err != nil {
		return 1
	}
	return 0
}

func listVCFFiles(paths []string) (files []string, err error) {
	for _, path := range paths {
		if fi, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("%s: stat failed: %s", path, err)
		} else if !fi.IsDir() {
			files = append(files, path)
			continue
		}
		d, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("%s: open failed: %s", path, err)
		}
		defer d.Close()
		names, err := d.Readdirnames(0)
		if err != nil {
			return nil, fmt.Errorf("%s: readdir failed: %s", path, err)
		}
		sort.Strings(names)
		for _, name := range names {
			if strings.HasSuffix(name, ".vcf") || strings.HasSuffix(name, ".vcf.gz") {
				files = append(files, filepath.Join(path, name))
			}
		}
		d.Close()
	}
	for _, file := range files {
		if _, err := os.Stat(file + ".csi"); err == nil {
			continue
		} else if _, err = os.Stat(file + ".tbi"); err == nil {
			continue
		} else {
			return nil, fmt.Errorf("%s: cannot read without .tbi or .csi index file", file)
		}
	}
	return
}

func (cmd *gvcf2numpy) tileGVCFs(tilelib *tileLibrary, infiles []string) ([]tileSeq, error) {
	starttime := time.Now()
	errs := make(chan error, 1)
	tseqs := make([]tileSeq, len(infiles)*2)
	todo := make(chan func() error, len(infiles)*2)
	var wg sync.WaitGroup
	for i, infile := range infiles {
		for phase := 0; phase < 2; phase++ {
			i, infile, phase := i, infile, phase
			todo <- func() (err error) {
				log.Printf("%s phase %d starting", infile, phase+1)
				defer log.Printf("%s phase %d done", infile, phase+1)
				tseqs[i*2+phase], err = cmd.tileGVCF(tilelib, infile, phase)
				return
			}
		}
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fn := range todo {
				if len(errs) > 0 {
					return
				}
				err := fn()
				if err != nil {
					select {
					case errs <- err:
					default:
					}
				}
				remain := len(todo)
				ttl := time.Now().Sub(starttime) * time.Duration(remain) / time.Duration(cap(todo)-remain)
				eta := time.Now().Add(ttl)
				log.Printf("progress %d/%d, eta %v (%v)", cap(todo)-remain, cap(todo), eta, ttl)
			}
		}()
	}
	wg.Wait()
	go close(errs)
	return tseqs, <-errs
}

func (cmd *gvcf2numpy) printVariants(tseqs []tileSeq) error {
	maxtag := tagID(-1)
	for _, tseq := range tseqs {
		for _, path := range tseq {
			for _, tvar := range path {
				if maxtag < tvar.tag {
					maxtag = tvar.tag
				}
			}
		}
	}
	out := make([]uint16, len(tseqs)*int(maxtag+1))
	for i := 0; i < len(tseqs)/2; i++ {
		for phase := 0; phase < 2; phase++ {
			for _, path := range tseqs[i*2+phase] {
				for _, tvar := range path {
					out[2*int(tvar.tag)+phase] = uint16(tvar.variant)
				}
			}
		}
	}
	w := bufio.NewWriter(cmd.output)
	npw, err := gonpy.NewWriter(nopCloser{w})
	if err != nil {
		return err
	}
	npw.Shape = []int{len(tseqs) / 2, 2 * (int(maxtag) + 1)}
	npw.WriteUint16(out)
	return w.Flush()
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

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

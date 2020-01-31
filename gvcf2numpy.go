package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
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

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	infiles, err := listInputFiles(flags.Args())
	if err != nil {
		return 1
	}

	tilelib, err := cmd.loadTileLibrary()
	if err != nil {
		return 1
	}
	go func() {
		for range time.Tick(10 * time.Second) {
			log.Printf("tilelib.Len() == %d", tilelib.Len())
		}
	}()
	variants, err := cmd.tileGVCFs(tilelib, infiles)
	if err != nil {
		return 1
	}
	err = cmd.printVariants(variants)
	if err != nil {
		return 1
	}
	return 0
}

func (cmd *gvcf2numpy) tileFasta(tilelib *tileLibrary, infile string) (tileSeq, error) {
	var input io.ReadCloser
	input, err := os.Open(infile)
	if err != nil {
		return nil, err
	}
	defer input.Close()
	if strings.HasSuffix(infile, ".gz") {
		input, err = gzip.NewReader(input)
		if err != nil {
			return nil, err
		}
		defer input.Close()
	}
	return tilelib.TileFasta(infile, input)
}

func (cmd *gvcf2numpy) loadTileLibrary() (*tileLibrary, error) {
	log.Printf("tag library %s load starting", cmd.tagLibraryFile)
	f, err := os.Open(cmd.tagLibraryFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rdr io.ReadCloser = f
	if strings.HasSuffix(cmd.tagLibraryFile, ".gz") {
		rdr, err = gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("%s: gzip: %s", cmd.tagLibraryFile, err)
		}
		defer rdr.Close()
	}
	var taglib tagLibrary
	err = taglib.Load(rdr)
	if err != nil {
		return nil, err
	}
	if taglib.Len() < 1 {
		return nil, fmt.Errorf("cannot tile: tag library is empty")
	}
	log.Printf("tag library %s load done", cmd.tagLibraryFile)
	return &tileLibrary{taglib: &taglib}, nil
}

func listInputFiles(paths []string) (files []string, err error) {
	for _, path := range paths {
		if fi, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("%s: stat failed: %s", path, err)
		} else if !fi.IsDir() {
			if !strings.HasSuffix(path, ".2.fasta") || strings.HasSuffix(path, ".2.fasta.gz") {
				files = append(files, path)
			}
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
			} else if strings.HasSuffix(name, ".1.fasta") || strings.HasSuffix(name, ".1.fasta.gz") {
				files = append(files, filepath.Join(path, name))
			}
		}
		d.Close()
	}
	for _, file := range files {
		if strings.HasSuffix(file, ".1.fasta") || strings.HasSuffix(file, ".1.fasta.gz") {
			continue
		} else if _, err := os.Stat(file + ".csi"); err == nil {
			continue
		} else if _, err = os.Stat(file + ".tbi"); err == nil {
			continue
		} else {
			return nil, fmt.Errorf("%s: cannot read without .tbi or .csi index file", file)
		}
	}
	return
}

func (cmd *gvcf2numpy) tileGVCFs(tilelib *tileLibrary, infiles []string) ([][]tileVariantID, error) {
	starttime := time.Now()
	errs := make(chan error, 1)
	variants := make([][]tileVariantID, len(infiles)*2)
	todo := make(chan func() error, len(infiles)*2)
	var wg sync.WaitGroup
	for i, infile := range infiles {
		i, infile := i, infile
		if strings.HasSuffix(infile, ".1.fasta") || strings.HasSuffix(infile, ".1.fasta.gz") {
			todo <- func() error {
				log.Printf("%s starting", infile)
				defer log.Printf("%s done", infile)
				tseqs, err := cmd.tileFasta(tilelib, infile)
				variants[i*2] = tseqs.Variants()
				return err
			}
			infile2 := regexp.MustCompile(`\.1\.fasta(\.gz)?$`).ReplaceAllString(infile, `.2.fasta$1`)
			todo <- func() error {
				log.Printf("%s starting", infile2)
				defer log.Printf("%s done", infile2)
				tseqs, err := cmd.tileFasta(tilelib, infile2)
				variants[i*2+1] = tseqs.Variants()
				return err
			}
		} else {
			for phase := 0; phase < 2; phase++ {
				phase := phase
				todo <- func() error {
					log.Printf("%s phase %d starting", infile, phase+1)
					defer log.Printf("%s phase %d done", infile, phase+1)
					tseqs, err := cmd.tileGVCF(tilelib, infile, phase)
					variants[i*2+phase] = tseqs.Variants()
					return err
				}
			}
		}
	}
	go close(todo)
	running := int64(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer atomic.AddInt64(&running, -1)
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
				remain := len(todo) + int(atomic.LoadInt64(&running)) - 1
				ttl := time.Now().Sub(starttime) * time.Duration(remain) / time.Duration(cap(todo)-remain)
				eta := time.Now().Add(ttl)
				log.Printf("progress %d/%d, eta %v (%v)", cap(todo)-remain, cap(todo), eta, ttl)
			}
		}()
	}
	wg.Wait()
	go close(errs)
	return variants, <-errs
}

func (cmd *gvcf2numpy) printVariants(variants [][]tileVariantID) error {
	maxlen := 0
	for _, v := range variants {
		if maxlen < len(v) {
			maxlen = len(v)
		}
	}
	rows := len(variants) / 2
	cols := maxlen * 2
	out := make([]uint16, rows*cols)
	for row := 0; row < len(variants)/2; row++ {
		for phase := 0; phase < 2; phase++ {
			for tag, variant := range variants[row*2+phase] {
				out[row*cols+2*int(tag)+phase] = uint16(variant)
			}
		}
	}
	w := bufio.NewWriter(cmd.output)
	npw, err := gonpy.NewWriter(nopCloser{w})
	if err != nil {
		return err
	}
	npw.Shape = []int{rows, cols}
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
	defer stdout.Close()
	if err != nil {
		return
	}
	err = consensus.Start()
	if err != nil {
		return
	}
	defer consensus.Wait()
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

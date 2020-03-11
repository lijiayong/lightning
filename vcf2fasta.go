package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type vcf2fasta struct {
	refFile           string
	genomeFile        string
	mask              bool
	gvcfRegionsPy     string
	gvcfRegionsPyData []byte
	projectUUID       string
	outputDir         string
	runLocal          bool
	vcpus             int

	stderr io.Writer
}

func (cmd *vcf2fasta) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cmd.refFile, "ref", "", "reference fasta `file`")
	flags.StringVar(&cmd.genomeFile, "genome", "", "reference genome `file`")
	flags.BoolVar(&cmd.mask, "mask", false, "mask uncalled regions (default: output hom ref)")
	flags.StringVar(&cmd.gvcfRegionsPy, "gvcf-regions.py", "https://raw.githubusercontent.com/lijiayong/gvcf_regions/master/gvcf_regions.py", "source of gvcf_regions.py")
	flags.StringVar(&cmd.projectUUID, "project", "", "project `UUID` for containers and output data")
	flags.StringVar(&cmd.outputDir, "output-dir", "", "output directory")
	flags.IntVar(&cmd.vcpus, "vcpus", 0, "number of VCPUs to request for arvados container (default: 2*number of input files, max 32)")
	flags.BoolVar(&cmd.runLocal, "local", false, "run on local host (default: run in an arvados container)")
	priority := flags.Int("priority", 500, "container request priority")
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	} else if cmd.refFile == "" {
		err = errors.New("reference data (-ref) not specified")
		return 2
	} else if flags.NArg() == 0 {
		flags.Usage()
		return 2
	}
	cmd.stderr = stderr

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	if cmd.mask {
		err = cmd.loadRegionsPy()
		if err != nil {
			return 1
		}
	}

	if !cmd.runLocal {
		if cmd.outputDir != "" {
			err = errors.New("cannot specify output dir in non-local mode")
			return 2
		}
		if cmd.vcpus < 1 {
			var infiles []string
			infiles, err = listInputFiles(flags.Args())
			if err != nil {
				return 1
			}
			if cmd.vcpus = len(infiles) * 2; cmd.vcpus > 32 {
				cmd.vcpus = 32
			}
		}
		runner := arvadosContainerRunner{
			Name:        "lightning vcf2fasta",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: cmd.projectUUID,
			RAM:         2<<30 + int64(cmd.vcpus)<<28,
			VCPUs:       cmd.vcpus,
			Priority:    *priority,
			Mounts: map[string]map[string]interface{}{
				"/gvcf_regions.py": map[string]interface{}{
					"kind":    "text",
					"content": string(cmd.gvcfRegionsPyData),
				},
			},
		}
		err = runner.TranslatePaths(&cmd.refFile)
		if err != nil {
			return 1
		}
		inputs := flags.Args()
		for i := range inputs {
			err = runner.TranslatePaths(&inputs[i])
			if err != nil {
				return 1
			}
		}
		runner.Args = append([]string{"vcf2fasta", "-local=true", "-ref", cmd.refFile, fmt.Sprintf("-mask=%v", cmd.mask), "-gvcf-regions.py", "/gvcf_regions.py", "-output-dir", "/mnt/output"}, inputs...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output)
		return 0
	}

	infiles, err := listInputFiles(flags.Args())
	if err != nil {
		return 1
	}

	type job struct {
		vcffile string
		phase   int
	}
	todo := make(chan job)
	go func() {
		for _, infile := range infiles {
			for phase := 1; phase <= 2; phase++ {
				todo <- job{vcffile: infile, phase: phase}
			}
		}
		close(todo)
	}()

	done := make(chan error, runtime.NumCPU()*2)
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range todo {
				if len(done) > 0 {
					// a different worker encountered an error
					return
				}
				err := cmd.vcf2fasta(job.vcffile, job.phase)
				if err != nil {
					done <- fmt.Errorf("%s phase %d: %s", job.vcffile, job.phase, err)
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	err = <-done
	if err != nil {
		return 1
	}
	return 0
}

func maybeInDocker(args, mountfiles []string) []string {
	if out, err := exec.Command("docker", "image", "ls", "-q", "lightning-runtime").Output(); err != nil || len(out) == 0 {
		return args
	}
	dockerrun := []string{
		"docker", "run", "--rm", "-i",
		"--log-driver=none",
	}
	for _, f := range mountfiles {
		dockerrun = append(dockerrun, "--volume="+f+":"+f+":ro")
	}
	dockerrun = append(dockerrun, "lightning-runtime")
	dockerrun = append(dockerrun, args...)
	return dockerrun
}

func (cmd *vcf2fasta) vcf2fasta(infile string, phase int) error {
	_, basename := filepath.Split(infile)
	outfile := filepath.Join(cmd.outputDir, fmt.Sprintf("%s.%d.fasta.gz", basename, phase))
	outf, err := os.OpenFile(outfile, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return fmt.Errorf("error opening output file: %s", err)
	}
	defer outf.Close()
	gzipw := gzip.NewWriter(outf)
	defer gzipw.Close()

	var maskfifo string // filename of mask fifo if we're running bedtools, otherwise ""

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	if cmd.mask {
		if cmd.genomeFile == "" {
			return errors.New("cannot apply mask without -genome argument")
		}
		bedr, bedw, err := os.Pipe()
		if err != nil {
			return err
		}
		bedargs := []string{"python", "-", "--gvcf_type", "gatk", infile}
		bed := exec.Command(bedargs[0], bedargs[1:]...)
		bed.Stdin = bytes.NewBuffer(cmd.gvcfRegionsPyData)
		bed.Stdout = bedw
		bed.Stderr = cmd.stderr
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer bedw.Close()
			log.Printf("running %v", bed.Args)
			err := bed.Run()
			log.Printf("exited %v", bed.Args)
			if err != nil {
				errs <- fmt.Errorf("gvcf_regions: %s", err)
			}
		}()

		// The bcftools --mask argument needs to end in ".bed"
		// in order to be parsed as a BED file, so we need to
		// use a named pipe instead of stdin.
		tempdir, err := ioutil.TempDir("", "")
		if err != nil {
			return fmt.Errorf("TempDir: %s", err)
		}
		defer os.RemoveAll(tempdir)
		maskfifo = filepath.Join(tempdir, "fifo.bed")
		err = syscall.Mkfifo(maskfifo, 0600)
		if err != nil {
			return fmt.Errorf("mkfifo: %s", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()

			maskfifow, err := os.OpenFile(maskfifo, os.O_WRONLY, 0)
			if err != nil {
				errs <- err
				return
			}
			defer maskfifow.Close()

			bedcompargs := []string{"bedtools", "complement", "-i", "/dev/stdin", "-g", cmd.genomeFile}
			bedcompargs = maybeInDocker(bedcompargs, []string{cmd.genomeFile})
			bedcomp := exec.Command(bedcompargs[0], bedcompargs[1:]...)
			bedcomp.Stdin = bedr
			bedcomp.Stdout = maskfifow
			bedcomp.Stderr = cmd.stderr
			log.Printf("running %v", bedcomp.Args)
			err = bedcomp.Run()
			log.Printf("exited %v", bedcomp.Args)
			if err != nil {
				errs <- fmt.Errorf("bedtools complement: %s", err)
				return
			}
			err = maskfifow.Close()
			if err != nil {
				errs <- err
				return
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		consargs := []string{"bcftools", "consensus", "--fasta-ref", cmd.refFile, "-H", fmt.Sprint(phase)}
		if maskfifo != "" {
			consargs = append(consargs, "--mask", maskfifo)
		}
		consargs = append(consargs, infile)
		indexsuffix := ".tbi"
		if _, err := os.Stat(infile + ".csi"); err == nil {
			indexsuffix = ".csi"
		}
		mounts := []string{infile, infile + indexsuffix, cmd.refFile}
		if maskfifo != "" {
			mounts = append(mounts, maskfifo)
		}
		consargs = maybeInDocker(consargs, mounts)

		consensus := exec.Command(consargs[0], consargs[1:]...)
		consensus.Stderr = os.Stderr
		consensus.Stdout = gzipw
		consensus.Stderr = cmd.stderr
		log.Printf("running %v", consensus.Args)
		err = consensus.Run()
		if err != nil {
			errs <- fmt.Errorf("bcftools consensus: %s", err)
			return
		}
		err = gzipw.Close()
		if err != nil {
			errs <- err
			return
		}
		errs <- outf.Close()
	}()

	go func() {
		wg.Wait()
		close(errs)
	}()

	for err := range errs {
		if err != nil {
			wg.Wait()
			return err
		}
	}
	return nil
}

func (cmd *vcf2fasta) loadRegionsPy() error {
	if strings.HasPrefix(cmd.gvcfRegionsPy, "http") {
		resp, err := http.Get(cmd.gvcfRegionsPy)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("get %q: http status %d", cmd.gvcfRegionsPy, resp.StatusCode)
		}
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("get %q: read body: %s", cmd.gvcfRegionsPy, err)
		}
		cmd.gvcfRegionsPyData = buf
		return nil
	} else {
		buf, err := ioutil.ReadFile(cmd.gvcfRegionsPy)
		if err != nil {
			return err
		}
		cmd.gvcfRegionsPyData = buf
		return nil
	}
}

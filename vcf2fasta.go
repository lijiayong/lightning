package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type vcf2fasta struct {
	refFile     string
	projectUUID string
	outputDir   string
	runLocal    bool
	vcpus       int
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
	flags.StringVar(&cmd.projectUUID, "project", "", "project `UUID` for containers and output data")
	flags.StringVar(&cmd.outputDir, "output-dir", "", "output directory")
	flags.IntVar(&cmd.vcpus, "vcpus", 0, "number of VCPUs to request for arvados container (default: 2*number of input files, max 32)")
	flags.BoolVar(&cmd.runLocal, "local", false, "run on local host (default: run in an arvados container)")
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

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
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
		runner.Args = append([]string{"vcf2fasta", "-local=true", "-ref", cmd.refFile, "-output-dir", "/mnt/output"}, inputs...)
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

func (cmd *vcf2fasta) vcf2fasta(infile string, phase int) error {
	args := []string{"bcftools", "consensus", "--fasta-ref", cmd.refFile, "-H", fmt.Sprint(phase), infile}
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

	_, basename := filepath.Split(infile)
	outfile := filepath.Join(cmd.outputDir, fmt.Sprintf("%s.%d.fasta.gz", basename, phase))
	outf, err := os.OpenFile(outfile, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0777)
	if err != nil {
		return fmt.Errorf("error opening output file: %s", err)
	}
	defer outf.Close()
	gzipw := gzip.NewWriter(outf)
	defer gzipw.Close()

	consensus := exec.Command(args[0], args[1:]...)
	consensus.Stderr = os.Stderr
	consensus.Stdout = gzipw
	err = consensus.Start()
	if err != nil {
		return err
	}
	err = consensus.Wait()
	if err != nil {
		return err
	}
	err = gzipw.Close()
	if err != nil {
		return err
	}
	err = outf.Close()
	if err != nil {
		return err
	}
	return nil
}

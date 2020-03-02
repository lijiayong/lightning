package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"git.arvados.org/arvados.git/sdk/go/arvadosclient"
	"git.arvados.org/arvados.git/sdk/go/keepclient"
	"golang.org/x/crypto/blake2b"
)

type arvadosContainerRunner struct {
	Client      *arvados.Client
	Name        string
	ProjectUUID string
	Args        []string
	Mounts      map[string]string
}

var (
	collectionInPathRe = regexp.MustCompile(`^(.*/)?([0-9a-f]{32}\+[0-9]+|[0-9a-z]{5}-[0-9a-z]{5}-[0-9a-z]{15})(/.*)?$`)
)

func (runner *arvadosContainerRunner) Run() error {
	if runner.ProjectUUID == "" {
		return errors.New("cannot run arvados container: ProjectUUID not provided")
	}
	prog := "/mnt/cmd/lightning"
	cmdUUID, err := runner.makeCommandCollection()
	if err != nil {
		return err
	}
	command := append([]string{prog}, runner.Args...)
	mounts := map[string]map[string]interface{}{
		"/mnt/cmd": {
			"kind": "collection",
			"uuid": cmdUUID,
		},
		"/mnt/output": {
			"kind":     "tmp",
			"writable": true,
			"capacity": 100000000000,
		},
	}
	for uuid, mnt := range runner.Mounts {
		mounts[mnt] = map[string]interface{}{
			"kind": "collection",
			"uuid": uuid,
		}
	}
	cpus := 16
	rc := arvados.RuntimeConstraints{
		VCPUs:        cpus,
		RAM:          64000000000,
		KeepCacheRAM: (1 << 26) * 2 * int64(cpus),
	}
	var cr arvados.ContainerRequest
	err = runner.Client.RequestAndDecode(&cr, "POST", "arvados/v1/container_requests", nil, map[string]interface{}{
		"container_request": map[string]interface{}{
			"owner_uuid":          runner.ProjectUUID,
			"name":                runner.Name,
			"container_image":     "lightning-runtime",
			"command":             command,
			"mounts":              mounts,
			"use_existing":        true,
			"output_path":         "/mnt/output",
			"runtime_constraints": rc,
			"priority":            1,
			"state":               arvados.ContainerRequestStateCommitted,
		},
	})
	log.Print(cr.UUID)
	return err
}

func (runner *arvadosContainerRunner) TranslatePaths(paths ...*string) error {
	if runner.Mounts == nil {
		runner.Mounts = make(map[string]string)
	}
	for _, path := range paths {
		if *path == "" {
			continue
		}
		m := collectionInPathRe.FindStringSubmatch(*path)
		if m == nil {
			return fmt.Errorf("cannot find uuid in path: %q", *path)
		}
		uuid := m[2]
		mnt, ok := runner.Mounts[uuid]
		if !ok {
			mnt = "/mnt/" + uuid
			runner.Mounts[uuid] = mnt
		}
		*path = mnt + m[3]
	}
	return nil
}

func (runner *arvadosContainerRunner) makeCommandCollection() (string, error) {
	exe, err := ioutil.ReadFile("/proc/self/exe")
	if err != nil {
		return "", err
	}
	b2 := blake2b.Sum256(exe)
	cname := fmt.Sprintf("lightning-%x", b2)
	var existing arvados.CollectionList
	err = runner.Client.RequestAndDecode(&existing, "GET", "arvados/v1/collections", nil, arvados.ListOptions{
		Limit: 1,
		Count: "none",
		Filters: []arvados.Filter{
			{"name", "=", cname},
			{"owner_uuid", "=", runner.ProjectUUID},
		},
	})
	if err != nil {
		return "", err
	}
	if len(existing.Items) > 0 {
		uuid := existing.Items[0].UUID
		log.Printf("using existing collection %q named %q (did not verify whether content matches)", uuid, cname)
		return uuid, nil
	}
	log.Printf("writing lightning binary to new collection %q", cname)
	ac, err := arvadosclient.New(runner.Client)
	if err != nil {
		return "", err
	}
	kc := keepclient.New(ac)
	var coll arvados.Collection
	fs, err := coll.FileSystem(runner.Client, kc)
	if err != nil {
		return "", err
	}
	f, err := fs.OpenFile("lightning", os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return "", err
	}
	_, err = f.Write(exe)
	if err != nil {
		return "", err
	}
	err = f.Close()
	if err != nil {
		return "", err
	}
	mtxt, err := fs.MarshalManifest(".")
	if err != nil {
		return "", err
	}
	err = runner.Client.RequestAndDecode(&coll, "POST", "arvados/v1/collections", nil, map[string]interface{}{
		"collection": map[string]interface{}{
			"owner_uuid":    runner.ProjectUUID,
			"manifest_text": mtxt,
			"name":          cname,
		},
	})
	if err != nil {
		return "", err
	}
	log.Printf("collection: %#v", coll)
	return coll.UUID, nil
}

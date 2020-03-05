package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"git.arvados.org/arvados.git/sdk/go/arvadosclient"
	"git.arvados.org/arvados.git/sdk/go/keepclient"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/net/websocket"
)

type arvadosContainerRunner struct {
	Client      *arvados.Client
	Name        string
	ProjectUUID string
	VCPUs       int
	RAM         int64
	Prog        string // if empty, run /proc/self/exe
	Args        []string
	Mounts      map[string]map[string]interface{}
}

func (runner *arvadosContainerRunner) Run() (string, error) {
	if runner.ProjectUUID == "" {
		return "", errors.New("cannot run arvados container: ProjectUUID not provided")
	}

	mounts := map[string]map[string]interface{}{
		"/mnt/output": {
			"kind":     "tmp",
			"writable": true,
			"capacity": 100000000000,
		},
	}
	for path, mnt := range runner.Mounts {
		mounts[path] = mnt
	}

	prog := runner.Prog
	if prog == "" {
		prog = "/mnt/cmd/lightning"
		cmdUUID, err := runner.makeCommandCollection()
		if err != nil {
			return "", err
		}
		mounts["/mnt/cmd"] = map[string]interface{}{
			"kind": "collection",
			"uuid": cmdUUID,
		}
	}
	command := append([]string{prog}, runner.Args...)

	rc := arvados.RuntimeConstraints{
		VCPUs:        runner.VCPUs,
		RAM:          runner.RAM,
		KeepCacheRAM: (1 << 26) * 2 * int64(runner.VCPUs),
	}
	var cr arvados.ContainerRequest
	err := runner.Client.RequestAndDecode(&cr, "POST", "arvados/v1/container_requests", nil, map[string]interface{}{
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
	log.Printf("container request UUID: %s", cr.UUID)
	log.Printf("container UUID: %s", cr.ContainerUUID)

	var logch <-chan eventMessage
	var logstream *logStream
	defer func() {
		if logstream != nil {
			logstream.Close()
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastState := cr.State
	refreshCR := func() {
		err = runner.Client.RequestAndDecode(&cr, "GET", "arvados/v1/container_requests/"+cr.UUID, nil, nil)
		if err != nil {
			log.Printf("error getting container request: %s", err)
			return
		}
		if lastState != cr.State {
			log.Printf("container state: %s", cr.State)
			lastState = cr.State
		}
	}

	subscribedUUID := ""
	for cr.State != arvados.ContainerRequestStateFinal {
		if logch == nil && cr.ContainerUUID != subscribedUUID {
			if logstream != nil {
				logstream.Close()
			}
			logstream = runner.logStream(cr.ContainerUUID)
			logch = logstream.C
		}
		select {
		case msg, ok := <-logch:
			if !ok {
				logstream.Close()
				logstream = nil
				logch = nil
				break
			}
			switch msg.EventType {
			case "update":
				refreshCR()
			default:
				for _, line := range strings.Split(msg.Properties.Text, "\n") {
					if line != "" {
						log.Print(line)
					}
				}
			}
		case <-ticker.C:
			refreshCR()
		}
	}

	var c arvados.Container
	err = runner.Client.RequestAndDecode(&c, "GET", "arvados/v1/containers/"+cr.ContainerUUID, nil, nil)
	if err != nil {
		return "", err
	}
	if c.ExitCode != 0 {
		return "", fmt.Errorf("container exited %d", c.ExitCode)
	}
	return cr.OutputUUID, err
}

var collectionInPathRe = regexp.MustCompile(`^(.*/)?([0-9a-f]{32}\+[0-9]+|[0-9a-z]{5}-[0-9a-z]{5}-[0-9a-z]{15})(/.*)?$`)

func (runner *arvadosContainerRunner) TranslatePaths(paths ...*string) error {
	if runner.Mounts == nil {
		runner.Mounts = make(map[string]map[string]interface{})
	}
	for _, path := range paths {
		if *path == "" || *path == "-" {
			continue
		}
		m := collectionInPathRe.FindStringSubmatch(*path)
		if m == nil {
			return fmt.Errorf("cannot find uuid in path: %q", *path)
		}
		uuid := m[2]
		mnt, ok := runner.Mounts["/mnt/"+uuid]
		if !ok {
			mnt = map[string]interface{}{
				"kind": "collection",
				"uuid": uuid,
			}
			runner.Mounts["/mnt/"+uuid] = mnt
		}
		*path = "/mnt/" + uuid + m[3]
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
			{Attr: "name", Operator: "=", Operand: cname},
			{Attr: "owner_uuid", Operator: "=", Operand: runner.ProjectUUID},
		},
	})
	if err != nil {
		return "", err
	}
	if len(existing.Items) > 0 {
		uuid := existing.Items[0].UUID
		log.Printf("using lightning binary in existing collection %s (name is %q; did not verify whether content matches)", uuid, cname)
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
	log.Printf("stored lightning binary in new collection %s", coll.UUID)
	return coll.UUID, nil
}

type eventMessage struct {
	Status     int
	ObjectUUID string `json:"object_uuid"`
	EventType  string `json:"event_type"`
	Properties struct {
		Text string
	}
}

type logStream struct {
	C     <-chan eventMessage
	Close func() error
}

func (runner *arvadosContainerRunner) logStream(uuid string) *logStream {
	ch := make(chan eventMessage)
	done := make(chan struct{})
	go func() {
		defer close(ch)
		var cluster arvados.Cluster
		runner.Client.RequestAndDecode(&cluster, "GET", arvados.EndpointConfigGet.Path, nil, nil)
		wsURL := cluster.Services.Websocket.ExternalURL
		wsURL.Scheme = strings.Replace(wsURL.Scheme, "http", "ws", 1)
		wsURL.Path = "/websocket"
		wsURL.RawQuery = url.Values{"api_token": []string{runner.Client.AuthToken}}.Encode()
		conn, err := websocket.Dial(wsURL.String(), "", cluster.Services.Controller.ExternalURL.String())
		if err != nil {
			log.Printf("websocket error: %s", err)
			return
		}
		w := json.NewEncoder(conn)
		go w.Encode(map[string]interface{}{
			"method": "subscribe",
			"filters": [][]interface{}{
				{"object_uuid", "=", uuid},
				{"event_type", "in", []string{"stderr", "crunch-run", "update"}},
			},
		})
		r := json.NewDecoder(conn)
		for {
			var msg eventMessage
			err := r.Decode(&msg)
			if err != nil {
				log.Printf("error decoding websocket message: %s", err)
				return
			}
			ch <- msg
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	return &logStream{
		C: ch,
		Close: func() error {
			close(done)
			return nil
		},
	}
}

// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	cp "github.com/soundcloud/cotterpin"
)

const runnersPath = "runners"

// Runner is representation of a bazooka-runner process.
type Runner struct {
	dir        *cp.Dir
	Addr       string
	InstanceID int64
}

// NewRunner creates a Runner for the given Instance.
func (s *Store) NewRunner(addr string, instanceID int64) *Runner {
	return &Runner{
		dir:        cp.NewDir(runnerPath(addr), s.GetSnapshot()),
		Addr:       addr,
		InstanceID: instanceID,
	}
}

// GetSnapshot satisfies the cp.Snapshotable interface.
func (r *Runner) GetSnapshot() cp.Snapshot {
	return r.dir.Snapshot
}

// Register saves the runner in the coordinator.
func (r *Runner) Register() (*Runner, error) {
	sp, err := r.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	exists, _, err := sp.Exists(r.dir.Name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrConflict
	}

	f := cp.NewFile(r.dir.Name, []string{strconv.FormatInt(r.InstanceID, 10)}, new(cp.ListCodec), sp)
	f, err = f.Save()
	if err != nil {
		return nil, err
	}
	r.dir = r.dir.Join(f)

	return r, nil
}

// Unregister removes the Runner from the store.
func (r *Runner) Unregister() error {
	sp, err := r.GetSnapshot().FastForward()
	if err != nil {
		return err
	}
	return r.dir.Join(sp).Del("/")
}

// Runners returns all runners known.
func (s *Store) Runners() (runners []*Runner, err error) {
	hosts, err := s.GetSnapshot().Getdir(runnersPath)
	if err != nil {
		return
	}

	for _, host := range hosts {
		rns, err := s.RunnersByHost(host)
		if err != nil {
			return runners, err
		}
		runners = append(runners, rns...)
	}
	return
}

// RunnersByHost returns all Runners for a given host.
func (s *Store) RunnersByHost(host string) ([]*Runner, error) {
	sp, err := s.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	ids, err := sp.Getdir(path.Join(runnersPath, host))
	if err != nil {
		return nil, err
	}
	ch, errch := cp.GetSnapshotables(ids, func(id string) (cp.Snapshotable, error) {
		return getRunner(runnerAddr(host, id), sp)
	})
	runners := []*Runner{}
	for i := 0; i < len(ids); i++ {
		select {
		case r := <-ch:
			runners = append(runners, r.(*Runner))
		case err := <-errch:
			if err != nil {
				return nil, err
			}
		}
	}
	return runners, nil
}

// GetRunner returns the Runner for the given addr.
func (s *Store) GetRunner(addr string) (*Runner, error) {
	sp, err := s.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	return getRunner(addr, sp)
}

// WatchRunnerStart sends all runners transitioned to start.
func (s *Store) WatchRunnerStart(ch chan *Runner, errch chan error) {
	var sp cp.Snapshotable = s
	for {
		ev, err := waitRunners(sp)
		if err != nil {
			errch <- err
			return
		}
		sp = ev

		if !ev.IsSet() {
			continue
		}
		addr := addrFromPath(ev.Path)

		runner, err := getRunner(addr, ev)
		if err != nil {
			errch <- err
			return
		}
		ch <- runner
	}
}

// WatchRunnerStop sends all Runners transitioned to stop.
func (s *Store) WatchRunnerStop(ch chan string, errch chan error) {
	var sp cp.Snapshotable = s
	for {
		ev, err := waitRunners(sp)
		if err != nil {
			errch <- err
			return
		}
		sp = ev

		if !ev.IsDel() {
			continue
		}
		ch <- addrFromPath(ev.Path)
	}
}

func addrFromPath(path string) string {
	parts := strings.Split(path, "/")
	addr := runnerAddr(parts[2], parts[3])

	return addr
}

func getRunner(addr string, s cp.Snapshotable) (*Runner, error) {
	sp := s.GetSnapshot()
	f, err := sp.GetFile(runnerPath(addr), new(cp.ListCodec))
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = errorf(ErrNotFound, "runner '%s' not found", addr)
		}
		return nil, err
	}
	data := f.Value.([]string)
	insIDStr := data[0]
	insID, err := parseInstanceID(insIDStr)
	if err != nil {
		return nil, err
	}

	return storeFromSnapshotable(sp).NewRunner(addr, insID), nil
}

func waitRunners(s cp.Snapshotable) (cp.Event, error) {
	sp := s.GetSnapshot()
	return sp.Wait(path.Join(runnersPath, "*", "*"))
}

func runnerAddr(host, port string) string {
	return fmt.Sprintf("%s:%s", host, port)
}

func runnerPath(addr string) string {
	parts := strings.Split(addr, ":")
	return path.Join(runnersPath, parts[0], parts[1])
}

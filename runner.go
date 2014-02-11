// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"fmt"
	cp "github.com/soundcloud/cotterpin"
	"path"
	"strconv"
	"strings"
)

const runnersPath = "runners"

type Runner struct {
	dir        *cp.Dir
	Addr       string
	InstanceId int64
}

func (s *Store) NewRunner(addr string, instanceId int64) *Runner {
	return &Runner{
		dir:        cp.NewDir(runnerPath(addr), s.GetSnapshot()),
		Addr:       addr,
		InstanceId: instanceId,
	}
}

func (r *Runner) GetSnapshot() cp.Snapshot {
	return r.dir.Snapshot
}

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

	f := cp.NewFile(r.dir.Name, []string{strconv.FormatInt(r.InstanceId, 10)}, new(cp.ListCodec), sp)
	f, err = f.Save()
	if err != nil {
		return nil, err
	}
	r.dir = r.dir.Join(f)

	return r, nil
}

func (r *Runner) Unregister() error {
	sp, err := r.GetSnapshot().FastForward()
	if err != nil {
		return err
	}
	return r.dir.Join(sp).Del("/")
}

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

func (s *Store) GetRunner(addr string) (*Runner, error) {
	sp, err := s.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	return getRunner(addr, sp)
}

func (s *Store) WatchRunnerStart(host string, ch chan *Runner, errch chan error) {
	var sp cp.Snapshotable = s
	for {
		ev, err := waitRunnersByHost(host, sp)
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

func (s *Store) WatchRunnerStop(host string, ch chan string, errch chan error) {
	var sp cp.Snapshotable = s
	for {
		ev, err := waitRunnersByHost(host, sp)
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
	insIdStr := data[0]
	insId, err := parseInstanceId(insIdStr)
	if err != nil {
		return nil, err
	}

	return storeFromSnapshotable(sp).NewRunner(addr, insId), nil
}

func waitRunnersByHost(host string, s cp.Snapshotable) (cp.Event, error) {
	sp := s.GetSnapshot()
	return sp.Wait(path.Join(runnersPath, host, "*"))
}

func runnerAddr(host, port string) string {
	return fmt.Sprintf("%s:%s", host, port)
}

func runnerPath(addr string) string {
	parts := strings.Split(addr, ":")
	return path.Join(runnersPath, parts[0], parts[1])
}

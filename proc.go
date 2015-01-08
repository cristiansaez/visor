// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	cp "github.com/soundcloud/cotterpin"
)

var reProcName = regexp.MustCompile("^[[:alnum:]]+$")

// Proc represents a process type with a certain scale.
type Proc struct {
	dir        *cp.Dir
	Name       string
	App        *App
	Port       int
	Attrs      ProcAttrs
	Registered time.Time
}

// Mutable extra Proc attributes.
type ProcAttrs struct {
	Limits         ResourceLimits `json:"limits"`
	LogPersistence bool           `json:"log_persistence"`
}

// Per-proc resource limits.
type ResourceLimits struct {
	// Maximum memory allowance in MB for an instance of this Proc.
	MemoryLimitMb *int `json:"memory-limit-mb,omitemproc"`
}

const (
	procsPath      = "procs"
	procsPortPath  = "port"
	procsAttrsPath = "attrs"
)

func (s *Store) NewProc(app *App, name string) *Proc {
	return &Proc{
		Name: name,
		App:  app,
		dir:  cp.NewDir(app.dir.Prefix(procsPath, string(name)), s.GetSnapshot()),
	}
}

func (p *Proc) GetSnapshot() cp.Snapshot {
	return p.dir.Snapshot
}

// Register registers a proc with the registry.
func (p *Proc) Register() (*Proc, error) {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	exists, _, err := sp.Exists(p.dir.Name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrConflict
	}

	if !reProcName.MatchString(p.Name) {
		return nil, ErrBadProcName
	}

	p.Port, err = claimNextPort(sp)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("couldn't claim port: %s", err.Error()))
	}

	port := cp.NewFile(p.dir.Prefix(procsPortPath), p.Port, new(cp.IntCodec), sp)
	port, err = port.Save()
	if err != nil {
		return nil, err
	}

	reg := time.Now()
	d, err := p.dir.Join(sp).Set(registeredPath, formatTime(reg))
	if err != nil {
		return nil, err
	}
	p.Registered = reg
	p.dir = d

	return p, nil
}

// Unregister unregisters a proc from the registry.
func (p *Proc) Unregister() error {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return err
	}
	return p.dir.Join(sp).Del("/")
}

// DoneInstancesPath returns the doozerd path where done instances are stored.
func (p *Proc) DoneInstancesPath() string {
	return p.dir.Prefix(donePath)
}

func (p *Proc) instancesPath() string {
	return p.dir.Prefix(instancesPath)
}

func (p *Proc) failedInstancesPath() string {
	return p.dir.Prefix(failedPath)
}

func (p *Proc) lostInstancesPath() string {
	return p.dir.Prefix(lostPath)
}

func (p *Proc) NumInstances() (int, error) {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return -1, err
	}
	revs, err := sp.Getdir(p.dir.Prefix("instances"))
	if err != nil {
		return -1, err
	}
	total := 0

	for _, rev := range revs {
		size, _, err := sp.Stat(p.dir.Prefix("instances", rev), &sp.Rev)
		if err != nil {
			return -1, err
		}
		total += size
	}
	return total, nil
}

// GetDoneInstances returns all instances that were unregistered for this proc.
// As those Instances are reconstructed from serialised state it should be
// avoided to operate on those.
func (p *Proc) GetDoneInstances() ([]*Instance, error) {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	ids, err := sp.Getdir(p.DoneInstancesPath())
	if err != nil {
		return nil, err
	}
	return getSerialisedInstances(ids, InsStatusDone, p, sp)
}

func (p *Proc) GetFailedInstances() ([]*Instance, error) {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	ids, err := sp.Getdir(p.failedInstancesPath())
	if err != nil {
		return nil, err
	}
	return getSerialisedInstances(ids, InsStatusFailed, p, sp)
}

func (p *Proc) GetLostInstances() ([]*Instance, error) {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	ids, err := sp.Getdir(p.lostInstancesPath())
	if err != nil {
		return nil, err
	}
	return getSerialisedInstances(ids, InsStatusLost, p, sp)
}

func (p *Proc) GetInstances() ([]*Instance, error) {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	ids, err := getProcInstanceIds(p, sp)
	if err != nil {
		return nil, err
	}
	idStrs := []string{}
	for _, id := range ids {
		s := strconv.FormatInt(id, 10)
		idStrs = append(idStrs, s)
	}
	return getProcInstances(idStrs, sp)
}

func (p Proc) GetRunningRevs() ([]string, error) {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	revs, err := sp.Getdir(p.dir.Prefix("instances"))
	if err != nil {
		return nil, err
	}
	return revs, nil
}

func (p *Proc) StoreAttrs() (*Proc, error) {
	sp, err := p.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	attrs := cp.NewFile(p.dir.Prefix(procsAttrsPath), p.Attrs, new(cp.JsonCodec), sp)
	attrs, err = attrs.Save()
	if err != nil {
		return nil, err
	}
	p.dir = p.dir.Join(attrs)

	return p, nil
}

func (p *Proc) String() string {
	return fmt.Sprintf("Proc<%s:%s>", p.App.Name, p.Name)
}

// GetProc fetches a Proc from the coordinator
func (a *App) GetProc(name string) (*Proc, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	return getProc(a, name, sp)
}

func getProc(app *App, name string, s cp.Snapshotable) (*Proc, error) {
	p := &Proc{
		dir:  cp.NewDir(app.dir.Prefix(procsPath, name), s.GetSnapshot()),
		Name: name,
		App:  app,
	}

	port, err := p.dir.GetFile(procsPortPath, new(cp.IntCodec))
	if err != nil {
		return nil, errorf(ErrNotFound, "port not found for %s-%s", app.Name, name)
	}
	p.Port = port.Value.(int)

	_, err = p.dir.GetFile(procsAttrsPath, &cp.JsonCodec{DecodedVal: &p.Attrs})
	if err != nil && !cp.IsErrNoEnt(err) {
		return nil, err
	}

	f, err := p.dir.GetFile(registeredPath, new(cp.StringCodec))
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = errorf(ErrNotFound, "registered not found for %s:%s", app.Name, name)
		}
		return nil, err
	}
	p.Registered, err = parseTime(f.Value.(string))
	if err != nil {
		// FIXME remove backwards compatible parsing of timestamps before b4fbef0
		p.Registered, err = time.Parse(UTCFormat, f.Value.(string))
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func getProcInstances(ids []string, s cp.Snapshotable) ([]*Instance, error) {
	ch, errch := cp.GetSnapshotables(ids, func(idstr string) (cp.Snapshotable, error) {
		id, err := parseInstanceId(idstr)
		if err != nil {
			return nil, err
		}
		return getInstance(id, s)
	})
	ins := []*Instance{}
	for i := 0; i < len(ids); i++ {
		select {
		case r := <-ch:
			ins = append(ins, r.(*Instance))
		case err := <-errch:
			return nil, err
		}
	}
	return ins, nil
}

func getProcInstanceIds(p *Proc, s cp.Snapshotable) ([]int64, error) {
	sp := s.GetSnapshot()
	revs, err := sp.Getdir(p.dir.Prefix("instances"))
	if err != nil {
		return nil, err
	}
	ids := []int64{}
	for _, rev := range revs {
		iids, err := getInstanceIds(p.App.Name, rev, p.Name, sp)
		if err != nil {
			return nil, err
		}
		ids = append(ids, iids...)
	}
	return ids, nil
}

func getSerialisedInstances(
	ids []string,
	state InsStatus,
	p *Proc,
	sp cp.Snapshot,
) ([]*Instance, error) {
	is := []*Instance{}
	for _, idstr := range ids {
		id, err := parseInstanceId(idstr)
		if err != nil {
			return nil, err
		}

		ins, err := getSerialisedInstance(p.App.Name, p.Name, id, state, sp)
		if err != nil {
			return nil, err
		}

		is = append(is, ins)
	}

	return is, nil
}

func claimNextPort(s cp.Snapshot) (int, error) {
	for {
		var err error
		s, err = s.FastForward()
		if err != nil {
			return -1, err
		}

		f, err := s.GetFile(nextPortPath, new(cp.IntCodec))
		if err == nil {
			port := f.Value.(int)

			f, err = f.Set(port + 1)
			if err == nil {
				return port, nil
			} else {
				time.Sleep(time.Second / 10)
			}
		} else {
			return -1, err
		}
	}

	return -1, nil
}

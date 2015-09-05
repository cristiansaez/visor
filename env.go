// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"strings"
	"time"

	cp "github.com/soundcloud/cotterpin"
)

const (
	envsPath = "envs"
	varsPath = "vars"
)

// Env is a set of config variables which will be passed to instances.
type Env struct {
	dir        *cp.Dir
	App        *App
	Ref        string
	Vars       map[string]string
	Registered time.Time
}

// NewEnv returns a new Env given an App, the ref and the map of vars.
func (a *App) NewEnv(ref string, vars map[string]string) *Env {
	path := a.dir.Prefix(envsPath, ref)
	return &Env{
		dir:  cp.NewDir(path, a.GetSnapshot()),
		App:  a,
		Ref:  ref,
		Vars: vars,
	}
}

// GetSnapshot satisfies the cp.Snapshotable interface.
func (e *Env) GetSnapshot() cp.Snapshot {
	return e.dir.Snapshot
}

// Register adds the Env to the Apps envs.
func (e *Env) Register() (*Env, error) {
	sp, err := e.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	exists, _, err := sp.Exists(e.dir.Name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errorf(ErrConflict, `env "%s" can't be overwritten`, e.Ref)
	}

	for k := range e.Vars {
		if len(k) == 0 {
			return nil, errorf(ErrInvalidKey, `env keys can't be emproc`)
		}
		if strings.Contains(k, "=") {
			return nil, errorf(ErrInvalidKey, `env keys can't contain "="`)
		}
	}

	attrs := cp.NewFile(e.dir.Prefix(varsPath), e.Vars, new(cp.JsonCodec), sp)
	attrs, err = attrs.Save()
	if err != nil {
		return nil, err
	}

	reg := time.Now()
	d, err := e.dir.Set(registeredPath, formatTime(reg))
	if err != nil {
		return nil, err
	}
	e.Registered = reg

	e.dir = d

	return e, nil
}

// Unregister removes the Env from the Apps envs.
func (e *Env) Unregister() error {
	sp, err := e.GetSnapshot().FastForward()
	if err != nil {
		return err
	}
	exists, _, err := sp.Exists(e.dir.Name)
	if err != nil {
		return err
	}
	if !exists {
		return errorf(ErrNotFound, `env "%s" not found`, e.Ref)
	}
	return e.dir.Join(sp).Del("/")
}

// GetEnv retrieves the Env for the passed ref.
func (a *App) GetEnv(ref string) (*Env, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	return getEnv(a, ref, sp)
}

// GetEnvs returns a list of all Envs for the app.
func (a *App) GetEnvs() ([]*Env, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	refs, err := sp.Getdir(a.dir.Prefix(envsPath))
	if err != nil {
		return nil, err
	}

	envs := []*Env{}
	ch, errch := cp.GetSnapshotables(refs, func(ref string) (cp.Snapshotable, error) {
		return getEnv(a, ref, sp)
	})
	for i := 0; i < len(refs); i++ {
		select {
		case e := <-ch:
			envs = append(envs, e.(*Env))
		case err := <-errch:
			return nil, err
		}
	}
	return envs, nil
}

func getEnv(app *App, ref string, s cp.Snapshotable) (*Env, error) {
	e := &Env{
		dir: cp.NewDir(app.dir.Prefix(envsPath, ref), s.GetSnapshot()),
		App: app,
		Ref: ref,
	}

	_, err := e.dir.GetFile(varsPath, &cp.JsonCodec{DecodedVal: &e.Vars})
	if err != nil {
		if cp.IsErrNoEnt(err) {
			exists, _, err := s.GetSnapshot().Exists(e.dir.Name)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, errorf(ErrNotFound, `env "%s" not found for app %s`, ref, app.Name)
			}
			return nil, errorf(ErrNotFound, "vars not found for %s#%s", app.Name, ref)
		}
		return nil, err
	}

	f, err := e.dir.GetFile(registeredPath, new(cp.StringCodec))
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = errorf(ErrNotFound, `registered not found for %s`, ref)
		}
		return nil, err
	}
	e.Registered, err = parseTime(f.Value.(string))
	if err != nil {
		return nil, err
	}

	return e, nil
}

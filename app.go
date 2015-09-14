// Copyright (c) 2012, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"fmt"
	"path"
	"strings"
	"time"

	cp "github.com/soundcloud/cotterpin"
)

// DeployLXC defines the cannonical name for lxc deploy type.
const DeployLXC = "lxc"
const appsPath = "apps"

// App is the representation of a repository of coherent changes.
type App struct {
	dir        *cp.Dir
	Name       string
	RepoURL    string
	Stack      string
	Env        map[string]string
	DeployType string
	Registered time.Time
}

// NewApp returns a new App given a name, repository url and stack.
func (s *Store) NewApp(name string, repourl string, stack string) (app *App) {
	app = &App{Name: name, RepoURL: repourl, Stack: stack, Env: map[string]string{}}
	app.dir = cp.NewDir(path.Join(appsPath, app.Name), s.GetSnapshot())

	return
}

// GetSnapshot satisfies the cp.Snapshotable interface.
func (a *App) GetSnapshot() cp.Snapshot {
	return a.dir.Snapshot
}

// Register adds the App to the global process state.
func (a *App) Register() (*App, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	exists, _, err := sp.Exists(a.dir.Name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errorf(ErrConflict, `app "%s" already exists`, a.Name)
	}

	if a.DeployType == "" {
		a.DeployType = DeployLXC
	}

	v := map[string]interface{}{
		"repo-url":    a.RepoURL,
		"stack":       a.Stack,
		"deploy-type": a.DeployType,
	}
	attrs := cp.NewFile(a.dir.Prefix("attrs"), v, new(cp.JsonCodec), sp)

	attrs, err = attrs.Save()
	if err != nil {
		return nil, err
	}

	a.dir = a.dir.Join(sp)

	for k, v := range a.Env {
		_, err = a.SetEnvironmentVar(k, v)
		if err != nil {
			return nil, err
		}
	}

	reg := time.Now()
	d, err := a.dir.Set(registeredPath, formatTime(reg))
	if err != nil {
		return nil, err
	}
	a.Registered = reg

	a.dir = d

	return a, err
}

// Unregister removes the App form the global process state.
func (a *App) Unregister() error {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return err
	}
	exists, _, err := sp.Exists(a.dir.Name)
	if err != nil {
		return err
	}
	if !exists {
		return errorf(ErrNotFound, `app "%s" not found`, a)
	}
	return a.dir.Join(sp).Del("/")
}

// SetStack sets the application's stack
func (a *App) SetStack(stack string) (*App, error) {
	a.Stack = stack
	return a.StoreAttrs()
}

// StoreAttrs saves the current App attrs.
func (a *App) StoreAttrs() (*App, error) {
	f, err := a.dir.GetFile("attrs", new(cp.JsonCodec))
	if err != nil {
		return nil, err
	}

	v := map[string]interface{}{
		"repo-url":    a.RepoURL,
		"stack":       a.Stack,
		"deploy-type": a.DeployType,
	}
	f.Value = v
	f, err = f.Save()
	if err != nil {
		return nil, err
	}

	return a, nil
}

// EnvironmentVars returns all set variables for this app as a map.
func (a *App) EnvironmentVars() (vars map[string]string, err error) {
	vars = map[string]string{}

	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return vars, err
	}
	names, err := sp.Getdir(a.dir.Prefix("env"))
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = nil
		}
		return
	}
	a.dir = a.dir.Join(sp)

	type resp struct {
		key, val string
		err      error
	}
	ch := make(chan resp, len(names))

	if err != nil {
		if cp.IsErrNoEnt(err) {
			return vars, nil
		}
		return
	}

	for _, name := range names {
		go func(name string) {
			v, err := a.GetEnvironmentVar(name)
			if err != nil {
				ch <- resp{err: err}
			} else {
				ch <- resp{key: name, val: v}
			}
		}(name)
	}
	for i := 0; i < len(names); i++ {
		r := <-ch
		if r.err != nil {
			return nil, err
		}
		vars[strings.Replace(r.key, "-", "_", -1)] = r.val
	}
	return
}

// GetEnvironmentVar returns the value stored for the given key.
func (a *App) GetEnvironmentVar(k string) (value string, err error) {
	k = strings.Replace(k, "_", "-", -1)
	val, _, err := a.dir.Get("env/" + k)
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = errorf(ErrNotFound, `"%s" not found in %s's environment`, k, a.Name)
		}
		return
	}
	value = string(val)

	return
}

// SetEnvironmentVar stores the value for the given key.
func (a *App) SetEnvironmentVar(k string, v string) (*App, error) {
	d, err := a.dir.Set("env/"+strings.Replace(k, "_", "-", -1), v)
	if err != nil {
		return nil, err
	}
	if _, present := a.Env[k]; !present {
		a.Env[k] = v
	}
	a.dir = d
	return a, nil
}

// DelEnvironmentVar removes the env variable for the given key.
func (a *App) DelEnvironmentVar(k string) (*App, error) {
	err := a.dir.Del("env/" + strings.Replace(k, "_", "-", -1))
	if err != nil {
		return nil, err
	}
	sp, err := a.dir.Snapshot.FastForward()
	if err != nil {
		return nil, err
	}
	a.dir = a.dir.Join(sp)
	return a, nil
}

// GetRevisions returns all registered Revisions for the App
func (a *App) GetRevisions() ([]*Revision, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	revs, err := sp.Getdir(a.dir.Prefix("revs"))
	if err != nil {
		return nil, err
	}

	revisions := []*Revision{}
	ch, errch := cp.GetSnapshotables(revs, func(name string) (cp.Snapshotable, error) {
		return getRevision(a, name, sp)
	})
	for i := 0; i < len(revs); i++ {
		select {
		case r := <-ch:
			revisions = append(revisions, r.(*Revision))
		case err := <-errch:
			return nil, err
		}
	}
	return revisions, nil
}

// GetProcs returns all registered Procs for the App
func (a *App) GetProcs() (procs []*Proc, err error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return
	}
	names, err := sp.Getdir(a.dir.Prefix(procsPath))
	if err != nil || len(names) == 0 {
		if cp.IsErrNoEnt(err) {
			err = nil
		}
		return
	}
	ch, errch := cp.GetSnapshotables(names, func(name string) (cp.Snapshotable, error) {
		return getProc(a, name, sp)
	})
	for i := 0; i < len(names); i++ {
		select {
		case r := <-ch:
			procs = append(procs, r.(*Proc))
		case err := <-errch:
			return nil, err
		}
	}
	return
}

// GetInstances returns all running instances for the app.
func (a *App) GetInstances() ([]*Instance, error) {
	procs, err := a.GetProcs()
	if err != nil {
		return nil, err
	}
	var result []*Instance
	for _, proc := range procs {
		instances, err := proc.GetInstances()
		if err != nil {
			return nil, err
		}
		result = append(result, instances...)
	}
	return result, nil
}

// WatchEvent watches for events related to the app
func (a *App) WatchEvent(listener chan *Event) {
	ch := make(chan *Event)

	go storeFromSnapshotable(a).WatchEvent(ch)

	for e := range ch {
		if e.Path.App != nil && *e.Path.App == a.Name {
			listener <- e
		}
		if i, ok := e.Source.(*Instance); ok && i.AppName == a.Name {
			listener <- e
		}
	}
}

func (a *App) String() string {
	return fmt.Sprintf("App<%s>{stack: %s, type: %s}", a.Name, a.Stack, a.DeployType)
}

// GetApp fetches an app with the given name.
func (s *Store) GetApp(name string) (*App, error) {
	sp, err := s.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	return getApp(name, sp)
}

// GetApps returns the list of all registered Apps.
func (s *Store) GetApps() ([]*App, error) {
	sp, err := s.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	exists, _, err := sp.Exists(appsPath)
	if err != nil || !exists {
		return nil, err
	}
	names, err := sp.Getdir(appsPath)
	if err != nil {
		return nil, err
	}

	apps := []*App{}
	ch, errch := cp.GetSnapshotables(names, func(name string) (cp.Snapshotable, error) {
		return getApp(name, sp)
	})
	for i := 0; i < len(names); i++ {
		select {
		case r := <-ch:
			apps = append(apps, r.(*App))
		case err := <-errch:
			return nil, err
		}
	}
	return apps, nil
}

func getApp(name string, s cp.Snapshotable) (*App, error) {
	sp := s.GetSnapshot()
	app := storeFromSnapshotable(s).NewApp(name, "", "")

	f, err := sp.GetSnapshot().GetFile(app.dir.Prefix("attrs"), new(cp.JsonCodec))
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = errorf(ErrNotFound, `app "%s" not found`, app.Name)
		}
		return nil, err
	}

	value := f.Value.(map[string]interface{})

	app.RepoURL = value["repo-url"].(string)
	app.Stack = value["stack"].(string)
	app.DeployType = value["deploy-type"].(string)

	f, err = app.dir.GetFile(registeredPath, new(cp.StringCodec))
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = errorf(ErrNotFound, "registered not found for %s", app.Name)
		}
		return nil, err
	}
	app.Registered, err = parseTime(f.Value.(string))
	if err != nil {
		return nil, err
	}

	return app, nil
}

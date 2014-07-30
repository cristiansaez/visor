// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"fmt"
	"testing"
)

func appSetup(name string) (*Store, *App) {
	s, err := DialUri(DefaultUri, "/app-test")
	if err != nil {
		panic(fmt.Errorf("Failed to connect to doozer on '%s: %s", DefaultUri, err.Error()))
	}
	err = s.reset()
	if err != nil {
		panic(err)
	}
	s, err = s.FastForward()
	if err != nil {
		panic(err)
	}
	s, err = s.Init()
	if err != nil {
		panic(err)
	}

	app := s.NewApp(name, "git://cat.git", "whiskers")

	return s, app
}

func TestAppRegistration(t *testing.T) {
	_, app := appSetup("lolcatapp")

	check, _, err := app.GetSnapshot().Exists(app.dir.Name)
	if err != nil {
		t.Fatal(err)
	}
	if check {
		t.Fatal("App already registered")
	}

	app, err = app.Register()
	if err != nil {
		t.Error(err)
		return
	}
	check, _, err = app.GetSnapshot().Exists(app.dir.Name)
	if err != nil {
		t.Error(err)
		return
	}
	if !check {
		t.Error("App registration failed")
		return
	}
	_, err = app.Register()
	if err == nil {
		t.Error("App allowed to be registered twice")
	}
}

func TestEnvPersistenceOnRegister(t *testing.T) {
	_, app := appSetup("envyapp")

	app.Env["VAR1"] = "VAL1"
	app.Env["VAR2"] = "VAL2"

	app, err := app.Register()
	if err != nil {
		t.Error(err)
		return
	}

	env, err := app.EnvironmentVars()
	if err != nil {
		t.Error(err)
		return
	}
	for key, val := range app.Env {
		if env[key] != val {
			t.Errorf("%s should be '%s', got '%s'", key, val, env[key])
		}
	}
}

func TestAppUnregister(t *testing.T) {
	_, app := appSetup("dog")

	app, err := app.Register()
	if err != nil {
		t.Error(err)
		return
	}

	err = app.Unregister()
	if err != nil {
		t.Error(err)
		return
	}

	sp, err := app.GetSnapshot().FastForward()
	if err != nil {
		t.Error(err)
	}

	check, _, err := sp.Exists(app.dir.Name)
	if err != nil {
		t.Error(err)
	}
	if check {
		t.Error("App still registered")
	}
}

func TestAppUnregistrationFailure(t *testing.T) {
	_, app := appSetup("dog-fail")

	app, err := app.Register()
	if err != nil {
		t.Error(err)
		return
	}

	err = app.Unregister()
	if err != nil {
		t.Error(err)
		return
	}

	err = app.Unregister()
	if err == nil {
		t.Error("App not present still unregistered")
	}
	if err != nil && !IsErrNotFound(err) {
		t.Fatal(err)
	}
}

func TestSetAndGetEnvironmentVar(t *testing.T) {
	_, app := appSetup("lolcatapp")

	app, err := app.SetEnvironmentVar("meow", "w00t")
	if err != nil {
		t.Error(err)
		return
	}
	if app.Env["meow"] != "w00t" {
		t.Error("app.Env should be updated")
	}

	value, err := app.GetEnvironmentVar("meow")
	if err != nil {
		t.Error(err)
		return
	}

	if value != "w00t" {
		t.Errorf("EnvironmentVar 'meow' expected %s got %s", "w00t", value)
	}
}

func TestSetAndDelEnvironmentVar(t *testing.T) {
	_, app := appSetup("catalolna")

	app, err := app.SetEnvironmentVar("wuff", "lulz")
	if err != nil {
		t.Error(err)
	}

	app, err = app.DelEnvironmentVar("wuff")
	if err != nil {
		t.Error(err)
		return
	}

	v, err := app.GetEnvironmentVar("wuff")
	if err == nil {
		t.Errorf("EnvironmentVar wasn't deleted: %#v", v)
		return
	}
}

func TestEnvironmentVars(t *testing.T) {
	_, app := appSetup("cat-A-log")

	_, err := app.SetEnvironmentVar("whiskers", "purr")
	if err != nil {
		t.Error(err)
	}
	app, err = app.SetEnvironmentVar("lasers", "pew pew")
	if err != nil {
		t.Error(err)
	}

	vars, err := app.EnvironmentVars()
	if err != nil {
		t.Error(err)
	}
	if vars["whiskers"] != "purr" {
		t.Error("Var not set")
	}
	if vars["lasers"] != "pew pew" {
		t.Error("Var not set")
	}
}

func TestAppGetProcs(t *testing.T) {
	s, app := appSetup("bob-the-sponge")
	names := map[string]bool{"api": true, "web": true, "worker": true}

	var proc *Proc
	var err error

	for name := range names {
		proc = s.NewProc(app, name)
		proc, err = proc.Register()
		if err != nil {
			t.Fatal(err)
		}
	}

	procs, err := app.GetProcs()
	if err != nil {
		t.Fatal(err)
	}
	if len(procs) != len(names) {
		t.Errorf("expected length %d returned length %d", len(names), len(procs))
	} else {
		for i := range procs {
			if !names[procs[i].Name] {
				t.Errorf("expected proc to be in map")
			}
		}
	}
}

func TestApps(t *testing.T) {
	s, _ := appSetup("mat-the-sponge")
	names := map[string]bool{"cat": true, "dog": true, "lol": true}

	for k := range names {
		a := s.NewApp(k, "zebra", "joke")
		a, err := a.Register()
		if err != nil {
			t.Fatal(err)
		}
	}

	apps, err := s.GetApps()
	if err != nil {
		t.Error(err)
	}
	if len(apps) != len(names) {
		t.Fatalf("expected length %d returned length %d", len(names), len(apps))
	}

	for i := range apps {
		if !names[apps[i].Name] {
			t.Errorf("expected %s to be in %s", apps[i].Name, names)
		}
	}
}

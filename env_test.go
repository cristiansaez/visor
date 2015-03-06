// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"testing"
)

var store *Store

func envSetup(t *testing.T) *App {
	if store == nil {
		s, err := DialURI(DefaultURI, "/env-test")
		if err != nil {
			t.Fatal(err)
		}
		store = s
	}

	err := store.reset()
	if err != nil {
		t.Fatal(err)
	}
	store, err = store.FastForward()
	if err != nil {
		t.Fatal(err)
	}
	store, err = store.Init()
	if err != nil {
		t.Fatal(err)
	}

	app := store.NewApp("env-test", "git://env.git", "environments")

	return app
}

func TestEnvRegister(t *testing.T) {
	app := envSetup(t)
	ref := "1234"
	vars := map[string]string{"KEY0": "VAL0", "KEY1": "VAL1"}
	env := app.NewEnv(ref, vars)

	check, _, err := app.GetSnapshot().Exists(env.dir.Name)
	if err != nil {
		t.Fatal(err)
	}
	if check {
		t.Fatal("Env already registered")
	}

	env, err = env.Register()
	if err != nil {
		t.Fatal(err)
	}

	check, _, err = env.GetSnapshot().Exists(env.dir.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !check {
		t.Fatal("Env registration failed")
	}

	_, err = env.Register()
	if err == nil {
		t.Error("Env allowed to be overwritten")
	}

	env1, err := app.GetEnv(ref)
	if err != nil {
		t.Fatal(err)
	}
	for key, val := range env.Vars {
		v, ok := env1.Vars[key]
		if !ok {
			t.Errorf("expected key '%s' not found", key)
		}
		if val != v {
			t.Errorf("expected value for '%s' missmatch: %s != %s", key, val, v)
		}
	}
}

func TestEnvUnregister(t *testing.T) {
	app := envSetup(t)
	vars := map[string]string{"KEY0": "VAL0", "KEY1": "VAL1"}
	env := app.NewEnv("4321", vars)

	env, err := env.Register()
	if err != nil {
		t.Fatal(err)
	}

	err = env.Unregister()
	if err != nil {
		t.Fatal(err)
	}

	sp, err := app.GetSnapshot().FastForward()
	if err != nil {
		t.Fatal(err)
	}
	check, _, err := sp.Exists(env.dir.Name)
	if err != nil {
		t.Error(err)
	}
	if check {
		t.Error("Env still registered")
	}
}

func TestEnvKeyValidation(t *testing.T) {
	app := envSetup(t)
	vars := map[string]string{"": "VAL0"}
	env := app.NewEnv("abcd", vars)

	_, err := env.Register()
	if err == nil {
		t.Error("validation didn't catch wrong key")
	}
	if !IsErrInvalidKey(err) {
		t.Fatal(err)
	}

	vars = map[string]string{"KEY=PAIR": "VAL0"}
	env = app.NewEnv("abcd", vars)
	_, err = env.Register()
	if err == nil {
		t.Error("validation didn't catch wrong key")
	}
	if !IsErrInvalidKey(err) {
		t.Fatal(err)
	}
}

func TestEnvList(t *testing.T) {
	app := envSetup(t)
	vars := map[string]string{"KEY0": "VAL0", "KEY1": "VAL1"}
	envs := map[string]*Env{
		"first":  app.NewEnv("first", vars),
		"second": app.NewEnv("second", vars),
		"third":  app.NewEnv("third", vars),
	}

	for _, e := range envs {
		_, err := e.Register()
		if err != nil {
			t.Fatal(err)
		}
	}

	envs1, err := app.GetEnvs()
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != len(envs1) {
		t.Error("GetEnvs didn't return the same amount of envs")
	}
}

package visor

import (
	"testing"
)

func TestHookRegister(t *testing.T) {
	var (
		app    = hookSetup(t)
		name   = "scale"
		script = `#!/bin/sh\n\necho "foo"`
		hook   = app.NewHook(name, script)
	)

	check, _, err := app.GetSnapshot().Exists(hook.file.Path)
	if err != nil {
		t.Fatal(err)
	}
	if check {
		t.Fatal("Hook already registered")
	}

	hook, err = hook.Register()
	if err != nil {
		t.Fatal(err)
	}

	check, _, err = hook.GetSnapshot().Exists(hook.file.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !check {
		t.Fatal("Hook registration failed")
	}

	hook1, err := app.GetHook(name)
	if err != nil {
		t.Fatal(err)
	}
	if hook.Script != hook1.Script {
		t.Errorf("retrieved hook differs: %s - %s", hook.Script, hook1.Script)
	}
}

func TestHookUnregister(t *testing.T) {
	var (
		app    = hookSetup(t)
		name   = "stop"
		script = `#!/bin/bash\necho "bar"`
		hook   = app.NewHook(name, script)
	)

	hook, err := hook.Register()
	if err != nil {
		t.Fatal(err)
	}

	err = hook.Unregister()
	if err != nil {
		t.Fatal(err)
	}

	sp, err := hook.GetSnapshot().FastForward()
	if err != nil {
		t.Fatal(err)
	}
	check, _, err := sp.Exists(hook.file.Path)
	if err != nil {
		t.Fatal(err)
	}
	if check {
		t.Error("Hook still registered")
	}
}

func TestHookList(t *testing.T) {
	app := hookSetup(t)
	script := `#!/bin/sh\necho "list"`
	hooks := []*Hook{
		app.NewHook("foo", script),
		app.NewHook("bar", script),
		app.NewHook("baz", script),
	}

	for _, h := range hooks {
		_, err := h.Register()
		if err != nil {
			t.Fatal(err)
		}
	}

	hooks1, err := app.GetHooks()
	if err != nil {
		t.Fatal(err)
	}
	if len(hooks) != len(hooks1) {
		t.Error("GetHooks didn't return correct amount of hooks")
	}
}

var hookStore *Store

func hookSetup(t *testing.T) *App {
	if hookStore == nil {
		s, err := DialUri(DefaultUri, "/hook-test")
		if err != nil {
			t.Fatal(err)
		}
		hookStore = s
	}

	err := hookStore.reset()
	if err != nil {
		t.Fatal(err)
	}

	hookStore, err = hookStore.FastForward()
	if err != nil {
		t.Fatal(err)
	}

	hookStore, err = hookStore.Init()
	if err != nil {
		t.Fatal(err)
	}

	return hookStore.NewApp("hook-test", "git://hook.git", "hooks")
}

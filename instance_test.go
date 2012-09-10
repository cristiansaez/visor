// Copyright (c) 2012, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"strconv"
	"testing"
)

func instanceSetup(addr string, pType string) (ins *Instance) {
	s, err := Dial(DefaultAddr, "/instance-test")
	if err != nil {
		panic(err)
	}
	s.conn.Del("/", s.Rev)

	s = s.FastForward(-1)

	r, err := Init(s)
	if err != nil {
		panic(err)
	}

	s = s.FastForward(r)

	app := NewApp("ins-test", "git://ins.git", "insane", s)
	rev := NewRevision(app, "7abcde6", s)
	rev.ArchiveUrl = "archive"

	pty := NewProcType(app, pType, s)
	ins, err = NewInstance(string(pty.Name), rev.Ref, app.Name, addr, s)
	if err != nil {
		panic(err)
	}

	_, err = app.Register()
	if err != nil {
		panic(err)
	}
	_, err = rev.Register()
	if err != nil {
		panic(err)
	}
	_, err = pty.Register()
	if err != nil {
		panic(err)
	}

	return
}

func TestInstanceRegister(t *testing.T) {
	ins := instanceSetup("localhost:12345", "web")

	check, _, err := ins.conn.Exists(ins.dir.Name)
	if err != nil {
		t.Error(err)
	}
	if check {
		t.Error("Instance already registered")
	}

	_, err = ins.Register()
	if err != nil {
		t.Error(err)
	}

	check, _, err = ins.conn.Exists(ins.dir.Name)
	if err != nil {
		t.Error(err)
	}
	if !check {
		t.Error("Instance registration failed")
	}

	check, _, err = ins.conn.Exists(ins.proctypePath())
	if err != nil {
		t.Error(err)
	}
	if !check {
		t.Error("Instance registration failed")
	}

	_, err = ins.Register()
	if err == nil {
		t.Error("Instance allowed to be registered twice")
	}
}

func TestGetInstance(t *testing.T) {
	ins := instanceSetup("localhost:9494", "web")
	ins1, err := ins.Register()
	if err != nil {
		t.Errorf("Instance registration failed: %s", err)
	}
	i, err := GetInstance(ins1.Snapshot, ins.Id())
	if err != nil {
		t.Error(err)
	}
	if i.Name != ins.Id() ||
		i.Port != ins.Port ||
		i.RevisionName != ins.RevisionName ||
		i.AppName != ins.AppName ||
		i.ProcessName != ins.ProcessName ||
		i.Host != ins.Host {
		t.Error("Instance fields don't match Instance")
	}
}

func TestInstanceUnregister(t *testing.T) {
	ins := instanceSetup("localhost:54321", "worker")

	ins, err := ins.Register()
	if err != nil {
		t.Error(err)
		return
	}

	err = ins.Unregister()
	if err != nil {
		t.Error(err)
	}

	check, _, err := ins.conn.Exists(ins.dir.Name)
	if err != nil {
		t.Error(err)
	}
	if check {
		t.Error("Instance still registered")
	}
}

func TestInstanceUpdateState(t *testing.T) {
	ins := instanceSetup("localhost:54321", "stateChangeWorker")

	ins, err := ins.Register()
	if err != nil {
		t.Error(err)
	}

	newIns, err := ins.UpdateState(InsStateStarted)
	if err != nil {
		t.Error(err)
	}

	if newIns.State != InsStateStarted {
		t.Error("Instance state wasn't updated")
	}

	if newIns.Rev <= ins.Rev {
		t.Error("Instance wasn't fast forwarded")
	}

	val, _, err := newIns.conn.Get(newIns.dir.prefix("state"), &newIns.Rev)
	if err != nil {
		t.Error(err)
	}

	if State(val) != InsStateStarted {
		t.Error("Instance state wasn't persisted in the coordinator")
	}
}

func TestInstances(t *testing.T) {
	addrs := []string{
		"10.20.3.215:21078",
		"10.20.3.215:21079",
		"10.20.3.215:21080",
	}
	s, err := Dial(DefaultAddr, DefaultRoot)
	if err != nil {
		t.Fatal(err)
	}
	err = s.ResetCoordinator()
	if err != nil {
		t.Fatal(err)
	}

	for i := range addrs {
		instance, err := NewInstance("web", "12345", "test-app", addrs[i], s)
		if err != nil {
			t.Fatal(err)
		}
		instance, err = instance.Register()
		if err != nil {
			t.Fatal(err)
		}
		s = instance.Snapshot
	}

	instances, err := Instances(s)
	if err != nil {
		t.Error(err)
	}
	if len(instances) != len(addrs) {
		t.Errorf("expected length %d returned length %d", len(addrs), len(instances))
	} else {
		for i := range instances {
			addr := instances[i].Host + ":" + strconv.Itoa(instances[i].Port)
			if addr != addrs[i] {
				t.Errorf("expected %s got %s", addrs[i], addr)
			}
		}
	}
}

// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"errors"
	cp "github.com/soundcloud/cotterpin"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func eventSetup() (*Store, chan *Event) {
	s, err := DialUri(DefaultUri, "/event-test")
	if err != nil {
		panic(err)
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

	return s, make(chan *Event)
}

func eventAppSetup(s *Store, name string) *App {
	return s.NewApp(name, "git://"+name, name+"stack")
}

func expectEvent(etype EventType, s cp.Snapshotable, l chan *Event, t *testing.T) (event *Event) {
	for {
		select {
		case event = <-l:
			if event.Type == etype {
				if reflect.TypeOf(event.Source) != reflect.TypeOf(s) {
					t.Errorf("types are not equal %#v != %#v", event.Source, s)
				}
			} else {
				t.Errorf("received incorrect event type: expected %s got %s %s", etype, event, event.Type)
			}
			return
		case <-time.After(time.Second):
			t.Fatalf("expected event type %s got timeout", etype)
		}
	}
	return
}

func TestEventAppRegistered(t *testing.T) {
	s, l := eventSetup()
	app := eventAppSetup(s, "regcat")

	go s.WatchEvent(l)

	_, err := app.Register()
	if err != nil {
		t.Error(err)
	}

	ev := expectEvent(EvAppReg, app, l, t)
	if ev.Path.App == nil || (*ev.Path.App != app.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
}

func TestEventAppUnregistered(t *testing.T) {
	s, l := eventSetup()
	app := eventAppSetup(s, "unregcat")

	app, err := app.Register()
	if err != nil {
		t.Fatal(err)
	}

	go app.WatchEvent(l)

	err = app.Unregister()
	if err != nil {
		t.Error(err)
	}

	ev := expectEvent(EvAppUnreg, nil, l, t)
	if ev.Path.App == nil || (*ev.Path.App != app.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
}

func TestEventRevRegistered(t *testing.T) {
	s, l := eventSetup()
	app := eventAppSetup(s, "regdog")

	app, err := app.Register()
	if err != nil {
		t.Error(err)
	}
	s = storeFromSnapshotable(app)

	rev := s.NewRevision(app, "stable", "stable.img")

	go s.WatchEvent(l)

	_, err = rev.Register()
	if err != nil {
		t.Error(err)
	}

	ev := expectEvent(EvRevReg, rev, l, t)
	if ev.Path.Revision == nil || (*ev.Path.Revision != rev.Ref) {
		t.Error("event.Path doesn't contain expected data")
	}
	if ev.Path.App == nil || (*ev.Path.App != app.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
}

func TestEventRevUnregistered(t *testing.T) {
	s, l := eventSetup()
	app := eventAppSetup(s, "unregdog")

	app, err := app.Register()
	if err != nil {
		t.Error(err)
	}

	rev := s.NewRevision(app, "stable", "stable.img")
	rev, err = rev.Register()
	if err != nil {
		t.Error(err)
		return
	}
	go storeFromSnapshotable(rev).WatchEvent(l)

	err = rev.Unregister()
	if err != nil {
		t.Error(err)
	}

	ev := expectEvent(EvRevUnreg, nil, l, t)
	if ev.Path.Revision == nil || (*ev.Path.Revision != rev.Ref) {
		t.Error("event.Path doesn't contain expected data")
	}
}

func TestEventProcRegistered(t *testing.T) {
	var (
		s, l = eventSetup()
		app  = eventAppSetup(s, "proc-register")
		proc = s.NewProc(app, "all")
	)

	app, err := app.Register()
	if err != nil {
		t.Error(err)
	}

	go storeFromSnapshotable(app).WatchEvent(l)

	proc, err = proc.Register()
	if err != nil {
		t.Error(err)
	}

	ev := expectEvent(EvProcReg, proc, l, t)
	if ev.Path.Proc == nil || (*ev.Path.Proc != proc.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
	if ev.Path.App == nil || (*ev.Path.App != app.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
}

func TestEventProcUnregistered(t *testing.T) {
	var (
		s, l = eventSetup()
		app  = eventAppSetup(s, "proc-unregister")
		proc = s.NewProc(app, "all")
	)

	proc, err := proc.Register()
	if err != nil {
		t.Fatal(err)
	}

	go storeFromSnapshotable(proc).WatchEvent(l)

	err = proc.Unregister()
	if err != nil {
		t.Fatal(err)
	}

	ev := expectEvent(EvProcUnreg, nil, l, t)

	if ev.Path.Proc == nil || (*ev.Path.Proc != proc.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
}

func TestEventProcAttrs(t *testing.T) {
	var (
		s, l    = eventSetup()
		app     = eventAppSetup(s, "proc-attrs")
		proc    = s.NewProc(app, "mightymouse")
		srvInfo = &SrvInfo{
			Env:     "prod",
			Job:     "mighty",
			Product: "mouse",
			Service: "http",
		}
	)

	app, err := app.Register()
	if err != nil {
		t.Error(err)
	}

	proc, err = proc.Register()
	if err != nil {
		t.Fatal(err)
	}

	go storeFromSnapshotable(proc).WatchEvent(l)

	proc.Attrs.SrvInfo = srvInfo

	proc, err = proc.StoreAttrs()
	if err != nil {
		t.Fatal(err)
	}

	ev := expectEvent(EvProcAttrs, proc, l, t)
	if ev.Path.Proc == nil || (*ev.Path.Proc != proc.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
	if ev.Path.App == nil || (*ev.Path.App != app.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
	if !reflect.DeepEqual(srvInfo, ev.Source.(*Proc).Attrs.SrvInfo) {
		t.Errorf("event proc attrs differ: %#v != %#v", srvInfo, ev.Source.(*Proc).Attrs.SrvInfo)
	}
}

func TestEventInstanceRegistered(t *testing.T) {
	s, l := eventSetup()
	app := eventAppSetup(s, "regmouse")

	go s.WatchEvent(l)

	ins, err := s.RegisterInstance(app.Name, "stable", "web", "default")
	if err != nil {
		t.Fatal(err)
	}

	ev := expectEvent(EvInsReg, ins, l, t)

	if ev.Path.Instance == nil || (*ev.Path.Instance != strconv.FormatInt(ins.Id, 10)) {
		t.Error("event.Path doesn't contain expected data")
	}
}

func TestEventInstanceUnregistered(t *testing.T) {
	s, l := eventSetup()

	ins, err := s.RegisterInstance("unregmouse", "stable", "web", "default")
	if err != nil {
		t.Fatal(err)
	}
	go storeFromSnapshotable(ins).WatchEvent(l)

	err = ins.Unregister("common-host", errors.New("exited"))
	if err != nil {
		t.Fatal(err)
	}

	ev := expectEvent(EvInsUnreg, nil, l, t)
	if ev.Path.Instance == nil || (*ev.Path.Instance != strconv.FormatInt(ins.Id, 10)) {
		t.Error("event.Path doesn't contain expected data")
	}
}

func TestEventInstanceStateChange(t *testing.T) {
	ip := "10.0.0.1"
	port := 9999
	tPort := 10000
	host := "mouse.org"
	s, l := eventSetup()

	ins, err := s.RegisterInstance("statemouse", "stable-state", "web-state", "default-state")
	if err != nil {
		t.Fatal(err)
	}
	ins, err = ins.Claim(ip)
	if err != nil {
		t.Fatal(err)
	}

	go storeFromSnapshotable(ins).WatchEvent(l)

	ins, err = ins.Started(ip, host, port, tPort)
	if err != nil {
		t.Error(err)
	}
	ev := expectEvent(EvInsStart, ins, l, t)
	if ev.Path.Instance == nil || (*ev.Path.Instance != strconv.FormatInt(ins.Id, 10)) {
		t.Error("event.Path doesn't contain expected data")
	}

	instance := ev.Source.(*Instance)

	if instance.Ip != ip || instance.Host != host || instance.Port != port {
		t.Fatal("instance fields don't match")
	}

	ins, err = ins.Failed(ip, errors.New("no reason."))
	if err != nil {
		t.Error(err)
	}
	expectEvent(EvInsFail, ins, l, t)

	ins, err = ins.Exited(ip)
	if err != nil {
		t.Error(err)
	}
	expectEvent(EvInsExit, ins, l, t)
}

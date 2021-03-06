// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	cp "github.com/soundcloud/cotterpin"
)

func eventSetup() (*Store, chan *Event) {
	s, err := DialURI(DefaultURI, "/event-test")
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

func expectEvent(etype EventType, s cp.Snapshotable, l chan *Event, t *testing.T) *Event {
	for {
		select {
		case event := <-l:
			if event.Type == etype {
				if reflect.TypeOf(event.Source) != reflect.TypeOf(s) {
					t.Errorf("%s types are not equal: %#v != %#v", etype, event.Source, s)
				}
			} else {
				t.Errorf("received incorrect event type: expected %s got %s %s", etype, event, event.Type)
			}
			return event
		case <-time.After(time.Second):
			t.Fatalf("expected event type %s got timeout", etype)
		}
	}
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
	s, l := eventSetup()
	app := eventAppSetup(s, "proc-register")

	app, err := app.Register()
	if err != nil {
		t.Error(err)
	}

	rev := s.NewRevision(app, "bang", "bang.img")
	rev, err = rev.Register()
	if err != nil {
		t.Fatal(err)
	}
	proc := s.NewProc(app, "all")

	go storeFromSnapshotable(rev).WatchEvent(l)

	_, err = proc.Register()
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
	s, l := eventSetup()
	app := eventAppSetup(s, "proc-unregister")
	proc := s.NewProc(app, "all")

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
		control = &TrafficControl{
			Share: 80,
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

	go func() {
		err := storeFromSnapshotable(proc).WatchEvent(l)
		if err != nil {
			t.Fatal(err)
		}
	}()

	proc.Attrs.TrafficControl = control

	proc, err = proc.StoreAttrs()
	if err != nil {
		t.Fatal(err)
	}

	var (
		ev = expectEvent(EvProcAttrs, proc, l, t)
		p  = ev.Source.(*Proc)
	)

	if ev.Path.Proc == nil || (*ev.Path.Proc != proc.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
	if ev.Path.App == nil || (*ev.Path.App != app.Name) {
		t.Error("event.Path doesn't contain expected data")
	}
	if want, have := control, p.Attrs.TrafficControl; !reflect.DeepEqual(want, have) {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestEventInstanceRegistered(t *testing.T) {
	s, l := eventSetup()
	app := eventAppSetup(s, "regmouse")

	go func() {
		err := s.WatchEvent(l)
		if err != nil {
			t.Fatal(err)
		}
	}()

	ins, err := s.RegisterInstance(app.Name, "stable", "web", "default")
	if err != nil {
		t.Fatal(err)
	}

	ev := expectEvent(EvInsReg, ins, l, t)

	if ev.Path.Instance == nil || (*ev.Path.Instance != strconv.FormatInt(ins.ID, 10)) {
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
	if ev.Path.Instance == nil || (*ev.Path.Instance != strconv.FormatInt(ins.ID, 10)) {
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

	go storeFromSnapshotable(ins).WatchEvent(l)

	if _, err = ins.Claim("0.0.0.0"); err != nil {
		t.Fatal(err)
	}

	ins, err = ins.Unclaim("0.0.0.0")
	if err != nil {
		t.Error(err)
	}
	expectEvent(EvInsUnclaim, ins, l, t)

	if _, err = ins.Claim(ip); err != nil {
		t.Fatal(err)
	}

	ins, err = ins.Started(ip, host, port, tPort)
	if err != nil {
		t.Error(err)
	}
	ev := expectEvent(EvInsStart, ins, l, t)
	if ev.Path.Instance == nil || (*ev.Path.Instance != strconv.FormatInt(ins.ID, 10)) {
		t.Error("event.Path doesn't contain expected data")
	}
	instance := ev.Source.(*Instance)
	if instance.IP != ip || instance.Host != host || instance.Port != port {
		t.Fatal("instance fields don't match")
	}

	if err := ins.Stop(); err != nil {
		t.Fatal(err)
	}
	expectEvent(EvInsStop, ins, l, t)

	ins, err = ins.Failed(ip, errors.New("no reason"))
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

func TestEventInstanceEnrichment(t *testing.T) {
	s, l := eventSetup()

	ins, err := s.RegisterInstance("foo", "bar", "baz", "qux")
	if err != nil {
		t.Fatal(err)
	}
	if err := ins.Unregister("common-host", errors.New("exited")); err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := s.WatchEvent(l); err != nil {
			t.Fatal(err)
		}
	}()

	expectEvent(EvInsReg, ins, l, t)
	expectEvent(EvInsUnreg, nil, l, t)
}

func TestEventFilter(t *testing.T) {
	s, l := eventSetup()

	ins, err := s.RegisterInstance("foo", "bar", "baz", "qux")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ins.Claim("1.2.3.4"); err != nil {
		t.Fatal(err)
	}
	if _, err = ins.Unclaim("1.2.3.4"); err != nil {
		t.Fatal(err)
	}
	if _, err = ins.Claim("1.2.8.9"); err != nil {
		t.Fatal(err)
	}
	if _, err = ins.Started("1.2.8.9", "host.com", 9090, 9095); err != nil {
		t.Fatal(err)
	}
	if err := ins.Unregister("common-host", errors.New("exited")); err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := s.WatchEvent(l, EvInsStart, EvInsUnreg); err != nil {
			t.Fatal(err)
		}
	}()

	expectEvent(EvInsStart, ins, l, t)
	expectEvent(EvInsUnreg, nil, l, t)
}

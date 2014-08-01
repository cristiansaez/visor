// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"errors"
	"testing"
)

func TestProcRegister(t *testing.T) {
	s, app := procSetup("reg123")
	proc := s.NewProc(app, "whoop")

	proc, err := proc.Register()
	if err != nil {
		t.Error(err)
	}

	check, _, err := proc.GetSnapshot().Exists(proc.dir.Name)
	if err != nil {
		t.Error(err)
	}
	if !check {
		t.Errorf("proc %s isn't registered", proc)
	}
}

func TestProcRegisterWithInvalidName(t *testing.T) {
	s, app := procSetup("reg1232")
	proc := s.NewProc(app, "who-op")

	proc, err := proc.Register()
	if err != ErrBadProcName {
		t.Errorf("invalid proc type name (who-op) did not raise error")
	}
	if err != ErrBadProcName && err != nil {
		t.Fatal("wrong error was raised for invalid proc type name")
	}

	proc = s.NewProc(app, "who_op")

	proc, err = proc.Register()
	if err != ErrBadProcName {
		t.Errorf("invalid proc type name (who_op) did not raise error")
	}
	if err != ErrBadProcName && err != nil {
		t.Fatal("wrong error was raised for invalid proc type name")
	}
}

func TestProcUnregister(t *testing.T) {
	s, app := procSetup("unreg123")
	proc := s.NewProc(app, "whoop")

	proc, err := proc.Register()
	if err != nil {
		t.Error(err)
	}

	err = proc.Unregister()
	if err != nil {
		t.Error(err)
	}

	check, _, err := s.GetSnapshot().Exists(proc.dir.Name)
	if check {
		t.Errorf("proc %s is still registered", proc)
	}
}

func TestProcGetInstances(t *testing.T) {
	appid := "get-instances-app"
	s, app := procSetup(appid)

	proc := s.NewProc(app, "web")
	proc, err := proc.Register()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		ins, err := s.RegisterInstance(appid, "128af90", "web", "default")
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Claim("10.0.0.1")
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Started("10.0.0.1", appid+".org", 9999, 10000)
		if err != nil {
			t.Fatal(err)
		}
	}

	is, err := proc.GetInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(is) != 3 {
		t.Errorf("list is missing instances: %s", is)
	}
}

func TestProcGetDoneInstances(t *testing.T) {
	var (
		appid  = "get-done-instances-app"
		s, app = procSetup(appid)
		host   = "10.0.2.12"
	)

	proc, err := s.NewProc(app, "worker").Register()
	if err != nil {
		t.Fatal(err)
	}

	is := []*Instance{}

	for i := 0; i < 13; i++ {
		ins, err := s.RegisterInstance(appid, "643asd3", "worker", "prod")
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Claim(host)
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Started(host, appid+".org", 9898, 9899)
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Exited(host)
		if err != nil {
			t.Fatal(err)
		}
		err = ins.Unregister("proc-test", errors.New("done here."))
		if err != nil {
			t.Fatal(err)
		}

		is = append(is, ins)
	}

	done, err := proc.GetDoneInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != len(is) {
		t.Errorf("wrong number of done instances returned: %d != %d", len(done), len(is))
	}
}

func TestProcGetFailedInstances(t *testing.T) {
	appid := "get-failed-instances-app"
	s, app := procSetup(appid)

	proc := s.NewProc(app, "web")
	proc, err := proc.Register()
	if err != nil {
		t.Fatal(err)
	}

	instances := []*Instance{}

	for i := 0; i < 7; i++ {
		ins, err := s.RegisterInstance(appid, "128af9", "web", "default")
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Claim("10.0.0.1")
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Started("10.0.0.1", appid+".org", 9999, 10000)
		if err != nil {
			t.Fatal(err)
		}
		instances = append(instances, ins)
	}
	for i := 0; i < 4; i++ {
		_, err := instances[i].Failed("10.0.0.1", errors.New("no reason."))
		if err != nil {
			t.Fatal(err)
		}
	}

	failed, err := proc.GetFailedInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 4 {
		t.Errorf("list is missing instances: %s", len(failed))
	}

	is, err := proc.GetInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(is) != 3 {
		t.Errorf("remaining instances list wrong: %d", len(is))
	}
}

func TestProcGetLostInstances(t *testing.T) {
	appid := "get-lost-instances-app"
	s, app := procSetup(appid)

	proc, err := s.NewProc(app, "worker").Register()
	if err != nil {
		t.Fatal(err)
	}

	instances := []*Instance{}

	for i := 0; i < 9; i++ {
		ins, err := s.RegisterInstance(appid, "83jad2f", "worker", "mem-leak")
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Claim("10.3.2.1")
		if err != nil {
			t.Fatal(err)
		}
		ins, err = ins.Started("10.3.2.1", "box00.vm", 9898, 9899)
		if err != nil {
			t.Fatal(err)
		}
		instances = append(instances, ins)
	}

	for i := 0; i < 3; i++ {
		_, err := instances[i].Lost("watchman", errors.New("it's gone!!!"))
		if err != nil {
			t.Fatal(err)
		}
	}
	lost, err := proc.GetLostInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 3 {
		t.Errorf("lost list is missing instances: %d", len(lost))
	}

	is, err := proc.GetInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(is) != 6 {
		t.Errorf("remaining instances list wrong: %d", len(is))
	}
}

func TestProcAttrs(t *testing.T) {
	appid := "app-with-attributes"
	var memoryLimitMb = 100
	s, app := procSetup(appid)

	proc := s.NewProc(app, "web")
	proc, err := proc.Register()
	if err != nil {
		t.Fatal(err)
	}

	proc, err = app.GetProc("web")
	if err != nil {
		t.Fatal(err)
	}
	if proc.Attrs.Limits.MemoryLimitMb != nil {
		t.Fatal("MemoryLimitMb should not be set at this point")
	}

	proc.Attrs.Limits.MemoryLimitMb = &memoryLimitMb
	proc, err = proc.StoreAttrs()
	if err != nil {
		t.Fatal(err)
	}

	proc, err = app.GetProc("web")
	if err != nil {
		t.Fatal(err)
	}
	if proc.Attrs.Limits.MemoryLimitMb == nil {
		t.Fatalf("MemoryLimitMb is nil")
	}
	if *proc.Attrs.Limits.MemoryLimitMb != memoryLimitMb {
		t.Fatalf("MemoryLimitMb does not contain the value that was set")
	}
}

func procSetup(appid string) (*Store, *App) {
	s, err := DialUri(DefaultUri, "/proc-test")
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

	app := s.NewApp(appid, "git://proc.git", "master")

	return s, app
}

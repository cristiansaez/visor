// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	cp "github.com/soundcloud/cotterpin"
)

func instanceSetup() *Store {
	s, err := DialURI(DefaultURI, "/instance-test")
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

	return s
}

func instanceSetupClaimed(name, host string) (i *Instance) {
	s := instanceSetup()

	i, err := s.RegisterInstance(name, "128af9", "web", "default")
	if err != nil {
		panic(err)
	}

	i, err = i.Claim(host)
	if err != nil {
		panic(err)
	}
	return
}

func TestInstanceRegisterAndGet(t *testing.T) {
	s := instanceSetup()

	ins, err := s.RegisterInstance("cat", "128af9", "web", "default")
	if err != nil {
		t.Fatal(err)
	}

	if ins.Status != InsStatusPending {
		t.Error("instance status wasn't set correctly")
	}
	if ins.ID <= 0 {
		t.Error("instance id wasn't set correctly")
	}

	ins1, err := s.GetInstance(ins.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ins1.ID != ins.ID {
		t.Error("ids don't match")
	}
	if ins1.Status != ins.Status {
		t.Error("statuses don't match")
	}
	if ins1.Restarts.Fail != 0 {
		t.Error("restarts != 0")
	}
}

func TestInstanceUnregister(t *testing.T) {
	app := "dog"
	rev := "7654321"
	proc := "batch"
	env := "prod"
	ip := "10.10.0.5"
	port := 58585
	tPort := 58586
	host := "box13.kool.aid"
	s := instanceSetup()

	i, err := s.RegisterInstance(app, rev, proc, env)
	if err != nil {
		t.Fatal(err)
	}
	i, err = i.Claim(ip)
	if err != nil {
		t.Fatal(err)
	}
	i, err = i.Started(ip, host, port, tPort)
	if err != nil {
		t.Fatal(err)
	}
	i, err = i.Exited(ip)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Unregister(ip, errors.New("exited"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.GetInstance(i.ID)
	if !IsErrNotFound(err) {
		t.Fatal(err)
	}

	done, err := i.IsDone()
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Errorf("expected instance %d to be done", i.ID)
	}
}

func TestInstanceClaiming(t *testing.T) {
	hostA := "10.0.0.1"
	hostB := "10.0.0.2"
	hostC := "10.0.0.3"
	s := instanceSetup()

	ins, err := s.RegisterInstance("bat", "128af9", "web", "default")
	if err != nil {
		t.Fatal(err)
	}

	ins1, err := ins.Claim(hostA)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ins.Claim(hostA) // Already claimed
	if !IsErrInsClaimed(err) {
		t.Error("expected re-claim to fail")
	}

	_, err = ins1.Claim(hostA) // Already claimed
	if !IsErrInsClaimed(err) {
		t.Error("expected re-claim to fail")
	}

	claims, err := ins1.Claims()
	if err != nil {
		t.Fatal(err)
	}

	if len(claims) == 0 {
		t.Error("instance claim was unsuccessful")
	}
	if claims[0] != hostA {
		t.Error("instance claims doesn't include claimer")
	}

	ins2, err := ins1.Unclaim(hostA)
	if err != nil {
		t.Fatal(err)
	}

	claimer, err := ins2.getClaimer()
	if err != nil {
		t.Fatal(err)
	}
	if claimer != nil {
		t.Error("ticket wasn't unclaimed properly")
	}

	_, err = ins1.Unclaim(hostB) // Wrong host
	if !IsErrUnauthorized(err) {
		t.Error("expected unclaim to fail")
	}

	_, err = ins2.Unclaim(hostA) // Already unclaimed
	if !IsErrUnauthorized(err) {
		t.Error("expected unclaim to fail")
	}

	i, err := ins1.Claim(hostB)
	if err != nil {
		t.Fatal(err)
	}
	i, err = i.Started(hostB, "box13.friday.net", 9999, 10000)
	if err != nil {
		t.Fatal(err)
	}
	i, err = i.Exited(hostB)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Unregister(hostB, errors.New("scaled down"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = ins.Claim(hostC)
	if !IsErrUnauthorized(err) {
		t.Error("expect claim of done ticket to fail")
	}
}

func TestInstanceStarted(t *testing.T) {
	app := "fat"
	rev := "128af9"
	proc := "web"
	env := "default"
	ip := "10.0.0.1"
	port := 25790
	tPort := 25791
	host := "fat.the-pink-rabbit.co"
	s := instanceSetup()

	ins, err := s.RegisterInstance(app, rev, proc, env)
	if err != nil {
		t.Fatal(err)
	}
	ins1, err := ins.Claim(ip)
	if err != nil {
		t.Fatal(err)
	}

	ins2, err := ins1.Started(ip, host, port, tPort)
	if err != nil {
		t.Fatal(err)
	}

	if ins2.Status != InsStatusRunning {
		t.Errorf("unexpected status '%s'", ins2.Status)
	}

	if ins2.Port != port || ins2.Host != host || ins2.IP != ip {
		t.Errorf("instance attributes not set correctly for %#v", ins2)
	}

	ins3, err := s.GetInstance(ins2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ins3.Port != port || ins3.Host != host || ins3.IP != ip {
		t.Errorf("instance attributes not stored correctly for %#v", ins3)
	}

	ids, err := getInstanceIds(app, rev, proc, ins3)
	if err != nil {
		t.Fatal(err)
	}

	if !func() bool {
		for _, id := range ids {
			if id == ins.ID {
				return true
			}
		}
		return false
	}() {
		t.Errorf("instance wasn't found under proc '%s'", proc)
	}
}

func TestInstanceStop(t *testing.T) {
	ip := "10.0.0.1"
	s := instanceSetup()

	ins, err := s.RegisterInstance("rat", "128af9", "web", "default")
	if err != nil {
		t.Fatal(err)
	}
	_, err = ins.Claim(ip)
	if err != nil {
		t.Fatal(err)
	}

	ins, err = ins.Started(ip, "localhost", 5555, 5556)
	if err != nil {
		t.Fatal(err)
	}

	err = ins.Stop()
	if err != nil {
		t.Fatal(err)
	}

	err = ins.Unregister("test-suite", fmt.Errorf("cleanup"))
	if err != nil {
		t.Fatal(err)
	}

	err = ins.Stop()
	if !IsErrNotFound(err) {
		t.Errorf("have %v, want %v", err, ErrNotFound)
	}
	// Note: we aren't checking that the files are created in the coordinator,
	// that is better tested via events in event.go, as we don't want to couple
	// the tests with the schema.
}

func TestInstanceExited(t *testing.T) {
	ip := "10.0.0.1"
	port := 25790
	tPort := 25791
	host := "fat.the-pink-rabbit.co"
	s := instanceSetup()

	ins, err := s.RegisterInstance("rat-cat", "128af9", "web", "default")
	if err != nil {
		t.Fatal(err)
	}
	ins, err = ins.Claim(ip)
	if err != nil {
		t.Fatal(err)
	}

	ins, err = ins.Started(ip, host, port, tPort)
	if err != nil {
		t.Fatal(err)
	}

	err = ins.Stop()
	if err != nil {
		t.Fatal(err)
	}

	ins, err = ins.Exited(ip)
	if err != nil {
		t.Fatal(err)
	}
	testInstanceStatus(s, t, ins.ID, InsStatusExited)
}

func TestInstanceRestarted(t *testing.T) {
	ip := "10.0.0.1"
	ins := instanceSetupClaimed("fat-pat", ip)

	ins, err := ins.Started(ip, "fat-pat.com", 9999, 10000)
	if err != nil {
		t.Fatal(err)
	}

	if ins.Restarts.Fail != 0 {
		t.Error("expected restart count to be 0")
	}

	ins1, err := ins.Restarted(InsRestarts{0, 1})
	if err != nil {
		t.Fatal(err)
	}

	if ins1.Restarts.Fail != 1 {
		t.Error("expected restart count to be set to 1")
	}

	ins2, err := ins1.Restarted(InsRestarts{0, 2})
	if err != nil {
		t.Fatal(err)
	}

	if ins2.Restarts.Fail != 2 {
		t.Error("expected restart count to be set to 2")
	}
	ins3, err := storeFromSnapshotable(ins).GetInstance(ins.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ins3.Restarts.Fail != 2 {
		t.Error("expected restart count to be set to 2")
	}

	err = ins1.Unregister("test-bot", fmt.Errorf("cleanup"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = ins1.Restarted(InsRestarts{0, 3})
	if have, want := err, ErrNotFound; !IsErrNotFound(err) {
		t.Errorf("have %v, want %v", have, want)
	}
}

func TestInstanceFailed(t *testing.T) {
	ip := "10.0.0.1"
	ins := instanceSetupClaimed("fat-cat", ip)

	ins, err := ins.Started(ip, "fat-cat.com", 9999, 10000)
	if err != nil {
		t.Fatal(err)
	}

	ins1, err := ins.Failed(ip, errors.New("because"))
	if err != nil {
		t.Fatal(err)
	}
	testInstanceStatus(storeFromSnapshotable(ins1), t, ins.ID, InsStatusFailed)

	_, err = ins.Failed("9.9.9.9", errors.New("no reason"))
	if !IsErrUnauthorized(err) {
		t.Error("expected command to fail")
	}

	// Note: we do not test whether or not failed instances can be retrieved
	// here. See the proc tests & (*Proc).GetFailedInstances()
}

func TestPendingInstanceFailed(t *testing.T) {
	var (
		store = instanceSetup()

		ins1, _ = store.RegisterInstance("bat", "128af9", "web", "default")
		ins2, _ = store.GetInstance(ins1.ID)
	)

	if _, err := ins1.Failed("9.9.9.8", errors.New("fail1")); err != nil {
		t.Fatal(err)
	}

	_, err := ins2.Failed("9.9.9.9", errors.New("fail2"))
	if !cp.IsErrRevMismatch(err) {
		t.Fatalf("expected REV_MISMATCH, got: %q", err)
	}

	ins, _ := store.GetInstance(ins1.ID)

	if ins.Status != InsStatusFailed {
		t.Fatalf("expected status to be failed, got %q", ins.Status)
	}

	if info, _ := ins.GetStatusInfo(); strings.Contains(info, "fail2") {
		t.Fatalf("expected info not to include 'fail2', got %q", info)
	}
}

func TestInstanceLost(t *testing.T) {
	ip := "10.0.0.2"
	ins := instanceSetupClaimed("slim-cat", ip)

	ins, err := ins.Started(ip, "box00.vm", 9898, 9899)
	if err != nil {
		t.Fatal(err)
	}
	ins, err = ins.Lost("watchdog", errors.New("dunno if cat is dead or not"))
	if err != nil {
		t.Fatal(err)
	}
	testInstanceStatus(storeFromSnapshotable(ins), t, ins.ID, InsStatusLost)

	// Note: we do not test whether or not lost instances can be retrieved
	// here. See the proc tests & (*Proc).GetLostInstances()
}

func TestWatchInstanceStartAndStop(t *testing.T) {
	app := "w-app"
	rev := "w-rev"
	proc := "w-proc"
	env := "w-env"
	s := instanceSetup()
	l := make(chan *Instance)

	go s.WatchInstanceStart(l, make(chan error))

	ins, err := s.RegisterInstance(app, rev, proc, env)
	if err != nil {
		t.Error(err)
	}

	select {
	case ins = <-l:
		// TODO Check other fields
		if ins.AppName == app && ins.RevisionName == rev && ins.ProcessName == proc {
			break
		}
		t.Errorf("received unexpected instance: %s", ins.String())
	case <-time.After(time.Second):
		t.Errorf("expected instance, got timeout")
	}

	ins, err = ins.Claim("10.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	ins, err = ins.Started("10.0.0.1", "localhost", 5555, 5556)
	if err != nil {
		t.Fatal(err)
	}

	// Stop test

	ch := make(chan *Instance)

	go func() {
		ins, err := ins.WaitStop()
		if err != nil {
			t.Fatal(err)
		}
		ch <- ins
	}()

	err = ins.Stop()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case ins1 := <-ch:
		if ins1 == nil {
			t.Error("instance is nil")
		}
	case <-time.After(time.Second):
		t.Errorf("expected instance, got timeout")
	}
}

func TestInstanceWait(t *testing.T) {
	s := instanceSetup()
	ins, err := s.RegisterInstance("bob", "985245a", "web", "default")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if _, err := ins.Claim("127.0.0.1"); err != nil {
			panic(err)
		}
	}()
	ins1, err := ins.WaitClaimed()
	if err != nil {
		t.Error(err)
	}
	if ins1.Status != InsStatusClaimed {
		t.Errorf("expected instance status to be %s", InsStatusClaimed)
	}

	go func() {
		if _, err := ins1.Started("127.0.0.1", "localhost", 9000, 9001); err != nil {
			panic(err)
		}
	}()
	ins2, err := ins1.WaitStarted()
	if err != nil {
		t.Error(err)
	}
	if ins2.Status != InsStatusRunning {
		t.Errorf("expected instance status to be %s", InsStatusRunning)
	}
	if ins2.IP != "127.0.0.1" || ins2.Port != 9000 || ins2.Host != "localhost" {
		t.Errorf("expected ip/port/host to match for %#v", ins2)
	}
}

func TestInstanceWaitStop(t *testing.T) {
	s := instanceSetup()
	ins, err := s.RegisterInstance("bobby", "985245a", "web", "default")
	if err != nil {
		t.Fatal(err)
	}
	ins, err = ins.Claim("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ins.Started("127.0.0.1", "localhost", 9000, 9001); err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := ins.Stop(); err != nil {
			panic(err)
		}
	}()
	_, err = ins.WaitStop()
	if err != nil {
		t.Fatal(err)
	}
	ins, err = s.GetInstance(ins.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ins.Status != InsStatusStopping {
		t.Error("expected instance to be stopped")
	}
	// println(ins.GetSnapshot().Rev)
	// println(ins1.GetSnapshot().Rev)
	// if ins1.GetSnapshot().Rev <= ins.GetSnapshot().Rev {
	// 	t.Error("expected new revision to be greater than previous")
	// }
}

func TestInstanceWaitUnregister(t *testing.T) {
	s := instanceSetup()

	ins, err := s.RegisterInstance("jin", "7e45c4a", "worker", "prod")
	if err != nil {
		t.Fatal(err)
	}

	ins, err = ins.Claim("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}

	ins, err = ins.Started("127.0.0.1", "localhost", 20000, 20001)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		err := ins.Unregister("visor-test", fmt.Errorf("gone"))
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = ins.WaitUnregister()
	if err != nil {
		t.Fatal(err)
	}
}

func TestInstanceLocking(t *testing.T) {
	ip := "10.0.10.0"
	ins := instanceSetupClaimed("grumpy-cat", ip)

	ins, err := ins.Started(ip, "box01.vm", 7676, 7677)
	if err != nil {
		t.Fatal(err)
	}
	ins, err = ins.Lock("schroedinger", errors.New("to be rescheduled"))
	if err != nil {
		t.Fatal(err)
	}
	locked, err := ins.IsLocked()
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("expected instance to be locked")
	}
	_, err = ins.Lock("somebody", errors.New("steal the lock"))
	if !IsErrUnauthorized(err) {
		t.Fatal("expected to not allow aquire lock twice")
	}
	ins, err = ins.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	locked, err = ins.IsLocked()
	if err != nil {
		t.Fatal(err)
	}
	if locked {
		t.Error("expected instance to not be locked")
	}
}

func TestInstanceSerialisation(t *testing.T) {
	var (
		ip  = "10.0.10.3"
		s   = instanceSetup()
		ins = instanceSetupClaimed("extra-done", ip)
	)

	ins, err := ins.Started(ip, "box02.vm", 7777, 7778)
	if err != nil {
		t.Fatal(err)
	}

	err = ins.Unregister("test-client", errors.New("done with this"))
	if err != nil {
		t.Fatal(err)
	}

	s, err = s.FastForward()
	if err != nil {
		t.Fatal(err)
	}

	ins1, err := s.GetSerialisedInstance(ins.AppName, ins.ProcessName, ins.ID, InsStatusDone)
	if err != nil {
		t.Fatal(err)
	}

	ins1.dir = ins.dir
	ins1.Claimed = ins1.Claimed.In(ins.Claimed.Location())
	ins1.Registered = ins1.Registered.In(ins.Registered.Location())
	ins1.Termination.Time = ins1.Termination.Time.In(ins.Termination.Time.Location())

	if !reflect.DeepEqual(ins, ins1) {
		t.Errorf("serialised instance doesn't match original:\n%#v\n%#v", ins, ins1)
	}
}

func testInstanceStatus(s *Store, t *testing.T, id int64, status InsStatus) {
	ins, err := s.GetInstance(id)
	if err != nil {
		t.Fatal(err)
	}
	if ins.Status != status {
		t.Errorf("expected instance status to be '%s' got '%s'", status, ins.Status)
	}
}

// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"testing"
)

func revSetup() (s *Store, app *App) {
	s, err := DialURI(DefaultURI, "/revision-test")
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

	app = s.NewApp("rev-test", "git://rev.git", "references")

	return
}

func TestRevisionRegister(t *testing.T) {
	s, app := revSetup()
	rev := s.NewRevision(app, "stable", "stable.img")

	check, _, err := s.GetSnapshot().Exists(rev.dir.Name)
	if err != nil {
		t.Error(err)
		return
	}
	if check {
		t.Error("Revision already registered")
		return
	}

	rev, err = rev.Register()
	if err != nil {
		t.Error(err)
		return
	}

	check, _, err = rev.GetSnapshot().Exists(rev.dir.Name)
	if err != nil {
		t.Error(err)
	}
	if !check {
		t.Error("Revision registration failed")
	}

	_, err = rev.Register()
	if err == nil {
		t.Error("Revision allowed to be registered twice")
	}
}

func TestRevisionUnregister(t *testing.T) {
	s, app := revSetup()
	rev := s.NewRevision(app, "master", "master.img")

	rev, err := rev.Register()
	if err != nil {
		t.Error(err)
	}

	err = rev.Unregister()
	if err != nil {
		t.Error(err)
	}

	check, _, err := s.GetSnapshot().Exists(rev.dir.Name)
	if err != nil {
		t.Error(err)
	}
	if check {
		t.Error("Revision still registered")
	}
}

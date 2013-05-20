// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"fmt"
	cp "github.com/soundcloud/cotterpin"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

const charPat = `[-.[:alnum:]]`

type EventData struct {
	App      *string
	Instance *string
	Proctype *string
	Revision *string
	Service  *string
}

func (d EventData) String() string {
	fields := []string{}
	t := reflect.TypeOf(d)

	for i := 0; i < t.NumField(); i++ {
		v := reflect.ValueOf(d).Field(i)

		if !v.IsNil() {
			fields = append(fields, fmt.Sprintf("%s: %v", t.Field(i).Name, v.Elem().Interface()))
		}
	}

	return fmt.Sprintf("EventData{%s}", strings.Join(fields, ", "))
}

// An Event represents a change to a file in the registry.
type Event struct {
	Type   EventType // Type of event
	Body   string    // Body of the changed file
	Source cp.Snapshotable
	Path   EventData
	raw    *cp.Event // Original event returned by cotterpin
	Rev    int64
}

type EventType string

const (
	EvAppReg    = EventType("app-register")
	EvAppUnreg  = EventType("app-unregister")
	EvRevReg    = EventType("rev-register")
	EvRevUnreg  = EventType("rev-unregister")
	EvProcReg   = EventType("proc-register")
	EvProcUnreg = EventType("proc-unregister")
	EvInsReg    = EventType("instance-register")
	EvInsUnreg  = EventType("instance-unregister")
	EvInsStart  = EventType("instance-start")
	EvInsFail   = EventType("instance-fail")
	EvInsExit   = EventType("instance-exit")
	EvUnknown   = EventType("UNKNOWN")
)

const (
	globPlural = "**"
)

type eventPath int

const (
	pathApp eventPath = iota
	pathRev
	pathProc
	pathIns
	pathInsStatus
	pathInsStart
	pathInsStop
)

var eventPatterns = map[*regexp.Regexp]eventPath{
	regexp.MustCompile("^/apps/(" + charPat + "+)/registered$"):                          pathApp,
	regexp.MustCompile("^/apps/(" + charPat + "+)/revs/(" + charPat + "+)/registered$"):  pathRev,
	regexp.MustCompile("^/apps/(" + charPat + "+)/procs/(" + charPat + "+)/registered$"): pathProc,
	regexp.MustCompile("^/instances/([-0-9]+)/object$"):                                  pathIns,
	regexp.MustCompile("^/instances/([-0-9]+)/status$"):                                  pathInsStatus,
	regexp.MustCompile("^/instances/([-0-9]+)/start$"):                                   pathInsStart,
	regexp.MustCompile("^/instances/([-0-9]+)/stop$"):                                    pathInsStop,
}

func (ev *Event) String() string {
	return fmt.Sprintf("%#v", ev)
}

// WatchEventRaw watches for changes to the registry and sends
// them as *Event objects to the provided channel.
func (s *Store) WatchEventRaw(listener chan *Event) error {
	sp := s.GetSnapshot()
	for {
		ev, err := sp.Wait(globPlural)
		if err != nil {
			return err
		}
		sp = sp.Join(ev)

		event, err := enrichEvent(&ev, ev)
		if err != nil {
			return err
		}

		listener <- event
	}
	return nil
}

// WatchEvent wraps WatchEventRaw with additional information.
func (s *Store) WatchEvent(listener chan *Event) error {
	sp := s.GetSnapshot()
	for {
		ev, err := sp.Wait(globPlural)
		if err != nil {
			return err
		}
		sp = sp.Join(ev)

		event, err := enrichEvent(&ev, ev)
		if err != nil {
			return err
		}

		if event.Type == EvUnknown {
			continue
		}

		listener <- event
	}
	return nil
}

func canonicalizeMetadata(etype EventType, uncanonicalized EventData, s cp.Snapshotable) (source cp.Snapshotable, err error) {
	var (
		app *App
		rev *Revision
		pty *ProcType
		ins *Instance
	)

	if uncanonicalized.App != nil {
		app, err = getApp(*uncanonicalized.App, s)

		if err != nil {
			return
		}
	}

	if uncanonicalized.Revision != nil {
		rev, err = getRevision(app, *uncanonicalized.Revision, s)

		if err != nil {
			return
		}
	}

	if uncanonicalized.Proctype != nil {
		pty, err = getProcType(app, *uncanonicalized.Proctype, s)
		if err != nil {
			return
		}
	}

	if uncanonicalized.Instance != nil {
		var id int64 = -1
		if id, err = strconv.ParseInt(*uncanonicalized.Instance, 10, 64); err != nil {
			return
		}
		if ins, err = getInstance(id, s); err != nil {
			return
		}
	}

	switch etype {
	case EvAppReg:
		source = app
	case EvRevReg:
		source = rev
	case EvProcReg:
		source = pty
	case EvInsReg, EvInsStart, EvInsFail, EvInsExit:
		source = ins
	}

	return
}

func enrichEvent(src *cp.Event, s cp.Snapshotable) (event *Event, err error) {
	var canonicalized cp.Snapshotable

	path := src.Path
	etype := EvUnknown
	uncanonicalized := EventData{}

	for re, ev := range eventPatterns {
		if match := re.FindStringSubmatch(path); match != nil {
			switch ev {
			case pathApp:
				uncanonicalized.App = &match[1]

				if src.IsSet() {
					etype = EvAppReg
				} else if src.IsDel() {
					etype = EvAppUnreg
				}
			case pathRev:
				uncanonicalized.App = &match[1]
				uncanonicalized.Revision = &match[2]

				if src.IsSet() {
					etype = EvRevReg
				} else if src.IsDel() {
					etype = EvRevUnreg
				}
			case pathProc:
				uncanonicalized.App = &match[1]
				uncanonicalized.Proctype = &match[2]

				if src.IsSet() {
					etype = EvProcReg
				} else if src.IsDel() {
					etype = EvProcUnreg
				}
			case pathIns:
				uncanonicalized.Instance = &match[1]

				if src.IsSet() {
					etype = EvInsReg
				} else if src.IsDel() {
					etype = EvInsUnreg
				}
			case pathInsStart:
				uncanonicalized.Instance = &match[1]

				if !src.IsSet() {
					break
				}
				body := string(src.Body)
				if body == "" {
					break
				} else {
					fields := strings.Fields(body)
					if len(fields) > 1 {
						etype = EvInsStart
					}
				}
			case pathInsStatus:
				uncanonicalized.Instance = &match[1]

				if !src.IsSet() {
					break
				}

				switch InsStatus(src.Body) {
				case InsStatusRunning:
					etype = EvInsStart
				case InsStatusExited:
					etype = EvInsExit
				case InsStatusFailed:
					etype = EvInsFail
				}
			}
			break
		}
	}

	if src.IsSet() {
		canonicalized, err = canonicalizeMetadata(etype, uncanonicalized, s)
		if err != nil {
			return nil, fmt.Errorf("error canonicalizing inputs: %s", err)
		}
	}

	return &Event{
		Type:   etype,
		Body:   string(src.Body),
		Source: canonicalized,
		Path:   uncanonicalized,
		raw:    src,
		Rev:    src.Rev,
	}, nil
}

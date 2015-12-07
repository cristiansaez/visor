// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	cp "github.com/soundcloud/cotterpin"
)

// Event represents a change to a file in the registry.
type Event struct {
	Type   EventType // Type of event
	Path   EventData // Unique part of the event path
	Rev    int64
	Source cp.Snapshotable
	raw    cp.Event // Original event returned by cotterpin
}

// EventData is used to represent information encoded in the file path.
type EventData struct {
	App      *string
	Instance *string
	Proc     *string
	Revision *string
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

// EventType is the used to distinguish events.
type EventType string

// EventTypes.
const (
	EvAppReg     = EventType("app-register")
	EvAppUnreg   = EventType("app-unregister")
	EvRevReg     = EventType("rev-register")
	EvRevUnreg   = EventType("rev-unregister")
	EvProcReg    = EventType("proc-register")
	EvProcUnreg  = EventType("proc-unregister")
	EvProcAttrs  = EventType("proc-attrs")
	EvInsReg     = EventType("instance-register")
	EvInsUnclaim = EventType("instance-unclaim")
	EvInsUnreg   = EventType("instance-unregister")
	EvInsStart   = EventType("instance-start")
	EvInsStop    = EventType("instance-stop")
	EvInsFail    = EventType("instance-fail")
	EvInsExit    = EventType("instance-exit")
	EvInsLost    = EventType("instance-lost")
	EvUnknown    = EventType("UNKNOWN")
)

type eventPath int

const (
	pathApp eventPath = iota
	pathRev
	pathProc
	pathProcAttrs
	pathInsRegistered
	pathInsStatus
	pathInsStart
	pathInsStop
)

const (
	charPat    = `[-.[:alnum:]]`
	globPlural = "**"
)

var eventPatterns = map[*regexp.Regexp]eventPath{
	regexp.MustCompile("^/apps/(" + charPat + "+)/registered$"):                          pathApp,
	regexp.MustCompile("^/apps/(" + charPat + "+)/revs/(" + charPat + "+)/registered$"):  pathRev,
	regexp.MustCompile("^/apps/(" + charPat + "+)/procs/(" + charPat + "+)/registered$"): pathProc,
	regexp.MustCompile("^/apps/(" + charPat + "+)/procs/(" + charPat + "+)/attrs$"):      pathProcAttrs,
	regexp.MustCompile("^/instances/([-0-9]+)/registered$"):                              pathInsRegistered,
	regexp.MustCompile("^/instances/([-0-9]+)/status$"):                                  pathInsStatus,
	regexp.MustCompile("^/instances/([-0-9]+)/start$"):                                   pathInsStart,
	regexp.MustCompile("^/instances/([-0-9]+)/stop$"):                                    pathInsStop,
}

func (ev *Event) String() string {
	return fmt.Sprintf("%#v", ev)
}

// WatchEvent watches for changes on the store, enriches them with the
// corresponding domain object and sends them as Event object to the given
// channel.
// Optionally any number of EventTypes can be given in order to filter which
// events will be sent over the given channel.
func (s *Store) WatchEvent(listener chan *Event, filter ...EventType) error {
	sp := s.GetSnapshot()
	for {
		ev, err := sp.Wait(globPlural)
		if err != nil {
			return err
		}
		sp = sp.Join(ev)

		event, err := newEvent(ev)
		if err != nil {
			return err
		}
		if !event.match(filter) {
			continue
		}
		if err := event.enrich(); err != nil {
			return err
		}
		listener <- event
	}
}

func newEvent(src cp.Event) (*Event, error) {
	event := &Event{
		Type: EvUnknown,
		Rev:  src.Rev,
		raw:  src,
	}

	for re, ev := range eventPatterns {
		if match := re.FindStringSubmatch(src.Path); match != nil {
			switch ev {
			case pathApp:
				if src.IsSet() {
					event.Type = EvAppReg
				} else if src.IsDel() {
					event.Type = EvAppUnreg
				}
				event.Path = EventData{App: &match[1]}
			case pathRev:
				if src.IsSet() {
					event.Type = EvRevReg
				} else if src.IsDel() {
					event.Type = EvRevUnreg
				}
				event.Path = EventData{App: &match[1], Revision: &match[2]}
			case pathProc:
				if src.IsSet() {
					event.Type = EvProcReg
				} else if src.IsDel() {
					event.Type = EvProcUnreg
				}
				event.Path = EventData{App: &match[1], Proc: &match[2]}
			case pathProcAttrs:
				if !src.IsSet() {
					break
				}
				event.Type = EvProcAttrs
				event.Path = EventData{App: &match[1], Proc: &match[2]}
			case pathInsRegistered:
				if src.IsSet() {
					event.Type = EvInsReg
				} else if src.IsDel() {
					event.Type = EvInsUnreg
				}
				event.Path = EventData{Instance: &match[1]}
			case pathInsStart:
				if !src.IsSet() {
					break
				}
				// The start file can be in three different states:
				// 1. "" - instance got registered or unclaimed
				// 2. "<ip>" - instance got claimed
				// 3. "<ip> <host> <port> <tport> - instance got started
				if len(bytes.Fields(src.Body)) > 1 {
					event.Type = EvInsStart
				} else if len(src.Body) == 0 {
					// The file is empty, so distinguish between registered and
					// unclaimed by whether the file existed before already.
					existed, err := pathExistedBefore(src)
					if err != nil {
						return nil, err
					}
					if existed {
						event.Type = EvInsUnclaim
					}
				}
				event.Path = EventData{Instance: &match[1]}
			case pathInsStop:
				if !src.IsSet() {
					break
				}
				event.Type = EvInsStop
				event.Path = EventData{Instance: &match[1]}
			case pathInsStatus:
				if !src.IsSet() {
					break
				}
				switch InsStatus(src.Body) {
				case InsStatusRunning:
					event.Type = EvInsStart
				case InsStatusExited:
					event.Type = EvInsExit
				case InsStatusFailed:
					event.Type = EvInsFail
				case InsStatusLost:
					event.Type = EvInsLost
				}
				event.Path = EventData{Instance: &match[1]}
			}
			break
		}
	}

	return event, nil
}

func (e *Event) match(filter []EventType) bool {
	if e.Type == EvUnknown {
		return false
	}
	if len(filter) == 0 {
		return true
	}
	for _, t := range filter {
		if e.Type == t {
			return true
		}
	}
	return false
}

func (e *Event) enrich() error {
	var (
		app *App
		err error
	)

	if !e.raw.IsSet() {
		return nil
	}

	if e.Path.App != nil {
		app, err = getApp(*e.Path.App, e.raw)
		if err != nil {
			return err
		}
	}

	switch e.Type {
	case EvAppReg:
		e.Source, err = app, nil
	case EvRevReg:
		e.Source, err = getRevision(app, *e.Path.Revision, e.raw)
	case EvProcReg, EvProcAttrs:
		e.Source, err = getProc(app, *e.Path.Proc, e.raw)
	case EvInsReg, EvInsUnclaim, EvInsStart, EvInsStop, EvInsFail, EvInsExit, EvInsLost:
		id, err := strconv.ParseInt(*e.Path.Instance, 10, 64)
		if err != nil {
			return err
		}
		e.Source, err = getInstance(id, e.raw)
	}
	if err != nil {
		return fmt.Errorf("error enriching event %+v: %s", e.raw, err)
	}
	return nil
}

func pathExistedBefore(e cp.Event) (bool, error) {
	if e.Rev == 0 {
		return false, nil
	}

	sp := e.GetSnapshot()
	sp.Rev--

	exists, _, err := sp.Exists(e.Path)
	return exists, err
}

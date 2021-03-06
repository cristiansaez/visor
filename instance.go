// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"fmt"
	"path"
	"sort"
	"strconv"
	"time"

	cp "github.com/soundcloud/cotterpin"
)

const (
	claimsPath    = "claims"
	instancesPath = "instances"
	donePath      = "done"
	failedPath    = "failed"
	lostPath      = "lost"
	lockPath      = "lock"
	objectPath    = "object"
	startPath     = "start"
	statusPath    = "status"
	stopPath      = "stop"
	restartsPath  = "restarts"

	restartFailField = 0
	restartOOMField  = 1

	InsStatusPending  InsStatus = "pending"
	InsStatusClaimed  InsStatus = "claimed"
	InsStatusRunning  InsStatus = "running"
	InsStatusStopping InsStatus = "stopping"
	InsStatusFailed   InsStatus = "failed"
	InsStatusExited   InsStatus = "exited"
	InsStatusLost     InsStatus = "lost"
	InsStatusDone     InsStatus = "done"
)

// InsStatus describes the current state of the instance state machine.
type InsStatus string

// InsRestarts combines the information about general restarts and OOMs.
type InsRestarts struct {
	OOM, Fail int
}

// Fields returns the list representation of InsRestarts.
func (r InsRestarts) Fields() []int {
	return []int{r.Fail, r.OOM}
}

// Int64Slice is a sortable list of int64s.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Termination represents extra information for an Instance termination.
type Termination struct {
	Client string    `json:"client"`
	Reason string    `json:"reason"`
	Time   time.Time `json:"time"`
}

// Instance represents service instances.
type Instance struct {
	dir          *cp.Dir
	ID           int64       `json:"id"`
	AppName      string      `json:"app"`
	RevisionName string      `json:"rev"`
	ProcessName  string      `json:"proc"`
	Env          string      `json:"env"`
	IP           string      `json:"ip"`
	Port         int         `json:"port"`
	TelePort     int         `json:"telePort"`
	Host         string      `json:"host"`
	Status       InsStatus   `json:"status"`
	Restarts     InsRestarts `json:"restarts"`
	Registered   time.Time   `json:"registered"`
	Claimed      time.Time   `json:"claimed"`
	Termination  Termination `json:"termination,omitempty"`
}

// GetSnapshot satisfies the cp.Snapshotable interface.
func (i *Instance) GetSnapshot() cp.Snapshot {
	return i.dir.Snapshot
}

// GetInstance returns an Instance from the given id
func (s *Store) GetInstance(id int64) (ins *Instance, err error) {
	sp, err := s.GetSnapshot().FastForward()
	if err != nil {
		return
	}
	return getInstance(id, sp)
}

// GetSerialisedInstance returns an instance for the given id and status.
func (s *Store) GetSerialisedInstance(
	app, proc string,
	id int64,
	status InsStatus,
) (*Instance, error) {
	sp, err := s.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	return getSerialisedInstance(app, proc, id, status, sp)
}

func getSerialisedInstance(
	app, proc string,
	id int64,
	status InsStatus,
	sp cp.Snapshot,
) (*Instance, error) {
	var (
		i = &Instance{
			ID:          id,
			AppName:     app,
			ProcessName: proc,
			dir:         cp.NewDir(instancePath(id), sp),
		}
		c = &cp.JsonCodec{
			DecodedVal: i,
		}
	)

	_, err := sp.GetFile(i.procStatusPath(status), c)
	if err != nil {
		return nil, errorf(err, "fetching instance %d: %s", id, err)
	}

	return i, nil
}

// RegisterInstance stores the Instance.
func (s *Store) RegisterInstance(app, rev, proc, env string) (ins *Instance, err error) {
	//
	//   instances/
	//       6868/
	// +         object = <app> <rev> <proc>
	// +         start  =
	//
	//   apps/<app>/procs/<proc>/instances/<rev>
	// +     6868 = 2012-07-19 16:41 UTC
	//
	id, err := s.GetSnapshot().Getuid()
	if err != nil {
		return
	}
	ins = &Instance{
		ID:           id,
		AppName:      app,
		RevisionName: rev,
		ProcessName:  proc,
		Env:          env,
		Registered:   time.Now(),
		Status:       InsStatusPending,
		dir:          cp.NewDir(instancePath(id), s.GetSnapshot()),
	}

	object := cp.NewFile(ins.dir.Prefix(objectPath), ins.objectArray(), new(cp.ListCodec), s.GetSnapshot())
	object, err = object.Save()
	if err != nil {
		return nil, err
	}

	start := cp.NewFile(ins.dir.Prefix(startPath), "", new(cp.StringCodec), s.GetSnapshot())
	start, err = start.Save()
	if err != nil {
		return nil, err
	}

	// Create the file used for lookups of existing instances per proc.
	_, err = ins.GetSnapshot().Set(ins.procStatusPath(InsStatusRunning), formatTime(ins.Registered))
	if err != nil {
		return nil, err
	}

	// This should be the last path set in order for the event system to work properly.
	registered, err := ins.dir.Set(registeredPath, formatTime(ins.Registered))
	if err != nil {
		return
	}

	ins.dir = ins.dir.Join(registered)

	return
}

// Unregister removes the instance tree representation.
func (i *Instance) Unregister(client string, reason error) error {
	i, err := i.updateLookup(i.Status, InsStatusDone, client, reason)
	if err != nil {
		return err
	}
	return i.dir.Del("/")
}

// Claim locks the instance to the specified host.
func (i *Instance) Claim(host string) (*Instance, error) {
	done, err := i.IsDone()
	if err != nil {
		return nil, err
	}
	if done {
		return nil, errorf(ErrUnauthorized, "%s is done", i)
	}

	//
	//   instances/
	//       6868/
	//           claims/
	// +             10.0.0.1 = 2012-07-19 16:22 UTC
	//           object = <app> <rev> <proc>
	// -         start  =
	// +         start  = 10.0.0.1
	//
	f, err := i.dir.GetFile(startPath, new(cp.ListCodec))
	if err != nil {
		return nil, err
	}
	fields := f.Value.([]string)
	if len(fields) > 0 {
		return nil, errorf(ErrInsClaimed, "%s already claimed", i)
	}
	d := i.dir.Join(f)

	d, err = d.Set(startPath, host)
	if err != nil {
		if cp.IsErrRevMismatch(err) {
			err = errorf(ErrInsClaimed, "%s already claimed", i)
		}
		return i, err
	}

	claimed := time.Now()
	d, err = i.claimDir().Join(d).Set(host, formatTime(claimed))
	if err != nil {
		return nil, err
	}
	i.Claimed = claimed
	i.dir = i.dir.Join(d)
	return i, err
}

// Claims returns the list of claimers.
func (i *Instance) Claims() (claims []string, err error) {
	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return
	}
	claims, err = sp.Getdir(i.dir.Prefix("claims"))
	if cp.IsErrNoEnt(err) {
		claims = []string{}
		err = nil
	}
	return
}

// Unclaim removes the lock applied by Claim of the Ticket.
func (i *Instance) Unclaim(host string) (*Instance, error) {
	//
	//   instances/
	//       6868/
	// -         start = 10.0.0.1
	// +         start =
	//
	err := i.verifyClaimer(host)
	if err != nil {
		return nil, err
	}

	d, err := i.setClaimer("")
	if err != nil {
		return nil, err
	}
	i.dir = d

	return i, nil
}

// Started puts the Instance into start state.
func (i *Instance) Started(host, hostname string, port, telePort int) (*Instance, error) {
	//
	//   instances/
	//       6868/
	//           object = <app> <rev> <proc>
	// -         start  = 10.0.0.1
	// +         start  = 10.0.0.1 24690 localhost 24691
	//
	if i.Status == InsStatusRunning {
		return i, nil
	}
	err := i.verifyClaimer(host)
	if err != nil {
		return nil, err
	}
	i.started(host, hostname, port, telePort)

	start := cp.NewFile(i.dir.Prefix(startPath), i.startArray(), new(cp.ListCodec), i.GetSnapshot())
	start, err = start.Save()
	if err != nil {
		return nil, err
	}
	i.dir = i.dir.Join(start)

	return i, nil
}

// Restarted tells the coordinator that the instance has been restarted.
func (i *Instance) Restarted(restarts InsRestarts) (*Instance, error) {
	//
	//   instances/
	//       6868/
	//           object   = <app> <rev> <proc>
	//           start    = 10.0.0.1 24690 localhost
	// -         restarts = 1 4
	// +         restarts = 2 4
	//
	//   instances/
	//       6869/
	//           object   = <app> <rev> <proc>
	//           start    = 10.0.0.1 24691 localhost
	// +         restarts = 1 0
	//
	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return i, err
	}

	i, err = getInstance(i.ID, sp)
	if err != nil {
		return nil, err
	}

	if i.Status != InsStatusRunning {
		return i, nil
	}

	f := cp.NewFile(i.dir.Prefix(restartsPath), nil, new(cp.ListIntCodec), sp)

	f, err = f.Set(restarts.Fields())
	if err != nil {
		return nil, err
	}

	i.Restarts = restarts
	i.dir = i.dir.Join(f)

	return i, nil
}

// Stop communicates the intend that the Instance should be stopped.
func (i *Instance) Stop() error {
	//
	//   instances/
	//       6868/
	//           ...
	// +         stop =
	//
	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return err
	}

	i, err = getInstance(i.ID, sp)
	if err != nil {
		return err
	}

	if i.Status != InsStatusRunning {
		return ErrInvalidState
	}
	_, err = i.dir.Set(stopPath, "")
	if err != nil {
		return err
	}

	return nil
}

// Failed transitions the instance to failed.
// It returns ErrUnauthorized if the instance status is not pending and was not
// claimed by host.
// It returns a revision mismatch error if the status is pending, but another
// caller has already failed this instance.
func (i *Instance) Failed(host string, reason error) (*Instance, error) {
	status := i.Status

	if status != InsStatusPending {
		if err := i.verifyClaimer(host); err != nil {
			return nil, err
		}
	}

	if _, err := i.updateStatus(InsStatusFailed); err != nil {
		return nil, err
	}
	return i.updateLookup(status, InsStatusFailed, host, reason)
}

// Lost transitions the instance into lost state and updates the
// coordinator with client and reason.
func (i *Instance) Lost(client string, reason error) (*Instance, error) {
	current := i.Status

	_, err := i.updateStatus(InsStatusLost)
	if err != nil {
		return nil, err
	}
	return i.updateLookup(current, InsStatusLost, client, reason)
}

// Exited tells the coordinator that the instance has exited.
func (i *Instance) Exited(host string) (i1 *Instance, err error) {
	if err = i.verifyClaimer(host); err != nil {
		return
	}
	i1, err = i.updateStatus(InsStatusExited)
	if err != nil {
		return nil, err
	}
	err = i.dir.Snapshot.Del(i.procStatusPath(InsStatusExited))

	return
}

// WaitStatus blocks until a state change happened to the Instance and returns
// the Instance with the new information.
func (i *Instance) WaitStatus() (*Instance, error) {
	p := path.Join(instancesPath, strconv.FormatInt(i.ID, 10), statusPath)
	sp := i.GetSnapshot()
	ev, err := sp.Wait(p)
	if err != nil {
		return nil, err
	}
	i.Status = InsStatus(string(ev.Body))
	i.dir = i.dir.Join(ev)

	return i, nil
}

// WaitClaimed blocks until the Instance is claimed.
func (i *Instance) WaitClaimed() (i1 *Instance, err error) {
	return i.waitStartPathStatus(InsStatusClaimed)
}

// WaitStarted blocks until the Instnaces is started.
func (i *Instance) WaitStarted() (i1 *Instance, err error) {
	return i.waitStartPathStatus(InsStatusRunning)
}

// WaitStop blocks until the Instance is stopped.
func (i *Instance) WaitStop() (*Instance, error) {
	p := path.Join(instancesPath, strconv.FormatInt(i.ID, 10), stopPath)
	sp := i.GetSnapshot()
	ev, err := sp.Wait(p)
	if err != nil {
		return nil, err
	}
	i.Status = InsStatusStopping
	i.dir = i.dir.Join(ev)

	return i, nil
}

// WaitExited blocks until the instance exited.
func (i *Instance) WaitExited() (*Instance, error) {
	for {
		i, err := i.WaitStatus()
		if err != nil {
			return nil, err
		}
		if i.Status == InsStatusExited {
			break
		}
	}
	return i, nil
}

// WaitFailed blocks until the instance failed.
func (i *Instance) WaitFailed() (*Instance, error) {
	sp := i.GetSnapshot()
	ev, err := sp.Wait(i.procFailedPath())
	if err != nil {
		return nil, err
	}

	ins := &Instance{}
	if _, err := (&cp.JsonCodec{DecodedVal: ins}).Decode(ev.Body); err != nil {
		return nil, err
	}
	i.Status = ins.Status
	i.Termination = ins.Termination
	i.dir = i.dir.Join(ev)

	return i, nil
}

// WaitLost blocks until the instance is lost.
func (i *Instance) WaitLost() (*Instance, error) {
	for {
		i, err := i.WaitStatus()
		if err != nil {
			return nil, err
		}
		if i.Status == InsStatusLost {
			break
		}
	}
	return i, nil
}

// WaitUnregister blocks until the instance is unregistered.
func (i *Instance) WaitUnregister() error {
	p := path.Join(instancesPath, strconv.FormatInt(i.ID, 10), objectPath)
	sp := i.GetSnapshot()
	ev, err := sp.Wait(p)
	if err != nil {
		return err
	}
	if ev.IsDel() {
		return nil
	}

	return fmt.Errorf("unexpected turn of events: %s", ev)
}

// GetStatusInfo returns the status value.
func (i *Instance) GetStatusInfo() (string, error) {
	info, _, err := i.dir.Snapshot.Get(i.procStatusPath(i.Status))
	if err != nil {
		return "", err
	}
	return info, nil
}

// Lock sets the lock path to the given client and reason.
func (i *Instance) Lock(client string, reason error) (*Instance, error) {
	locked, err := i.IsLocked()
	if err != nil {
		return nil, err
	}
	if locked {
		return nil, errorf(ErrUnauthorized, "instance %d is already locked", i.ID)
	}

	i.dir, err = i.dir.Set(lockPath, fmt.Sprintf("%s %s %s", timestamp(), client, reason))
	if err != nil {
		return nil, err
	}

	return i, nil
}

// Unlock removes the instance lock path.
func (i *Instance) Unlock() (*Instance, error) {
	err := i.dir.Del(lockPath)
	if err != nil {
		return nil, err
	}
	return i, nil
}

// IsLocked checks if a lock path is present for the instance.
func (i *Instance) IsLocked() (bool, error) {
	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return false, err
	}
	exists, _, err := sp.Exists(i.dir.Prefix(lockPath))
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	return false, nil
}

// IsDone checks if the instance is in done state.
func (i *Instance) IsDone() (bool, error) {
	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return false, err
	}
	exists, _, err := sp.Exists(i.procDonePath())
	if err != nil {
		return false, err
	}
	return exists, nil
}

// EnvString returns the cannonical string representation of an instance with
// env.
func (i *Instance) EnvString() string {
	return fmt.Sprintf("%s:%s#%s", i.AppName, i.ProcessName, i.Env)
}

// RevString returns the cannonical string representation of an instance with
// rev.
func (i *Instance) RevString() string {
	return fmt.Sprintf("%s:%s@%s", i.AppName, i.ProcessName, i.RevisionName)
}

// RefString returns the cannonical string representation of an instance.
func (i *Instance) RefString() string {
	return fmt.Sprintf("%s:%s@%s#%s", i.AppName, i.ProcessName, i.RevisionName, i.Env)
}

// ServiceName returns the cannonical string representation of an instance
// service.
func (i *Instance) ServiceName() string {
	return fmt.Sprintf("%s:%s", i.AppName, i.ProcessName)
}

// WorkerID returns the cannonical string representation of an instance with its
// rev and port.
func (i *Instance) WorkerID() string {
	return fmt.Sprintf("%s-%s-%s-%d", i.AppName, i.ProcessName, i.RevisionName, i.Port)
}

// Fields reutrns the string representation of an instance as a space separated
// list.
func (i *Instance) Fields() string {
	return fmt.Sprintf("%d %s %s %s %s %d %d", i.ID, i.AppName, i.RevisionName, i.ProcessName, i.IP, i.Port, i.TelePort)
}

// String returns the Go-syntax representation of Instance.
func (i *Instance) String() string {
	return fmt.Sprintf("Instance{id=%d, app=%s, rev=%s, proc=%s, env=%s, addr=%s:%d}", i.ID, i.AppName, i.RevisionName, i.ProcessName, i.Env, i.IP, i.Port)
}

// IDString returns a string of the format "INSTANCE[id]"
func (i *Instance) IDString() string {
	return fmt.Sprintf("INSTANCE[%d]", i.ID)
}

func (i *Instance) claimPath(host string) string {
	return i.dir.Prefix("claims", host)
}

func (i *Instance) claimDir() *cp.Dir {
	return cp.NewDir(i.dir.Prefix(claimsPath), i.GetSnapshot())
}

func (i *Instance) idString() string {
	return fmt.Sprintf("%d", i.ID)
}

func (i *Instance) objectArray() []string {
	return []string{i.AppName, i.RevisionName, i.ProcessName, i.Env}
}

func (i *Instance) startArray() []string {
	return []string{i.IP, i.portString(), i.Host, i.telePortString()}
}

func (i *Instance) portString() string {
	return fmt.Sprintf("%d", i.Port)
}

func (i *Instance) telePortString() string {
	return fmt.Sprintf("%d", i.TelePort)
}

func (i *Instance) procDonePath() string {
	return path.Join(appsPath, i.AppName, procsPath, i.ProcessName, donePath, i.idString())
}

func (i *Instance) procFailedPath() string {
	return path.Join(appsPath, i.AppName, procsPath, i.ProcessName, failedPath, i.idString())
}

func (i *Instance) procInstancesPath() string {
	return path.Join(appsPath, i.AppName, procsPath, i.ProcessName, instancesPath, i.RevisionName, i.idString())
}

func (i *Instance) procLostPath() string {
	return path.Join(appsPath, i.AppName, procsPath, i.ProcessName, lostPath, i.idString())
}

func (i *Instance) claimed(ip string) {
	i.IP = ip
	i.Status = InsStatusClaimed
}

func (i *Instance) getRestarts() (InsRestarts, *cp.File, error) {
	var restarts InsRestarts

	f, err := i.dir.GetFile(restartsPath, new(cp.ListIntCodec))
	if err == nil {
		fields := f.Value.([]int)

		restarts.Fail = fields[restartFailField]
		restarts.OOM = fields[restartOOMField]
	} else if !cp.IsErrNoEnt(err) {
		return restarts, nil, err
	}

	return restarts, f, nil
}

func (i *Instance) started(ip, host string, port, telePort int) {
	i.IP = ip
	i.Port = port
	i.TelePort = telePort
	i.Host = host
	i.Status = InsStatusRunning
}

func (i *Instance) updateStatus(s InsStatus) (*Instance, error) {
	d, err := i.dir.Set("status", string(s))
	if err != nil {
		return nil, err
	}
	i.Status = s
	i.dir = d

	return i, nil
}

func (i *Instance) getClaimer() (*string, error) {
	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	i.dir = i.dir.Join(sp)
	f, err := sp.GetFile(i.dir.Prefix(startPath), new(cp.ListCodec))
	if err != nil {
		return nil, err
	}
	fields := f.Value.([]string)

	if len(fields) == 0 {
		return nil, nil
	}
	return &fields[0], nil
}

func (i *Instance) setClaimer(claimer string) (*cp.Dir, error) {
	d, err := i.dir.Set(startPath, claimer)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (i *Instance) verifyClaimer(host string) error {
	claimer, err := i.getClaimer()
	if err != nil {
		return err
	}

	if claimer == nil {
		return errorf(ErrUnauthorized, "instance %d is not claimed", i.ID)
	}

	if *claimer != host {
		return errorf(ErrUnauthorized, "instance %d has different claimer: %s != %s", i.ID, *claimer, host)
	}
	return nil
}

func (i *Instance) procStatusPath(status InsStatus) string {
	switch status {
	case InsStatusDone:
		return i.procDonePath()
	case InsStatusFailed:
		return i.procFailedPath()
	case InsStatusLost:
		return i.procLostPath()
	default:
		return i.procInstancesPath()
	}
}

func (i *Instance) updateLookup(
	from, to InsStatus,
	client string,
	reason error,
) (*Instance, error) {
	i.Termination = Termination{
		Client: client,
		Reason: reason.Error(),
		Time:   time.Now(),
	}

	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	if from == InsStatusFailed || from == InsStatusLost {
		ins, err := getSerialisedInstance(i.AppName, i.ProcessName, i.ID, from, sp)
		if err != nil {
			return nil, err
		}

		i.Termination = ins.Termination
	}

	f := cp.NewFile(sp.Prefix(i.procStatusPath(to)), i, new(cp.JsonCodec), sp)
	f, err = f.Save()
	if err != nil {
		return nil, err
	}

	i.dir = i.dir.Join(f)

	err = i.dir.Snapshot.Del(i.procStatusPath(from))
	if err != nil {
		return nil, err
	}

	return i, nil
}

func (i *Instance) waitStartPath() (*Instance, error) {
	p := path.Join(instancesPath, strconv.FormatInt(i.ID, 10), startPath)
	sp := i.GetSnapshot()
	ev, err := sp.Wait(p)
	if err != nil {
		return nil, err
	}
	i.dir = i.dir.Join(ev)
	parts, err := new(cp.ListCodec).Decode(ev.Body)
	if err != nil {
		return nil, err
	}
	fields := parts.([]string)
	if len(fields) >= 3 {
		ip, host := fields[0], fields[2]
		port, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, err
		}

		telePort, err := strconv.Atoi(fields[3])
		if err != nil {
			return nil, err
		}

		i.started(ip, host, port, telePort)
	} else if len(fields) > 0 {
		i.claimed(fields[0])
	} else {
		// TODO
	}
	return i, nil
}

func (i *Instance) waitStartPathStatus(s InsStatus) (i1 *Instance, err error) {
	for {
		i, err = i.waitStartPath()
		if err != nil {
			return i, err
		}
		if i.Status == s {
			break
		}
	}
	return i, nil
}

// GetInstances returns all existing instances.
func (s *Store) GetInstances() ([]*Instance, error) {
	sp, err := s.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	ids, err := sp.Getdir(instancesPath)
	if err != nil {
		return nil, err
	}

	instances := []*Instance{}
	ch, errch := cp.GetSnapshotables(ids, func(idstr string) (cp.Snapshotable, error) {
		id, err := parseInstanceID(idstr)
		if err != nil {
			return nil, err
		}
		return getInstance(id, sp)
	})
	errStr := ""
	for i := 0; i < len(ids); i++ {
		select {
		case i := <-ch:
			instances = append(instances, i.(*Instance))
		case err := <-errch:
			errStr = fmt.Sprintf("%s\n%s", errStr, err)
		}
	}
	if len(errStr) > 0 {
		return instances, NewError(ErrNotFound, errStr)
	}

	return instances, nil
}

// GetLostInstances returns all existing instances in lost state.
func (s *Store) GetLostInstances() ([]*Instance, error) {
	is, err := s.GetInstances()
	if err != nil {
		return nil, err
	}

	ls := []*Instance{}
	for _, i := range is {
		if i.Status == InsStatusLost {
			ls = append(ls, i)
		}
	}

	return ls, nil
}

// WatchInstanceStart sends Instance over the given listener channel which
// transitioned to start.
//
// DEPRECATED: This method is deprecated. WatchEvent should be used directly.
func (s *Store) WatchInstanceStart(listener chan *Instance, errors chan error) {
	eventc := make(chan *Event)
	go func() {
		for {
			listener <- (<-eventc).Source.(*Instance)
		}
	}()
	if err := s.WatchEvent(eventc, EvInsReg, EvInsUnclaim); err != nil {
		errors <- err
	}
}

func instancePath(id int64) string {
	return path.Join(instancesPath, strconv.FormatInt(id, 10))
}

func procInstancesPath(app, rev, proc string) string {
	return path.Join(appsPath, app, procsPath, proc, instancesPath, rev)
}

func parseInstanceID(idstr string) (int64, error) {
	return strconv.ParseInt(idstr, 10, 64)
}

func getInstance(id int64, s cp.Snapshotable) (*Instance, error) {
	i := &Instance{
		ID:     id,
		Status: InsStatusPending,
		dir:    cp.NewDir(instancePath(id), s.GetSnapshot()),
	}

	exists, _, err := s.GetSnapshot().Exists(i.dir.Name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errorf(ErrNotFound, `instance '%d' not found`, id)
	}

	f, err := i.dir.GetFile(startPath, new(cp.ListCodec))
	if cp.IsErrNoEnt(err) {
		// Ignore
	} else if err != nil {
		return nil, err
	} else {
		fields := f.Value.([]string)

		if len(fields) > 0 { // IP
			i.Status = InsStatusClaimed
			i.IP = fields[0]
		}
		if len(fields) > 1 { // Port
			i.Status = InsStatusRunning
			i.Port, err = strconv.Atoi(fields[1])
			if err != nil {
				return nil, errorf(ErrInvalidPort, "invalid port: " + fields[1])
			}
		}
		if len(fields) > 2 { // Hostname
			i.Host = fields[2]
		}
		if len(fields) > 3 { // TelePort
			i.TelePort, err = strconv.Atoi(fields[3])
			if err != nil {
				return nil, errorf(ErrInvalidPort, "invalid teleport: " + fields[3])
			}
		}
	}

	statusStr, _, err := i.dir.Get(statusPath)
	if cp.IsErrNoEnt(err) {
		err = nil
	} else if err == nil {
		i.Status = InsStatus(statusStr)
	} else {
		return nil, err
	}

	if i.Status == InsStatusRunning {
		_, _, err := i.dir.Get(stopPath)
		if err == nil {
			i.Status = InsStatusStopping
		} else if !cp.IsErrNoEnt(err) {
			return nil, err
		}
	}

	f, err = i.dir.GetFile(objectPath, new(cp.ListCodec))
	if err != nil {
		return nil, errorf(ErrNotFound, "object file not found for instance %d", id)
	}

	fields := f.Value.([]string)
	if len(fields) < 3 {
		return nil, errorf(ErrInvalidFile, "object file for %d has %d instead %d fields", id, len(fields), 3)
	}

	i.AppName = fields[0]
	i.RevisionName = fields[1]
	i.ProcessName = fields[2]
	i.Env = fields[3]

	i.Restarts, _, err = i.getRestarts()
	if err != nil {
		return nil, err
	}

	f, err = i.dir.GetFile(registeredPath, new(cp.StringCodec))
	if err != nil {
		return nil, err
	}
	i.Registered, err = parseTime(f.Value.(string))
	if err != nil {
		return nil, err
	}

	f, err = i.claimDir().GetFile(i.IP, new(cp.StringCodec))
	if err != nil {
		if cp.IsErrNoEnt(err) {
			return i, nil
		}
		return nil, err
	}

	i.Claimed, err = parseTime(f.Value.(string))
	if err != nil {
		return nil, err
	}

	return i, nil
}

func getInstanceIds(app, rev, proc string, s cp.Snapshotable) (ids Int64Slice, err error) {
	sp := s.GetSnapshot()
	p := procInstancesPath(app, rev, proc)
	exists, _, err := sp.Exists(p)
	if err != nil || !exists {
		return
	}

	dir, err := sp.Getdir(p)
	if err != nil {
		return
	}
	ids = Int64Slice{}
	for _, f := range dir {
		id, e := parseInstanceID(f)
		if e != nil {
			return nil, e
		}
		ids = append(ids, id)
	}
	sort.Sort(ids)
	return
}

// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

package visor

import (
	"fmt"
	cp "github.com/soundcloud/cotterpin"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
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
)

const (
	restartFailField = 0
	restartOOMField  = 1
)

const (
	InsStatusPending  InsStatus = "pending"
	InsStatusClaimed            = "claimed"
	InsStatusRunning            = "running"
	InsStatusStopping           = "stopping"

	InsStatusFailed = "failed"
	InsStatusExited = "exited"
	InsStatusLost   = "lost"
	InsStatusDone   = "done"
)

type InsStatus string

type InsRestarts struct {
	OOM, Fail int
}

func (r InsRestarts) Fields() []int {
	return []int{r.Fail, r.OOM}
}

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
	Id           int64       `json:"id"`
	AppName      string      `json:"app"`
	RevisionName string      `json:"rev"`
	ProcessName  string      `json:"proc"`
	Env          string      `json:"env"`
	Ip           string      `json:"ip"`
	Port         int         `json:"port"`
	TelePort     int         `json:"telePort"`
	Host         string      `json:"host"`
	Status       InsStatus   `json:"status"`
	Restarts     InsRestarts `json:"restarts"`
	Registered   time.Time   `json:"registered"`
	Claimed      time.Time   `json:"claimed"`
	Termination  Termination `json:"termination,omitempty"`
}

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
			Id:          id,
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
		return nil, fmt.Errorf("fetching instance %d: %s", id, err)
	}

	return i, nil
}

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
		Id:           id,
		AppName:      app,
		RevisionName: rev,
		ProcessName:  proc,
		Env:          env,
		Status:       InsStatusPending,
		dir:          cp.NewDir(instancePath(id), s.GetSnapshot()),
	}

	object := cp.NewFile(ins.dir.Prefix("object"), ins.objectArray(), new(cp.ListCodec), s.GetSnapshot())
	object, err = object.Save()
	if err != nil {
		return nil, err
	}
	reg := time.Now()
	_, err = ins.dir.Set(registeredPath, formatTime(reg))
	if err != nil {
		return
	}
	ins.Registered = reg
	_, err = ins.GetSnapshot().Set(ins.procStatusPath(InsStatusRunning), formatTime(reg))
	if err != nil {
		return nil, err
	}

	start := cp.NewFile(ins.dir.Prefix(startPath), "", new(cp.StringCodec), s.GetSnapshot())
	start, err = start.Save()
	if err != nil {
		return nil, err
	}
	ins.dir = ins.dir.Join(start)

	return
}

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
	if i.Status != InsStatusRunning {
		return i, nil
	}

	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return i, err
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

func (i *Instance) Stop() error {
	//
	//   instances/
	//       6868/
	//           ...
	// +         stop =
	//
	if i.Status != InsStatusRunning {
		return ErrInvalidState
	}
	_, err := i.dir.Set("stop", "")
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

func (i *Instance) WaitStatus() (*Instance, error) {
	p := path.Join(instancesPath, strconv.FormatInt(i.Id, 10), statusPath)
	sp := i.GetSnapshot()
	ev, err := sp.Wait(p)
	if err != nil {
		return nil, err
	}
	i.Status = InsStatus(string(ev.Body))
	i.dir = i.dir.Join(ev)

	return i, nil
}

func (i *Instance) WaitClaimed() (i1 *Instance, err error) {
	return i.waitStartPathStatus(InsStatusClaimed)
}

func (i *Instance) WaitStarted() (i1 *Instance, err error) {
	return i.waitStartPathStatus(InsStatusRunning)
}

func (i *Instance) WaitStop() (*Instance, error) {
	p := path.Join(instancesPath, strconv.FormatInt(i.Id, 10), stopPath)
	sp := i.GetSnapshot()
	ev, err := sp.Wait(p)
	if err != nil {
		return nil, err
	}
	i.Status = InsStatusStopping
	i.dir = i.dir.Join(ev)

	return i, nil
}

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

func (i *Instance) WaitFailed() (*Instance, error) {
	for {
		i, err := i.WaitStatus()
		if err != nil {
			return nil, err
		}
		if i.Status == InsStatusFailed {
			break
		}
	}
	return i, nil
}

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

func (i *Instance) GetStatusInfo() (string, error) {
	info, _, err := i.dir.Snapshot.Get(i.procStatusPath(i.Status))
	if err != nil {
		return "", err
	}
	return info, nil
}

func (i *Instance) Lock(client string, reason error) (*Instance, error) {
	locked, err := i.IsLocked()
	if err != nil {
		return nil, err
	}
	if locked {
		return nil, errorf(ErrUnauthorized, "instance %d is already locked", i.Id)
	}

	i.dir, err = i.dir.Set(lockPath, fmt.Sprintf("%s %s %s", timestamp(), client, reason))
	if err != nil {
		return nil, err
	}

	return i, nil
}

func (i *Instance) Unlock() (*Instance, error) {
	err := i.dir.Del(lockPath)
	if err != nil {
		return nil, err
	}
	return i, nil
}

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

func (i *Instance) EnvString() string {
	return fmt.Sprintf("%s:%s#%s", i.AppName, i.ProcessName, i.Env)
}

func (i *Instance) RevString() string {
	return fmt.Sprintf("%s:%s@%s", i.AppName, i.ProcessName, i.RevisionName)
}

func (i *Instance) RefString() string {
	return fmt.Sprintf("%s:%s@%s#%s", i.AppName, i.ProcessName, i.RevisionName, i.Env)
}

func (i *Instance) ServiceName() string {
	return fmt.Sprintf("%s:%s", i.AppName, i.ProcessName)
}

func (i *Instance) WorkerId() string {
	return fmt.Sprintf("%s-%s-%s-%d", i.AppName, i.ProcessName, i.RevisionName, i.Port)
}

func (i *Instance) Fields() string {
	return fmt.Sprintf("%d %s %s %s %s %d %d", i.Id, i.AppName, i.RevisionName, i.ProcessName, i.Ip, i.Port, i.TelePort)
}

// String returns the Go-syntax representation of Instance.
func (i *Instance) String() string {
	return fmt.Sprintf("Instance{id=%d, app=%s, rev=%s, proc=%s, env=%s, addr=%s:%d}", i.Id, i.AppName, i.RevisionName, i.ProcessName, i.Env, i.Ip, i.Port)
}

// IdString returns a string of the format "INSTANCE[id]"
func (i *Instance) IdString() string {
	return fmt.Sprintf("INSTANCE[%d]", i.Id)
}

func (i *Instance) claimPath(host string) string {
	return i.dir.Prefix("claims", host)
}

func (i *Instance) claimDir() *cp.Dir {
	return cp.NewDir(i.dir.Prefix(claimsPath), i.GetSnapshot())
}

func (i *Instance) idString() string {
	return fmt.Sprintf("%d", i.Id)
}

func (i *Instance) objectArray() []string {
	return []string{i.AppName, i.RevisionName, i.ProcessName, i.Env}
}

func (i *Instance) startArray() []string {
	return []string{i.Ip, i.portString(), i.Host, i.telePortString()}
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
	i.Ip = ip
	i.Status = InsStatusClaimed
}

func (i *Instance) getRestarts() (InsRestarts, *cp.File, error) {
	var restarts InsRestarts

	sp, err := i.GetSnapshot().FastForward()
	if err != nil {
		return restarts, nil, err
	}
	i.dir = i.dir.Join(sp)

	f, err := sp.GetFile(i.dir.Prefix(restartsPath), new(cp.ListIntCodec))
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
	i.Ip = ip
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
		return errorf(ErrUnauthorized, "instance %d is not claimed", i.Id)
	}

	if *claimer != host {
		return errorf(ErrUnauthorized, "instance %d has different claimer: %s != %s", i.Id, *claimer, host)
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
		ins, err := getSerialisedInstance(i.AppName, i.ProcessName, i.Id, from, sp)
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
	p := path.Join(instancesPath, strconv.FormatInt(i.Id, 10), startPath)
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

		// TODO (alx) This branch becomes unnecessary as soon as all components are migrated.
		telePort := 0
		if len(fields) >= 4 {
			telePort, err = strconv.Atoi(fields[3])
			if err != nil {
				return nil, err
			}
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
		id, err := parseInstanceId(idstr)
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

func (s *Store) WatchInstanceStart(listener chan *Instance, errors chan error) {
	// instances/*/start =
	sp := s.GetSnapshot()
	for {
		ev, err := sp.Wait(path.Join(instancesPath, "*", startPath))
		if err != nil {
			errors <- err
			return
		}
		sp = sp.Join(ev)

		if !ev.IsSet() || string(ev.Body) != "" {
			continue
		}
		idstr := strings.Split(ev.Path, "/")[2]

		id, err := parseInstanceId(idstr)
		if err != nil {
			errors <- err
			return
		}
		ins, err := getInstance(id, ev.GetSnapshot())
		if err != nil {
			errors <- err
			return
		}
		listener <- ins
	}
}

func instancePath(id int64) string {
	return path.Join(instancesPath, strconv.FormatInt(id, 10))
}

func procInstancesPath(app, rev, proc string) string {
	return path.Join(appsPath, app, procsPath, proc, instancesPath, rev)
}

func parseInstanceId(idstr string) (int64, error) {
	return strconv.ParseInt(idstr, 10, 64)
}

func getInstance(id int64, s cp.Snapshotable) (*Instance, error) {
	i := &Instance{
		Id:     id,
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
			i.Ip = fields[0]
		}
		if len(fields) > 1 { // Port
			i.Status = InsStatusRunning
			i.Port, err = strconv.Atoi(fields[1])
			if err != nil {
				panic("invalid port number: " + fields[1])
			}
		}
		if len(fields) > 2 { // Hostname
			i.Host = fields[2]
		}
		if len(fields) > 3 { // TelePort
			i.TelePort, err = strconv.Atoi(fields[3])
			if err != nil {
				panic("invalid port number: " + fields[3])
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
	// FIXME remove as soon as env migration is done
	if len(fields) == 4 {
		i.Env = fields[3]
	}

	i.Restarts, _, err = i.getRestarts()
	if err != nil {
		return nil, err
	}

	f, err = i.dir.GetFile(registeredPath, new(cp.StringCodec))
	if err != nil {
		// FIXME remove as soon as instances have consistent registered field
		if !cp.IsErrNoEnt(err) {
			return nil, err
		}
	} else {
		i.Registered, err = parseTime(f.Value.(string))
		if err != nil {
			// FIXME remove backwards compatible parsing of timestamps before b4fbef0
			i.Registered, err = time.Parse(UTCFormat, f.Value.(string))
			if err != nil {
				return nil, err
			}
		}
	}

	f, err = i.claimDir().GetFile(i.Ip, new(cp.StringCodec))
	if err != nil {
		if cp.IsErrNoEnt(err) {
			return i, nil
		}
		return nil, err
	} else {
		i.Claimed, err = parseTime(f.Value.(string))
		if err != nil {
			return nil, err
		}
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
		id, e := parseInstanceId(f)
		if e != nil {
			return nil, e
		}
		ids = append(ids, id)
	}
	sort.Sort(ids)
	return
}

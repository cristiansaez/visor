package visor

import (
	"errors"
	"regexp"
	"time"

	cp "github.com/soundcloud/cotterpin"
)

const (
	hooksPath = "hooks"
)

var (
	rHookName = regexp.MustCompile("^[[:alnum:]]+$")
)

// Hook represents a named executable script.
type Hook struct {
	file       *cp.File
	App        *App      `json:"-"`
	Name       string    `json:"name"`
	Script     string    `json:"script"`
	Registered time.Time `json:"registered"`
}

// NewHook returns a new Hook given an App, a name and the script.
func (a *App) NewHook(name, script string) *Hook {
	return &Hook{
		file:   cp.NewFile(a.dir.Prefix(hooksPath, name), nil, new(cp.JsonCodec), a.GetSnapshot()),
		App:    a,
		Name:   name,
		Script: script,
	}
}

func (h *Hook) GetSnapshot() cp.Snapshot {
	return h.file.Snapshot
}

// Register stores the Hook with the App.
func (h *Hook) Register() (*Hook, error) {
	var err error

	h.Registered = time.Now()

	h.file, err = h.file.Set(h)
	if err != nil {
		return nil, err
	}

	return h, nil
}

// Unregister removes the stored Hook from the App.
func (h *Hook) Unregister() error {
	sp, err := h.GetSnapshot().FastForward()
	if err != nil {
		return err
	}
	exists, _, err := sp.Exists(h.file.Path)
	if err != nil {
		return err
	}
	if !exists {
		return errorf(ErrNotFound, `hook "%s" not found`, h.Name)
	}
	return h.file.Del()
}

// GetHook retrieves the Hook for the passed name.
func (a *App) GetHook(name string) (*Hook, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	return getHook(a, name, sp)
}

// GetHooks returns a list of all Hooks for the app.
func (a *App) GetHooks() ([]*Hook, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	names, err := sp.Getdir(a.dir.Prefix(hooksPath))
	if err != nil {
		return nil, err
	}

	hooks := []*Hook{}
	ch, errch := cp.GetSnapshotables(names, func(name string) (cp.Snapshotable, error) {
		return getHook(a, name, sp)
	})
	for i := 0; i < len(names); i++ {
		select {
		case h := <-ch:
			hooks = append(hooks, h.(*Hook))
		case err := <-errch:
			return nil, err
		}
	}
	return hooks, nil
}

func getHook(app *App, name string, s cp.Snapshotable) (*Hook, error) {
	c := new(cp.JsonCodec)
	c.DecodedVal = &Hook{}

	f, err := s.GetSnapshot().GetFile(app.dir.Prefix(hooksPath, name), c)
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = errorf(ErrNotFound, `hook not found for "%s"`, name)
		}
		return nil, err
	}

	h, ok := f.Value.(*Hook)
	if !ok {
		return nil, errors.New("retrieved file is not a hook")
	}
	h.file = f
	h.App = app

	return h, nil
}

package visor

import (
	"time"

	cp "github.com/soundcloud/cotterpin"
)

const (
	tagsPath = "tags"
)

// Tag represents a human readable alias for a revision. It's analogous to a
// branch in git referencing a specific commit. It's possible that multiple
// tags reference the same revision.
type Tag struct {
	file       *cp.File
	App        *App      `json:"-"`
	Name       string    `json:"name"`
	Ref        string    `json:"ref"`
	Registered time.Time `json:"registered"`
}

// NewTag returns a named Tag referencing a given ref.
func (a *App) NewTag(name, ref string) *Tag {
	return &Tag{
		file: cp.NewFile(
			a.dir.Prefix(tagsPath, name),
			nil,
			new(cp.JsonCodec), a.GetSnapshot(),
		),
		App:  a,
		Name: name,
		Ref:  ref,
	}
}

// GetSnapshot satisfies the cp.Snapshotable interface.
func (t *Tag) GetSnapshot() cp.Snapshot {
	return t.file.Snapshot
}

// Register stores the Tag in store. It does permit overwriting an existing tag
// with the same name to enable atomic updates.
func (t *Tag) Register() error {
	var err error

	revs, err := t.App.GetRevisions()
	if err != nil {
		return err
	}

	found := false
	for _, r := range revs {
		if r.Ref == t.Name {
			return errorf(ErrTagShadowing, `revision already exists with tag name "%s"`, t.Name)
		}
		if r.Ref == t.Ref {
			found = true
		}
	}
	if !found {
		return errorf(ErrNotFound, `revision "%s" not found for app "%s"`, t.Ref, t.App.Name)
	}

	t.Registered = time.Now()
	t.file, err = t.file.Set(t)
	if err != nil {
		return err
	}
	return nil
}

// Unregister removes the stored Tag from store.
func (t *Tag) Unregister() error {
	sp, err := t.GetSnapshot().FastForward()
	if err != nil {
		return err
	}
	exists, _, err := sp.Exists(t.file.Path)
	if err != nil {
		return err
	}
	if !exists {
		return errorf(ErrNotFound, `tag "%s" not found`, t.Name)
	}
	return t.file.Del()
}

// GetTag retrieves the Tag with the given name.
func (a *App) GetTag(name string) (*Tag, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}
	return getTag(a, name, sp)
}

// GetTags retrieves all tags for the revision.
func (r *Revision) GetTags() ([]*Tag, error) {
	tags, err := r.App.GetTags()
	if err != nil {
		return nil, err
	}

	rtags := []*Tag{}
	for _, tag := range tags {
		if tag.Ref == r.Ref {
			rtags = append(rtags, tag)
		}
	}
	return rtags, nil
}

// GetTags returns a list of all Tags for the app.
func (a *App) GetTags() ([]*Tag, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	names, err := sp.Getdir(a.dir.Prefix(tagsPath))
	if err != nil {
		return nil, err
	}

	tags := []*Tag{}
	ch, errch := cp.GetSnapshotables(names, func(name string) (cp.Snapshotable, error) {
		return getTag(a, name, sp)
	})
	for i := 0; i < len(names); i++ {
		select {
		case t := <-ch:
			tags = append(tags, t.(*Tag))
		case err := <-errch:
			return nil, err
		}
	}
	return tags, nil
}

// LookupRevision retrieves a revision by ref or tag.
func (a *App) LookupRevision(ref string) (*Revision, error) {
	sp, err := a.GetSnapshot().FastForward()
	if err != nil {
		return nil, err
	}

	rev, rerr := getRevision(a, ref, sp)
	if rerr != nil && !IsErrNotFound(rerr) {
		return nil, rerr
	}
	if rev != nil {
		return rev, nil
	}
	tag, err := getTag(a, ref, sp)
	if err != nil && !IsErrNotFound(err) {
		return nil, err
	}
	if tag == nil {
		return nil, rerr
	}
	return getRevision(a, tag.Ref, sp)
}

func getTag(a *App, name string, s cp.Snapshotable) (*Tag, error) {
	t := &Tag{}
	c := &cp.JsonCodec{DecodedVal: t}

	f, err := s.GetSnapshot().GetFile(a.dir.Prefix(tagsPath, name), c)
	if err != nil {
		if cp.IsErrNoEnt(err) {
			err = errorf(ErrNotFound, `tag "%s" not found`, name)
		}
		return nil, err
	}

	t.file = f
	t.App = a

	return t, nil
}

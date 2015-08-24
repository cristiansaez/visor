package visor

import "testing"

func TestTagRegister(t *testing.T) {
	var (
		app  = tagSetup(t)
		name = "register"
		ref1 = "123abcd"
		ref2 = "d1324cs"
		rev1 = tagStore.NewRevision(app, ref1, "http://unknown")
		rev2 = tagStore.NewRevision(app, ref2, "http://unknown")
	)

	for _, rev := range []*Revision{rev1, rev2} {
		if _, err := rev.Register(); err != nil {
			t.Fatal(err)
		}
	}

	// test precondition
	if _, err := app.GetTag(name); !IsErrNotFound(err) {
		t.Fatal("want GetTag to fail for unregistered tag")
	}

	// test registration of unknown revision
	if err := app.NewTag(name, "unknw13").Register(); !IsErrNotFound(err) {
		t.Fatal("want Register to fail for unknown revision")
	}

	// test valid registration
	if err := app.NewTag(name, ref1).Register(); err != nil {
		t.Fatal("want Register to succeed, have %v", err)
	}
	tag, err := app.GetTag(name)
	if err != nil {
		t.Fatal(err)
	}
	if tag.Name != name {
		t.Errorf("want tag name %s, have %s", name, tag.Name)
	}
	if tag.Ref != ref1 {
		t.Errorf("want tag ref %s, have %s", ref1, tag.Ref)
	}
	if tag.App.Name != app.Name {
		t.Errorf("want tag app %s, have %s", app.Name, tag.App.Name)
	}

	// test re-registration of existing name
	tag.Ref = ref2
	if err := tag.Register(); err != nil {
		t.Fatal(err)
	}
	tag, err = app.GetTag(name)
	if err != nil {
		t.Fatal(err)
	}
	if tag.Ref != ref2 {
		t.Errorf("want tag ref %s, have %s", ref2, tag.Ref)
	}

	// test shadowing of existing revision name
	if err := app.NewTag(ref1, ref1).Register(); !IsErrTagShadowing(err) {
		t.Errorf("want tag shadowing error, have %v", err)
	}
}

func TestTagUnregister(t *testing.T) {
	var (
		app  = tagSetup(t)
		name = "unregister"
		ref  = "u123abd"
		tag  = app.NewTag(name, ref)
		rev  = tagStore.NewRevision(app, ref, "http://unknown")
	)

	if err := tag.Unregister(); !IsErrNotFound(err) {
		t.Fatal("want Unregister to fail for unregistered tag")
	}
	if _, err := rev.Register(); err != nil {
		t.Fatal(err)
	}
	if err := tag.Register(); err != nil {
		t.Fatal(err)
	}
	if _, err := app.GetTag(name); err != nil {
		t.Fatal(err)
	}
	if err := tag.Unregister(); err != nil {
		t.Fatal(err)
	}
	if _, err := app.GetTag(name); !IsErrNotFound(err) {
		t.Fatal("want GetTag fail for unregistered tag")
	}
}

func TestTagList(t *testing.T) {
	app := tagSetup(t)
	rev := tagStore.NewRevision(app, "adf3kk3h", "")
	tags := []*Tag{
		app.NewTag("foo", rev.Ref),
		app.NewTag("bar", rev.Ref),
		app.NewTag("baz", rev.Ref),
	}

	if _, err := rev.Register(); err != nil {
		t.Fatal(rev)
	}

	for _, tag := range tags {
		if err := tag.Register(); err != nil {
			t.Fatal(err)
		}
	}

	tags1, err := app.GetTags()
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != len(tags1) {
		t.Error("GetTags didn't return correct amount of tags")
	}
}

func TestTagLookup(t *testing.T) {
	var (
		app  = tagSetup(t)
		name = "lookup"
		ref1 = "lup1234"
		ref2 = "lup9876"
		rev1 = tagStore.NewRevision(app, ref1, "http://unknown")
		rev2 = tagStore.NewRevision(app, ref2, "http://unknown")
	)

	for _, rev := range []*Revision{rev1, rev2} {
		if _, err := rev.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.NewTag(name, ref1).Register(); err != nil {
		t.Fatal(t)
	}

	if _, err := app.LookupRevision(name); err != nil {
		t.Fatal(err)
	}
	if _, err := app.LookupRevision(ref2); err != nil {
		t.Fatal(err)
	}
	if _, err := app.LookupRevision("unknown"); !IsErrNotFound(err) {
		t.Fatal("want lookup to fail for unknown revision")
	}
}

var tagStore *Store

func tagSetup(t *testing.T) *App {
	if tagStore == nil {
		s, err := DialURI(DefaultURI, "/tag-test")
		if err != nil {
			t.Fatal(err)
		}
		tagStore = s
	}

	err := tagStore.reset()
	if err != nil {
		t.Fatal(err)
	}

	tagStore, err = tagStore.FastForward()
	if err != nil {
		t.Fatal(err)
	}

	tagStore, err = tagStore.Init()
	if err != nil {
		t.Fatal(err)
	}

	return tagStore.NewApp("tag-test", "git://tag.git", "tags")
}

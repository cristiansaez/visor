package visor

import (
	"math/rand"
)

var appNames = []string{"cat", "dog", "bird", "wolf", "bear", "lion", "tiger"}
var revNames = []string{"master", "slave", "e7fa81", "a91748", "f7ea91", "dev", "stable"}

func genApp(s *Store) (app *App) {
	name := randItem(appNames)
	app = s.NewApp(name, "git://"+name+".git", "my-stack")
	app, err := app.Register()
	if err != nil {
		panic(err)
	}
	return
}

func genRevision(app *App) (rev *Revision) {
	s := storeFromSnapshotable(app)
	name := randItem(revNames)
	rev = s.NewRevision(app, name, "foo.img")
	rev, err := rev.Register()
	if err != nil {
		panic(err)
	}
	return
}

func genProc(app *App, name string) (proc *Proc) {
	s := storeFromSnapshotable(app)
	proc = s.NewProc(app, name)
	proc, err := proc.Register()
	if err != nil {
		panic(err)
	}
	return
}

func genEnv(app *App, ref string, vars map[string]string) *Env {
	env := app.NewEnv(ref, vars)
	env, err := env.Register()
	if err != nil {
		panic(err)
	}
	return env
}

//func Instance(proc *visor.Proc, rev *visor.Revision, s visor.Snapshot) (ins *visor.Instance) {
//	if proc == nil {
//		proc = Proc(nil, s, randItem(procNames))
//	}
//	if rev == nil {
//		rev = Revision(nil, s)
//	}
//	addr := fmt.Sprintf("127.0.0.1:%d", 8000+rand.Int63n(1000))
//	ins, err := visor.NewInstance(string(proc.Name), rev.Ref, rev.App.Name, addr, s)
//	if err != nil {
//		panic(err)
//	}
//	return
//}

func randItem(items []string) string {
	return items[rand.Int63n(int64(len(items)))]
}

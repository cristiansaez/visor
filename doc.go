// Copyright (c) 2013, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Source code and contact info at http://github.com/soundcloud/visor

/*
To understand how Visor works, we need to understand how it works with *time*. Each
of the Visor data-types *App*, *Revision*, *Proc* and *Instance* are snapshots of
a specific point in time in the coordinator. When a mutating operation is successfully
performed on one of these data-types, a **new snapshot** is returned, representing the state
of the coordinator *after* the operation. If the operation would fail, the old snapshot is
returned with an error.

With the new snapshot, we can perform an operation on this new state, and so on with every new snapshot.

  package main

  import "github.com/soundcloud/visor"
  import "log"

  func main() {
      snapshot, err := visor.DialUri("doozer:?ca=localhost:8046", "/")

      name  := "rocket"
      stack := "HEAD" // Runtime stack revision
      repo  := "http://github.com/bazooka/rocket"

      app   := visor.NewApp(name, repo, stack, snapshot)

      _, err := app.Register()
      if err != nil {
          log.Fatalf("error registering app: %s", err)
      }

      rev := visor.NewRevision(app, "f84e19", snapshot)
      rev.ArchiveUrl = "http://artifacts/rocket/f84e19.img"

      _, err = rev.Register()
      if err != nil {
          log.Fatalf("error registering revision: %s", err)
      }
  }

Working with Snapshots

  // Get a snapshot of the latest coordinator state
  snapshot, err := visor.DialUri("doozer:?ca=localhost:8046", "/")

  // Get the list of applications at snapshot
  apps, _ := visor.Apps(snapshot)
  app := apps[0] // app.Rev == snapshot.Rev == 1

  // Set some environment vars on *app*. Every time state is
  // changed in the coordinator, a new App snapshot is returned.
  app, _ = app.SetEnvironmentVar("cow", "moo")  // app.Rev == 2
  app, _ = app.SetEnvironmentVar("cat", "meow") // app.Rev == 3

  // Attempt to get a recently set environment var from an old snapshot (apps[0].Rev == 1)
  apps[0].GetEnvironmentVar("cat") // "", ErrKeyNotFound

  // Get a recently set environment var from the latest snapshot (app.Rev == 3)
  app.GetEnvironmentVar("cat")     // "meow", nil

Watching for Events

  package main

  import "soundcloud/visor"

  func main() {
    snapshot, err := visor.DialUri("doozer:?ca=localhost:8046", "/")
    if err != nil {
      panic(err)
    }

    c := make(chan *visor.Event)

    go visor.WatchEvent(snapshot, c)

    // Read one event from the channel
    fmt.Println(<-c)
  }
*/
package visor

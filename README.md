# Visor [![Build Status][1]][2] [![Coverage Status][3]][4]

Visor is a library which provides an abstraction over a global process state on top of [doozerd][5].

[1]: https://travis-ci.org/soundcloud/visor.svg?branch=master
[2]: https://travis-ci.org/soundcloud/visor
[3]: https://coveralls.io/repos/soundcloud/visor/badge.png?branch=master
[4]: https://coveralls.io/r/soundcloud/visor
[5]: https://github.com/ha/doozerd

## Installing

Install [Go 1][6], either [from source][7] or [with a prepackaged binary][8].
Then,

```bash
$ go get github.com/soundcloud/visor
```

[6]: http://golang.org
[7]: http://golang.org/doc/install/source
[8]: http://golang.org/doc/install

## Documentation

See [the godoc page](http://godoc.org/github.com/soundcloud/visor) for up-to-the-minute documentation and usage.

## Contributing

Pull requests are very much welcomed.  Create your pull request on a non-master branch, make sure a test or example is included that covers your change and your commits represent coherent changes that include a reason for the change.

To run the integration tests, make sure you have Doozerd reachable under the [DefaultUri][9] and run `go test`. TravisCI will also run the integration tests.

[9]: https://github.com/soundcloud/visor/blob/master/visor.go#L46

## Credits

* [Alexis Sellier][10]
* [Alexander Simmerl][11]
* [Daniel Bornkessel][12]
* [François Wurmus][13]
* [Matt T. Proud][14]
* [Tomás Senart][15]
* [Julius Volz][16]
* [Patrick Ellis][17]
* [Lars Gierth][18]
* [Tobias Schmidt][19]

[10]: https://github.com/cloudhead
[11]: https://github.com/xla
[12]: https://github.com/kesselborn
[13]: https://github.com/fronx
[14]: https://github.com/matttproud-soundcloud
[15]: https://github.com/tsenart
[16]: https://github.com/juliusv
[17]: https://github.com/pje
[18]: https://github.com/lgierth
[19]: https://github.com/grobie

## License

BSD 2-Clause, see [LICENSE][20] for more details.

[20]: https://github.com/soundcloud/cotterpin/blob/master/LICENSE

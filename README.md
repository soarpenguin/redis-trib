# **redis-trib** <sup><sub>_redis cluster Command Line tool_</sub></sup>
[![Build Status](https://travis-ci.org/soarpenguin/redis-trib.svg?branch=master)](https://travis-ci.org/soarpenguin/redis-trib)

Create and administrate your [Redis Cluster][cluster-tutorial] from the Command Line.

Inspired heavily by [redis-trib.go][] and the original [redis-trib.rb][].

## Dependencies

* [redigo][]
* [cli][]

Dependencies are handled by [godep][], simple install it and type `godep restore` to fetch them.

## Install

#### Restore project env in first build
```console
$ git clone https://github.com/soarpenguin/redis-trib.git
$ cd redis-trib
$ make godep
$ make bin
$ PROG=./redis-trib source ./autocomplete/bash_autocomplete
```

#### Build the code
```console
$ cd redis-trib
$ make bin
```

## Usage

```console
NAME:
   redis-trib - Redis Cluster command line utility.

For check, fix, reshard, del-node, set-timeout you can specify the host and port
of any working node in the cluster.

USAGE:
   redis-trib [global options] command [command options] [arguments...]

VERSION:
   v0.1.0
commit: 533d96aba4b0e73649dcf81209156230698666aa

AUTHOR(S):
   soarpenguin <soarpenguin@gmail.com>

COMMANDS:
     add-node, add  add a new redis node to existed cluster.
     call           run command in redis cluster.
     check          check the redis cluster.
     create         create a new redis cluster.
     del-node, del  del a redis node from existed cluster.
     fix            fix the redis cluster.
     import         import operation for redis cluster.
     info           display the info of redis cluster.
     rebalance      rebalance the redis cluster.
     reshard        reshard the redis cluster.
     set-timeout    set timeout configure for redis cluster.

GLOBAL OPTIONS:
   --debug             enable debug output for logging
   --verbose           verbose global flag for output.
   --log value         set the log file path where internal debug information is written
   --log-format value  set the format used by logs ('text' (default), or 'json') (default: "text")
   --help, -h          show help
   --version, -v       print the version
```

[cluster-tutorial]: http://redis.io/topics/cluster-tutorial
[redis-trib.go]: https://github.com/badboy/redis-trib.go
[redis-trib.rb]: https://github.com/antirez/redis/blob/unstable/src/redis-trib.rb
[redigo]: https://github.com/garyburd/redigo/
[cli]: https://github.com/codegangsta/cli
[godep]: https://github.com/tools/godep

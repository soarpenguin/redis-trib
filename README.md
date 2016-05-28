# **redis-trib** <sup><sub>_redis cluster Command Line tool_</sub></sup>

Create and administrate your [Redis Cluster][cluster-tutorial] from the Command Line.

Inspired heavily by [redis-trib.go][] and the original [redis-trib.rb][].

## Dependencies

* [redigo][]
* [cli][]

Dependencies are handled by [godep][], simple install it and type `godep restore` to fetch them.

## Install

```console
$ git clone https://github.com/soarpenguin/redis-trib.git
$ cd redis-trib
$ make bin
```


[cluster-tutorial]: http://redis.io/topics/cluster-tutorial
[redis-trib.go]: https://github.com/badboy/redis-trib.go
[redis-trib.rb]: https://github.com/antirez/redis/blob/unstable/src/redis-trib.rb
[redigo]: https://github.com/garyburd/redigo/
[cli]: https://github.com/codegangsta/cli
[godep]: https://github.com/tools/godep

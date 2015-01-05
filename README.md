Redis Instance Contolling and Distribution Service

Server
===

Create tables in MySQL via `scripts/mysql.sql`.

Make a copy of `config.yaml` (suggest naming the copy as `local.yaml` which is added to gitignore).

Edit the copy, change

* `log_level`: Python logging level, `info`, `debug`, `error`, etc
* `debug`: whether the server runs under the debug mode
* `mysql` section: connection arguments

Then run

    python main.py local.yaml

Polling Daemon
===

Process to polling redis nodes status. (via `info` command)

Run

    python daemon.py local.yaml

Stats include

* memory usage
* cpu usage
* keys stats (hits, misses, expired, evicted)
* connected clients (including the polling connection, possibly including connections from controller)
* if aof enabled

IPC
===

The server and daemon uses `/tmp/instances.json` and `/tmp/poll.json` as default IPC files.

The programs don't use redis to do the communication, however, because they are the controllers of redis.

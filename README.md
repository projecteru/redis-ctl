Redis Instance Contolling and Distribution Service

Server
===

Create tables in MySQL via `scripts/mysql.sql`.

Install dependencies via

    pip install -r requirements.txt

Make a copy of `config.yaml` (suggest naming the copy as `local.yaml` which is added to gitignore).

Edit the copy, change

* `port`: server listen port
* `log_level`: Python logging level, `info`, `debug`, `error`, etc
* `debug`: whether the server runs under the debug mode
* `mysql` section: MySQL connection arguments
* `influxdb` section: InfluxDB connection arguments

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

Usage
===

For web interface usage, please read [here (CN)](https://github.com/HunanTV/redis-ctl/wiki/Web-%E7%95%8C%E9%9D%A2%E4%BD%BF%E7%94%A8)

Manage your Redis Clusters in a web GUI

![Redis Overview](http://zlo.gs/image_data/8fb3021522baf6414c107cfd3fc0ad406461d5e5)

Commit a cluster migration from a HTML form

![Slot Migrating](http://zlo.gs/image_data/5dc602f756975f97a9264e6e7a94a3ad08518a2f)

Display cluster nodes in a tidy table

![CLUSTER NODES](http://zlo.gs/image_data/4fda367ddacd1501337bc725002dc98384083179)

RedisCtl is a set of Python toolkit with a web UI based on Flask that makes it easy to manage Redis and clusters.

# Overview

RedisCtl contains

* a web UI that displays Redis status and receives commands
* a daemon that polls each Redis and collect info and runs tasks like slots migrating for clusters

By default its optional external dependencies are

* [OpenFalcon](https://github.com/open-falcon) with this statistics service, RedisCtl is able to draw charts for Redis instances on historic usage of memory, CPU, etc.
* [Eru](https://github.com/projecteru/eru-core) with this dockerization service, RedisCtl is able to launch Redis as docker containers, this will also enable automatical deployment and migration when a cluster mode Redis serves too much data.

Already has your own statistics / containerization service? You could [make your own overridings](https://github.com/HunanTV/redis-ctl/wiki/Customize_App) ([in Chinese](https://github.com/HunanTV/redis-ctl/wiki/WIP_v0_9_customize_app_zh)).

# Setup

First, install Python-dev header files and libs

    # debain / ubuntu
    apt-get install python-dev

    # centos
    yum install python-devel

Then clone this project and cd in to install dependencies

    pip install -r requirements.txt

Run with all configurations default

    python main.py

Use env vars, like

    MYSQL_USERNAME=redisctl MYSQL_PASSWORD=p@55w0rd python main.py

Check `config.py` for configurable items.

To use a configure file, copy `override_config.py.example` to `override_config.py`, change anything you want. This file would be imported and override any default config or env vars in `config.py` if available.

Run the daemon that collects Redis info

    python daemon.py

Also you could use similar ways to configure daemon, just like setup up the main server.

# IPC

The server and daemon uses `/tmp/details.json` and `/tmp/poll.json` as default IPC files. You could change the directory for those temp files by passing the same `PERMDIR` environ to the web application and the daemon.

# Usage

For web interface usage, please read [here (CN)](https://github.com/HunanTV/redis-ctl/wiki/WebUI)

Redis Instance Contolling and Distribution Service

Dependency
===

Python-dev header files and libs

    # debain / ubuntu
    apt-get install python-dev

    # centos
    yum install python-devel

Install dependencies via

    pip install -r requirements.txt

Influxdb (optional): influxd >= 0.9; influxdb (python lib) >= 1.0.0

Configure and Run the Server
===

Run with all configurations default

    python main.py

Use env vars, like

    MYSQL_USERNAME=redisctl MYSQL_PASSWORD=p@55w0rd python main.py

Check `config.py` for configurable items.

To use a configure file, copy `override_config.py.example` to `override_config.py`, change anything you want. This file would be imported and override any default config or env vars in `config.py` if available.

Run the Polling Daemon
===

Process to polling redis nodes and proxy status.

Run

    python daemon.py

Also you could use similar ways to configure daemon, just like setup up the main server.

IPC
===

The server and daemon uses `/tmp/instances.json` and `/tmp/poll.json` as default IPC files.

The programs don't use redis to do the communication, however, because they are the controllers of redis.

Usage
===

For web interface usage, please read [here (CN)](https://github.com/HunanTV/redis-ctl/wiki/Web-%E7%95%8C%E9%9D%A2%E4%BD%BF%E7%94%A8)

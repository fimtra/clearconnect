## Log files 

At runtime, the VM running any ClearConnect platform component produces log files that reside in the _logs_ directory (configurable) of the working directory of the VM. The logs directory is automatically created. All the files automatically roll-over after 4Mb (configurable) and automatically delete or archive files older than 24hrs on VM startup (also configurable).

| Log file name | Purpose |
| --- | --- |
| `[main classname]-messages_[yyyyMMdd]_[HHmmss].log` | Platform messages and events are written to this file. |
| `[main classname]-threaddump_[yyyyMMdd]_[HHmmss].log` | Periodic thread dumps are written to this file (if enabled) |
| `[main classname]-Qstats_[yyyyMMdd]_[HHmmss].log` | Periodic core task queue statistics are written to this file (if enabled) |


e.g. `RemoteTestRunner-messages_20141020_200504.log` is the log file created when running the JUnit tests (the main class is RemoteTestRunner)

### Logging Format 

The format of the log messages is:

`datetime` `|` `thread` `|` `class instance` `|` `message`

e.g. `20141028-09:21:27:882|fission-core3|Context:1255364991|Notifying initial image 'Services'`
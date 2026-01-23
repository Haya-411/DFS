# DFS
## Master Server
```shell-session 
$ java DFSMaster
```

##  Child Server
```shell-session 
$ java DFSServer <port_number>
```

##  Client
```shell-session 
$ java DFSClient

[usage]   $ open <path> <mode>
[example] $ open ./A/sample.txt RW
[usage]   $ read / close / list / keep 
[usage]   $ write <content>
[example] $ write 1234

```

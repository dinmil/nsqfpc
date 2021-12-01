## nsqfpc

This is sample which shows how to use nsq golang message queue with fpc.
Project supports sending and receiving messages with plain receiver TClsNSQReceiver.
It also support NSQ Lookup system to locate NSQ deamons.
Initial version was written with CodeTyphoon 7.50 for MAC.
It is adjusted for Windows too. 

## Test program is written to receive messages and
1. send one message
2. send many messages
3. send delayed messages
4. close connection

Client library tries to follow directions from https://nsq.io/clients/building_client_libraries.html

## About NSQ
- It is simple, 
- It is fast, 
- It is written in GO language, 
- It is easy for setup (no need to setup at all), 
- It suppport all OS-es
- It....

## NSQ Installation
Follow directions from https://nsq.io/deployment/installing.html
Download version for your OS. Unpack package into some directory then try to start nsqd.
This is all you need. But if you want more then you need to start 
nsqd, nsqlookupd and nsqadmin

Script to start nsq system for windows environment:
```
start cmd /c nsqlookupd.exe
timeout 5
rem this will start nsqd and set it to call nsqlookupd. After that nsqlookupd will know that if have new producer deamon
start cmd /c nsqd.exe -lookupd-tcp-address 127.0.0.1:4160
rem timeout 5
rem start cmd /c nsqadmin.exe --nsqd-http-address 127.0.0.1:4151
timeout 5
start cmd /c nsqadmin.exe --lookupd-http-address 127.0.0.1:4161
```

Open browser and configure topics with nsqadmin service. This is the easy way to get things done
Url is http://localhost:4171 or http://127.0.0.1:4171

## Motivation
To move fpc closer to new fancy message queue techologies.

### PS
Library is not yet finished. It is written in my spare time.

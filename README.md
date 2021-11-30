# nsqfpc

This is sample which shows how to use nsq golang message queue with fpc.
Project supports sending and receiving messages with plain receiver TClsNSQReceiver.
It also support NSQ Lookup system to locate NSQ deamons.
Initial version was written with CodeTyphoon 7.50 for MAC.
It is adjusted for Windows also.

Test program is written to receive messages, 
1. send one message
2. send many message
3. send delayed messages
4. close connection

Client library try to follow dirrections from https://nsq.io/clients/building_client_libraries.html

About NSQ. 
It is simple, it is fast, it is written in GO language, it is easy for setup (no need to setup at all), it suppport all OS-es.

NSQ Installation
Follow directions from https://nsq.io/deployment/installing.html
Script to start nsq system for windows environment:
start cmd /c nsqlookupd.exe
timeout 5
start cmd /c nsqd.exe -lookupd-tcp-address 127.0.0.1:4160
rem timeout 5
rem start cmd /c nsqadmin.exe --nsqd-http-address 127.0.0.1:4151
timeout 5
start cmd /c nsqadmin.exe --lookupd-http-address 127.0.0.1:4161

Open browser and configure topics with nsqadmin service. Url is http://localhost:4171

Motivation.
To move fpc closer to new message techologies.

PS: Library is not yet finished. It is written in my spare time.

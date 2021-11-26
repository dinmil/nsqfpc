# nsqfpc

This is sample which shows how to use nsq golang message queue with fpc.
Project support sending and receiving messages with plain receiver TClsNSQReceiver.
It also support NSQ Lookup system to locate NSQ deamons.
Initial version was written with CodeTyphoon 7.50 for MAC.
It was not yet tested for Windows and Linux but it should work.

Test program is written to receive messages, 
1. send one message
2. send many message
3. send delayed messages
4. close connection

Client library try to follow dirrections from https://nsq.io/clients/building_client_libraries.html

About NSQ. 
It is simple, it is fast, it is written in GO language, it is easy for setup (no need to setup at all), it suppport all OS-es.

Motivation.
To move fpc closer to new message techologies.

PS: Library is not yet finished. It is written in my spare time.

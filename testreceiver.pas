unit TestReceiver;

{$mode ObjFPC}{$H+}

interface

uses
  Classes,
  SysUtils,
  Crt,
  IdTCPClient,
  NSQFunctions, NSQReceiver, NSQLookup, NSQTypes;

procedure TestNSQTopic;
procedure TestNSQReceiver;
procedure TestNSQLookup;

implementation

var NSQ_TEST_TOPIC: string = 'igraci';
    NSQ_TEST_CHANNEL: string = 'dinko-test';
    NSQ_TEST_MESSAGE1: string = 'This is some text';
    NSQ_TEST_MESSAGE2: string = 'Second Message';


procedure MyNSQCallback(InTimestampNanosecond: Int64;
                         InAttempts: Int32;
                         InMessageID: string;
                         InBody: string;
                         var OutHowToHandle: TNSQCallbackResponse;
                         var OutParam: Int32
                         );
begin
  if NSQ_DEBUG then begin
    NSQWrite('MyNSQCallback Received message: %s', [InBody]);
  end;
  OutHowToHandle := nsqCallFIN;
  OutParam := 0;
end;

procedure TestNSQTopic;
var Topic: TClsNSQTopic;
    Producer: TNSQProducer;
begin
  // parse test
  Topic := TClsNSQTopic.Create;
  Topic.GetTopicData(NSQ_LOOKUP_URL, 'igraci', NSQ_COMPUTER_NAME);

  if Topic._producers.GetCount > 0 then begin
    Producer := Topic._producers.GetItem(0)._data;
    NSQ_IP := Producer.broadcast_address;
    NSQ_PORT := Producer.tcp_port;
  end;

  FreeAndNil(Topic);
end;

procedure SendSomething(InTCPClient: TIdTCPClient; InObject: TObject);
var F: Integer;
    MyKey: Char;
begin
  F := 0;
  while true do begin
    if KeyPressed then begin
      F := F +1;
      MyKey := ReadKey;
      crt.ClrScr;
      NSQWrite('Press ctrl-c to finish', []);
      NSQWrite('Possible options (1-One msg; 2-Many msg, 3-Delay msg, 4-CloseConnection', []);
      NSQWrite('****************', []);
      NSQWrite(FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now), []);

      if (MyKey = ^c) then begin
        if InObject is TNSQReceiverThread then begin
          NSQWrite('Received ^c for TNSQReceiverThread', []);
          (InObject as TNSQReceiverThread).Terminate;
        end
        else if InObject is TNSQLookupThread then begin
        NSQWrite('Received ^c for TNSQLookupThread',[]);
          (InObject as TNSQLookupThread).TerminateThread;
        end;
        break;
      end;
      if (InTCPClient = nil) OR ((InTCPClient <> nil) and (InTCPClient.Connected = false)) then begin
        // lookup takes some time to connect and find producers
        if InObject is TNSQReceiverThread then begin
          InTCPClient := (InObject as TNSQReceiverThread)._tcpClient;
        end
        else if InObject is TNSQLookupThread then begin
          InTCPClient := (InObject as TNSQLookupThread)._nsqTopic.GetTcpClient;
        end;
      end;
      if InTCPClient <> nil then begin
        if InTCPClient.Connected then begin
          if MyKey = '1' then begin
            NSQWrite('1. Publish one message', []);
            NSQWritePUBMessage(InTCPClient, NSQ_TEST_TOPIC, NSQ_TEST_MESSAGE1 + IntToStr(F));
          end
          else if MyKey = '2' then begin
            NSQWrite('2. Publish many messages', []);
            NSQWriteMPUBMessage(InTCPClient, NSQ_TEST_TOPIC, [NSQ_TEST_MESSAGE1 + IntToStr(F), NSQ_TEST_MESSAGE2 + IntToStr(F)]);
          end
          else if MyKey = '3' then begin
            NSQWrite('3. Publish message with delay', []);
            NSQWriteDPUBMessage(InTCPClient, NSQ_TEST_TOPIC, 10000, NSQ_TEST_MESSAGE1 + IntToStr(F));
          end
          else if MyKey = '4' then begin
            NSQWrite('4. Close connection to queue nicely', []);
            NSQWriteCLSMessage(InTCPClient);
            InTCPClient := nil;
          end;
        end
        else begin
          NSQWrite('Client is not connected ', [])
        end;
      end
      else begin
        NSQWrite('Client is nil ', [])
      end;
    end
  end;
end;

procedure TestNSQReceiver;
var Reader: TNSQReceiverThread;
begin
  // Create reader
  NSQ_DEBUG := true;
  Reader := TNSQReceiverThread.Create(NSQ_IP, NSQ_PORT, NSQ_TEST_TOPIC, NSQ_TEST_CHANNEL);
  Reader.InstallCallback(@MyNSQCallback);
  Reader.Start;

  SendSomething(Reader._tcpClient, Reader);

  FreeAndNil(Reader);
  NSQWrite('', []);
  NSQWrite('Finished', []);
end;


procedure TestNSQLookup;
var Lookup: TNSQLookupThread;
begin
  // Create reader
  NSQ_DEBUG := true;

  Lookup := TNSQLookupThread.Create(NSQ_LOOKUP_URL, NSQ_TEST_TOPIC, NSQ_TEST_CHANNEL, 10);
  Lookup.InstallDataCallback(@MyNSQCallback);
  Lookup.Start;

  SendSomething(Lookup._nsqTopic.GetTcpClient, Lookup);

  FreeAndNil(Lookup);

  NSQWrite('',[]);
  NSQWrite('Finished', []);
end;

end.

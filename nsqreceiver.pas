unit NSQReceiver;

{$mode ObjFPC}{$H+}

interface

uses
  Classes, SysUtils, IdTCPClient, NSQFunctions, NSQTypes;

type
  { TNSQReceiverThread }

  TNSQReceiverThread = class(TThread)
  protected
    procedure Execute; override;
  public
    _isTerminated: boolean;
    _tcpClient: TIdTCPClient;
    _nsqIdentify: TNSQIdentify;
    _nsqBroadcastAddress: string;
    _nsqPort: Integer;
    _nsqTopic: string;
    _nsqChannel: string;

    _nsqCallback: TNSQCallback;
    constructor Create(InBroadcastAddress: string;
                       InPort: Integer;
                       InTopic: string;
                       InChannel: string
                    );
    destructor Destroy; override;
    procedure Connect;
    procedure TerminateTread;
    procedure InstallCallback(InNSQCallback: TNSQCallback);
  end;

implementation

{ TNSQReceiverThread }

procedure TNSQReceiverThread.Execute;
var MyAttempts: integer;            // Field returned when message arrive
    MyTimestampNanosecond: int64;   // Field returned when message arrive
    MyMessageID: string;            // Field returned when message arrive
    MyBody: string;                 // Field returned when message arrive

    MyFrameType: Int32;             // Field to detect FrameType
    MyMemoryStream: TMemoryStream;      // Input memory stream

    MyMessageType: Int32;
    MyMessage: String;
    MyCallbackResponse: TNSQCallbackResponse;
    MyCallbackParam: Int32;
begin
  MyMemoryStream := nil;
  try
    MyMemoryStream := TMemoryStream.Create;
    _isTerminated := false;
    while not Terminated do begin
      Sleep(1);
      if (_tcpClient = nil) or
         ((_tcpClient <> nil) and (not _tcpClient.Connected)) then begin
        Sleep(1);
        Connect;
      end;

      if (_tcpClient = nil) or
         ((_tcpClient <> nil) and (not _tcpClient.Connected)) then begin
        Continue;
      end;

      MyMessageType := 0;
      MyMessage := '';

      MyMemoryStream.Clear;
      _tcpClient.IOHandler.ReadStream(MyMemoryStream);
      NSQReadStream(MyMemoryStream, MyMessageType, MyMessage);
      if NSQ_DEBUG then Writeln('THREAD: RecvMsg: ', MyMemoryStream.Size, ', ', MyMessageType, ', "', MyMessage, '"', NSQ_CR);

(*
Data is streamed asynchronously to the client and framed in order to support the various reply bodies, ie:

[x][x][x][x][x][x][x][x][x][x][x][x]...
|  (int32) ||  (int32) || (binary)
|  4-byte  ||  4-byte  || N-byte
------------------------------------...
    size     frame type     data
A client should expect one of the following frame types:

FrameTypeResponse int32 = 0
FrameTypeError    int32 = 1
FrameTypeMessage  int32 = 2
And finally, the message format:

[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
|       8-byte         ||    ||                 16-byte                      || N-byte
------------------------------------------------------------------------------------------...
  nanosecond timestamp    ^^                   message ID                       message body
                       (uint16)
                        2-byte
                       attempts
*)

      MyMemoryStream.Position := 0;
      if MyMemoryStream.Size >= 4 then begin
        MyMemoryStream.ReadBuffer(MyFrameType, 4);  // Read first 4 bytes and swap it
        MyFrameType := BEtoN(MyFrameType);
        if NSQ_DEBUG then Writeln('THREAD: MyFrameType, BEtoN: ', MyFrameType, NSQ_CR);
      end
      else begin
        if NSQ_DEBUG then Writeln('THREAD:  Error: Received less then 4 bytes', NSQ_CR)
      end;


      case NSQGetFrameType(MyFrameType) of
        NSQ_FRAMETYPERESPONSE:
          begin
            MyBody := MyMessage;
            if MyBody = 'OK' then begin
              if NSQ_DEBUG then Writeln('THREAD: Recv: OK', NSQ_CR);
              Continue;
            end
            else if MyBody = '_heartbeat_' then begin
              if NSQ_DEBUG then Writeln('THREAD: Recv: _heartbeat_', NSQ_CR);
              NSQWriteNOPMessage(_tcpClient)
            end
            else begin
              if NSQ_DEBUG then Writeln('THREAD: Recv: unknown body: ', MyBody, NSQ_CR);
              // unknown MyBody
            end;
            MyBody := '';
          end;
        NSQ_FRAMETYPEERROR:
          begin
            // Extract error from rest of data
            if NSQ_DEBUG then Writeln('THREAD: ', MyMessage, NSQ_CR);
          end;
        NSQ_FRAMETYPEMESSAGE:
          begin
            MyTimestampNanosecond := 0;
            MyAttempts := 0;
            MyMessageId := '';
            MyBody := '';
            NSQReadMessage(MyMemoryStream, _tcpClient, MyTimestampNanosecond, MyAttempts, MyMessageID, MyBody);
            if _nsqCallback <> nil then begin
              _nsqCallback(MyTimestampNanosecond, MyAttempts,
                          MyMessageID, MyBody,
                          MyCallbackResponse,
                          MyCallbackParam
                          );
              if MyCallbackResponse = nsqCallFIN then begin
                NSQWriteFINMessage(_tcpClient, MyMessageID);
              end
              else if MyCallbackResponse = nsqCallREQ then begin
                NSQWriteREQMessage(_tcpClient, MyMessageID, MyCallbackParam);
              end
              else if MyCallbackResponse = nsqCallTOUCH then begin
                NSQWriteTOUCHMessage(_tcpClient, MyMessageID);
              end
            end
            else begin
              NSQWriteFINMessage(_tcpClient, MyMessageID);
            end;
            // For Test
            // NSQWriteREQMessage(_tcpClient, MyMessageID, 3000);
            // NSQWriteTOUCHMessage(_tcpClient, MyMessageID);
            // NSQWritePUBMessage(_tcpClient, 'igraci', 'Evo nekog teksta: ' + IntToStr(MyLima));
            MyBody := '';
          end;
      end;

    end;
    if MyMemoryStream <> nil then begin
      FreeAndNil(MyMemoryStream)
    end;
  except
    on E: exception do begin
      if MyMemoryStream <> nil then begin
        FreeAndNil(MyMemoryStream)
      end;
      if NSQ_DEBUG then Writeln(E.message, NSQ_CR)
    end;
  end;
  _isTerminated := True;
end;

constructor TNSQReceiverThread.Create(
  InBroadcastAddress: string; InPort: Integer; InTopic: string; InChannel: string);
begin
  inherited Create(True);
  FreeOnTerminate := false;
  _isTerminated := true;
  _tcpClient := nil;
  _nsqIdentify := InitTNSQIdentify;
  _nsqBroadcastAddress := InBroadcastAddress;
  _nsqPort := InPort;
  _nsqTopic := InTopic;
  _nsqChannel := InChannel;
end;

destructor TNSQReceiverThread.Destroy;
begin
  if _tcpClient <> nil then begin
    FreeAndNil(_tcpClient);
  end;
  TerminateTread;
  inherited Destroy;
end;

procedure TNSQReceiverThread.Connect;
begin
  if _tcpClient <> nil then begin
    FreeAndNil(_tcpClient);
  end;
  _tcpClient := TIdTCPClient.Create;

  NSQConnect(_tcpClient, _nsqIdentify, _nsqBroadcastAddress, _nsqPort, _nsqTopic, _nsqChannel);
end;

procedure TNSQReceiverThread.TerminateTread;
begin
  while (_isTerminated = false) do begin
    Terminate;
    sleep(100)
  end;
end;

procedure TNSQReceiverThread.InstallCallback(InNSQCallback: TNSQCallback);
begin
  _nsqCallback := InNSQCallback;
end;

end.


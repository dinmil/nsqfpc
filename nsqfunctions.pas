unit NSQFunctions;

{$mode ObjFPC}{$H+}

interface

uses
  lazutf8, Interfaces, Classes, SysUtils,
  IdTCPClient, IdHTTP,
  fpjson, DateUtils, NSQTypes;

  function NSQConnect(InClient: TIdTCPClient;
                      InIdentify: TNSQIdentify;
                      InHost: string;
                      InPort: Integer;
                      InTopic: string;
                      InChannel: string): boolean;

  function NSQGetFrameType(typeId: Integer): TNSQFrameType;
  function NSQGetHostName: string;
  function FavForJSON(InValue: Integer): string; overload;
  function FavForJSON(InValue: string): string; overload;
  function FavForJSONWithNull(InValue: string): string;
  function FavForJSON(InValue: Boolean): string; overload;

  function NSQGetBigEndianInt(InInteger: integer): integer;
  function NSQIdentifyToJson(var InIdentify: TNSQIdentify): string;

  function NSQCreateMessage(InMemoryStream: TMemoryStream; InString: string): integer;
  function NSQWriteMessage(InMemoryStream: TMemoryStream; InTCPClient: TIdTCPClient): integer;
  function NSQSWriteSubscribeMessage(InTCPClient: TIdTCPClient; InTopic, InChannel: string): integer;
  function NSQWriteMagicMessage(InTCPClient: TIdTCPClient): integer;
  function NSQWriteIdentifyMessage(InTCPClient: TIdTCPClient; var InIdentify: TNSQIdentify): integer;
  function NSQWriteNOPMessage(InTCPClient: TIdTCPClient): integer;
  function NSQWritePUBMessage(InTCPClient: TIdTCPClient; InTopicName, InMessage: string): integer;
  function NSQWriteMPUBMessage(InTCPClient: TIdTCPClient; InTopicName: string; InMessages: array of string): integer;
  function NSQWriteDPUBMessage(InTCPClient: TIdTCPClient; InTopicName: string; InDeferTime: Int32; InMessage: string): integer;
  function NSQWriteRDYMessage(InTCPClient: TIdTCPClient; InCount: Integer): integer;
  function NSQWriteFINMessage(InTCPClient: TIdTCPClient; InMessageId: string): integer;
  function NSQWriteREQMessage(InTCPClient: TIdTCPClient; InMessageId: string; InTimeout: Integer): integer;
  function NSQWriteTOUCHMessage(InTCPClient: TIdTCPClient; InMessageId: string): integer;
  function NSQWriteCLSMessage(InTCPClient: TIdTCPClient): integer;

  function NSQReadMessage(MyInStream: TMemoryStream; IdTCPClient1: TIdTCPClient;
    var OutNanoSecondTimestamp: int64; var OutAttempts: integer;
    var OutMessageID: string; var OutBody: string): Integer;

  procedure NSQProtoStream(InStream: TMemoryStream);
  procedure NSQReadStream(InStream: TMemoryStream; var OutType: int32; var OutString: string);


  function NSQGetTopicData(InNSQLookupUrl: string; InTopicName: string): string;


  function FavEpochToDateTime(InEpoch: Int64): TDateTime;
  function FavDateTimeToEpoch(InDateTime: TDateTime; InWithMilliSec: Boolean): Int64;

implementation


function NSQGetTopicData(InNSQLookupUrl: string; InTopicName: string): string;
var MyHttp: TIdHTTP;
    MyResponse: string;
begin
  MyHttp := nil;
  MyResponse := '';

  try
    MyHttp := TIdHTTP.Create;
    MyHttp.Request.ContentType := 'text/json';
    MyHttp.Request.CharSet := 'utf-8';
    MyHttp.Request.ContentEncoding := 'utf-8';
    MyHttp.Request.Accept := 'text/json';
    // MyHttp.Request.AcceptEncoding := 'utf-8, gzip';

    MyResponse := MyHttp.Get(Format(InNSQLookupUrl + '/lookup?topic=%s', [InTopicName]));

    if MyHttp <> nil then FreeAndNil(MyHttp);
  except
    on E: Exception do begin
      if MyHttp <> nil then FreeAndNil(MyHttp);
      raise
    end;
  end;
  Result := MyResponse;
end;

function FavEpochToDateTime(InEpoch: Int64): TDateTime;
var MyResult: TDateTime;
    MyMilliSec: Integer;
begin
  MyResult := 0;
  try
    if InEpoch <> 0 then begin
      MyResult := UnixToDateTime(InEpoch);

      // if time is toob big that means InEpoch contains milliseconds
      // so devide with 1000 to get seconds and
      // calculate millisecond
      if MyResult > 200000 then begin
        MyMilliSec := InEpoch mod 1000;
        InEpoch := InEpoch div 1000;
        MyResult := UnixToDateTime(InEpoch) + (MyMilliSec * OneMillisecond);
      end
      else begin
        if MyResult < -200000 then begin
          MyMilliSec := InEpoch mod 1000;
          InEpoch := InEpoch div 1000;
          MyResult := UnixToDateTime(InEpoch) - (MyMilliSec * OneMillisecond);
        end
      end;
    end;
  except
    MyResult := 0;
  end;
  Result := MyResult;
end;

function FavDateTimeToEpoch(InDateTime: TDateTime; InWithMilliSec: Boolean
  ): Int64;
var MyResult: Int64;
begin
  MyResult := DateTimeToUnix(InDateTime);
  if InWithMilliSec then begin
    if MyResult >= 0 then begin
      MyResult := (MyResult * 1000) + StrToInt(FormatDateTime('zzz', InDateTime));
    end
    else begin
      MyResult := (MyResult * 1000) - StrToInt(FormatDateTime('zzz', InDateTime));
    end;
  end;
  Result := MyResult;
end;

procedure NSQProtoStream(InStream: TMemoryStream);
var F: integer;
    MyByte: Byte;
begin
  InStream.Position := 0;
  for F := 0 to InStream.Size-1 do begin
    InStream.read(MyByte, 1);
    Write(Format('%2.2X', [MyByte]));
  end;
  InStream.Position := 0;
  Writeln;

  InStream.Position := 0;
  for F := 0 to InStream.Size-1 do begin
    if F > 3 then begin
      InStream.read(MyByte, 1);
      Write(Format('%s', [Char(MyByte)]));
    end;
  end;
  Writeln;
  InStream.Position := 0;
end;

procedure NSQReadStream(InStream: TMemoryStream; var OutType: Int32; var OutString: string);
var F: integer;
    MyByte: Byte;
    MyByteArray: array [0..3] of byte;
begin
  OutType := -1;
  OutString := '';
  FillByte(MyByteArray, SizeOf(MyByteArray), 0);

  InStream.Position := 0;
  for F := 0 to InStream.Size-1 do begin
    InStream.read(MyByte, 1);
    if F > 3 then begin
      OutString := OutString + Char(MyByte);
    end
    else begin
      MyByteArray[F] := MyByte;
      if F = 3 then begin
        Move(MyByteArray, OutType, SizeOf(MyByteArray));
      end;
    end;
  end;
  InStream.Position := 0;
end;

function FavForJSON(InValue: Integer): string;
begin
  Result := Format('%d', [InValue]);
end;

function FavForJSON(InValue: string): string;
begin
  Result := '"' + StringToJSONString(InValue) + '"'
end;

function FavForJSONWithNull(InValue: string): string;
var MyResult: string;
begin
  // this function returns null or string encloed in "string"
  MyResult := FavForJSON(InValue);
  if MyResult = '' then MyResult := 'null'
  else MyResult := '"' + MyResult + '"';
  Result := MyResult;
end;

function FavForJSON(InValue: Boolean): string;
begin
  if InValue then Result := 'true'
  else Result := 'false';
end;

function NSQGetBigEndianInt(InInteger: integer): integer;
begin
  Result := NtoBE(InInteger)
end;

function NSQConnect(InClient: TIdTCPClient; InIdentify: TNSQIdentify;
  InHost: string; InPort: Integer; InTopic: string; InChannel: string): boolean;
var MyResult: Boolean;
begin
  MyResult := False;
  try
    if InClient = nil then begin
      raise Exception.Create('TCP client object is nil');
    end;
    if InHost = '' then begin
      raise Exception.Create('Host is empty')
    end;


    InClient.Host := InHost;
    InClient.Port := InPort;
    try
      InClient.Connect;
      while InClient.Connected = false do begin
        sleep(100);
      end;
      if InClient.Connected then begin;
        if NSQ_DEBUG then begin
          NSQWrite('Port: %s', [InClient.BoundPort.ToString]);
        end;
      end;
    except
      on E: Exception do begin
        NSQWrite('%s', [E.Message]);
        raise E;
      end;
    end;

    NSQWriteMagicMessage(InClient);
    NSQWriteIdentifyMessage(InClient, InIdentify);

    NSQSWriteSubscribeMessage(InClient, InTopic, InChannel);
    NSQWriteRDYMessage(InClient, 10);

    MyResult := True;
  except
    on E: exception do begin
      raise
    end;
  end;

  Result := MyResult;
end;

function NSQGetFrameType(typeId: Integer): TNSQFrameType;
begin
  case (typeId) of
    0: result := TNSQFrameType.NSQ_FRAMETYPERESPONSE;
    1: result := TNSQFrameType.NSQ_FRAMETYPEERROR;
    2: result := TNSQFrameType.NSQ_FRAMETYPEMESSAGE;
    else result := TNSQFrameType.NSQ_FRAMETYPEUNKNOWN;
  end;
end;

function NSQGetHostName: string;
begin
  Result := 'Dinko'
end;

function NSQIdentifyToJson(var InIdentify: TNSQIdentify): string;
begin
  Result := Format(
                '{' +
                '"client_id": %s,' +
                '"deflate": %s,' +
                '"deflate_level": %s,' +
                '"feature_negotiation": %s,' +
                '"heartbeat_interval": %s,' +
                '"hostname": %s,' +
                '"long_id": %s,' +
                '"msg_timeout": %s,' +
                '"output_buffer_size": %s,' +
                '"output_buffer_timeout": %s,' +
                '"sample_rate": %s,' +
                '"short_id": %s,' +
                '"snappy": %s,' +
                '"tls_v1": %s,' +
                '"user_agent": %s' +
                '}',
                [
                  FavForJSON(InIdentify.client_id),
                  FavForJSON(InIdentify.deflate),
                  FavForJSON(InIdentify.deflate_level),
                  FavForJSON(InIdentify.feature_negotiation),
                  FavForJSON(InIdentify.heartbeat_interval),
                  FavForJSON(InIdentify.hostname),
                  FavForJSON(InIdentify.long_id),
                  FavForJSON(InIdentify.msg_timeout),
                  FavForJSON(InIdentify.output_buffer_size),
                  FavForJSON(InIdentify.output_buffer_timeout),
                  FavForJSON(InIdentify.sample_rate),
                  FavForJSON(InIdentify.short_id),
                  FavForJSON(InIdentify.snappy),
                  FavForJSON(InIdentify.tls_v1),
                  FavForJSON(InIdentify.user_agent)
                ]
              );


  {"client_id":"LAPTOP-FV2TOBTH","deflate":false,"deflate_level":6,"feature_negotiation":true,
      "heartbeat_interval":30000,"hostname":"LAPTOP-FV2TOBTH","long_id":"LAPTOP-FV2TOBTH","msg_timeout":0,
      "output_buffer_size":16384,"output_buffer_timeout":250,"sample_rate":0,"short_id":"LAPTOP-FV2TOBTH",
      "snappy":false,"tls_v1":false,"user_agent":"go-nsq/1.0.7"}
end;

function NSQCreateMessage(InMemoryStream: TMemoryStream; InString: string
  ): integer;
var MyLen: Int32;
    MyResult: Integer;
begin
  MyResult := 0;
  try
    InMemoryStream.Clear;
    MyLen := Length(InString);
    MyLen := NtoBE(MyLen); // natural to big endian - big endian is network way
    MyResult := MyResult + InMemoryStream.Write(MyLen, SizeOf(MyLen));
    MyResult := MyResult + InMemoryStream.Write(@InString[1], Length(InString));
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  Result := MyResult;
end;

function NSQWriteMessage(InMemoryStream: TMemoryStream;
  InTCPClient: TIdTCPClient): integer;
var MyResult: Integer;
begin
  MyResult := 0;
  try
    InMemoryStream.Position := 0;
    InTCPClient.IOHandler.Write(InMemoryStream, InMemoryStream.Size);
    MyResult := InMemoryStream.Size;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  Result := MyResult;
end;

function NSQSWriteSubscribeMessage(InTCPClient: TIdTCPClient; InTopic,
  InChannel: string): integer;
var MyString: string;
    MyResult: Integer;
begin
  MyResult := 0;
  // SUB <topic_name> <channel_name>\n
  try
    MyString := Format('SUB %s %s%s', [InTopic, InChannel, #10]);
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));

    if NSQ_DEBUG then begin
      NSQWrite('SUB: "%s"', [MyString])
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  Result := MyResult;
end;

function NSQWriteMagicMessage(InTCPClient: TIdTCPClient): Integer;
var MyResult: Integer;
begin
  MyResult := 0;
  // V2 (4-byte ASCII [space][space][V][2]) a push based streaming protocol
  // for consuming (and request/response for publishing)
  try
    InTCPClient.IOHandler.Write(NSQ_MAGIC_V2);
    if NSQ_DEBUG then begin
      NSQWrite('MAGIC: %s', [NSQ_MAGIC_V2])
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise;
    end;
  end;
  Result := MyResult;
end;

function NSQWriteIdentifyMessage(InTCPClient: TIdTCPClient;
  var InIdentify: TNSQIdentify): Integer;
var MyString: string;
    MySize: Int32;
    MyResult: Integer;
begin
  MyResult := 0;
  // IDENTIFY\n
  // [ 4-byte size in bytes ][ N-byte JSON data ]
  try
    MyString := NSQIdentifyToJson(InIdentify);
    InTCPClient.IOHandler.Write('IDENTIFY');
    InTCPClient.IOHandler.Write(NSQ_NL);

    MySize := Length(MyString);
    if NSQ_DEBUG then begin
      NSQWrite('IDENTIFY My Size: %d', [MySize]);
    end;

    InTCPClient.IOHandler.Write(MySize);  // Indy automatically swap bytes to newtwork bigendian format
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));

    if NSQ_DEBUG then begin
      NSQWrite('IDENTIFY: %s', [MyString]);
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise;
    end;
  end;

  Result := MyResult;
end;

function NSQWriteNOPMessage(InTCPClient: TIdTCPClient): integer;
var MyString: string;
    MyResult: Integer;
begin
  MyResult := 0;
  // NOP\n
  try
    MyString := 'NOP';
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));
    InTCPClient.IOHandler.Write(NSQ_NL);

    if NSQ_DEBUG then begin
      NSQWrite('NOP: %s', [MyString]);
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  Result := MyResult;
end;

function NSQWritePUBMessage(InTCPClient: TIdTCPClient; InTopicName,
  InMessage: string): integer;
var MyString: string;
    MySize: Int32;
    MyResult: Integer;
begin
  MyResult := 0;
  // PUB <topic_name>\n
  try
    MyString := Format('PUB %s%s', [InTopicName, string(#10)]);
    if NSQ_DEBUG then begin
      NSQWrite('PUB: %s', [MyString]);
    end;
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));

    MySize := Length(InMessage);
    if NSQ_DEBUG then begin
      NSQWrite('PUB: MySize: %d', [MySize]);
    end;

    InTCPClient.IOHandler.Write(MySize);
    InTCPClient.IOHandler.Write(@InMessage[1], Length(InMessage));

    if NSQ_DEBUG then begin
      NSQWrite('PUB: %s; "%s"', [InTopicName, InMessage]);
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  Result := MyResult;
end;

function NSQWriteMPUBMessage(InTCPClient: TIdTCPClient; InTopicName: string;
  InMessages: array of string): integer;
var MyString: string;
    MySize: Int32;
    F: Integer;
    MyBodySize: Int32;
    MyMessageCount: Int32;
    MyResult: Integer;
begin
  // MPUB <topic_name>\n
  // [ 4-byte body size ]
  // [ 4-byte num messages ]
  // [ 4-byte message #1 size ][ N-byte binary data ]
  //       ... (repeated <num_messages> times)
  // <topic_name> - a valid string (optionally having #ephemeral suffix)

  MyResult := 0;
  try
    MyMessageCount := Length(InMessages);
    MyBodySize := 0;
    for F := 0 to Length(InMessages)-1 do begin
      MyBodySize := MyBodySize + Length(InMessages[F]);
    end;

    MyString := Format('MPUB %s%s', [InTopicName, string(#10)]);
    if NSQ_DEBUG then begin
      NSQWrite('MPUB: %s', [MyString]);
    end;
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));

    if NSQ_DEBUG then begin
      NSQWrite('MPUB: MyBodySize: %d', [MyBodySize]);
    end;
    InTCPClient.IOHandler.Write(MyBodySize);

    if NSQ_DEBUG then begin
      NSQWrite('MPUB: MyMessageCount: %d', [MyMessageCount]);
    end;
    InTCPClient.IOHandler.Write(MyMessageCount);


    for F := 0 to Length(InMessages)-1 do begin
      MySize := Length(InMessages[F]);
      if NSQ_DEBUG then Writeln('MPUB: MySize: ', MySize, NSQ_CR);
      InTCPClient.IOHandler.Write(MySize);
      InTCPClient.IOHandler.Write(@InMessages[F][1], Length(InMessages[F]));
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;

  Result := MyResult;
end;

function NSQWriteDPUBMessage(InTCPClient: TIdTCPClient; InTopicName: string; InDeferTime: Int32;
  InMessage: string): integer;
var MyString: string;
    MySize: Int32;
    MyResult: Integer;
begin
  //  DPUB <topic_name> <defer_time>\n
  //  [ 4-byte size in bytes ][ N-byte binary data ]
  //<topic_name> - a valid string (optionally having #ephemeral suffix)
  //<defer_time> - a string representation of integer D which defines the time for how long to defer where 0 <= D < max-requeue-timeout
  MyResult := 0;
  try
    MyString := Format('DPUB %s %d%s', [InTopicName, InDeferTime, string(#10)]);
    if NSQ_DEBUG then begin
      NSQWrite('DPUB: %s', [MyString]);
    end;
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));

    MySize := Length(InMessage);
    if NSQ_DEBUG then begin
      NSQWrite('DPUB: MySize: %d', [MySize]);
    end;

    InTCPClient.IOHandler.Write(MySize);
    InTCPClient.IOHandler.Write(@InMessage[1], Length(InMessage));

    if NSQ_DEBUG then begin
      NSQWrite('DPUB: %s; %s', [InTopicName, InMessage]);
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;

  Result := MyResult;
end;

function NSQWriteRDYMessage(InTCPClient: TIdTCPClient; InCount: Integer
  ): integer;
var MyString: string;
    MyResult: Integer;
begin
  //RDY <count>\n
  //Update RDY state (indicate you are ready to receive N messages)
  MyResult := 0;
  try
    MyString := Format('RDY %d%s', [InCount, string(#10)]);
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));

    if NSQ_DEBUG then begin
      NSQWrite('RDY: %s', [MyString]);
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;

  Result := MyResult;
end;

function NSQWriteFINMessage(InTCPClient: TIdTCPClient; InMessageId: string
  ): integer;
var MyString: string;
    MyResult: Integer;
begin
  //FIN <message_id>\n
  //Finish a message (indicate successful processing)
  MyResult := 0;
  try
    MyString := Format('FIN %s%s', [InMessageID, string(#10)]);
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));

    if NSQ_DEBUG then begin
      NSQWrite('FIN: %s', [MyString])
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  Result := MyResult;
end;

function NSQWriteREQMessage(InTCPClient: TIdTCPClient; InMessageId: string;
  InTimeout: Integer): Integer;
var MyString: string;
    MyResult: Integer;
begin
  //REQ <message_id><timeout>\n
  //Re-queue a message (indicate failure to process)
  MyResult := 0;
  try
    MyString := Format('REQ %16.16s %d%s', [InMessageID, InTimeout, string(#10)]);
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  if NSQ_DEBUG then begin
    NSQWrite('REQ: %s', [MyString]);
  end;
  Result := MyResult;
end;

function NSQWriteTOUCHMessage(InTCPClient: TIdTCPClient; InMessageId: string
  ): integer;
var MyString: string;
    MyResult: Integer;
begin
  //TOUCH <message_id><timeout>\n
  // Reset the timeout for an in-flight message
  MyResult := 0;
  try
    MyString := Format('TOUCH %16.16s%s', [InMessageID, string(#10)]);
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  if NSQ_DEBUG then begin
    NSQWrite('TOUCH: %s', [MyString]);
  end;
  Result := MyResult;
end;

function NSQWriteCLSMessage(InTCPClient: TIdTCPClient): integer;
var MyString: string;
    MyResult: Integer;
begin
  //CLS\n
  // Cleanly close your connection (no more messages are sent)
  MyResult := 0;
  try
    MyString := Format('CLS%s', [string(#10)]);
    InTCPClient.IOHandler.Write(@MyString[1], Length(MyString));
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  if NSQ_DEBUG then begin
    NSQWrite('CLS: %s', [MyString]);
  end;
  Result := MyResult;
end;

function NSQReadMessage(MyInStream: TMemoryStream; IdTCPClient1: TIdTCPClient;
  var OutNanoSecondTimestamp: int64; var OutAttempts: integer;
  var OutMessageID: string; var OutBody: string): Integer;
var F: integer;
    MyTimestamp: Int64;   // message timestamp from server in nanoseconds
    MyAttempts: Int16;    // how many OutAttempts to deliver message to client
    MyMessageID: array[0..15] of char;  // message id in hex format
    MyFrameType: Int32;   // frame type - usually 2 as indicator that this is normal message with OutBody
    MyResult: Integer;
begin
  // DecodeMessage deserializes data (as []byte) and creates a new Message
  // message format:
  //  [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
  //  |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
  //  |       8-byte         ||    ||                 16-byte                      || N-byte
  //  ------------------------------------------------------------------------------------------...
  //    nanosecond timestamp    ^^                   message ID                       message OutBody
  //                         (uint16)
  //                          2-byte
  //                         OutAttempts
  //                  00 00 00 23 00 00 00 02 15 56 EA 20 C3 34 07 58
  //00 01 30 61 35 36 38 35 34 32 37 38 63 63 32 30
  //30 30 45 64 69 74 31

  MyResult := 0;
  OutNanoSecondTimestamp := 0;
  OutAttempts := 0;
  OutMessageID := '';
  OutBody := '';

  try
    //OutNanoSecondTimestamp 8byte
    MyInStream.Position := 0;
    if NSQ_DEBUG then begin
      NSQWrite('READ: SizeOfMessage: %d', [MyInStream.Size]);
    end;

    MyInStream.ReadBuffer(MyFrameType, 4);
    MyFrameType := BEtoN(MyFrameType);
    if NSQ_DEBUG then begin
      NSQWrite('READ: MyFrameType: %d', [MyFrameType]);
    end;
    MyFrameType := BEtoN(MyFrameType);

    MyInStream.ReadBuffer(MyTimestamp, 8);
    MyTimeStamp := BEtoN(MyTimestamp);
    if NSQ_DEBUG then begin
      NSQWrite('READ: MyTimeStamp: %d', [MyTimestamp]);
      NSQWrite('READ: MyTimeStamp: %s', [FormatDateTime('yyyy-mm-dd nn:hh:ss.zzz', FavEpochToDateTime(MyTimestamp div 1000000))]);
    end;
    OutNanoSecondTimestamp := MyTimestamp;

    //OutAttempts 2byte
    MyInStream.ReadBuffer(MyAttempts, 2);
    MyAttempts := BEtoN(MyAttempts);
    if NSQ_DEBUG then begin
      NSQWrite('READ: Attempts: %d', [MyAttempts]);
    end;
    OutAttempts := MyAttempts;


    //OutMessageID 16 byte
    FillChar(MyMessageID, 16, 0);
    MyInStream.ReadBuffer(MyMessageID, 16);

    OutMessageID := '';
    for F := 0 to 15 do
    begin
      OutMessageID := OutMessageID + MyMessageID[F];
    end;
    if NSQ_DEBUG then begin
      NSQWrite('READ: MessageID: %s', [string(MyMessageID)]);
    end;

    SetLength(OutBody,MyInStream.Size - 8 - 2 - 16);
    MyInStream.Read(OutBody[1], MyInStream.Size - 8 - 2 - 16);
    if NSQ_DEBUG then begin
      NSQWrite('READ: MsgBody: %s', [OutBody]);
    end;
  except
    on E: Exception do begin
      MyResult := -1;
      if NSQ_DEBUG then begin
        NSQWrite('%s', [E.Message]);
      end;
      raise
    end;
  end;
  Result := MyResult;
end;

end.

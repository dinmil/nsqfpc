unit NSQLookup;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils,
  fpjson, jsonparser,
  IdTCPClient,
  NSQTypes, NSQFunctions,
  NSQReceiver;


type
  { TClsNSQChannel }

  TClsNSQChannel = class (TNSQCls)
    _data: TNSQChannel;
    _lastCompareResult: boolean;
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(InCls: TClsNSQChannel);
    function Compare(InCls: TClsNSQChannel): boolean;
  end;

  { TClsNSQChannels }

  TClsNSQChannels = class(TNSQClss)
    constructor Create; override;
    destructor Destroy; override;
    function GetItem(InIndex: Integer): TClsNSQChannel;
    function AddItem(var InData: TNSQChannel): TClsNSQChannel;
    procedure ParseJSONResponse(InJSON: TJSONData);
    function Compare(InCls: TClsNSQChannels): boolean;
  end;


  { TClsNSQProducer }

  TClsNSQProducer = class (TNSQCls)
    _data: TNSQProducer;
    _lastCompareResult: boolean;
    _action: string; // shoud I remove or add or '' as nothing is changed
    _receiver: TNSQReceiverThread;
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(InCls: TClsNSQProducer);
    function Compare(InCls: TClsNSQProducer): boolean;
    function CreateReceivers(InTopicName, InChannel: string): TNSQReceiverThread;
  end;

  { TClsNSQProducers }

  TClsNSQProducers = class(TNSQClss)
    constructor Create; override;
    destructor Destroy; override;
    function GetItem(InIndex: Integer): TClsNSQProducer;
    function AddItem(var InData: TNSQProducer): TClsNSQProducer;
    procedure ParseJSONResponse(InJSON: TJSONData);
    function Compare(InCls: TClsNSQProducers): boolean;
  end;

  TNSQCallbackProducerChanged = function (InAction: string; InProducer: TClsNSQProducer): string;

  { TClsNSQTopic }

  TClsNSQTopic = class (TNSQCls)
    _nsqLookupUrl: string;
    _nsqTopicName: string;
    _nsqChannelName: string;
    _channels: TClsNSQChannels;
    _producers: TClsNSQProducers;
    _CallbackProducerChanged: TNSQCallbackProducerChanged;
    _CallbackData: TNSQCallback;
    constructor Create; override;
    destructor Destroy; override;
    procedure ClearAll;
    procedure GetTopicData(InNSQLookupUrl: string; InTopicName: string; InChannelName: String);
    procedure ParseJSONResponse(InJSON: string);

    function CompareData(InNSQTopic: TClsNSQTopic): boolean;
    procedure FindProducerDifferences(InNSQTopic: TClsNSQTopic);
    procedure InstallProcedureChangedCallback(InCallback: TNSQCallbackProducerChanged);
    procedure InstallDataCallback(InCallback: TNSQCallback);
    function GetTcpClient: TIdTCPClient;
    procedure TerminateThreads;
  end;

  { TNSQLookupThread }

  TNSQLookupThread = class(TThread)
  protected
    procedure Execute; override;
  public
    _isTerminated: boolean;
    _nsqLookupUrl: string;
    _nsqTopicName: string;
    _nsqChannelName: string;
    _nsqPoolInterval: Integer;
    _nsqTopic: TClsNSQTopic;
    _lastErrorMessage: string;
    _lastErrorTimestamp: TDateTime;
    constructor Create(InNSQLookupUrl: string;
                       InNSQTopicName: string;
                       InNSQChannelName: string;
                       InNSQPoolInterval: Integer
                    );
    destructor Destroy; override;
    function CheckTopicData: boolean;
    procedure TerminateThread;
    procedure InstallProcedureChangedCallback(InCallback: TNSQCallbackProducerChanged);
    procedure InstallDataCallback(InCallback: TNSQCallback);
  end;


implementation

{ TNSQLookupThread }

procedure TNSQLookupThread.Execute;
var MyCount: Integer;
    MyCheckTopicData: Boolean;
begin
  MyCount := 0;
  _isTerminated := False;
  while not Terminated do begin
    Inc(MyCount);
    if ((MyCount mod _nsqPoolInterval) = 0) then begin
      MyCheckTopicData := CheckTopicData;
      if MyCheckTopicData = false then begin
        // something went wrong
      end;
    end;
    Sleep(1000);
  end;
  _isTerminated := True;
end;

constructor TNSQLookupThread.Create(InNSQLookupUrl: string;
  InNSQTopicName: string; InNSQChannelName: string; InNSQPoolInterval: Integer);
begin
  inherited Create(True);
  FreeOnTerminate := false;
  _isTerminated := true;
  _nsqLookupUrl := InNSQLookupUrl;
  _nsqTopicName := InNSQTopicName;
  _nsqChannelName := InNSQChannelName;

  _nsqTopic := TClsNSQTopic.Create;
  _nsqTopic._nsqLookupUrl := InNSQLookupUrl;
  _nsqTopic._nsqTopicName := InNSQTopicName;
  _nsqTopic._nsqChannelName := InNSQChannelName;

  _nsqPoolInterval := InNSQPoolInterval;
  _lastErrorMessage := '';
  _lastErrorTimestamp := 0;
end;

destructor TNSQLookupThread.Destroy;
begin
  inherited Destroy;
  TerminateThread;
  FreeAndNil(_nsqTopic);
end;

function TNSQLookupThread.CheckTopicData: boolean;
var MyTopic: TClsNSQTopic;
    MyResult: Boolean;
begin
  MyTopic := nil;
  MyResult := false;
  try
    MyTopic := TClsNSQTopic.Create;
    MyTopic.GetTopicData(_nsqLookupUrl, _nsqTopicName, _nsqChannelName);
    MyResult := _nsqTopic.CompareData(MyTopic);  // return false if something is changed
    if MyResult = false then begin
      _nsqTopic.FindProducerDifferences(MyTopic);
    end;
    if MyTopic <> nil then FreeAndNil(MyTopic)
  except
    on E: Exception do begin
      _lastErrorMessage := E.Message;
      _lastErrorTimestamp := NowUTC;
      if MyTopic <> nil then FreeAndNil(MyTopic);
      // do not raise exception because this is called from thread
      if NSQ_DEBUG then begin
        Writeln('CheckTopicData Error: ', E.Message, NSQ_CR);
      end;
    end;
  end;
  Result := MyResult;
end;

procedure TNSQLookupThread.TerminateThread;
begin
  _nsqTopic.TerminateThreads;
  while (_isTerminated = false) do begin
    Terminate;
    Sleep(100);
  end;
end;

procedure TNSQLookupThread.InstallProcedureChangedCallback(
  InCallback: TNSQCallbackProducerChanged);
begin
  if _nsqTopic <> nil then begin
    _nsqTopic.InstallProcedureChangedCallback(InCallback);
  end;
end;

procedure TNSQLookupThread.InstallDataCallback(InCallback: TNSQCallback);
begin
  if _nsqTopic <> nil then begin
    _nsqTopic.InstallDataCallback(InCallback);
  end;

end;

constructor TClsNSQTopic.Create;
begin
  inherited Create;
  _channels := TClsNSQChannels.Create;
  _producers := TClsNSQProducers.Create;
end;

destructor TClsNSQTopic.Destroy;
begin
  FreeAndNil(_channels);
  FreeAndNil(_producers);
  inherited Destroy;
end;

procedure TClsNSQTopic.ClearAll;
begin
  _channels.DeleteAllItems;
  _producers.DeleteAllItems;
end;

procedure TClsNSQTopic.GetTopicData(InNSQLookupUrl: string;
  InTopicName: string; InChannelName: String);
var MyResponse: string;
begin
  if NSQ_DEBUG then begin
    Writeln('GetTopicData: ', InNSQLookupUrl, ': ', InTopicName, NSQ_CR)
  end;
  MyResponse := NSQGetTopicData(InNSQLookupUrl, InTopicName);
  if NSQ_DEBUG then begin
    Writeln(MyResponse, NSQ_CR)
  end;
  ParseJSONResponse(MyResponse);
  _nsqLookupUrl := InNSQLookupUrl;
  _nsqTopicName := InTopicName;
  _nsqChannelName := InChannelName;
end;

procedure TClsNSQTopic.ParseJSONResponse(InJSON: string);
var MyJSONData: TJSONData;
    MyJSONItem: TJSONData;
begin
  MyJSONData := nil;
  MyJSONItem := nil;

  ClearAll;

  try
    (*
    InJson :=
    '{' + #13#10 +
    ' "channels": [' + #13#10 +
    '    "bonus",' + #13#10 +
    '    "slips-dbi",' + #13#10 +
    '    "web_app_api-dev01",' + #13#10 +
    '    "nsq_to_mongo"' + #13#10 +
    '    ],' + #13#10 +
    ' "producers": [{' + #13#10 +
    '    "remote_address": "127.0.0.1:50353",' + #13#10 +
    '    "hostname": "MacBook-Pro-od-Dinko.local",' + #13#10 +
    '    "broadcast_address": "127.0.0.1",' + #13#10 +
    '    "tcp_port": 4150,' + #13#10 +
    '    "http_port": 4151,' + #13#10 +
    '    "version": "1.2.1"' + #13#10 +
    '    }]' + #13#10 +
    '}';
    *)

    MyJSONData := GetJSON(InJSON);

    try
      MyJSONItem := MyJSONData.FindPath('channels');
      if MyJSONItem <> nil then begin
        _channels.ParseJSONResponse(MyJSONItem);
      end;

      MyJSONItem := MyJSONData.FindPath('producers');
      if MyJSONItem <> nil then begin
        _producers.ParseJSONResponse(MyJSONItem);
      end;
    except
      on E: Exception do begin
        raise
      end;
    end;

    if MyJSONData <> nil then FreeAndNil(MyJSONData);

  except
    on E: Exception do begin
      // MyError := E.Message;
      if MyJSONData <> nil then FreeAndNil(MyJSONData);
      raise;
    end;
  end;
end;

function TClsNSQTopic.CompareData(InNSQTopic: TClsNSQTopic): boolean;
var MyResult: boolean;
begin
  MyResult := _producers.Compare(InNSQTopic._producers);
  MyResult := MyResult and _channels.Compare(InNSQTopic._channels);
  Result := MyResult;
end;

procedure TClsNSQTopic.FindProducerDifferences(InNSQTopic: TClsNSQTopic);
var F, G: Integer;
    MyProducer1, MyProducer2: TClsNSQProducer;
    MyFound: Boolean;
begin
  // this part loops through current producers and mark every producer that do not exists
  // any more as action = remove
  for F := 0 to _producers.GetCount-1 do begin
    MyFound := False;
    MyProducer1 := _producers.GetItem(F);
    MyProducer1._action := ''; // nothing is changed
    for G := 0 to InNSQTopic._producers.GetCount-1 do begin
      MyProducer2 := InNSQTopic._producers.GetItem(G);
      if (MyProducer1._data.broadcast_address = MyProducer2._data.broadcast_address) and
         (MyProducer1._data.tcp_port = MyProducer2._data.tcp_port) then
      begin
        MyFound := True;
      end;
      if MyFound then Break;
    end;
    if MyFound then MyProducer1._action := ''
    else MyProducer1._action := 'remove';
  end;

  // this part loops through current producers and add new producer if
  // action = add
  for F := 0 to InNSQTopic._producers.GetCount-1 do begin
    MyFound := False;
    MyProducer1 := InNSQTopic._producers.GetItem(F);
    MyProducer1._action := ''; // nothing is changed
    for G := 0 to _producers.GetCount-1 do begin
      MyProducer2 := _producers.GetItem(G);
      if (MyProducer1._data.broadcast_address = MyProducer2._data.broadcast_address) and
         (MyProducer1._data.tcp_port = MyProducer2._data.tcp_port) then
      begin
        MyFound := True;
      end;
      if MyFound then Break;
    end;
    if MyFound = false then begin
      _producers.AddItem(MyProducer1._data)._action := 'add';
    end;
  end;

  // notify all with callback that something is changed and what action is needed
  F := 0;
  while F < _producers.GetCount do begin
    MyProducer1 := _producers.GetItem(F);
    if _CallbackProducerChanged <> nil then begin
      // if Callback is defined then nofity my
      _CallbackProducerChanged(MyProducer1._action, MyProducer1)
    end;

    if MyProducer1._action = 'remove' then begin
      // this will stop receiver and free and remove item from list
      _producers.DeleteItem(F);
      MyProducer1 := nil;
    end
    else if MyProducer1._action = 'add' then begin
      // start new thread to fetch data
      F := F + 1;
      MyProducer1._action := '';
      MyProducer1.CreateReceivers(_nsqTopicName, _nsqChannelName);
      MyProducer1._receiver.InstallCallback(_CallbackData);
      MyProducer1._receiver.Start;
    end
    else begin
      // do nothing
      F := F + 1;
      MyProducer1._action := '';  // item is checked and it not OK
    end;
  end;
end;

procedure TClsNSQTopic.InstallProcedureChangedCallback(
  InCallback: TNSQCallbackProducerChanged);
begin
  _CallbackProducerChanged := InCallback;
end;

procedure TClsNSQTopic.InstallDataCallback(InCallback: TNSQCallback);
begin
  _CallbackData := InCallback;
end;

function TClsNSQTopic.GetTcpClient: TIdTCPClient;
var F: Integer;
    MyProducer: TClsNSQProducer;
    MyResult: TIdTCPClient;
begin
  MyResult := nil;
  for F := 0 to _producers.GetCount-1 do begin
    MyProducer := _producers.GetItem(F);
    if MyProducer._receiver <> nil then begin
      if MyProducer._receiver._tcpClient <> nil then begin
        if MyProducer._receiver._tcpClient.Connected = true then begin
          MyResult := MyProducer._receiver._tcpClient;
          Break;
        end;
      end;
    end;
  end;
  Result := MyResult;
end;

procedure TClsNSQTopic.TerminateThreads;
var F: Integer;
begin
  for F := 0 to _producers.GetCount-1 do begin
    if _producers.GetItem(F)._receiver <> nil then begin
      _producers.GetItem(F)._receiver.TerminateTread;
    end;
  end;
end;

{ TClsNSQProducers }

constructor TClsNSQProducers.Create;
begin
  inherited Create;
end;

destructor TClsNSQProducers.Destroy;
begin
  inherited Destroy;
end;

function TClsNSQProducers.GetItem(InIndex: Integer): TClsNSQProducer;
var MyResult: TNSQCls;
begin
  MyResult := inherited GetItem(InIndex);
  if MyResult <> nil then begin
    Result := TClsNSQProducer(MyResult);
  end
  else begin
    Result := nil;
  end;
end;

function TClsNSQProducers.AddItem(var InData: TNSQProducer): TClsNSQProducer;
var MyCls: TClsNSQProducer;
begin
  MyCls := TClsNSQProducer.Create;
  MyCls._data := InData;
  _list.Add(MyCls);
  Result := MyCls;
end;

procedure TClsNSQProducers.ParseJSONResponse(InJSON: TJSONData);
var MyJSONArray: TJSONArray;

    MyItem, MyItem1: TJSONData;
    MyJSONObject: TJSONObject;
    MyObjectName: string;

    MyData: TNSQProducer;
    F, G: Integer;
begin
  MyJSONArray := nil;

(*
{
 "channels": [
    "bonus",
    "slips-dbi",
    "web_app_api-dev01",
    "nsq_to_mongo"
    ],
 "producers": [{
    "remote_address": "127.0.0.1:50353",
    "hostname": "MacBook-Pro-od-Dinko.local",
    "broadcast_address": "127.0.0.1",
    "tcp_port": 4150,
    "http_port": 4151,
    "version": "1.2.1"
    }]
}
*)

  try
    if InJSON <> nil then begin
      if (InJSON is TJSONArray) then begin
        MyJSONArray := TJSONArray(InJSON);
        for F := 0 to MyJSONArray.Count-1 do begin
          MyData := InitTNSQProducer;
          MyItem := MyJSONArray.Items[F];
          for G := 0 to MyItem.Count-1 do begin
            MyItem1 := MyItem.Items[G];
            MyJSONObject := TJSONObject(MyItem1);
            MyObjectName := TJSONObject(MyItem).Names[G];
            if MyJSONObject.IsNull = false then begin
              if MyObjectName = 'remote_address' then MyData.remote_address := MyJSONObject.AsString
              else if MyObjectName = 'hostname' then MyData.hostname := MyJSONObject.AsString
              else if MyObjectName = 'broadcast_address' then MyData.broadcast_address := MyJSONObject.AsString
              else if MyObjectName = 'tcp_port' then MyData.tcp_port := MyJSONObject.AsInt64
              else if MyObjectName = 'http_port' then MyData.http_port := MyJSONObject.AsInt64
              else if MyObjectName = 'version' then MyData.version := MyJSONObject.AsString
            end;
          end;
          AddItem(MyData);
        end;
      end
    end;
  except
    on E: Exception do begin
      raise
    end;
  end;
end;

function TClsNSQProducers.Compare(InCls: TClsNSQProducers): boolean;
var MyResult: boolean;
    F, G: Integer;
    MyCls1, MyCls2: TClsNSQProducer;
    MyFound: boolean;
begin
  MyResult := True;
  MyResult := InCls.GetCount = GetCount;
  if MyResult then begin
    for F := 0 to GetCount-1 do begin
      MyFound := false;
      MyCls1 := GetItem(F);
      for G := 0 to InCls.GetCount-1 do begin
        MyCls2 := InCls.GetItem(G);
        if (MyCls1._data.broadcast_address = MyCls2._data.broadcast_address) and
           (MyCls1._data.tcp_port = MyCls2._data.tcp_port) then
        begin
          MyFound := true
        end;
        if MyFound then Break;
      end;
      MyResult := MyResult and MyCls1.Compare(MyCls2);
    end;
  end;
  Result := MyResult;
end;

{ TClsNSQProducer }

constructor TClsNSQProducer.Create;
begin
  inherited Create;
  _data := InitTNSQProducer;
  _action := '';
  _receiver := nil;
  _lastCompareResult := false;
end;

destructor TClsNSQProducer.Destroy;
begin
  _action := '';
  _lastCompareResult := false;
  _data := InitTNSQProducer;
  if _receiver <> nil then begin
    _receiver.TerminateTread;
  end;
  inherited Destroy;
end;

procedure TClsNSQProducer.Assign(InCls: TClsNSQProducer);
begin
  if InCls <> nil then begin
    _data := InCls._data;
  end
  else begin
    _data := InitTNSQProducer;
  end;
end;

function TClsNSQProducer.Compare(InCls: TClsNSQProducer): boolean;
begin
  _lastCompareResult :=
    (InCls._data.remote_address = _data.remote_address) and
    (InCls._data.hostname = _data.hostname)  and
    (InCls._data.broadcast_address = _data.broadcast_address) and
    (InCls._data.tcp_port = _data.tcp_port) and
    (InCls._data.http_port = _data.http_port) and
    (InCls._data.version = _data.version);
  Result := _lastCompareResult;
end;

function TClsNSQProducer.CreateReceivers(InTopicName, InChannel: string
  ): TNSQReceiverThread;
begin
  if _receiver = nil then begin
    _receiver := TNSQReceiverThread.Create(
                      _data.broadcast_address,
                      _data.tcp_port,
                      InTopicName,
                      InChannel);

  end;
  Result := _receiver;
end;

{ TClsNSQChannel }

constructor TClsNSQChannel.Create;
begin
  inherited Create;
  _data := InitTNSQChannel;
end;

destructor TClsNSQChannel.Destroy;
begin
  _data := InitTNSQChannel;
  inherited Destroy;
end;

procedure TClsNSQChannel.Assign(InCls: TClsNSQChannel);
begin
  if InCls <> nil then begin
    _data := InCls._data;
  end
  else begin
    _data := InitTNSQChannel;
  end;
end;

function TClsNSQChannel.Compare(InCls: TClsNSQChannel): boolean;
begin
  _lastCompareResult := (InCls._data.name = _data.name);
  Result := _lastCompareResult;
end;


{ TClsNSQChannels }

constructor TClsNSQChannels.Create;
begin
  inherited Create;
end;

destructor TClsNSQChannels.Destroy;
begin
  inherited Destroy;
end;

function TClsNSQChannels.GetItem(InIndex: Integer
  ): TClsNSQChannel;
var MyResult: TNSQCls;
begin
  MyResult := inherited GetItem(InIndex);
  if MyResult <> nil then begin
    Result := TClsNSQChannel(MyResult);
  end
  else begin
    Result := nil;
  end;
end;

function TClsNSQChannels.AddItem(var InData: TNSQChannel
  ): TClsNSQChannel;
var MyCls: TClsNSQChannel;
begin
  MyCls := TClsNSQChannel.Create;
  MyCls._data := InData;
  _list.Add(MyCls);
  Result := MyCls;
end;


procedure TClsNSQChannels.ParseJSONResponse(InJSON: TJSONData);
var MyJSONArray: TJSONArray;
    MyData: TNSQChannel;
    F: Integer;
begin
  MyJSONArray := nil;
(*
{
 "channels": [
    "bonus",
    "slips-dbi",
    "web_app_api-dev01",
    "nsq_to_mongo"
    ],
 "producers": [{
    "remote_address": "127.0.0.1:50353",
    "hostname": "MacBook-Pro-od-Dinko.local",
    "broadcast_address": "127.0.0.1",
    "tcp_port": 4150,
    "http_port": 4151,
    "version": "1.2.1"
    }]
}
*)
  try
    if InJSON <> nil then begin
      if (InJSON is TJSONArray) then begin
        MyJSONArray := TJSONArray(InJSON);
        for F := 0 to MyJSONArray.Count-1 do begin
          MyData := InitTNSQChannel;
          MyData.name := MyJSONArray.Items[F].AsString;
          AddItem(MyData);
        end;
      end
    end;
  except
    on E: Exception do begin
      raise
    end;
  end;
end;


function TClsNSQChannels.Compare(InCls: TClsNSQChannels): boolean;
var MyResult: boolean;
    F, G: Integer;
    MyCls1, MyCls2: TClsNSQChannel;
    MyFound: boolean;
begin
  MyResult := True;
  MyResult := InCls.GetCount = GetCount;
  if MyResult then begin
    for F := 0 to GetCount-1 do begin
      MyFound := false;
      MyCls1 := GetItem(F);
      for G := 0 to InCls.GetCount-1 do begin
        MyCls2 := InCls.GetItem(G);
        if (MyCls1._data.name = MyCls2._data.name) then
        begin
          MyFound := true
        end;
        if MyFound then Break;
      end;
      MyResult := MyResult and MyCls1.Compare(MyCls2);
    end;
  end;
  Result := MyResult;
end;

end.



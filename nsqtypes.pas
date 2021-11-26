unit NSQTypes;

{$mode ObjFPC}{$H+}

interface

uses
  Classes, SysUtils;

var
  NSQ_DEBUG: Boolean = true;

  NSQ_CR: char = #13;
  NSQ_NL: Byte = 10;
  NSQ_MAGIC_V2: string = '  V2';

var
  NSQ_LOOKUP_URL: string = 'http://localhost:4161';
  NSQ_IP: string = '127.0.0.1';
  NSQ_PORT: Int32 = 4150;
  NSQ_COMPUTER_NAME: string = 'MACDINKO_LAPTOP';
  NSQ_USER_AGENT: string = 'go-nsq/1.0.7';

type
  TNSQCallbackResponse = (nsqCallFIN, nsqCallREQ, nsqCallTOUCH);

  TNSQCallback = procedure(InTimestampNanosecond: Int64;
                           InAttempts: Int32;
                           InMessageID: string;
                           InBody: string;
                           var OutHowToHandle: TNSQCallbackResponse;
                           var OutParam: Int32
                           );

  TNSQFrameType = (NSQ_FRAMETYPERESPONSE, NSQ_FRAMETYPEERROR, NSQ_FRAMETYPEMESSAGE, NSQ_FRAMETYPEUNKNOWN);
  TByteArray = array of byte;

  TNSQChannel = record
    name: string;
  end;

  TNSQProducer = record
    remote_address: string;
    hostname: string;
    broadcast_address: string;
    tcp_port: Integer;
    http_port: Integer;
    version: string;
  end;

  TNSQIdentify = record
    client_id: string;
    deflate: Boolean;
    deflate_level: integer;
    feature_negotiation: Boolean;
    heartbeat_interval: Integer;
    hostname: string;
    long_id: string;
    msg_timeout: Integer;
    output_buffer_size: Integer;
    output_buffer_timeout: Integer;
    sample_rate: Integer;
    short_id: string;
    snappy: Boolean;
    tls_v1: Boolean;
    user_agent: string;       //go-nsq/1.0.7
  end;

type
  TNSQCls = class
    constructor Create; virtual;
    destructor Destroy; override;
  end;

  TNSQClss = Class
    _isListSorted: Boolean;
    _list: TList;
    constructor Create; virtual;
    destructor Destroy; override;
    procedure DeleteAllItems;
    function GetItem(InIndex: Integer): TNSQCls;
    function AddItem(InItem: TNSQCls): TNSQCls;
    procedure DeleteItem(InIndex: Integer);
    function GetCount: Integer;
    procedure MovePointers(InClss: TNSQClss);
  end;

  procedure NSQWrite(const InFormatStr: string; const InArgs: array of Const);


const InitTNSQChannel: TNSQChannel = ({%H-});
const InitTNSQProducer: TNSQProducer = ({%H-});
const InitTNSQIdentify: TNSQIdentify = ({%H-});


implementation

procedure NSQWrite(const InFormatStr: string; const InArgs: array of Const);
var MyString: String;
    MyLen: Integer;
begin
  MyString := Format(InFormatStr, InArgs);
  MyLen := Length(MyString);
  if MyLen > 0 then begin
    if MyString[MyLen] = char(NSQ_NL) then begin
      Write(FormatDateTime('dd HH:nn:ss.zzz', Now), ': ', MyString, NSQ_CR);
    end
    else begin
      Writeln(FormatDateTime('dd HH:nn:ss.zzz', Now), ': ', MyString, NSQ_CR);
    end;
  end
  else begin
    Writeln(FormatDateTime('dd HH:nn:ss.zzz', Now), ': ', NSQ_CR);
  end;
end;

{ TNSQClss }

constructor TNSQClss.Create;
begin
  _isListSorted := False;
  _list := TList.Create;
end;

destructor TNSQClss.Destroy;
begin
  DeleteAllItems;
  FreeAndNil(_list);
  inherited Destroy;
end;

procedure TNSQClss.DeleteAllItems;
var F: Integer;
    MyCls: TNSQCls;
begin
  for F := 0 to _list.Count-1 do begin
    MyCls := TNSQCls(_list.Items[F]);
    FreeAndNil(MyCls);
  end;
  _list.Clear;
  _isListSorted := False;
end;

function TNSQClss.GetItem(InIndex: Integer): TNSQCls;
var MyResult: TNSQCls;
begin
  if (InIndex >= 0) and (InIndex < _list.Count) then begin
    MyResult := TNSQCls(_List.Items[InIndex]);
  end
  else begin
    MyResult := nil;
  end;
  Result := MyResult;
end;

function TNSQClss.AddItem(InItem: TNSQCls): TNSQCls;
begin
  _list.Add(InItem);
  Result := InItem;
end;

procedure TNSQClss.DeleteItem(InIndex: Integer);
var MyCls: TNSQCls;
begin
  if (InIndex >= 0) and (InIndex < _list.Count) then begin
    MyCls := TNSQCls(_list.Items[InIndex]);
    FreeAndNil(MyCls);
    _list.Delete(InIndex);
  end;
end;

function TNSQClss.GetCount: Integer;
begin
  Result := _List.Count;
end;

procedure TNSQClss.MovePointers(InClss: TNSQClss);
begin
  // remove my objects
  Self.DeleteAllItems;

  // Copy new pointers to objects
  _List.Assign(InClss._list);

  // Copy flag is it sorted
  Self._isListSorted := InClss._isListSorted;

  // remove old pointers
  InClss._list.Clear;
end;

{ TNSQCls }

constructor TNSQCls.Create;
begin

end;

destructor TNSQCls.Destroy;
begin
  inherited Destroy;
end;


end.


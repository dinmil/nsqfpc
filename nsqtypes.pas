unit NSQTypes;

{$mode ObjFPC}{$H+}

interface

uses
  Classes, SysUtils,
  {$ifdef Windows} Windows {$endif}
  {$ifdef UNIX}   Unix, unixtype, pthreads, BaseUnix {$endif}
  ;

{$ifdef UNIX}
const
 EPERM = 1;
 ENOENT = 2;
 ESRCH = 3;
 EINTR = 4;
 EIO = 5;
 ENXIO = 6;
 E2BIG = 7;
 ENOEXEC = 8;
 EBADF = 9;
 ECHILD = 10;
 EAGAIN = 11;
 ENOMEM = 12;
 EACCES = 13;
 EFAULT = 14;
 ENOTBLK = 15;
 EBUSY = 16;
 EEXIST = 17;
 EXDEV = 18;
 ENODEV = 19;
 ENOTDIR = 20;
 EISDIR = 21;
 EINVAL = 22;
 ENFILE = 23;
 EMFILE = 24;
 ENOTTY = 25;
 ETXTBSY = 26;
 EFBIG = 27;
 ENOSPC = 28;
 ESPIPE = 29;
 EROFS = 30;
 EMLINK = 31;
 EPIPE = 32;
 EDOM = 33;
 ERANGE = 34;
 EDEADLK = 35;
 ENAMETOOLONG = 36;
 ENOLCK = 37;
 ENOSYS = 38;
 ENOTEMPTY = 39;
 ELOOP = 40;
 EWOULDBLOCK = EAGAIN;
 ENOMSG = 42;
 EIDRM = 43;
 ECHRNG = 44;
 EL2NSYNC = 45;
 EL3HLT = 46;
 EL3RST = 47;
 ELNRNG = 48;
 EUNATCH = 49;
 ENOCSI = 50;
 EL2HLT = 51;
 EBADE = 52;
 EBADR = 53;
 EXFULL = 54;
 ENOANO = 55;
 EBADRQC = 56;
 EBADSLT = 57;
 EDEADLOCK = EDEADLK;
 EBFONT = 59;
 ENOSTR = 60;
 ENODATA = 61;
 ETIME = 62;
 ENOSR = 63;
 ENONET = 64;
 ENOPKG = 65;
 EREMOTE = 66;
 ENOLINK = 67;
 EADV = 68;
 ESRMNT = 69;
 ECOMM = 70;
 EPROTO = 71;
 EMULTIHOP = 72;
 EDOTDOT = 73;
 EBADMSG = 74;
 EOVERFLOW = 75;
 ENOTUNIQ = 76;
 EBADFD = 77;
 EREMCHG = 78;
 ELIBACC = 79;
 ELIBBAD = 80;
 ELIBSCN = 81;
 ELIBMAX = 82;
 ELIBEXEC = 83;
 EILSEQ = 84;
 ERESTART = 85;
 ESTRPIPE = 86;
 EUSERS = 87;
 ENOTSOCK = 88;
 EDESTADDRREQ = 89;
 EMSGSIZE = 90;
 EPROTOTYPE = 91;
 ENOPROTOOPT = 92;
 EPROTONOSUPPORT = 93;
 ESOCKTNOSUPPORT = 94;
 EOPNOTSUPP = 95;
 EPFNOSUPPORT = 96;
 EAFNOSUPPORT = 97;
 EADDRINUSE = 98;
 EADDRNOTAVAIL = 99;
 ENETDOWN = 100;
 ENETUNREACH = 101;
 ENETRESET = 102;
 ECONNABORTED = 103;
 ECONNRESET = 104;
 ENOBUFS = 105;
 EISCONN = 106;
 ENOTCONN = 107;
 ESHUTDOWN = 108;
 ETOOMANYREFS = 109;
 ETIMEDOUT = 110;
 ECONNREFUSED = 111;
 EHOSTDOWN = 112;
 EHOSTUNREACH = 113;
 EALREADY = 114;
 EINPROGRESS = 115;
 ESTALE = 116;
 EUCLEAN = 117;
 ENOTNAM = 118;
 ENAVAIL = 119;
 EISNAM = 120;
 EREMOTEIO = 121;
 EDQUOT = 122;
 ENOMEDIUM = 123;
 EMEDIUMTYPE = 124;

 (*
 LOCK_SH = 1;
 LOCK_EX = 2;
 LOCK_NB = 4;
 LOCK_UN = 8;
 *)

 SEM_FAILED    = Psem_t(nil);
 SEM_VALUE_MAX = ((not 0) shr 1);
{$endif}


var
  NSQ_DEBUG: Boolean = false;

  NSQ_CR: char = #13;
  NSQ_NL: Byte = 10;
  NSQ_MAGIC_V2: string = '  V2';

var
  NSQ_LOOKUP_URL: string = 'http://127.0.0.1:4161';
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

  { TFavMutex }

  TFavMutex = class
  private
    _LastErroNo: longint;
    _LastErrorMsg: String;
    _Name: String;
    _InitialValue: Boolean;
    {$ifdef Windows}
    FMutex: Cardinal;
    {$endif}
    {$ifdef UNIX}
    FMutex: pthread_mutex_t;
    FMutexFile: Longint;
    {$endif}
  public
    constructor Create(InName: String = ''; InInitialValue: Boolean=False);
    destructor  Destroy; override;
    procedure   Release;
    procedure   Wait;
    function GetError(InAction: String; InError: Int64): String;
  end;

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


var LogMutex: TFavMutex;

implementation

procedure NSQWrite(const InFormatStr: string; const InArgs: array of Const);
var MyString: String;
{$IFDEF UNIX}
    MyLen: Integer;
{$ENDIF}
begin
  {$IFDEF UNIX}
  MyString := Format(InFormatStr, InArgs);
  MyLen := Length(MyString);
  if MyLen > 0 then begin
    Writeln(FormatDateTime('dd HH:nn:ss.zzz', Now), ': ', MyString, NSQ_CR);
  end
  else begin
    Writeln(FormatDateTime('dd HH:nn:ss.zzz', Now) + ': ');
  end;
  {$ELSE}
  MyString := Format(InFormatStr, InArgs);
  try
    LogMutex.Wait;
    Writeln(FormatDateTime('dd HH:nn:ss.zzz', Now), ': ', MyString);
  finally
    LogMutex.Release;
  end;
  {$ENDIF}
end;

{ TFavMutex }


constructor TFavMutex.Create(InName: String; InInitialValue: Boolean);
{$ifdef Windows}
var MySecurDesc: SECURITY_DESCRIPTOR;
    MyMutexAttributes: TSecurityAttributes;
{$endif}
{$ifdef UNIX}
var MyMutexAttributes: pthread_mutexattr_t;
    MyRV: Longint;
{$endif}
begin
  _Name := InName;
  _InitialValue:= InInitialValue;
  {$ifdef Windows}
  if ( InitializeSecurityDescriptor (@MySecurDesc, SECURITY_DESCRIPTOR_REVISION )) then begin
    if ( SetSecurityDescriptorDacl (@MySecurDesc, TRUE, nil, FALSE) ) then begin
      // initialize security attributes
      MyMutexAttributes.nLength := sizeof ( SECURITY_ATTRIBUTES );
      MyMutexAttributes.lpSecurityDescriptor := @MySecurDesc;
      MyMutexAttributes.bInheritHandle := TRUE;
    end;
  end;
  if _Name = '' then begin
    FMutex := CreateMutex(@MyMutexAttributes, InInitialValue, nil);
  end
  else begin
    FMutex := CreateMutex(@MyMutexAttributes, InInitialValue, PChar(_Name));
  end;
  if FMutex = 0 then begin
    raise Exception.Create(GetError('CREATE', GetLastError));
  end;
  {$endif}
  {$ifdef UNIX}
  if InName = '' then begin
    FillChar(MyMutexAttributes, SizeOf(MyMutexAttributes), 0);
    pthread_mutexattr_settype(@MyMutexAttributes, Longint(PTHREAD_MUTEX_RECURSIVE));
    MyRv := pthread_mutex_init(@FMutex, @MyMutexAttributes);
    if (MyRv <> 0) then begin
      raise Exception.Create(GetError('INIT', MyRV));
    end;
  end
  else begin
    FMutexFile := FpOpen(PChar(_Name),
                  O_RDWR      +   // open the file for both read and write access
                  O_CREAT     +   // create file if it does not already exist
                  $02000000, //O_CLOEXEC   ,   // close on execute
                  S_IRUSR     +   // user permission: read
                  S_IWUSR     );  // user permission: write
    if FMutexFile <= 0 then begin
      raise Exception.Create(GetError('NAMEDMUTEX-CREATE', MyRV));
    end;
  end;
  {$endif}
end;

destructor TFavMutex.Destroy;
{$ifdef Windows}
var MyRv: LongBool;
{$endif}
{$ifdef UNIX}
var MyRV: Longint;
{$endif}
begin
  {$ifdef Windows}
  MyRv := CloseHandle(FMutex);
  if MyRv = False then begin
    raise Exception.Create(GetError('DESTROY', GetLastError));
  end;
  {$endif}
  {$ifdef UNIX}
  if _Name = '' then begin;
    MyRv := pthread_mutex_destroy (@FMutex);
    if MyRv <> 0 then begin
      raise Exception.Create(GetError('DESTROY', MyRV));
    end;
  end
  else begin
    if FMutexFile <> 0 then begin;
      MyRv := FpFLock(FMutexFile,  LOCK_UN); // F_ULOCK, 0 );
      Fpclose(FMutexFile);
      if MyRv < 0 then begin
        raise Exception.Create(GetError('NAMEDMUTEX-DESTROY', fpgeterrno));
      end;
    end
    else begin
      raise Exception.Create(_LastErrorMsg);
    end;
  end;
  {$endif}
  inherited;
end;

procedure TFavMutex.Release;
{$ifdef Windows}
var MyRv: LongBool;
{$endif}
{$ifdef UNIX}
var MyRv: Longint;
{$endif}
begin
  {$ifdef Windows}
  MyRv := ReleaseMutex(FMutex);
  if MyRV = False then begin
    raise Exception.Create(GetError('RELEASE', GetLastError));
  end;
  {$endif}
  {$ifdef UNIX}
  if _Name = '' then begin
    MyRv := pthread_mutex_unlock (@FMutex);
//    _LastThreadID := 0;
    if MyRV <> 0 then begin
      raise Exception.Create(GetError('LOCK', MyRv));
    end;
  end
  else begin
    if FMutexFile <> 0 then begin;
      MyRv := fpFlock(FMutexFile, LOCK_UN); //F_ULOCK, 0 );    F_ULOCK := 0
//      FpClose(FMutexFile);
      if MyRv < 0 then begin
        raise Exception.Create(GetError('NAMEDMUTEX-RELEASE', fpgeterrno));
      end;
    end
    else begin
      raise Exception.Create('Named Mutex: ' + _Name + ' not created');
    end;
  end
  {$endif}
end;

procedure TFavMutex.Wait;
var MyRv: Longint;
{$ifdef UNIX}
{$endif}
begin
  {$ifdef Windows}
  MyRv := WaitForSingleObject(FMutex, Infinite);
  if MyRV <> WAIT_OBJECT_0 then begin
    raise Exception.Create(GetError('WAIT', MyRv));
  end;
  {$endif}
  {$ifdef UNIX}
  if _Name = '' then begin
    MyRv := pthread_mutex_lock (@FMutex);
    if MyRV <> 0 then begin
      raise Exception.Create(GetError('LOCK', MyRv));
    end;
  end
  else begin
    if FMutexFile <> 0 then begin;
      MyRv := fpFlock(FMutexFile, LOCK_SH); //LOCK_SH;// lock the "semaphore" F_LOCK - wait ifinitelly F_TLOCK - Try to lock
      if MyRv < 0 then begin
        raise Exception.Create(GetError('NAMEDMUTEX-WAIT', fpgeterrno));
      end;
    end
    else begin
      raise Exception.Create('Named Mutex: ' + _Name + ' not created');
    end;
  end;
  {$endif}
end;

function TFavMutex.GetError(InAction: String; InError: Int64): String;
var MyResult: String;
begin
  MyResult := '';
  _LastErroNo:=InError;
{$ifdef UNIX}
  if InAction = 'INIT' then begin
    if InError = EAGAIN then
      MyResult := 'The system lacked the necessary resources (other than memory) to initialise another mutex'
    else if InError = ENOMEM then
      MyResult := 'Insufficient memory exists to initialise the mutex.'
    else if InError = EPERM then
      MyResult := 'The caller does not have the privilege to perform the operation.'
    else if InError = EBUSY then
      MyResult := 'The implementation has detected an attempt to re-initialise the object referenced by mutex, a previously initialised, but not yet destroyed, mutex.'
    else if InError = EINVAL then
      MyResult := 'The value specified by attr is invalid.'
    else
      MyResult := 'Unknown error';
  end
  else if InAction = 'DESTROY' then begin
    if InError = EBUSY then
      MyResult := 'The implementation has detected an attempt to destroy the object referenced by mutex while it is locked or referenced (for example, while being used in a pthread_cond_wait() or pthread_cond_timedwait()) by another thread.'
    else if InError = EINVAL then
      MyResult := 'The value specified by mutex is invalid.'
    else
      MyResult := 'Unknown error';
  end
  else if InAction = 'UNLOCK' then begin
    if InError = EINVAL then
      MyResult := 'Mutex is not an initialized mutex. '
    else if InError = EFAULT then
      MyResult := 'Mutex is an invalid pointer. '
    else if InError = EPERM then
      MyResult := 'The calling thread does not own the mutex. '
    else
      MyResult := 'Unknown error';
  end
  else if InAction = 'LOCK' then begin
    if InError = EINVAL then
      MyResult := 'The mutex was created with the protocol attribute having the value PTHREAD_PRIO_PROTECT and the calling thread''s priority is higher than the mutex''s current priority ceiling. The value specified by mutex does not refer to an initialised mutex object.'
    else if InError = EBUSY then
      MyResult := 'The mutex could not be acquired because it was already locked.'
    else if InError = EFAULT then
      MyResult := 'mutex is an invalid pointer. '
    else if InError = EAGAIN then
      MyResult := 'The mutex could not be acquired because the maximum number of recursive locks for mutex has been exceeded.'
    else if InError = EDEADLK then
      MyResult := 'The current thread already owns the mutex.'
    else if InError = EPERM then
      MyResult := 'The calling thread does not own the mutex. '
    else
      MyResult := 'Unknown error';
  end
  else begin
    if InError = EACCES then
      MyResult := 'The file is locked and F_TLOCK or F_TEST was specified, or the operation is prohibited because the file has been memory-mapped by another process.'
    else if InError = EAGAIN then
      MyResult := 'The file is locked and F_TLOCK or F_TEST was specified, or the operation is prohibited because the file has been memory-mapped by another process.'
    else if InError = EBADF then
      MyResult := 'fd is not an open file descriptor; or cmd is F_LOCK or F_TLOCK and fd is not a writable file descriptor.'
    else if InError = EDEADLK then
      MyResult := 'The command was T_LOCK and this lock operation would cause a deadlock.'
    else if InError = EINVAL then
      MyResult := 'An invalid operation was specified in fd.'
    else if InError = ENOLCK then
      MyResult := 'Too many segment locks open, lock table is full.'
    else
      MyResult := 'Unknown error';
  end;
{$endif}
{$ifdef Windows}
  if (InAction = 'WAIT') or (InAction = 'RELEASE') then begin
    if InError = WAIT_ABANDONED then
      MyResult := 'The specified object is a mutex object that was not released by the thread that owned the mutex object before the owning thread terminated. Ownership of the mutex object is granted to the calling thread and the mutex state is set to nonsignaled. If the mutex was protecting persistent state information, you should check it for consistency.'
    else if InError = WAIT_OBJECT_0 then
      MyResult := 'The state of the specified object is signaled.'
    else if InError = WAIT_TIMEOUT then
      MyResult := 'The time-out interval elapsed, and the object''s state is nonsignaled.'
    else if InError = WAIT_FAILED then
      MyResult := 'The function has failed. To get extended error information, call GetLastError: ' + IntToStr(GetLastError)
    else
      MyResult := 'Unknown error';
  end
  else if (InAction = 'CREATE') or (InAction = 'DESTROY') then begin
    MyResult := 'The function has failed. To get extended error information, call GetLastError: ' + IntToStr(GetLastError);
  end;
{$endif}

  MyResult := 'Mutex: ' + _Name + '; Action: ' + InAction + '; Rv: ' + IntToStr(InError) + '; ' + MyResult;
  _LastErrorMsg:=MyResult;

  Result := MyResult;
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

initialization
  {$IFDEF WINDOWS}
  LogMutex := TFavMutex.Create('LogMutex');
  {$ENDIF}

finalization
  {$IFDEF WINDOWS}
  if LogMutex <> nil then FreeAndNil(LogMutex);
  {$ENDIF}



end.


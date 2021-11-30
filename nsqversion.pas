unit NSQVersion;

{$mode ObjFPC}{$H+}

interface

uses
  Classes, SysUtils;

var
  NSQ_CLIENT_VERSION: string = '0.0.0.3';
  NSQ_CLIENT_TIMESTAMP: string = '2021-11-30 00:00:00';


// TODO
// - Update readme file
// - Install proper signal handling and avoid crt unit
// - Try on windows - is was intially written by CodeTyphoon 7.50 for MAC
// - Test reconnect without NSQLookup and with NSQLookup
//   With NSQLookup - reconnect will use NSQLookup info (e.g. every 5 seconds)
//   Without Lookup - reconnect will try in progress manner - 1, 2, 4, 8, 16, 32... seconds


// VERSION INFO
// 0.0.0.3 - 2021-11-30 00:00:00 - Add support for windows
// 0.0.0.1 - 2021-11-24 00:00:00 - Initial version

implementation

end.


program AMQPOutboxTestsFpc;

{ FPCUnit runner of the outbox bridge's tests (src/outbox, AMQP.Outbox).

  Console: .\AMQPOutboxTestsFpc.exe --all --format=plain
  GUI (Windows, no parameters): FPCUnit's test tree.
  Outside Windows it always runs in the console (no LCL).

  SQLite through SQLdb: on Windows sqlite.org's sqlite3.dll next to the
  runner (or AMQP_SQLITE_CLIENT); on Linux libsqlite3.so.0. }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  {$IFDEF MSWINDOWS}
  Interfaces, Forms, GuiTestRunner,
  {$ENDIF}
  Classes, consoletestrunner, testregistry,
  AMQP.OutboxAmqpTests;

var
  ConsoleApp: TTestRunner;
begin
  SetMultiByteConversionCodePage(CP_UTF8);

  {$IFDEF MSWINDOWS}
  if ParamCount = 0 then
  begin
    Application.Initialize;
    Application.CreateForm(TGUITestRunner, TestRunner);
    Application.Run;
  end
  else
  {$ENDIF}
  begin
    DefaultFormat := fPlain;
    DefaultRunAllTests := True;
    ConsoleApp := TTestRunner.Create(nil);
    try
      ConsoleApp.Initialize;
      ConsoleApp.Title := 'pascal-amqp-faa - outbox bridge tests (FPCUnit)';
      ConsoleApp.Run;
    finally
      ConsoleApp.Free;
    end;
  end;
end.

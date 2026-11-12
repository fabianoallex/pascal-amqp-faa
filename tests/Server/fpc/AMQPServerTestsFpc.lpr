program AMQPServerTestsFpc;

{ FPCUnit runner for the broker (server) sub-module tests.

  Console: .\AMQPServerTestsFpc.exe --all --format=plain
  GUI (Windows, no parameters): FPCUnit test tree.
  Outside Windows it always runs in console mode (no LCL). }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  {$IFDEF MSWINDOWS}
  Interfaces, Forms, GuiTestRunner,
  {$ENDIF}
  Classes, consoletestrunner, testregistry,
  AMQP.ServerSkeletonTests,
  AMQP.ServerEventsTests,
  AMQP.ServerHandshakeTests,
  AMQP.ServerEngineTests,
  AMQP.ServerRoutingTests,
  AMQP.ServerTestDoubles,
  AMQP.ServerQueueTests,
  AMQP.ServerDeliveryTests,
  AMQP.ServerEngineDispatchTests,
  AMQP.ServerWalTests,
  AMQP.ServerJournalTests,
  AMQP.ServerTopologyTests,
  AMQP.ServerPersistTests,
  AMQP.ServerConfirmTests,
  AMQP.ServerRecoveryTests,
  AMQP.ServerTracingTests;

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
      ConsoleApp.Title := 'pascal-amqp-faa - testes do broker (FPCUnit)';
      ConsoleApp.Run;
    finally
      ConsoleApp.Free;
    end;
  end;
end.

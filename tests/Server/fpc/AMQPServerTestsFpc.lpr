program AMQPServerTestsFpc;

{ Runner FPCUnit dos testes do sub-módulo broker (server).

  Console: .\AMQPServerTestsFpc.exe --all --format=plain
  GUI (Windows, sem parâmetros): árvore de testes FPCUnit.
  Fora do Windows roda sempre em console (sem LCL). }

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
  AMQP.ServerHandshakeTests,
  AMQP.ServerEngineTests,

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

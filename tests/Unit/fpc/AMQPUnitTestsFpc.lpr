program AMQPUnitTestsFpc;

{ FPCUnit runner for the unit tests (same coverage as the DUnitX/Delphi
  tests/Unit/*.pas, ported to FPCUnit). No broker.

  Console (text output), when called with any parameter:
    .\AMQPUnitTestsFpc.exe --all --format=plain
  GUI (window with test tree + green/red bar), without parameters:
    .\AMQPUnitTestsFpc.exe
  Outside Windows it always runs in console mode (no LCL/widgetset), with or
  without parameters.

  See CLAUDE.md for the workarounds for FPC 3.2.2 internal errors
  found while porting (chaining TValue.From<T> / TAMQPFieldTable indexer
  followed by .AsString/.AsExtended/.AsObject). }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  {$IFDEF MSWINDOWS}
  Interfaces, Forms, GuiTestRunner,
  {$ENDIF}
  Classes, consoletestrunner, testregistry,
  AMQP.WireTests,
  AMQP.FrameTests,
  AMQP.ConnectionMethodsTests,
  AMQP.Item2MethodsTests,
  AMQP.TracingTests,
  AMQP.TransportTests;

var
  ConsoleApp: TTestRunner;
begin
  // Plain FPC console (outside a Lazarus app): DefaultSystemCodePage is not UTF-8
  // by default, so accented strings in the tests (ShortStr_UsaUTF8,
  // ShortStr_ComAcentos etc.) would be transcoded wrongly. See CLAUDE.md.
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
      ConsoleApp.Title := 'pascal-amqp-faa - testes unitarios (FPCUnit)';
      ConsoleApp.Run;
    finally
      ConsoleApp.Free;
    end;
  end;
end.

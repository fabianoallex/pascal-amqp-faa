program AMQPUnitTestsFpc;

{ Runner FPCUnit dos testes unitarios (mesma cobertura do tests/Unit/*.pas
  DUnitX/Delphi, portada para FPCUnit). Sem broker.

    fpc -Fu..\..\..\src -Fi..\..\..\src AMQPUnitTestsFpc.lpr
    .\AMQPUnitTestsFpc.exe --all --format=plain

  Ver CLAUDE.md para os workarounds de erros internos do FPC 3.2.2
  encontrados ao portar (encadeamento de TValue.From<T> / indexador de
  TAMQPFieldTable seguido de .AsString/.AsExtended/.AsObject). }

{$mode delphi}{$H+}

uses
  Classes, consoletestrunner, testregistry,
  AMQP.WireTests,
  AMQP.FrameTests,
  AMQP.ConnectionMethodsTests,
  AMQP.Item2MethodsTests;

var
  App: TTestRunner;
begin
  // Console FPC puro (fora de app Lazarus): DefaultSystemCodePage nao e' UTF-8
  // por padrao, entao strings acentuadas nos testes (ShortStr_UsaUTF8,
  // ShortStr_ComAcentos etc.) seriam transcodificadas errado. Ver CLAUDE.md.
  SetMultiByteConversionCodePage(CP_UTF8);
  DefaultFormat := fPlain;
  DefaultRunAllTests := True;
  App := TTestRunner.Create(nil);
  try
    App.Initialize;
    App.Title := 'pascal-amqp-faa - testes unitarios (FPCUnit)';
    App.Run;
  finally
    App.Free;
  end;
end.

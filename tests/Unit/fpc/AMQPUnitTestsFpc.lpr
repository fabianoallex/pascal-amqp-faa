program AMQPUnitTestsFpc;

{ Runner FPCUnit dos testes unitarios (mesma cobertura do tests/Unit/*.pas
  DUnitX/Delphi, portada para FPCUnit). Sem broker.

  Console (saida de texto), quando chamado com qualquer parametro:
    .\AMQPUnitTestsFpc.exe --all --format=plain
  GUI (janela com arvore de testes + barra verde/vermelha), sem parametros:
    .\AMQPUnitTestsFpc.exe

  Ver CLAUDE.md para os workarounds de erros internos do FPC 3.2.2
  encontrados ao portar (encadeamento de TValue.From<T> / indexador de
  TAMQPFieldTable seguido de .AsString/.AsExtended/.AsObject). }

{$mode delphi}{$H+}

uses
  Interfaces, Forms,
  Classes, consoletestrunner, testregistry, GuiTestRunner,
  AMQP.WireTests,
  AMQP.FrameTests,
  AMQP.ConnectionMethodsTests,
  AMQP.Item2MethodsTests;

var
  ConsoleApp: TTestRunner;
begin
  // Console FPC puro (fora de app Lazarus): DefaultSystemCodePage nao e' UTF-8
  // por padrao, entao strings acentuadas nos testes (ShortStr_UsaUTF8,
  // ShortStr_ComAcentos etc.) seriam transcodificadas errado. Ver CLAUDE.md.
  SetMultiByteConversionCodePage(CP_UTF8);

  if ParamCount > 0 then
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
  end
  else
  begin
    Application.Initialize;
    Application.CreateForm(TGUITestRunner, TestRunner);
    Application.Run;
  end;
end.

program CrashHostFpc;

{ Host de crash-safety -- WS8 da Fase 4 (durabilidade, D28 camada 3).

  O QUE ELE E', E O QUE ELE NAO E'
  --------------------------------
  Este programa sobe o broker com DataDir, faz UM trabalho, imprime uma marca
  e FICA VIVO. Quem o mata e' o driver (tests/tools/matriz_de_queda.py), de
  fora, sem aviso. E' morte de processo de verdade: nenhum destrutor roda,
  nenhum Stop e' chamado, o journal nao drena.

  O QUE ISSO NAO TESTA, E ESTA' NA D28 COM ESTAS PALAVRAS: **matar o processo
  NAO testa fsync**. A cache do sistema operacional sobrevive a morte do
  processo -- so' a queda da MAQUINA a perde. O que esta matriz prova e' outra
  coisa, e ela vale por si: que o que o broker escreveu e' suficiente para
  reconstruir o estado, que a cauda torta nao impede o boot, e que o que foi
  confirmado como consumido nao ressuscita.

  As outras duas camadas da D28 ja' vivem na suite: a varredura exaustiva de
  truncamento (WS1) e o duble de arquivo que conta fsyncs e falha na hora
  escolhida (WS1/WS2, mais o TAMQPWalFileComFalha da WS7). Esta e' a terceira,
  e mora FORA do runner pelo mesmo motivo do SmokeTest: um teste que precisa
  matar o proprio processo nao cabe num runner que quer contar assercoes.

  MODOS

    CrashHostFpc publica <dir> <n> [inicio]
                                     sobe, publica n persistentes com confirm
                                     numerados a partir de `inicio` (1 se
                                     omitido), imprime PUBLICADAS n e espera
                                     ser morto. O `inicio` existe para varias
                                     quedas seguidas produzirem uma sequencia
                                     GLOBALMENTE crescente -- senao o confere
                                     acusaria "fora de ordem" so' porque cada
                                     rodada recomecou do 1
    CrashHostFpc consome <dir> <n>   sobe, consome e confirma n, imprime
                                     CONSUMIDAS n e espera ser morto
    CrashHostFpc despeja <dir>       sobe, publica sem parar imprimindo
                                     CONFIRMADA <seq> a cada confirm -- serve
                                     para o driver matar NO MEIO da escrita
    CrashHostFpc confere <dir>       sobe, conta o que voltou, imprime
                                     RECUPERADAS n e SAI (este encerra limpo).
                                     NAO CONSOME: pega em modo ack e nunca
                                     confirma, entao fechar a conexao devolve
                                     tudo a fila. E' o que permite conferir
                                     entre uma queda e a seguinte sem alterar o
                                     que se esta medindo.

  A fila e' sempre q.crash, duravel, e o corpo de cada mensagem e' o proprio
  numero -- assim o "confere" checa integridade, e nao so' contagem.

  UM CUIDADO QUE CUSTOU MEIA HORA: aqui NAO se usa Halt fora do bloco
  principal. `Halt` abandona os frames sem finalizar os locais GERENCIADOS
  (strings, TBytes, interfaces) das rotinas que estao na pilha -- e o heaptrc
  denuncia isso como vazamento, corretamente. Os modos devolvem o controle e
  quem encerra e' o fim do programa; erro vira `ExitCode`. }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  SysUtils,
  AMQP.Threading,
  AMQP.Queue.Methods,
  AMQP.Connection,
  AMQP.Server.Types,
  AMQP.Server.Broker;

const
  FILA = 'q.crash';

var
  GBroker: TAMQPServer;
  GConn: TAMQPConnection;
  GChan: TAMQPChannel;

procedure Diz(const AMsg: string);
begin
  WriteLn(AMsg);
  Flush(Output);
end;

// Sobe o broker no diretorio e liga um cliente nele. Porta efemera: o driver
// nao precisa saber, porque cliente e broker estao no mesmo processo.
procedure Sobe(const ADir: string);
var
  LParams: TAMQPConnectionParams;
begin
  GBroker := TAMQPServer.Create;
  GBroker.BindAddress := '127.0.0.1';
  GBroker.Port := 0;
  GBroker.DataDir := ADir;
  GBroker.Start;
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := GBroker.Port;
  GConn := TAMQPConnection.Create(LParams);
  GConn.Open;
  GChan := GConn.CreateChannel;
  GChan.DeclareQueue(TAMQPQueueDeclare.Create(FILA, True));
end;

/// Fica vivo ate' alguem matar o processo. NAO ha saida limpa daqui: e' o
/// ponto do exercicio.
procedure EsperaAMorte;
begin
  while True do
    Sleep(50);
end;

procedure ModoPublica(const ADir: string; AN, AInicio: Integer);
var
  I: Integer;
begin
  Sobe(ADir);
  GChan.ConfirmSelect;
  for I := AInicio to AInicio + AN - 1 do
    GChan.PublishText('', FILA, IntToStr(I), True);
  if not GChan.WaitForConfirms(30000) then
  begin
    Diz('ERRO nem tudo foi confirmado');
    ExitCode := 2;
    Exit;
  end;
  Diz('PUBLICADAS ' + IntToStr(AN));
  EsperaAMorte;
end;

procedure ModoConsome(const ADir: string; AN: Integer);
var
  I: Integer;
  LGet: TAMQPGetResult;
begin
  Sobe(ADir);
  for I := 1 to AN do
  begin
    LGet := GChan.BasicGet(FILA, False); // modo ack
    if not LGet.Found then
    begin
      Diz('ERRO fila vazia em ' + IntToStr(I));
      ExitCode := 2;
      Exit;
    end;
    GChan.Ack(LGet.DeliveryTag);
  end;
  // O ack viaja e o ator ainda tem de processa-lo e escrever o DEQ. Sem esta
  // espera o teste mediria a corrida, nao a durabilidade.
  Sleep(1000);
  Diz('CONSUMIDAS ' + IntToStr(AN));
  EsperaAMorte;
end;

procedure ModoDespeja(const ADir: string);
var
  I: Integer;
begin
  Sobe(ADir);
  GChan.ConfirmSelect;
  I := 0;
  while True do
  begin
    Inc(I);
    GChan.PublishText('', FILA, IntToStr(I), True);
    if GChan.WaitForConfirms(30000) then
      Diz('CONFIRMADA ' + IntToStr(I))
    else
    begin
      Diz('NACK ' + IntToStr(I));
      ExitCode := 3;
      Exit;
    end;
  end;
end;

procedure ModoConfere(const ADir: string);
var
  LGet: TAMQPGetResult;
  LN: Integer;
  LForaDeOrdem: Boolean;
  LUltimo, LAtual: Integer;
begin
  Sobe(ADir);
  LN := 0;
  LForaDeOrdem := False;
  LUltimo := 0;
  while True do
  begin
    // MODO ACK, e NUNCA confirma: fechar a conexao devolve todas a fila. Um
    // no-ack aqui esvaziaria a fila -- e o conferidor passaria a mudar o que
    // mede, o que estragaria qualquer cenario com mais de uma queda.
    LGet := GChan.BasicGet(FILA, False);
    if not LGet.Found then
      Break;
    Inc(LN);
    // O corpo E' o numero: da para checar INTEGRIDADE e ORDEM, e nao so'
    // contagem. Uma recuperacao que devolvesse as mensagens certas na ordem
    // errada passaria num teste que so' contasse.
    if TryStrToInt(LGet.BodyAsText, LAtual) then
    begin
      if LAtual <= LUltimo then
        LForaDeOrdem := True;
      LUltimo := LAtual;
    end
    else
      LForaDeOrdem := True;
  end;
  if LForaDeOrdem then
    Diz('FORA-DE-ORDEM');
  Diz('RECUPERADAS ' + IntToStr(LN));
  GConn.Close;
  GConn.Free;
  // As nao-confirmadas voltam para a fila no teardown da conexao, e isso
  // acontece no ATOR. Parar o broker sem dar essa volta perderia da MEMORIA o
  // que ainda esta' no disco -- nao seria perda de dado, mas o proximo confere
  // mediria uma corrida.
  Sleep(1000);
  GBroker.Stop;
  GBroker.Free;
  if LForaDeOrdem then
    ExitCode := 4;
end;

var
  LModo, LDir: string;
  LN, LInicio: Integer;
begin
  if ParamCount < 2 then
  begin
    Diz('uso: CrashHostFpc <publica|consome|despeja|confere> <dir> [n] [inicio]');
    ExitCode := 1;
    Exit;
  end;
  LModo := LowerCase(ParamStr(1));
  LDir := ParamStr(2);
  ForceDirectories(LDir);
  LN := 0;
  if ParamCount >= 3 then
    LN := StrToIntDef(ParamStr(3), 0);
  LInicio := 1;
  if ParamCount >= 4 then
    LInicio := StrToIntDef(ParamStr(4), 1);

  if LModo = 'publica' then
    ModoPublica(LDir, LN, LInicio)
  else if LModo = 'consome' then
    ModoConsome(LDir, LN)
  else if LModo = 'despeja' then
    ModoDespeja(LDir)
  else if LModo = 'confere' then
    ModoConfere(LDir)
  else
  begin
    Diz('modo desconhecido: ' + LModo);
    ExitCode := 1;
  end;
end.

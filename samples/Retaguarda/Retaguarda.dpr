program Retaguarda;

{ Cenario: PDV -> autorizador -> retaguarda. O autorizador (sample
  AutorizadorSim) publica o retorno de cada nota numa fila; a retaguarda
  consome e "busca o XML" (aqui, simulado com um Sleep aleatorio), como se
  varios PDVs estivessem aguardando resposta ao mesmo tempo.

  Diferenca em relacao ao mesmo cenario com outras libs AMQP para Delphi:
  o Channel.Consume desta lib ja despacha cada entrega para o thread pool
  proprio (AmqpPool, dentro de TAMQPChannel.DispatchDelivery) - o callback
  abaixo roda concorrente para mensagens diferentes sem nenhum despacho
  manual, e a thread de leitura nunca fica bloqueada esperando
  ProcessarChave terminar.

  Argumento opcional --dedicado: usa CreateChannel(True), que troca o pool
  global (concorrente) por um worker fixo do proprio canal - entregas
  processadas uma de cada vez, na ordem em que chegaram.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    fpc -Fu..\..\src -Fi..\..\src Retaguarda.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src Retaguarda.dpr

  Callbacks sao `of object` (nao anonimos — ver CLAUDE.md): o estado
  (NotasProntas/Lock) vive em TRetaguardaState, nao em globais de unit. }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ELSE}
  {$APPTYPE CONSOLE}
{$ENDIF}

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  {$ENDIF}
  SysUtils,
  SyncObjs,
  Classes,
  Generics.Collections,
  AMQP.Connection,
  AMQP.Queue.Methods;

const
  QUEUE_NAME = 'sefaz-respostas';

type
  TRetaguardaState = class
  private
    FNotasProntas: TDictionary<string, string>;
    FLock: TCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;
    // Escreve no console protegido pelo lock - varias threads do pool
    // chamando Writeln ao mesmo tempo, sem isso, embaralham a saida
    // (TCriticalSection e' reentrante na mesma thread).
    procedure Log(const S: string);
    // Simula a busca do XML (API ou banco). Ja roda numa thread do pool.
    procedure ProcessarChave(const Chave: string);
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure ImprimeStatus;
  end;

constructor TRetaguardaState.Create;
begin
  inherited Create;
  FNotasProntas := TDictionary<string, string>.Create;
  FLock := TCriticalSection.Create;
end;

destructor TRetaguardaState.Destroy;
begin
  FLock.Free;
  FNotasProntas.Free;
  inherited;
end;

procedure TRetaguardaState.Log(const S: string);
begin
  FLock.Enter;
  try
    Writeln(S);
  finally
    FLock.Leave;
  end;
end;

procedure TRetaguardaState.ProcessarChave(const Chave: string);
var
  Xml: string;
  AtrasoMs: Integer;
begin
  AtrasoMs := 300 + Random(1200);
  Log(Format('[worker %d] iniciando busca do XML da nota %s (~%dms)',
    [TThread.CurrentThread.ThreadID, Chave, AtrasoMs]));
  Sleep(AtrasoMs);

  Xml := Format('<xml da nota %s>', [Chave]);

  FLock.Enter;
  try
    FNotasProntas.AddOrSetValue(Chave, Xml);
  finally
    FLock.Leave;
  end;

  Log(Format('[worker %d] nota %s pronta', [TThread.CurrentThread.ThreadID, Chave]));
end;

// ANoAck=False (padrao do Consume): so confirmamos apos processar - garantia
// "pelo menos uma vez" de fabrica, sem passo extra.
procedure TRetaguardaState.OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
var
  Chave: string;
begin
  Chave := ADelivery.BodyAsText;
  Log('[Retaguarda] retorno recebido da fila: ' + Chave);
  try
    ProcessarChave(Chave);
    AChannel.Ack(ADelivery.DeliveryTag);
  except
    AChannel.Nack(ADelivery.DeliveryTag, True); // requeue em erro
  end;
end;

procedure TRetaguardaState.ImprimeStatus;
var
  Par: TPair<string, string>;
begin
  FLock.Enter;
  try
    Writeln('--- status atual ---');
    if FNotasProntas.Count = 0 then
      Writeln('(nenhuma nota pronta ainda)')
    else
      for Par in FNotasProntas do
        Writeln('  ', Par.Key, ' -> ', Par.Value);
  finally
    FLock.Leave;
  end;
end;

procedure Main;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChannel: TAMQPChannel;
  LState: TRetaguardaState;
  LConsumerTag: string;
  Linha: string;
  LDedicado: Boolean;
begin
  LDedicado := (ParamCount > 0) and SameText(ParamStr(1), '--dedicado');
  LState := TRetaguardaState.Create;
  try
    LParams := TAMQPConnectionParams.Localhost;
    LConn := TAMQPConnection.Create(LParams);
    try
      LConn.Open;
      LChannel := LConn.CreateChannel(LDedicado);
      try
        LChannel.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE_NAME, True));
        LChannel.Qos(10); // prefetch: limita mensagens nao confirmadas em voo

        LConsumerTag := LChannel.Consume(QUEUE_NAME, LState.OnDelivery);

        Writeln('[*] Aguardando retornos na fila "', QUEUE_NAME, '".');
        if LDedicado then
          Writeln('[*] Thread dedicada ativa: entregas processadas em ordem, sem concorrencia.');
        Writeln('[*] Pressione ENTER a qualquer momento pra ver o status (ou digite "sair" + ENTER pra fechar).');

        repeat
          Readln(Linha);
          if SameText(Trim(Linha), 'sair') then
            Break;
          LState.ImprimeStatus;
        until False;

        LChannel.Cancel(LConsumerTag);
      finally
        LChannel.Free;
      end;
    finally
      LConn.Free;
    end;
  finally
    LState.Free;
  end;
end;

begin
  Randomize;
  try
    Main;
  except
    on E: Exception do
      Writeln('Erro: ', E.Message);
  end;
end.

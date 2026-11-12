program Retaguarda;

{ Scenario: POS -> authorizer -> back office. The authorizer (sample
  AutorizadorSim) publishes each invoice's reply to a queue; the back office
  consumes it and "fetches the XML" (here, simulated with a random Sleep), as if
  several POS terminals were waiting for a reply at the same time.

  Difference from the same scenario with other AMQP libs for Delphi:
  this lib's Channel.Consume already dispatches each delivery to its own
  thread pool (PcPool, inside TAMQPChannel.DispatchDelivery) - the callback
  below runs concurrently for different messages without any manual
  dispatch, and the read thread is never blocked waiting for
  ProcessarChave to finish.

  Optional --dedicado argument: uses CreateChannel(True), which swaps the
  global (concurrent) pool for a fixed worker of the channel itself - deliveries
  processed one at a time, in the order they arrived.

  Compiles in both worlds from the SAME source:
    FPC:    fpc -Fu..\..\src -Fi..\..\src Retaguarda.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src Retaguarda.dpr

  Callbacks are `of object` (not anonymous; see CLAUDE.md): the state
  (NotasProntas/Lock) lives in TRetaguardaState, not in unit globals. }

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
    // Writes to the console under the lock - several pool threads
    // calling Writeln at the same time scramble the output without it
    // (TCriticalSection is reentrant on the same thread).
    procedure Log(const S: string);
    // Simulates fetching the XML (API or database). Already runs on a pool thread.
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

// ANoAck=False (the Consume default): we only ack after processing - an
// "at least once" guarantee out of the box, with no extra step.
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
    AChannel.Nack(ADelivery.DeliveryTag, True); // requeue on error
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
        LChannel.Qos(10); // prefetch: caps the unacknowledged messages in flight

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

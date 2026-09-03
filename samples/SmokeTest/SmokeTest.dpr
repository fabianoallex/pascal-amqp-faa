program SmokeTest;

{ Smoke test da pascal-amqp-faa contra um RabbitMQ real (docker/docker-compose.yml).

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    fpc -Fu..\..\src -Fi..\..\src SmokeTest.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src SmokeTest.dpr

  Exercita: conexao + handshake, canal, declare de exchange/fila/bind, publisher
  confirms (publish + WaitForConfirm), BasicGet, Consume com ack manual (callback
  despachado no thread pool), Cancel e teardown limpo. Sai com exit code 0 se
  tudo passou; 1 se algo falhou.

  Aceita --host <nome> e --port <n> para apontar para outro broker (é assim
  que o WS9 o roda contra o broker embutido desta lib, sem que este sample
  passe a depender do sub-módulo server).

  Com o argumento --tls, roda os mesmos passos sobre TLS (localhost:5671 do
  docker-compose.tls.yml, cert self-signed): SChannel no Windows, OpenSSL se
  compilado com -dAMQP_OPENSSL. }

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
  Classes,
  SyncObjs,
  AMQP.Threading,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Transport,
  AMQP.Connection;

const
  EXCHANGE = 'pascal-amqp-faa.smoke';
  QUEUE = 'pascal-amqp-faa.smoke.q';
  ROUTING_KEY = 'smoke';
  CONSUME_COUNT = 5;

type
  { Callbacks sao `of object`: o estado do teste vive nesta classe. }
  TSmoke = class
  private
    FExpected: Integer;
    FReceived: Integer;      // atomico via lock simples do teste
    FLock: TCriticalSection;
    FAllReceived: TEvent;
    FReconnected: TEvent;
    FLastBody: string;
  public
    constructor Create(AExpected: Integer);
    destructor Destroy; override;
    procedure OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    /// Handler de OnReconnect: dispara APOS o recovery completo da topologia —
    /// e' o sinal correto de "pode voltar a publicar" (IsOpen fica True antes).
    procedure OnReconnected(AConnection: TAMQPConnection);
    function WaitAll(ATimeoutMs: Cardinal): Boolean;
    function WaitReconnected(ATimeoutMs: Cardinal): Boolean;
    property LastBody: string read FLastBody;
  end;

constructor TSmoke.Create(AExpected: Integer);
begin
  inherited Create;
  FExpected := AExpected;
  FLock := TCriticalSection.Create;
  FAllReceived := TEvent.Create(nil, True, False, '');
  FReconnected := TEvent.Create(nil, True, False, '');
end;

destructor TSmoke.Destroy;
begin
  FReconnected.Free;
  FAllReceived.Free;
  FLock.Free;
  inherited;
end;

procedure TSmoke.OnReconnected(AConnection: TAMQPConnection);
begin
  FReconnected.SetEvent;
end;

function TSmoke.WaitReconnected(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FReconnected.WaitFor(ATimeoutMs) = wrSignaled;
end;

procedure TSmoke.OnDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
begin
  AChannel.Ack(ADelivery.DeliveryTag);
  FLock.Enter;
  try
    Inc(FReceived);
    FLastBody := ADelivery.BodyAsText;
    if FReceived >= FExpected then
      FAllReceived.SetEvent;
  finally
    FLock.Leave;
  end;
end;

function TSmoke.WaitAll(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FAllReceived.WaitFor(ATimeoutMs) = wrSignaled;
end;

// Parâmetros do teste. Argumentos aceitos, em qualquer ordem:
//   --tls          roda TUDO sobre TLS (localhost:5671, cert self-signed do
//                  docker-compose.tls.yml; SChannel no Windows ou OpenSSL com
//                  -dAMQP_OPENSSL)
//   --host <nome>  aponta para outro host (default: o de Localhost)
//   --port <n>     aponta para outra porta (default: 5672, ou 5671 com --tls)
//
// --host/--port existem para o SmokeTest poder rodar contra QUALQUER broker --
// inclusive o broker embutido desta própria lib, que o runner de aceitação
// (tests\Acceptance\fpc\BrokerHostFpc) sobe numa porta fixa. É o passo do
// WS9 que faltava sem transformar este sample num programa que depende do
// sub-módulo server: ele continua sendo 100% cliente.
function SmokeParams: TAMQPConnectionParams;
var
  I: Integer;
  LArg: string;
begin
  Result := TAMQPConnectionParams.Localhost;
  for I := 1 to ParamCount do
    if SameText(ParamStr(I), '--tls') then
      Result := TAMQPConnectionParams.LocalhostTls;

  I := 1;
  while I <= ParamCount do
  begin
    LArg := ParamStr(I);
    if SameText(LArg, '--host') and (I < ParamCount) then
    begin
      Result.Host := ParamStr(I + 1);
      Inc(I);
    end
    else if SameText(LArg, '--port') and (I < ParamCount) then
    begin
      Result.Port := StrToIntDef(ParamStr(I + 1), Result.Port);
      Inc(I);
    end;
    Inc(I);
  end;
end;

procedure Check(ACondition: Boolean; const AWhat: string);
begin
  if ACondition then
    WriteLn('  ok: ', AWhat)
  else
    raise Exception.Create('FALHOU: ' + AWhat);
end;

// Derruba o socket no meio do consumo e verifica que a lib reconecta sozinha,
// replaya a topologia (fila/bind/qos/confirm/consume) e volta a entregar.
procedure RunReconnectTest;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LSmoke: TSmoke;
  LBind: TAMQPQueueBind;
  LSeqNo: UInt64;
begin
  LParams := SmokeParams;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 500;
  LSmoke := TSmoke.Create(1);
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.OnReconnect := LSmoke.OnReconnected;
    LConn.Open;
    LChan := LConn.CreateChannel;
    LChan.DeclareExchange(TAMQPExchangeDeclare.Create(EXCHANGE));
    LChan.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE));
    LBind := Default(TAMQPQueueBind);
    LBind.QueueName := QUEUE;
    LBind.ExchangeName := EXCHANGE;
    LBind.RoutingKey := ROUTING_KEY;
    LChan.BindQueue(LBind);
    LChan.ConfirmSelect;
    LChan.Consume(QUEUE, LSmoke.OnDelivery);

    LConn.DropConnectionForTest; // queda abrupta simulada

    // Aguarda o OnReconnect (dispara apos handshake + recovery de topologia).
    Check(LSmoke.WaitReconnected(15000), 'reconectou e recuperou a topologia');
    Check(LConn.IsOpen, 'conexao aberta apos reconexao');

    // O canal foi recuperado: publish confirmado e consumer re-armado entregam.
    LSeqNo := LChan.PublishText(EXCHANGE, ROUTING_KEY, 'pos-reconexao');
    Check(LChan.WaitForConfirm(LSeqNo, 5000), 'publish pos-reconexao confirmado');
    Check(LSmoke.WaitAll(10000), 'mensagem entregue ao consumer recuperado');
    Check(LSmoke.LastBody = 'pos-reconexao', 'corpo integro pos-reconexao');

    LChan.Close;
    LChan.Free;
    LConn.Close;
  finally
    LConn.Free;
    LSmoke.Free;
  end;
end;

var
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LSmoke: TSmoke;
  LSeqNo: UInt64;
  LGet: TAMQPGetResult;
  LBind: TAMQPQueueBind;
  LTag: string;
  I: Integer;
begin
  ExitCode := 1;
  LSmoke := nil;
  LConn := TAMQPConnection.Create(SmokeParams);
  try
    try
      WriteLn('[1] conexao + handshake');
      LConn.Open;
      Check(LConn.IsOpen, 'conexao aberta');
      if SmokeParams.UseTls then
        WriteLn('  tls: ', AmqpTlsBackendInfo); // motor carregado de fato

      WriteLn('[2] canal + topologia');
      LChan := LConn.CreateChannel;
      LChan.DeclareExchange(TAMQPExchangeDeclare.Create(EXCHANGE));
      LChan.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE));
      LBind := Default(TAMQPQueueBind);
      LBind.QueueName := QUEUE;
      LBind.ExchangeName := EXCHANGE;
      LBind.RoutingKey := ROUTING_KEY;
      LChan.BindQueue(LBind);
      Check(LChan.IsOpen, 'canal aberto e topologia declarada');

      // Fila limpa para o teste ser deterministico (drena sobras de rodadas
      // anteriores; a lib nao expoe queue.purge no canal por enquanto).
      repeat
        LGet := LChan.BasicGet(QUEUE, True);
      until not LGet.Found;

      WriteLn('[3] publisher confirms');
      LChan.ConfirmSelect;
      LSeqNo := LChan.PublishText(EXCHANGE, ROUTING_KEY, 'ola do pascal-amqp-faa');
      Check(LSeqNo = 1, Format('publish recebeu seq-no 1 (veio %d)', [LSeqNo]));
      Check(LChan.WaitForConfirm(LSeqNo, 5000), 'broker confirmou o publish');

      WriteLn('[4] basic.get');
      LGet := LChan.BasicGet(QUEUE, True);
      Check(LGet.Found, 'mensagem encontrada na fila');
      Check(LGet.BodyAsText = 'ola do pascal-amqp-faa',
        'corpo integro (' + LGet.BodyAsText + ')');

      WriteLn('[5] consume com ack manual (thread pool)');
      LSmoke := TSmoke.Create(CONSUME_COUNT);
      LChan.Qos(10);
      LTag := LChan.Consume(QUEUE, LSmoke.OnDelivery);
      for I := 1 to CONSUME_COUNT do
        LChan.PublishText(EXCHANGE, ROUTING_KEY, Format('msg-%d', [I]));
      Check(LChan.WaitForConfirms(5000), 'todos os publishes confirmados');
      Check(LSmoke.WaitAll(10000),
        Format('%d mensagens entregues ao consumer', [CONSUME_COUNT]));
      LChan.Cancel(LTag);

      WriteLn('[6] teardown');
      LChan.Close;
      LChan.Free;
      LConn.Close;
      Check(not LConn.IsOpen, 'conexao fechada');

      WriteLn('[7] reconexao automatica + recovery de topologia');
      RunReconnectTest;

      WriteLn;
      WriteLn('SMOKE TEST: PASS');
      ExitCode := 0;
    except
      on E: Exception do
      begin
        WriteLn;
        WriteLn('SMOKE TEST: FAIL — ', E.ClassName, ': ', E.Message);
        ExitCode := 1;
      end;
    end;
  finally
    LSmoke.Free;
    LConn.Free;
  end;
end.

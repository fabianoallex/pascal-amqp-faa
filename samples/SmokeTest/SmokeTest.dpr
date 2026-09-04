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
  Rtti,
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
  // Fase 3: TTL+DLX e prioridade. Rodam contra o RabbitMQ E contra o broker
  // embutido -- e' a prova dupla de que a semantica implementada e' a da spec.
  DLX = 'pascal-amqp-faa.smoke.dlx';
  QUEUE_ESPERA = 'pascal-amqp-faa.smoke.espera';
  QUEUE_MORTA = 'pascal-amqp-faa.smoke.morta';
  QUEUE_PRIO = 'pascal-amqp-faa.smoke.prio';

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

// Espera ate' ACount mensagens aparecerem na fila, ou desiste no prazo.
// Sem isto seria Sleep fixo -- e o tempo de um TTL vencer mais o dead-letter
// atravessar duas filas varia entre o RabbitMQ e o broker embutido.
function EsperaContagem(AChan: TAMQPChannel; const AQueue: string;
  ACount: Cardinal; ATimeoutMs: Cardinal): Boolean;
var
  LDecl: TAMQPQueueDeclare;
  LFim: UInt64;
begin
  LFim := AmqpTickMs + ATimeoutMs;
  repeat
    LDecl := TAMQPQueueDeclare.Create(AQueue);
    LDecl.Passive := True; // passive: so' consulta, nao redeclara
    if AChan.DeclareQueue(LDecl).MessageCount >= ACount then
      Exit(True);
    Sleep(25);
  until AmqpTickMs > LFim;
  Result := False;
end;

// TTL + dead-letter: a mensagem vence na fila de espera e o broker a republica
// no DLX, de onde ela cai na fila final. E' o padrao retry-com-espera do
// sample RetryDlqVcl, reduzido ao essencial.
//
// Roda IGUAL contra o RabbitMQ e contra o broker embutido -- e' esse o ponto:
// os mesmos passos contra os dois dizem que a semantica implementada e' a da
// spec, e nao a nossa.
procedure PassoTtlDlx(AChan: TAMQPChannel);
var
  LArgs: TAMQPFieldTable;
  LDecl: TAMQPQueueDeclare;
  LBind: TAMQPQueueBind;
  LGet: TAMQPGetResult;
  LDel: TAMQPQueueDelete;
  LVal: TValue;
begin
  AChan.DeclareExchange(TAMQPExchangeDeclare.Create(DLX));
  // Apagar antes: redeclarar com argumentos diferentes seria
  // PRECONDITION_FAILED, e apagar fila inexistente e' no-op.
  LDel := Default(TAMQPQueueDelete);
  LDel.QueueName := QUEUE_ESPERA;
  AChan.DeleteQueue(LDel);
  LDel.QueueName := QUEUE_MORTA;
  AChan.DeleteQueue(LDel);

  // Fila final, ligada ao DLX.
  AChan.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE_MORTA));
  LBind := Default(TAMQPQueueBind);
  LBind.QueueName := QUEUE_MORTA;
  LBind.ExchangeName := DLX;
  // A chave e' o NOME DA FILA DE ESPERA, e nao ROUTING_KEY: sem
  // x-dead-letter-routing-key a mensagem morta mantem a chave ORIGINAL, e a
  // dela e' o nome da fila (foi publicada no exchange default). Amarrar isso
  // aqui e' o que faz este passo exercitar a regra de preservacao da chave,
  // em vez de mascara-la com uma chave fixa dos dois lados.
  LBind.RoutingKey := QUEUE_ESPERA;
  AChan.BindQueue(LBind);

  // Fila de espera: TTL curto + DLX. Sem consumidor -- e' o TTL que move.
  LArgs := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<Int64>(300);
    LArgs.Put('x-message-ttl', LVal);
    LVal := TValue.From<string>(DLX);
    LArgs.Put('x-dead-letter-exchange', LVal);
    LDecl := TAMQPQueueDeclare.Create(QUEUE_ESPERA);
    LDecl.Arguments := LArgs;
    AChan.DeclareQueue(LDecl);
  finally
    LArgs.Free; // o declare NAO libera: o dono e' o chamador
  end;

  AChan.PublishText('', QUEUE_ESPERA, 'vai-morrer');
  Check(EsperaContagem(AChan, QUEUE_MORTA, 1, 10000),
    'mensagem venceu o TTL e caiu na fila do DLX');

  LGet := AChan.BasicGet(QUEUE_MORTA, True);
  Check(LGet.Found, 'mensagem encontrada na fila morta');
  Check(LGet.BodyAsText = 'vai-morrer', 'corpo integro apos o dead-letter');
  Check(LGet.Properties.Has(bpHeaders), 'a mensagem morta ganhou headers');
  Check(LGet.Properties.Headers.ContainsKey('x-death'),
    'header x-death presente');
end;

// Prioridade: com o consumidor parado, um lote de prioridades misturadas sai
// em ordem DECRESCENTE. As duas condicoes que fazem isso valer estao aqui:
// backlog formado ANTES de qualquer leitura, e leitura uma a uma por Get.
procedure PassoPrioridade(AChan: TAMQPChannel);
var
  LArgs: TAMQPFieldTable;
  LDecl: TAMQPQueueDeclare;
  LGet: TAMQPGetResult;
  LVal: TValue;
  LProps: TAMQPBasicProperties;
  LDel: TAMQPQueueDelete;
  I, LAnterior, LAtual: Integer;
  LOrdem: string;
const
  PRIOS: array[0..5] of Byte = (2, 8, 5, 9, 1, 7);
begin
  LDel := Default(TAMQPQueueDelete);
  LDel.QueueName := QUEUE_PRIO;
  AChan.DeleteQueue(LDel);
  LArgs := TAMQPFieldTable.Create;
  try
    LVal := TValue.From<Int64>(9);
    LArgs.Put('x-max-priority', LVal);
    LDecl := TAMQPQueueDeclare.Create(QUEUE_PRIO);
    LDecl.Arguments := LArgs;
    AChan.DeclareQueue(LDecl);
  finally
    LArgs.Free;
  end;

  for I := Low(PRIOS) to High(PRIOS) do
  begin
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetPriority(PRIOS[I]);
    // PublishText nao leva propriedades: prioridade exige o Publish completo.
    LProps.SetContentType('text/plain');
    AChan.Publish('', QUEUE_PRIO,
      AmqpUtf8Encode('p' + IntToStr(PRIOS[I])), LProps);
  end;
  Check(EsperaContagem(AChan, QUEUE_PRIO, Length(PRIOS), 10000),
    'lote de prioridades enfileirado');

  LOrdem := '';
  LAnterior := 999;
  for I := Low(PRIOS) to High(PRIOS) do
  begin
    LGet := AChan.BasicGet(QUEUE_PRIO, True);
    if not LGet.Found then
      Break;
    LAtual := LGet.Properties.Priority;
    LOrdem := LOrdem + IntToStr(LAtual) + ' ';
    Check(LAtual <= LAnterior,
      'ordem decrescente de prioridade (saiu: ' + LOrdem + ')');
    LAnterior := LAtual;
  end;
  Check(LOrdem = '9 8 7 5 2 1 ', 'drenou em ordem de prioridade: ' + LOrdem);
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

      WriteLn('[6] ttl + dead-letter (Fase 3)');
      PassoTtlDlx(LChan);

      WriteLn('[7] fila com prioridade (Fase 3)');
      PassoPrioridade(LChan);

      WriteLn('[8] teardown');
      LChan.Close;
      LChan.Free;
      LConn.Close;
      Check(not LConn.IsOpen, 'conexao fechada');

      WriteLn('[9] reconexao automatica + recovery de topologia');
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

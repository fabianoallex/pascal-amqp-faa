unit AMQP.ReconnectIntegrationTests;

{$mode delphi}{$H+}

{ GENERATED FILE — produced by tools/gen_fpc_mirror.py from
  tests/Integration/AMQP.ReconnectIntegrationTests.pas (DUnitX). Do not edit by hand: edit the DUnitX
  master and run the script again. }

{ Automatic reconnection integration — needs RabbitMQ on localhost:5672.
  Simulates a drop (closes the socket), waits for the auto-reconnection +
  topology recovery (redeclares the queue and re-consumes) and checks that the
  consumer receives again messages published by a separate control connection. }


interface

uses
  fpcunit, testregistry,
  SysUtils, Classes, Generics.Collections,
  PascalCommon.Threading,
  AMQP.Connection,
  AMQP.IntegrationConfig,
  AMQP.Queue.Methods;

type
  TAMQPReconnectIntegrationTests = class(TTestCase)
  private
    FConsumerConn: TAMQPConnection;
    FConsumerChan: TAMQPChannel;
    FControlConn: TAMQPConnection;
    FControlChan: TAMQPChannel;
    FReceived: TThreadList<string>;
    FReconnected: Integer;
    FQueue: string;
    // Fields dedicated to the tests that open their own auto-reconnect
    // connection (they do not use the Setup's FConsumerConn) — closures are not
    // accepted by the library's 'of object' callbacks (see CLAUDE.md), so what
    // used to be a captured local variable now has to be a fixture field.
    // TInterlocked does not exist in FPC -> PcAtomic*.
    FDropTestReconnected: Integer;
    FRepublishTestReconnected: Integer;
    // Tests of closing during reconnection: drops noticed (OnDisconnect) and
    // Close calls made from inside OnDisconnect that have already returned.
    FCloseTestDisconnected: Integer;
    FSelfCloseDone: Integer;
    // Tests of cross-channel replay: completed reconnections and the attempt
    // failures reported by OnReconnectError ('n: reason').
    FCrossReconnected: Integer;
    FReconnectErrors: TThreadList<string>;
    function ReceivedContains(const AText: string): Boolean;
    procedure WaitReceived(const AText: string; ATimeoutMs: Integer);
    procedure WaitReconnected(ATimeoutMs: Integer);
    procedure HandleConsumerReconnect(AConnection: TAMQPConnection);
    procedure HandleConsumerDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure HandleDropTestReconnect(AConnection: TAMQPConnection);
    procedure HandleRepublishTestReconnect(AConnection: TAMQPConnection);
    procedure HandleCloseTestDisconnect(AConnection: TAMQPConnection);
    procedure HandleSelfCloseDisconnect(AConnection: TAMQPConnection);
    function NewReconnectingConnection(ADelayMs: Cardinal;
      AOnDisconnect: TAMQPConnectionEvent): TAMQPConnection;
    procedure WaitCounter(var ACounter: Integer; ATimeoutMs: Integer);
    procedure HandleCrossReconnect(AConnection: TAMQPConnection);
    procedure HandleReconnectError(AConnection: TAMQPConnection; AAttempt: Integer;
      const AError: string);
    function ReconnectErrorContaining(const AText: string): string;
    procedure WaitFor404(var AError: string; ATimeoutMs: Integer);
    function PublishError(AChannel: TAMQPChannel; const AQueue: string): string;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure Reconecta_E_ContinuaConsumindo;
    procedure Confirm_PerdaNaQueda_WaitForConfirmsRetornaFalse;
    procedure Confirm_Republish_ReenviaNaoConfirmadosAposQueda;
    procedure Close_DuranteEsperaDeReconexao_NaoEsperaODelay;
    procedure Close_ComReconnectDelayAlto_VoltaComAThreadEncerrada;
    procedure Close_DeDentroDoOnDisconnect_NaoTrava;
    procedure Reconexao_FilaDeclaradaSoNoutroCanal_Recupera;
    procedure Reconexao_ReplayFalha_ReportaOMotivo_E_RecuperaQuandoAFilaVolta;
    procedure SemAutoReconnect_PublishNaConexaoMorta_LevantaEAMQPConnection;
  end;

implementation

procedure TAMQPReconnectIntegrationTests.SetUp;
var
  LParams: TAMQPConnectionParams;
  LDecl: TAMQPQueueDeclare;
begin
  FReceived := TThreadList<string>.Create;
  FReconnected := 0;
  FDropTestReconnected := 0;
  FRepublishTestReconnected := 0;
  FCloseTestDisconnected := 0;
  FSelfCloseDone := 0;
  FCrossReconnected := 0;
  FReconnectErrors := TThreadList<string>.Create;
  FQueue := 'test-recon-' + IntToStr(PcTickMs);

  // Control connection (no auto-reconnect) just for publishing.
  FControlConn := TAMQPConnection.Create(IntegrationParams);
  FControlConn.Open;
  FControlChan := FControlConn.CreateChannel;

  // Consumer connection with auto-reconnection.
  LParams := IntegrationParams;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 500;
  LParams.ConnectionName := 'pascal-amqp-faa-recon-test-fpc';
  FConsumerConn := TAMQPConnection.Create(LParams);
  FConsumerConn.OnReconnect := HandleConsumerReconnect;
  FConsumerConn.Open;
  FConsumerChan := FConsumerConn.CreateChannel;

  // Named, auto-delete queue (goes away when the consumer drops; recovery
  // redeclares it). Declared on the consumer channel -> recorded for recovery.
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.QueueName := FQueue;
  LDecl.AutoDelete := True;
  FConsumerChan.DeclareQueue(LDecl);

  FConsumerChan.Consume(FQueue, HandleConsumerDelivery);
end;

procedure TAMQPReconnectIntegrationTests.TearDown;
begin
  FConsumerChan.Free; // cancels the consumer -> the auto-delete queue goes away
  FConsumerConn.Free;
  FControlChan.Free;
  FControlConn.Free;
  FReceived.Free;
  FReconnectErrors.Free;
end;

procedure TAMQPReconnectIntegrationTests.HandleConsumerReconnect(AConnection: TAMQPConnection);
begin
  PcAtomicSet(FReconnected, 1);
end;

procedure TAMQPReconnectIntegrationTests.HandleConsumerDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  FReceived.Add(ADelivery.BodyAsText);
  AChannel.Ack(ADelivery.DeliveryTag);
end;

procedure TAMQPReconnectIntegrationTests.HandleDropTestReconnect(AConnection: TAMQPConnection);
begin
  PcAtomicSet(FDropTestReconnected, 1);
end;

procedure TAMQPReconnectIntegrationTests.HandleRepublishTestReconnect(AConnection: TAMQPConnection);
begin
  PcAtomicSet(FRepublishTestReconnected, 1);
end;

procedure TAMQPReconnectIntegrationTests.HandleCloseTestDisconnect(AConnection: TAMQPConnection);
begin
  PcAtomicInc(FCloseTestDisconnected);
end;

procedure TAMQPReconnectIntegrationTests.HandleSelfCloseDisconnect(AConnection: TAMQPConnection);
begin
  // Runs ON the reconnection thread: Close cannot join its own thread.
  AConnection.Close;
  PcAtomicInc(FSelfCloseDone);
end;

function TAMQPReconnectIntegrationTests.NewReconnectingConnection(ADelayMs: Cardinal;
  AOnDisconnect: TAMQPConnectionEvent): TAMQPConnection;
var
  LParams: TAMQPConnectionParams;
begin
  LParams := IntegrationParams;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := ADelayMs;
  Result := TAMQPConnection.Create(LParams);
  Result.OnDisconnect := AOnDisconnect;
end;

procedure TAMQPReconnectIntegrationTests.WaitCounter(var ACounter: Integer;
  ATimeoutMs: Integer);
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (PcAtomicGet(ACounter) = 0) and (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
end;

function TAMQPReconnectIntegrationTests.ReceivedContains(const AText: string): Boolean;
var
  LList: TList<string>;
begin
  LList := FReceived.LockList;
  try
    Result := LList.IndexOf(AText) >= 0;
  finally
    FReceived.UnlockList;
  end;
end;

procedure TAMQPReconnectIntegrationTests.WaitReceived(const AText: string;
  ATimeoutMs: Integer);
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (not ReceivedContains(AText)) and (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(50);
    Inc(LWaited, 50);
  end;
end;

procedure TAMQPReconnectIntegrationTests.WaitReconnected(ATimeoutMs: Integer);
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (PcAtomicGet(FReconnected) = 0) and (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(50);
    Inc(LWaited, 50);
  end;
end;

procedure TAMQPReconnectIntegrationTests.Reconecta_E_ContinuaConsumindo;
begin
  // 1) Publishes before the drop and confirms consumption.
  FControlChan.PublishText('', FQueue, 'antes-da-queda');
  WaitReceived('antes-da-queda', 5000);
  TAssert.AssertTrue('deveria consumir antes da queda', ReceivedContains('antes-da-queda'));

  // 2) Simulates the network drop.
  FConsumerConn.DropConnectionForTest;

  // 3) Waits for the auto-reconnection + recovery (OnReconnect fires after
  //    redeclaring the queue and re-consuming).
  WaitReconnected(15000);
  TAssert.AssertEquals('deveria ter reconectado', 1, PcAtomicGet(FReconnected));
  TAssert.AssertTrue('conexão deveria estar aberta após reconectar', FConsumerConn.IsOpen);

  // 4) Publishes after the recovery and confirms consumption is back.
  //    15s: right after recovery the broker/channel may still be settling
  //    (it flaked with 5s under load — broker shared with another suite).
  FControlChan.PublishText('', FQueue, 'depois-da-recuperacao');
  WaitReceived('depois-da-recuperacao', 15000);
  TAssert.AssertTrue('deveria voltar a consumir após a reconexão',
    ReceivedContains('depois-da-recuperacao'));
end;

procedure TAMQPReconnectIntegrationTests.Confirm_PerdaNaQueda_WaitForConfirmsRetornaFalse;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  I, LWaited: Integer;
begin
  // Its own connection (isolated from the Setup's fixtures) with auto-reconnection.
  FDropTestReconnected := 0;
  LParams := IntegrationParams;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 500;
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.OnReconnect := HandleDropTestReconnect;
    LConn.Open;
    LChan := LConn.CreateChannel;
    LChan.ConfirmSelect;

    // Publishes a batch and drops IMMEDIATELY, before the acks arrive: the
    // publishes stay pending and the drop marks them as unconfirmed
    // (FailAllUnconfirmed -> FNacked).
    for I := 1 to 200 do
      LChan.PublishText('', 'confirm-drop-test', 'x');
    LConn.DropConnectionForTest;

    // Waits for the auto-reconnection + recovery (re-arms confirm mode).
    LWaited := 0;
    while (PcAtomicGet(FDropTestReconnected) = 0) and (LWaited < 15000) do
    begin
      TThread.Sleep(50);
      Inc(LWaited, 50);
    end;
    TAssert.AssertEquals('deveria ter reconectado', 1, PcAtomicGet(FDropTestReconnected));

    // The publishes pending at the drop were LOST: WaitForConfirms must
    // report False. (Bug: Recover cleared FNacked and this returned True.)
    TAssert.AssertFalse('WaitForConfirms deve reportar False para publishes perdidos na queda',
      LChan.WaitForConfirms(3000));
  finally
    LConn.Free;
  end;
end;

procedure TAMQPReconnectIntegrationTests.Confirm_Republish_ReenviaNaoConfirmadosAposQueda;
var
  LPubParams: TAMQPConnectionParams;
  LPubConn: TAMQPConnection;
  LPubChan: TAMQPChannel;
  LCtrlChan: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LQueue: string;
  I, LWaited, LCount: Integer;
  LGet: TAMQPGetResult;
begin
  // The queue is declared by the Setup's CONTROL connection (FControlConn),
  // exclusive to it — it goes away on teardown. Since it does not belong to the
  // publishing connection, it survives that connection's drop and accumulates
  // the messages (the publisher publishes through the default exchange).
  LCtrlChan := FControlChan;
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;
  LQueue := LCtrlChan.DeclareQueue(LDecl).QueueName;

  // Publishing connection: auto-reconnect + resending of unconfirmed ones.
  FRepublishTestReconnected := 0;
  LPubParams := IntegrationParams;
  LPubParams.AutoReconnect := True;
  LPubParams.ReconnectDelayMs := 500;
  LPubParams.RepublishUnconfirmedOnReconnect := True;
  LPubConn := TAMQPConnection.Create(LPubParams);
  try
    LPubConn.OnReconnect := HandleRepublishTestReconnect;
    LPubConn.Open;
    LPubChan := LPubConn.CreateChannel;
    LPubChan.ConfirmSelect;

    // Publishes a batch and drops IMMEDIATELY: most stay unconfirmed and
    // depend on the resend to reach the queue.
    for I := 1 to 100 do
      LPubChan.PublishText('', LQueue, 'msg-' + IntToStr(I));
    LPubConn.DropConnectionForTest;

    // Waits for the reconnection (which triggers the resend of the unconfirmed ones).
    LWaited := 0;
    while (PcAtomicGet(FRepublishTestReconnected) = 0) and (LWaited < 15000) do
    begin
      TThread.Sleep(50);
      Inc(LWaited, 50);
    end;
    TAssert.AssertEquals('deveria ter reconectado', 1, PcAtomicGet(FRepublishTestReconnected));

    // Drains through the control connection: all 100 must arrive (at-least-once —
    // there may be duplicates of messages whose ack was lost in the drop, hence >= 100).
    LCount := 0;
    LWaited := 0;
    while (LCount < 100) and (LWaited < 10000) do
    begin
      LGet := LCtrlChan.BasicGet(LQueue, True);
      if LGet.Found then
        Inc(LCount)
      else
      begin
        TThread.Sleep(50);
        Inc(LWaited, 50);
      end;
    end;
    TAssert.AssertTrue(Format('reenvio deveria entregar >= 100 msgs; entregou %d', [LCount]),
      LCount >= 100);
  finally
    LPubChan.Free;
    LPubConn.Free;
  end;
end;

procedure TAMQPReconnectIntegrationTests.Close_DuranteEsperaDeReconexao_NaoEsperaODelay;
var
  LConn: TAMQPConnection;
  LInicio, LGasto: UInt64;
begin
  // The connection drops and the reconnection enters the ReconnectDelayMs
  // wait (OnDisconnect fires right before it). Closing there has to be
  // immediate: Close wakes the wait, it does not wait for it to expire. It
  // used to be a Sleep, and Free took the whole delay (measured by
  // pascal-dfe-broker: 2.0-2.1 s with 2000 ms).
  LConn := NewReconnectingConnection(3000, HandleCloseTestDisconnect);
  try
    LConn.Open;
    LConn.DropConnectionForTest;
    WaitCounter(FCloseTestDisconnected, 10000);
    TAssert.AssertEquals('a queda foi percebida', 1, PcAtomicGet(FCloseTestDisconnected));
    LInicio := PcTickMs;
    FreeAndNil(LConn);
    LGasto := PcTickMs - LInicio;
  finally
    LConn.Free;
  end;
  TAssert.AssertTrue('fechar na espera da reconexão não espera os 3000 ms (' + IntToStr(LGasto) + ' ms)', LGasto < 1000);
  TAssert.AssertEquals('a thread de reconexão saiu', 0, AmqpReconnectThreadsAlive);
end;

procedure TAMQPReconnectIntegrationTests.Close_ComReconnectDelayAlto_VoltaComAThreadEncerrada;
var
  LConn: TAMQPConnection;
  LInicio, LGasto: UInt64;
begin
  // A delay larger than WaitReconnectStopped's old 12 s cap: before, Close
  // gave up waiting with the reconnection thread still sleeping, and it woke
  // up reading an already freed object. Now Close joins it: it returns quickly
  // AND with the thread finished.
  LConn := NewReconnectingConnection(60000, HandleCloseTestDisconnect);
  try
    LConn.Open;
    LConn.DropConnectionForTest;
    WaitCounter(FCloseTestDisconnected, 10000);
    TAssert.AssertEquals('a queda foi percebida', 1, PcAtomicGet(FCloseTestDisconnected));
    LInicio := PcTickMs;
    FreeAndNil(LConn);
    LGasto := PcTickMs - LInicio;
  finally
    LConn.Free;
  end;
  TAssert.AssertEquals('Close/Destroy só voltam com a thread de reconexão encerrada', 0, AmqpReconnectThreadsAlive);
  TAssert.AssertTrue('e voltam logo (' + IntToStr(LGasto) + ' ms)', LGasto < 1000);
end;

procedure TAMQPReconnectIntegrationTests.Close_DeDentroDoOnDisconnect_NaoTrava;
var
  LConn: TAMQPConnection;
  LInicio, LGasto: UInt64;
begin
  // OnDisconnect runs on the reconnection thread. A Close called from there
  // cannot wait for itself: before, it got stuck on the 12 s cap. Now it
  // returns at once, the reconnection does not happen, and Free (from another
  // thread) joins the thread.
  LConn := NewReconnectingConnection(60000, HandleSelfCloseDisconnect);
  try
    LConn.Open;
    LInicio := PcTickMs;
    LConn.DropConnectionForTest;
    WaitCounter(FSelfCloseDone, 10000);
    LGasto := PcTickMs - LInicio;
    TAssert.AssertEquals('o Close de dentro do OnDisconnect voltou', 1, PcAtomicGet(FSelfCloseDone));
    TAssert.AssertTrue('e voltou logo (' + IntToStr(LGasto) + ' ms)', LGasto < 3000);
    TAssert.AssertTrue('a conexão ficou fechada', not LConn.IsOpen);
  finally
    LConn.Free;
  end;
  TAssert.AssertEquals('a thread de reconexão saiu', 0, AmqpReconnectThreadsAlive);
end;

procedure TAMQPReconnectIntegrationTests.HandleCrossReconnect(AConnection: TAMQPConnection);
begin
  PcAtomicInc(FCrossReconnected);
end;

procedure TAMQPReconnectIntegrationTests.HandleReconnectError(AConnection: TAMQPConnection;
  AAttempt: Integer; const AError: string);
begin
  FReconnectErrors.Add(IntToStr(AAttempt) + ': ' + AError);
end;

function TAMQPReconnectIntegrationTests.ReconnectErrorContaining(const AText: string): string;
var
  LList: TList<string>;
  I: Integer;
begin
  Result := '';
  LList := FReconnectErrors.LockList;
  try
    for I := 0 to LList.Count - 1 do
      if Pos(AText, LList[I]) > 0 then
        Exit(LList[I]);
  finally
    FReconnectErrors.UnlockList;
  end;
end;

procedure TAMQPReconnectIntegrationTests.WaitFor404(var AError: string; ATimeoutMs: Integer);
var
  LWaited: Integer;
begin
  LWaited := 0;
  AError := ReconnectErrorContaining('404');
  while (AError = '') and (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
    AError := ReconnectErrorContaining('404');
  end;
end;

function TAMQPReconnectIntegrationTests.PublishError(AChannel: TAMQPChannel;
  const AQueue: string): string;
begin
  // 'Class: message' of the publish's exception, or '' if it went through.
  Result := '';
  try
    AChannel.PublishText('', AQueue, 'na-conexao-morta');
  except
    on E: Exception do
      Result := E.ClassName + ': ' + E.Message;
  end;
end;

procedure TAMQPReconnectIntegrationTests.Reconexao_FilaDeclaradaSoNoutroCanal_Recupera;
var
  LConn: TAMQPConnection;
  LPub, LConf, LCons: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LQueue: string;
begin
  // The scenario measured in pascal-api-infra-faa's messaging guide: one
  // channel declares the queue and publishes, another only consumes. The queue
  // goes away in the drop (auto-delete here; non-durable on the broker restart
  // there). Before, the reconnection replayed channel by channel in dictionary
  // order — channel 3 first —, the basic.consume got a 404 before channel 1's
  // declare came back and the attempt failed the same way forever, with no
  // signal at all.
  LQueue := 'test-recon-cross-' + IntToStr(PcTickMs);
  LConn := NewReconnectingConnection(500, nil);
  try
    LConn.OnReconnect := HandleCrossReconnect;
    LConn.Open;
    LPub := LConn.CreateChannel;   // channel 1: declares and publishes
    LConf := LConn.CreateChannel;  // channel 2: confirm mode, as in the sample there
    LConf.ConfirmSelect;
    LCons := LConn.CreateChannel;  // channel 3: only consumes
    LDecl := Default(TAMQPQueueDeclare);
    LDecl.QueueName := LQueue;
    LDecl.AutoDelete := True;
    LPub.DeclareQueue(LDecl);
    LCons.Consume(LQueue, HandleConsumerDelivery);

    LPub.PublishText('', LQueue, 'cruzado-antes');
    WaitReceived('cruzado-antes', 5000);
    TAssert.AssertTrue('deveria consumir antes da queda', ReceivedContains('cruzado-antes'));

    LConn.DropConnectionForTest;
    WaitCounter(FCrossReconnected, 15000);
    TAssert.AssertEquals('deveria ter reconectado (última falha: ' + LConn.LastReconnectError + ')', 1, PcAtomicGet(FCrossReconnected));
    TAssert.AssertEquals('reconexão bem-sucedida zera o motivo', '', LConn.LastReconnectError);

    LPub.PublishText('', LQueue, 'cruzado-depois');
    WaitReceived('cruzado-depois', 15000);
    TAssert.AssertTrue('o consumer do canal 3 deveria voltar a receber', ReceivedContains('cruzado-depois'));
  finally
    LConn.Free; // frees the three channels
  end;
end;

procedure TAMQPReconnectIntegrationTests.Reconexao_ReplayFalha_ReportaOMotivo_E_RecuperaQuandoAFilaVolta;
var
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LQueue, LErro, LPubErro: string;
begin
  // The queue belongs to the control connection (auto-delete) and the
  // reconnecting connection only consumes it: in the drop it goes away, and
  // nothing on this connection redeclares it. The replay CANNOT succeed — what
  // is required is that the failure shows up (callback, reason and the
  // publish exception's text) and that the reconnection comes back on its own
  // when the queue comes back.
  LQueue := 'test-recon-missing-' + IntToStr(PcTickMs);
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.QueueName := LQueue;
  LDecl.AutoDelete := True;
  FControlChan.DeclareQueue(LDecl);

  LConn := NewReconnectingConnection(1000, nil);
  try
    LConn.OnReconnect := HandleCrossReconnect;
    LConn.OnReconnectError := HandleReconnectError;
    LConn.Open;
    LChan := LConn.CreateChannel;
    LChan.Consume(LQueue, HandleConsumerDelivery);

    LConn.DropConnectionForTest;
    WaitFor404(LErro, 15000);
    TAssert.AssertTrue('OnReconnectError deveria trazer o 404 do replay', Pos('404', LErro) > 0);
    TAssert.AssertEquals('sem a fila, a reconexão não pode se dar por concluída', 0, PcAtomicGet(FCrossReconnected));
    TAssert.AssertTrue('LastReconnectError: ' + LConn.LastReconnectError, Pos('404', LConn.LastReconnectError) > 0);

    // Right after an attempt fails there are ReconnectDelayMs until the next
    // one: the publish lands on the unavailable connection and the exception
    // says why.
    LPubErro := PublishError(LChan, LQueue);
    TAssert.AssertTrue('publish durante a reconexão: ' + LPubErro, Pos('EAMQPConnection', LPubErro) = 1);
    TAssert.AssertTrue('a exceção do publish deveria trazer o motivo: ' + LPubErro, Pos('404', LPubErro) > 0);

    // The queue comes back (another process redeclares it, in the real
    // world): the next attempt recovers.
    FControlChan.DeclareQueue(LDecl);
    WaitCounter(FCrossReconnected, 15000);
    TAssert.AssertEquals('deveria reconectar quando a fila voltasse (última falha: ' + LConn.LastReconnectError + ')', 1, PcAtomicGet(FCrossReconnected));

    FControlChan.PublishText('', LQueue, 'fila-voltou');
    WaitReceived('fila-voltou', 15000);
    TAssert.AssertTrue('o consumer deveria voltar a receber', ReceivedContains('fila-voltou'));
  finally
    LConn.Free;
  end;
end;

procedure TAMQPReconnectIntegrationTests.SemAutoReconnect_PublishNaConexaoMorta_LevantaEAMQPConnection;
var
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LErro: string;
  LWaited: Integer;
begin
  // Without AutoReconnect, a publish on a dead connection raised the RTL's
  // EWriteError ("Stream write error"), not the documented EAMQPConnection.
  // Two moments: right after the drop (the read thread may not have noticed
  // yet: the socket write fails) and after it notices.
  LConn := TAMQPConnection.Create(IntegrationParams);
  try
    LConn.Open;
    LChan := LConn.CreateChannel;
    LChan.PublishText('', 'test-sem-reconexao', 'viva');

    LConn.DropConnectionForTest;
    LErro := PublishError(LChan, 'test-sem-reconexao');
    TAssert.AssertTrue('logo após a queda: ' + LErro, Pos('EAMQPConnection', LErro) = 1);

    LWaited := 0;
    while LConn.IsOpen and (LWaited < 5000) do
    begin
      TThread.Sleep(20);
      Inc(LWaited, 20);
    end;
    TAssert.AssertTrue('a queda deveria ter sido percebida', not LConn.IsOpen);
    LErro := PublishError(LChan, 'test-sem-reconexao');
    TAssert.AssertTrue('depois de percebida: ' + LErro, Pos('EAMQPConnection', LErro) = 1);
    TAssert.AssertTrue('a exceção diz que a conexão caiu: ' + LErro, Pos('connection closed', LErro) > 0);
  finally
    LConn.Free;
  end;
end;

initialization
  RegisterTest(TAMQPReconnectIntegrationTests);

end.

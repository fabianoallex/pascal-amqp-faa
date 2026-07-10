unit AMQP.ReconnectIntegrationTests;

{ Integração de reconexão automática — precisa de RabbitMQ em localhost:5672.
  Simula queda (fecha o socket), aguarda a auto-reconexão + recuperação de
  topologia (redeclara a fila e re-consome) e confere que o consumer volta a
  receber mensagens publicadas por uma conexão de controle separada. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  AMQP.Connection,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPReconnectIntegrationTests = class
  private
    FConsumerConn: TAMQPConnection;
    FConsumerChan: TAMQPChannel;
    FControlConn: TAMQPConnection;
    FControlChan: TAMQPChannel;
    FReceived: TThreadList<string>;
    FReconnected: Integer;
    FQueue: string;
    // Campos dedicados aos testes que abrem sua própria conexão auto-reconnect
    // (não usam a FConsumerConn do Setup) — closures não são aceitas pelos
    // callbacks 'of object' da lib (ver CLAUDE.md), então o que antes era
    // variável local capturada agora precisa ser campo da fixture.
    FDropTestReconnected: Integer;
    FRepublishTestReconnected: Integer;
    function ReceivedContains(const AText: string): Boolean;
    procedure WaitReceived(const AText: string; ATimeoutMs: Integer);
    procedure WaitReconnected(ATimeoutMs: Integer);
    procedure HandleConsumerReconnect(AConnection: TAMQPConnection);
    procedure HandleConsumerDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure HandleDropTestReconnect(AConnection: TAMQPConnection);
    procedure HandleRepublishTestReconnect(AConnection: TAMQPConnection);
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure Reconecta_E_ContinuaConsumindo;
    [Test] procedure Confirm_PerdaNaQueda_WaitForConfirmsRetornaFalse;
    [Test] procedure Confirm_Republish_ReenviaNaoConfirmadosAposQueda;
  end;

implementation

procedure TAMQPReconnectIntegrationTests.Setup;
var
  LParams: TAMQPConnectionParams;
  LDecl: TAMQPQueueDeclare;
begin
  FReceived := TThreadList<string>.Create;
  FReconnected := 0;
  FDropTestReconnected := 0;
  FRepublishTestReconnected := 0;
  FQueue := 'test-recon-' + IntToStr(TThread.GetTickCount64);

  // Conexão de controle (sem auto-reconnect) só para publicar.
  FControlConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  FControlConn.Open;
  FControlChan := FControlConn.CreateChannel;

  // Conexão consumidora com auto-reconexão.
  LParams := TAMQPConnectionParams.Localhost;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 500;
  LParams.ConnectionName := 'pascal-amqp-faa-recon-test';
  FConsumerConn := TAMQPConnection.Create(LParams);
  FConsumerConn.OnReconnect := HandleConsumerReconnect;
  FConsumerConn.Open;
  FConsumerChan := FConsumerConn.CreateChannel;

  // Fila nomeada, auto-delete (some quando o consumidor cai; a recuperação a
  // redeclara). Declarada no canal consumidor -> gravada para recuperação.
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.QueueName := FQueue;
  LDecl.AutoDelete := True;
  FConsumerChan.DeclareQueue(LDecl);

  FConsumerChan.Consume(FQueue, HandleConsumerDelivery);
end;

procedure TAMQPReconnectIntegrationTests.TearDown;
begin
  FConsumerChan.Free; // cancela o consumidor -> fila auto-delete some
  FConsumerConn.Free;
  FControlChan.Free;
  FControlConn.Free;
  FReceived.Free;
end;

procedure TAMQPReconnectIntegrationTests.HandleConsumerReconnect(AConnection: TAMQPConnection);
begin
  TInterlocked.Exchange(FReconnected, 1);
end;

procedure TAMQPReconnectIntegrationTests.HandleConsumerDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
begin
  FReceived.Add(ADelivery.BodyAsText);
  AChannel.Ack(ADelivery.DeliveryTag);
end;

procedure TAMQPReconnectIntegrationTests.HandleDropTestReconnect(AConnection: TAMQPConnection);
begin
  TInterlocked.Exchange(FDropTestReconnected, 1);
end;

procedure TAMQPReconnectIntegrationTests.HandleRepublishTestReconnect(AConnection: TAMQPConnection);
begin
  TInterlocked.Exchange(FRepublishTestReconnected, 1);
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
  while (TInterlocked.CompareExchange(FReconnected, 0, 0) = 0) and
        (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(50);
    Inc(LWaited, 50);
  end;
end;

procedure TAMQPReconnectIntegrationTests.Reconecta_E_ContinuaConsumindo;
begin
  // 1) Publica antes da queda e confirma o consumo.
  FControlChan.PublishText('', FQueue, 'antes-da-queda');
  WaitReceived('antes-da-queda', 5000);
  Assert.IsTrue(ReceivedContains('antes-da-queda'), 'deveria consumir antes da queda');

  // 2) Simula a queda de rede.
  FConsumerConn.DropConnectionForTest;

  // 3) Aguarda a auto-reconexão + recuperação (OnReconnect dispara após
  //    redeclarar a fila e re-consumir).
  WaitReconnected(15000);
  Assert.AreEqual(1, TInterlocked.CompareExchange(FReconnected, 0, 0),
    'deveria ter reconectado');
  Assert.IsTrue(FConsumerConn.IsOpen, 'conexão deveria estar aberta após reconectar');

  // 4) Publica depois da recuperação e confirma que o consumo voltou.
  FControlChan.PublishText('', FQueue, 'depois-da-recuperacao');
  WaitReceived('depois-da-recuperacao', 5000);
  Assert.IsTrue(ReceivedContains('depois-da-recuperacao'),
    'deveria voltar a consumir após a reconexão');
end;

procedure TAMQPReconnectIntegrationTests.Confirm_PerdaNaQueda_WaitForConfirmsRetornaFalse;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  I, LWaited: Integer;
begin
  // Conexão própria (isolada das fixtures do Setup) com auto-reconexão.
  FDropTestReconnected := 0;
  LParams := TAMQPConnectionParams.Localhost;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 500;
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.OnReconnect := HandleDropTestReconnect;
    LConn.Open;
    LChan := LConn.CreateChannel;
    LChan.ConfirmSelect;

    // Publica um lote e derruba IMEDIATAMENTE, antes de os acks chegarem: os
    // publishes ficam pendentes e a queda os marca como não confirmados
    // (FailAllUnconfirmed -> FNacked).
    for I := 1 to 200 do
      LChan.PublishText('', 'confirm-drop-test', 'x');
    LConn.DropConnectionForTest;

    // Aguarda a auto-reconexão + recuperação (re-arma o confirm mode).
    LWaited := 0;
    while (TInterlocked.CompareExchange(FDropTestReconnected, 0, 0) = 0) and
          (LWaited < 15000) do
    begin
      TThread.Sleep(50);
      Inc(LWaited, 50);
    end;
    Assert.AreEqual(1, TInterlocked.CompareExchange(FDropTestReconnected, 0, 0),
      'deveria ter reconectado');

    // Os publishes pendentes na queda foram PERDIDOS: WaitForConfirms deve
    // reportar False. (Bug: Recover limpava FNacked e isto retornava True.)
    Assert.IsFalse(LChan.WaitForConfirms(3000),
      'WaitForConfirms deve reportar False para publishes perdidos na queda');
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
  // A fila é declarada pela conexão de CONTROLE do Setup (FControlConn), exclusiva
  // dela — some no teardown. Como não pertence à conexão publicadora, sobrevive à
  // queda dela e acumula as mensagens (o publisher publica via exchange padrão).
  LCtrlChan := FControlChan;
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;
  LQueue := LCtrlChan.DeclareQueue(LDecl).QueueName;

  // Conexão publicadora: auto-reconnect + reenvio de não confirmados.
  FRepublishTestReconnected := 0;
  LPubParams := TAMQPConnectionParams.Localhost;
  LPubParams.AutoReconnect := True;
  LPubParams.ReconnectDelayMs := 500;
  LPubParams.RepublishUnconfirmedOnReconnect := True;
  LPubConn := TAMQPConnection.Create(LPubParams);
  try
    LPubConn.OnReconnect := HandleRepublishTestReconnect;
    LPubConn.Open;
    LPubChan := LPubConn.CreateChannel;
    LPubChan.ConfirmSelect;

    // Publica um lote e derruba IMEDIATAMENTE: a maioria fica sem confirmação e
    // depende do reenvio para chegar à fila.
    for I := 1 to 100 do
      LPubChan.PublishText('', LQueue, 'msg-' + IntToStr(I));
    LPubConn.DropConnectionForTest;

    // Aguarda a reconexão (que dispara o reenvio dos não confirmados).
    LWaited := 0;
    while (TInterlocked.CompareExchange(FRepublishTestReconnected, 0, 0) = 0) and (LWaited < 15000) do
    begin
      TThread.Sleep(50);
      Inc(LWaited, 50);
    end;
    Assert.AreEqual(1, TInterlocked.CompareExchange(FRepublishTestReconnected, 0, 0),
      'deveria ter reconectado');

    // Drena pela conexão de controle: as 100 devem chegar (at-least-once — pode
    // haver duplicatas de mensagens cujo ack se perdeu na queda, por isso >= 100).
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
    Assert.IsTrue(LCount >= 100,
      Format('reenvio deveria entregar >= 100 msgs; entregou %d', [LCount]));
  finally
    LPubChan.Free;
    LPubConn.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPReconnectIntegrationTests);

end.

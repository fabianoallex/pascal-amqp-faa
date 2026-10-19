unit AMQP.ReconnectIntegrationTests;

{ Integração de reconexão automática — precisa de RabbitMQ em localhost:5672.
  Simula queda (fecha o socket), aguarda a auto-reconexão + recuperação de
  topologia (redeclara a fila e re-consome) e confere que o consumer volta a
  receber mensagens publicadas por uma conexão de controle separada. }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Classes, Generics.Collections,
  PascalCommon.Threading,
  AMQP.Connection,
  AMQP.IntegrationConfig,
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
    // variável local capturada agora precisa ser campo da fixture. TInterlocked
    // não existe no FPC -> PcAtomic*.
    FDropTestReconnected: Integer;
    FRepublishTestReconnected: Integer;
    // Testes de fechamento durante a reconexão: quedas percebidas (OnDisconnect)
    // e Close chamados de dentro do OnDisconnect que já voltaram.
    FCloseTestDisconnected: Integer;
    FSelfCloseDone: Integer;
    // Testes de replay cruzado entre canais: reconexões concluídas e as falhas
    // de tentativa reportadas por OnReconnectError ('n: motivo').
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
  public
    [Setup] procedure SetUp;
    [TearDown] procedure TearDown;
    [Test] procedure Reconecta_E_ContinuaConsumindo;
    [Test] procedure Confirm_PerdaNaQueda_WaitForConfirmsRetornaFalse;
    [Test] procedure Confirm_Republish_ReenviaNaoConfirmadosAposQueda;
    [Test] procedure Close_DuranteEsperaDeReconexao_NaoEsperaODelay;
    [Test] procedure Close_ComReconnectDelayAlto_VoltaComAThreadEncerrada;
    [Test] procedure Close_DeDentroDoOnDisconnect_NaoTrava;
    [Test] procedure Reconexao_FilaDeclaradaSoNoutroCanal_Recupera;
    [Test] procedure Reconexao_ReplayFalha_ReportaOMotivo_E_RecuperaQuandoAFilaVolta;
    [Test] procedure SemAutoReconnect_PublishNaConexaoMorta_LevantaEAMQPConnection;
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

  // Conexão de controle (sem auto-reconnect) só para publicar.
  FControlConn := TAMQPConnection.Create(IntegrationParams);
  FControlConn.Open;
  FControlChan := FControlConn.CreateChannel;

  // Conexão consumidora com auto-reconexão.
  LParams := IntegrationParams;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 500;
  LParams.ConnectionName := 'pascal-amqp-faa-recon-test-fpc';
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
  // Roda NA thread de reconexão: o Close não pode unir a própria thread.
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
  // 1) Publica antes da queda e confirma o consumo.
  FControlChan.PublishText('', FQueue, 'antes-da-queda');
  WaitReceived('antes-da-queda', 5000);
  TAssert.AssertTrue('deveria consumir antes da queda', ReceivedContains('antes-da-queda'));

  // 2) Simula a queda de rede.
  FConsumerConn.DropConnectionForTest;

  // 3) Aguarda a auto-reconexão + recuperação (OnReconnect dispara após
  //    redeclarar a fila e re-consumir).
  WaitReconnected(15000);
  TAssert.AssertEquals('deveria ter reconectado', 1, PcAtomicGet(FReconnected));
  TAssert.AssertTrue('conexão deveria estar aberta após reconectar', FConsumerConn.IsOpen);

  // 4) Publica depois da recuperação e confirma que o consumo voltou.
  //    15s: logo após o recovery o broker/canal ainda podem estar assentando
  //    (flakeou com 5s sob carga — broker compartilhado com outra suíte).
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
  // Conexão própria (isolada das fixtures do Setup) com auto-reconexão.
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

    // Publica um lote e derruba IMEDIATAMENTE, antes de os acks chegarem: os
    // publishes ficam pendentes e a queda os marca como não confirmados
    // (FailAllUnconfirmed -> FNacked).
    for I := 1 to 200 do
      LChan.PublishText('', 'confirm-drop-test', 'x');
    LConn.DropConnectionForTest;

    // Aguarda a auto-reconexão + recuperação (re-arma o confirm mode).
    LWaited := 0;
    while (PcAtomicGet(FDropTestReconnected) = 0) and (LWaited < 15000) do
    begin
      TThread.Sleep(50);
      Inc(LWaited, 50);
    end;
    TAssert.AssertEquals('deveria ter reconectado', 1, PcAtomicGet(FDropTestReconnected));

    // Os publishes pendentes na queda foram PERDIDOS: WaitForConfirms deve
    // reportar False. (Bug: Recover limpava FNacked e isto retornava True.)
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
  // A fila é declarada pela conexão de CONTROLE do Setup (FControlConn), exclusiva
  // dela — some no teardown. Como não pertence à conexão publicadora, sobrevive à
  // queda dela e acumula as mensagens (o publisher publica via exchange padrão).
  LCtrlChan := FControlChan;
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;
  LQueue := LCtrlChan.DeclareQueue(LDecl).QueueName;

  // Conexão publicadora: auto-reconnect + reenvio de não confirmados.
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

    // Publica um lote e derruba IMEDIATAMENTE: a maioria fica sem confirmação e
    // depende do reenvio para chegar à fila.
    for I := 1 to 100 do
      LPubChan.PublishText('', LQueue, 'msg-' + IntToStr(I));
    LPubConn.DropConnectionForTest;

    // Aguarda a reconexão (que dispara o reenvio dos não confirmados).
    LWaited := 0;
    while (PcAtomicGet(FRepublishTestReconnected) = 0) and (LWaited < 15000) do
    begin
      TThread.Sleep(50);
      Inc(LWaited, 50);
    end;
    TAssert.AssertEquals('deveria ter reconectado', 1, PcAtomicGet(FRepublishTestReconnected));

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
  // A conexão cai e a reconexão entra na espera de ReconnectDelayMs (o
  // OnDisconnect dispara logo antes dela). Fechar ali tem de ser imediato: o
  // Close acorda a espera, não espera ela vencer. Antes era um Sleep, e o
  // Free levava o delay inteiro (medido pelo pascal-dfe-broker: 2,0-2,1 s com
  // 2000 ms).
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
  // Delay maior que o antigo teto de 12 s do WaitReconnectStopped: antes, o
  // Close desistia de esperar com a thread de reconexão ainda dormindo, e ela
  // acordava lendo um objeto já liberado. Agora o Close a une: volta rápido E
  // com ela encerrada.
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
  // O OnDisconnect roda na thread de reconexão. Um Close chamado dali não pode
  // esperar a si mesmo: antes ficava preso no teto de 12 s. Agora volta na
  // hora, a reconexão não acontece, e o Free (de outra thread) une a thread.
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
  // 'Classe: mensagem' da exceção do publish, ou '' se ele passou.
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
  // O cenário medido no guia de mensageria da pascal-api-infra-faa: um canal
  // declara a fila e publica, outro só consome. A fila some na queda
  // (auto-delete aqui; não durável no restart do broker de lá). Antes, a
  // reconexão replayava canal a canal na ordem do dicionário — o canal 3
  // primeiro —, o basic.consume levava 404 antes de o declare do canal 1 voltar
  // e a tentativa falhava igual para sempre, sem sinal nenhum.
  LQueue := 'test-recon-cross-' + IntToStr(PcTickMs);
  LConn := NewReconnectingConnection(500, nil);
  try
    LConn.OnReconnect := HandleCrossReconnect;
    LConn.Open;
    LPub := LConn.CreateChannel;   // canal 1: declara e publica
    LConf := LConn.CreateChannel;  // canal 2: confirm mode, como no sample de lá
    LConf.ConfirmSelect;
    LCons := LConn.CreateChannel;  // canal 3: só consome
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
    LConn.Free; // libera os três canais
  end;
end;

procedure TAMQPReconnectIntegrationTests.Reconexao_ReplayFalha_ReportaOMotivo_E_RecuperaQuandoAFilaVolta;
var
  LConn: TAMQPConnection;
  LChan: TAMQPChannel;
  LDecl: TAMQPQueueDeclare;
  LQueue, LErro, LPubErro: string;
begin
  // A fila é da conexão de controle (auto-delete) e a conexão que reconecta só
  // a consome: na queda ela some, e nada nesta conexão a redeclara. O replay
  // NÃO pode dar certo — o que se exige é que a falha apareça (callback, motivo
  // e texto da exceção do publish) e que a reconexão volte sozinha quando a
  // fila voltar.
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

    // Logo depois de uma tentativa falhar há ReconnectDelayMs até a próxima: o
    // publish cai na conexão indisponível e a exceção diz por quê.
    LPubErro := PublishError(LChan, LQueue);
    TAssert.AssertTrue('publish durante a reconexão: ' + LPubErro, Pos('EAMQPConnection', LPubErro) = 1);
    TAssert.AssertTrue('a exceção do publish deveria trazer o motivo: ' + LPubErro, Pos('404', LPubErro) > 0);

    // A fila volta (outro processo a redeclara, no mundo real): a próxima
    // tentativa recupera.
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
  // Sem AutoReconnect, publish numa conexão morta levantava o EWriteError da
  // RTL ("Stream write error"), não a EAMQPConnection documentada. Dois
  // instantes: logo após a queda (a thread de leitura pode ainda não ter
  // percebido: falha a escrita no socket) e depois de ela perceber.
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
  TDUnitX.RegisterTestFixture(TAMQPReconnectIntegrationTests);

end.

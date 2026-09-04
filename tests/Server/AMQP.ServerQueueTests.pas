unit AMQP.ServerQueueTests;

{ Testes da fila-ator (AMQP.Server.Queue) -- WS3 da Fase 2 (engine de
  roteamento em memoria, ver CLAUDE.md).

  Testes de estado puro: nenhum sobe broker nem abre socket. A fila e' um ator
  agendado no pool, entao a sincronizacao dos testes usa a propriedade que a
  caixa de comandos garante: a caixa e' SERIAL e FIFO, logo um comando
  SINCRONO (Stats/Get/Purge) posto depois de um assincrono (PostMessage,
  PostAck, ...) so' volta quando o assincrono ja rodou. Nenhum Sleep de
  sincronizacao, portanto -- so' no teste de estresse, que espera os
  produtores terminarem.

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerQueueTests.pas -- mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  AMQP.Wire,
  AMQP.Threading,
  AMQP.Server.Message,
  AMQP.Server.Resources,
  AMQP.Server.Header,
  System.Rtti,
  AMQP.Basic.Methods,
  AMQP.Server.Queue,
  AMQP.ServerTestDoubles;

type
  [TestFixture]
  TQueueActorTests = class
  public
    [Test] procedure Enqueue_ContaNasStats;
    [Test] procedure Get_OrdemFifo;
    [Test] procedure Get_FilaVazia_DevolveFalse;
    [Test] procedure Get_NoAck_NaoDeixaNaoConfirmada;
    [Test] procedure Get_ComAck_DeixaNaoConfirmadaEAckLimpa;
    [Test] procedure Ack_Multiple_LimpaAteATag;
    [Test] procedure Nack_Requeue_VoltaNaCabecaComRedelivered;
    [Test] procedure Nack_SemRequeue_Descarta;
    [Test] procedure Reject_Requeue_VoltaComRedelivered;
    [Test] procedure RemoveChannel_TiraConsumidoresEDevolveNaoConfirmadas;
    [Test] procedure Consumidores_AddERemove;
    [Test] procedure MaxLength_DescartaDaCabecaEConta;
    [Test] procedure MaxLengthZero_SemLimite;
    [Test] procedure Purge_LimpaProntasENaoAsNaoConfirmadas;
    [Test] procedure Delete_IfUnused_ComConsumidor_Recusa;
    [Test] procedure Delete_IfEmpty_ComMensagem_Recusa;
    [Test] procedure Delete_DevolveContagemEParaAFila;
    [Test] procedure Stop_DescartaPostsNovosSemVazar;
    [Test] procedure Stop_SincronoDepois_Levanta;
    [Test] procedure RefCount_FilaSeguraUmaReferencia;
    [Test] procedure Sincrono_DeDentroDoAtor_Levanta;
    [Test] procedure Estresse_ProdutoresConcorrentesESincronos;
    [Test] procedure Delete_AvisaConsumidoresComCancel;
    [Test] procedure EverHadConsumer_SoViraVerdadeComConsumidor;
    [Test] procedure CaixaSerial_PostNaJanelaDoFimDeRodada_NaoPerdeWakeup;
  end;

  [TestFixture]
  TQueuePriorityTtlTests = class
  public
    [Test] procedure Prioridade_MaiorSaiPrimeiro;
    [Test] procedure Prioridade_FifoDentroDoNivel;
    [Test] procedure Prioridade_AcimaDoTetoEhLimitada;
    [Test] procedure Prioridade_RequeueVoltaAoProprioBalde;
    [Test] procedure MaxLength_DescartaDoBaldeMenosPrioritario;
    [Test] procedure Ttl_DaFila_VenceNoPrazo;
    [Test] procedure Ttl_NaoVenceAntesDoPrazo;
    [Test] procedure Ttl_MenorEntreFilaEMensagemVence;
    [Test] procedure Ttl_GetNaoDevolveVencida;
    [Test] procedure Ttl_RequeuePreservaOPrazo;
  end;

  [TestFixture]
  TDeadLetterTests = class
  public
    [Test] procedure Rejeitada_VaiParaODlx;
    [Test] procedure Ack_NaoVaiParaODlx;
    [Test] procedure NackComRequeue_NaoVaiParaODlx;
    [Test] procedure Expirada_VaiParaODlxComRazaoExpired;
    [Test] procedure EstourouOTeto_VaiParaODlxComRazaoMaxlen;
    [Test] procedure SemRoutingKeyPropria_MantemAOriginal;
    [Test] procedure MesmaFilaEMesmaRazao_ColapsaNoCount;
    [Test] procedure ExpirationEhRetiradaEVaiParaOriginalExpiration;
    [Test] procedure PrimeiraMorte_RegistraXFirstDeath;
    [Test] procedure GuardaDeCiclo_ParaDepoisDoTeto;
    [Test] procedure SemDlx_MorteEhDescarte;
  end;

implementation

{ --- helpers --- }

function NovaMensagem(const ATexto: string): TAMQPMessage;
begin
  Result := TAMQPMessage.Create('ex', 'rk', 'guest', nil,
    AmqpUtf8Encode(ATexto));
end;

function CorpoDe(AMsg: TAMQPMessage): string;
begin
  Result := AmqpUtf8Decode(AMsg.Body);
end;

// Tira uma mensagem em no-ack e devolve o corpo, liberando a referencia que
// o Get entrega ao chamador.
function GetCorpo(AQueue: TAMQPServerQueue; ATag: UInt64): string;
var
  LMsg: TAMQPMessage;
  LRedelivered: Boolean;
begin
  if AQueue.Get(1, ATag, True, LMsg, LRedelivered) then
    try
      Result := CorpoDe(LMsg);
    finally
      LMsg.Release;
    end
  else
    Result := '';
end;

type
  { Subclasse-espia: tenta um comando SINCRONO de dentro do proprio ator, que
    e' exatamente o que o guard de thread tem de barrar. }
  TQueueEspia = class(TAMQPServerQueue)
  private
    FTentou: Boolean;
    FGuardBarrou: Boolean;
  protected
    procedure ExecuteCommand(ACmd: TAMQPQueueCommand); override;
  public
    property Tentou: Boolean read FTentou;
    property GuardBarrou: Boolean read FGuardBarrou;
  end;

procedure TQueueEspia.ExecuteCommand(ACmd: TAMQPQueueCommand);
begin
  if (ACmd.Kind = amqqcEnqueue) and not FTentou then
  begin
    FTentou := True;
    try
      Stats; // sincrono postado de DENTRO do ator: o ator esperaria por si
    except
      on E: EAMQPQueueActor do
        FGuardBarrou := True;
    end;
  end;
  inherited ExecuteCommand(ACmd);
end;

type
  { Produtor do teste de estresse. TParallel.For nao existe no FPC (ver
    CLAUDE.md), entao os produtores concorrentes sao work items do proprio
    pool da lib. }
  TProdutorWork = class(TAMQPWorkItem)
  private
    FQueue: TAMQPServerQueue;
    FQuantas: Integer;
    FConcluidos: PInteger;
  public
    constructor Create(AQueue: TAMQPServerQueue; AQuantas: Integer;
      AConcluidos: PInteger);
    procedure Execute; override;
  end;

constructor TProdutorWork.Create(AQueue: TAMQPServerQueue; AQuantas: Integer;
  AConcluidos: PInteger);
begin
  inherited Create;
  FQueue := AQueue;
  FQuantas := AQuantas;
  FConcluidos := AConcluidos;
end;

procedure TProdutorWork.Execute;
var
  I: Integer;
  LMsg: TAMQPMessage;
begin
  for I := 1 to FQuantas do
  begin
    LMsg := NovaMensagem('m' + IntToStr(I));
    try
      FQueue.PostMessage(LMsg); // a fila tira a referencia dela
    finally
      LMsg.Release; // a nossa
    end;
  end;
  AmqpAtomicInc(FConcluidos^);
end;

type
  { Injeta um comando SINCRONO de outra thread. Usado pelo teste da janela de
    fim de rodada. }
  TInjetorWork = class(TAMQPWorkItem)
  private
    FQueue: TAMQPServerQueue;
    FEnfileirou: PInteger;
    FConcluiu: PInteger;
    FOk: PInteger;
  public
    constructor Create(AQueue: TAMQPServerQueue;
      AEnfileirou, AConcluiu, AOk: PInteger);
    procedure Execute; override;
  end;

constructor TInjetorWork.Create(AQueue: TAMQPServerQueue;
  AEnfileirou, AConcluiu, AOk: PInteger);
begin
  inherited Create;
  FQueue := AQueue;
  FEnfileirou := AEnfileirou;
  FConcluiu := AConcluiu;
  FOk := AOk;
end;

procedure TInjetorWork.Execute;
begin
  // O ator esta parado na janela do fim de rodada, esperando este sinal.
  AmqpAtomicSet(FEnfileirou^, 1);
  try
    FQueue.Stats; // se o wakeup se perder, isto so' volta no timeout de 15 s
    AmqpAtomicSet(FOk^, 1);
  except
    on E: Exception do
      AmqpAtomicSet(FOk^, 0);
  end;
  AmqpAtomicSet(FConcluiu^, 1);
end;

type
  { Fila que injeta um post de OUTRA thread exatamente na janela entre "o laco
    de comandos terminou" e "o lock do fim de rodada foi tomado". Com a caixa
    correta o ator ve o comando novo e reagenda; com a checagem de caixa vazia
    feita fora do lock, o comando fica orfao e o sincrono do injetor so' volta
    no timeout. }
  TQueueCorrida = class(TAMQPServerQueue)
  private
    FInjetado: Integer;
    FEnfileirou: Integer;
    FConcluiu: Integer;
    FOk: Integer;
  protected
    procedure ActorRoundEnding; override;
  public
    property Concluiu: Integer read FConcluiu;
    property Ok: Integer read FOk;
  end;

procedure TQueueCorrida.ActorRoundEnding;
var
  LDeadline: UInt64;
begin
  if AmqpAtomicCompareExchange(FInjetado, 1, 0) <> 0 then
    Exit; // so' na primeira rodada
  AmqpPool.Queue(TInjetorWork.Create(Self, @FEnfileirou, @FConcluiu, @FOk));
  // Segura a rodada ate' o injetor ter POSTADO -- e' isso que poe o post
  // dentro da janela em vez de antes ou depois dela.
  LDeadline := AmqpTickMs + 5000;
  while (AmqpAtomicGet(FEnfileirou) = 0) and (AmqpTickMs < LDeadline) do
    Sleep(0);
  Sleep(5); // folga para o Post do injetor entrar de fato na caixa
end;

{ --- TQueueActorTests --- }

procedure TQueueActorTests.Enqueue_ContaNasStats;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;
    // O sincrono so' volta depois de o assincrono ter rodado (caixa serial).
    LStats := LQ.Stats;
    Assert.AreEqual(1, LStats.MessageCount, 'prontas');
    Assert.AreEqual(0, LStats.UnackedCount, 'nao-confirmadas');
    Assert.AreEqual(0, LStats.ConsumerCount, 'consumidores');
    Assert.AreEqual(0, Integer(LStats.DroppedCount), 'descartadas');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Get_OrdemFifo;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  I: Integer;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    for I := 1 to 3 do
    begin
      LMsg := NovaMensagem('m' + IntToStr(I));
      try
        LQ.PostMessage(LMsg);
      finally
        LMsg.Release;
      end;
    end;
    Assert.AreEqual('m1', GetCorpo(LQ, 1), 'primeira');
    Assert.AreEqual('m2', GetCorpo(LQ, 2), 'segunda');
    Assert.AreEqual('m3', GetCorpo(LQ, 3), 'terceira');
    Assert.AreEqual(0, LQ.Stats.MessageCount, 'esvaziou');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Get_FilaVazia_DevolveFalse;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LRedelivered: Boolean;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    Assert.IsFalse(LQ.Get(1, 1, True, LMsg, LRedelivered), 'fila vazia');
    Assert.IsTrue(LMsg = nil, 'sem mensagem');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Get_NoAck_NaoDeixaNaoConfirmada;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;
    Assert.AreEqual('a', GetCorpo(LQ, 7), 'corpo');
    Assert.AreEqual(0, LQ.Stats.UnackedCount, 'no-ack nao gera nao-confirmada');
    Assert.AreEqual(0, LQ.Stats.MessageCount, 'saiu do estoque');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Get_ComAck_DeixaNaoConfirmadaEAckLimpa;
var
  LQ: TAMQPServerQueue;
  LMsg, LSaida: TAMQPMessage;
  LRedelivered: Boolean;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;

    Assert.IsTrue(LQ.Get(5, 100, False, LSaida, LRedelivered), 'get');
    try
      Assert.IsFalse(LRedelivered, 'primeira entrega nao e reentrega');
      Assert.AreEqual(1, LQ.Stats.UnackedCount, 'ficou nao-confirmada');
      Assert.AreEqual(0, LQ.Stats.MessageCount, 'saiu das prontas');
      // Ack de outro canal com a mesma tag NAO pode limpar.
      LQ.PostAck(6, 100, False);
      Assert.AreEqual(1, LQ.Stats.UnackedCount, 'ack de outro canal nao conta');
      LQ.PostAck(5, 100, False);
      Assert.AreEqual(0, LQ.Stats.UnackedCount, 'ack limpou');
    finally
      LSaida.Release;
    end;
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Ack_Multiple_LimpaAteATag;
var
  LQ: TAMQPServerQueue;
  LMsg, LSaida: TAMQPMessage;
  LRedelivered: Boolean;
  I: Integer;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    for I := 1 to 3 do
    begin
      LMsg := NovaMensagem('m' + IntToStr(I));
      try
        LQ.PostMessage(LMsg);
      finally
        LMsg.Release;
      end;
    end;
    for I := 1 to 3 do
    begin
      Assert.IsTrue(LQ.Get(9, I, False, LSaida, LRedelivered), 'get ' + IntToStr(I));
      LSaida.Release;
    end;
    Assert.AreEqual(3, LQ.Stats.UnackedCount, 'tres nao-confirmadas');

    LQ.PostAck(9, 2, True); // multiple: tags 1 e 2
    Assert.AreEqual(1, LQ.Stats.UnackedCount, 'sobrou a tag 3');
    LQ.PostAck(9, 3, False);
    Assert.AreEqual(0, LQ.Stats.UnackedCount, 'limpou');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Nack_Requeue_VoltaNaCabecaComRedelivered;
var
  LQ: TAMQPServerQueue;
  LMsg, LSaida: TAMQPMessage;
  LRedelivered: Boolean;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LMsg := NovaMensagem('primeira');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;
    LMsg := NovaMensagem('segunda');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;

    Assert.IsTrue(LQ.Get(1, 10, False, LSaida, LRedelivered), 'get');
    Assert.AreEqual('primeira', CorpoDe(LSaida), 'tirou a primeira');
    LSaida.Release;

    LQ.PostNack(1, 10, False, True); // requeue
    Assert.AreEqual(2, LQ.Stats.MessageCount, 'voltou para as prontas');
    Assert.AreEqual(0, LQ.Stats.UnackedCount, 'saiu das nao-confirmadas');

    // Volta na CABECA (antes da 'segunda') e marcada como reentrega.
    Assert.IsTrue(LQ.Get(1, 11, True, LSaida, LRedelivered), 'get de novo');
    try
      Assert.AreEqual('primeira', CorpoDe(LSaida), 'voltou na cabeca');
      Assert.IsTrue(LRedelivered, 'redelivered');
    finally
      LSaida.Release;
    end;
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Nack_SemRequeue_Descarta;
var
  LQ: TAMQPServerQueue;
  LMsg, LSaida: TAMQPMessage;
  LRedelivered: Boolean;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
      Assert.IsTrue(LQ.Get(1, 1, False, LSaida, LRedelivered), 'get');
      LSaida.Release;
      LQ.PostNack(1, 1, False, False); // sem requeue: descarta (DLX e Fase 3)
      Assert.AreEqual(0, LQ.Stats.MessageCount, 'nao voltou');
      Assert.AreEqual(0, LQ.Stats.UnackedCount, 'saiu das nao-confirmadas');
      // So' a referencia do teste sobrou.
      Assert.AreEqual(1, LMsg.RefCount, 'fila soltou a referencia dela');
    finally
      LMsg.Release;
    end;
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Reject_Requeue_VoltaComRedelivered;
var
  LQ: TAMQPServerQueue;
  LMsg, LSaida: TAMQPMessage;
  LRedelivered: Boolean;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;
    Assert.IsTrue(LQ.Get(3, 42, False, LSaida, LRedelivered), 'get');
    LSaida.Release;

    LQ.PostReject(3, 42, True);
    Assert.AreEqual(1, LQ.Stats.MessageCount, 'voltou');

    Assert.IsTrue(LQ.Get(3, 43, True, LSaida, LRedelivered), 'get de novo');
    try
      Assert.IsTrue(LRedelivered, 'redelivered');
    finally
      LSaida.Release;
    end;
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.RemoveChannel_TiraConsumidoresEDevolveNaoConfirmadas;
var
  LQ: TAMQPServerQueue;
  LMsg, LSaida: TAMQPMessage;
  LRedelivered: Boolean;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ctag-1', False, False, AlvoQueRecusa(77)));
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ctag-2', False, False, AlvoQueRecusa(88)));

    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;
    Assert.IsTrue(LQ.Get(77, 1, False, LSaida, LRedelivered), 'get');
    LSaida.Release;

    LQ.PostRemoveChannel(77);
    LStats := LQ.Stats;
    Assert.AreEqual(1, LStats.ConsumerCount, 'sobrou o consumidor do canal 88');
    Assert.AreEqual(0, LStats.UnackedCount, 'nao-confirmada do canal morto saiu');
    Assert.AreEqual(1, LStats.MessageCount, 'e voltou para as prontas');

    Assert.IsTrue(LQ.Get(88, 1, True, LSaida, LRedelivered), 'get de novo');
    try
      Assert.IsTrue(LRedelivered, 'redelivered depois da queda do canal');
    finally
      LSaida.Release;
    end;
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Consumidores_AddERemove;
var
  LQ: TAMQPServerQueue;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct-a', False, False, AlvoQueRecusa(1)));
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct-b', True, False, AlvoQueRecusa(1)));
    Assert.AreEqual(2, LQ.Stats.ConsumerCount, 'dois consumidores');

    // Tag certa, canal errado: nao remove.
    LQ.PostRemoveConsumer(2, 'ct-a');
    Assert.AreEqual(2, LQ.Stats.ConsumerCount, 'canal errado nao remove');

    LQ.PostRemoveConsumer(1, 'ct-a');
    Assert.AreEqual(1, LQ.Stats.ConsumerCount, 'removeu o ct-a');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.MaxLength_DescartaDaCabecaEConta;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LStats: TAMQPQueueStats;
  I: Integer;
begin
  LQ := TAMQPServerQueue.Create('q', 3);
  try
    for I := 1 to 5 do
    begin
      LMsg := NovaMensagem('m' + IntToStr(I));
      try
        LQ.PostMessage(LMsg);
      finally
        LMsg.Release;
      end;
    end;
    LStats := LQ.Stats;
    Assert.AreEqual(3, LStats.MessageCount, 'segurou o teto');
    Assert.AreEqual(2, Integer(LStats.DroppedCount), 'contou os descartes');
    // Sobraram as MAIS NOVAS: descarte e' da cabeca.
    Assert.AreEqual('m3', GetCorpo(LQ, 1), 'cabeca depois do descarte');
    Assert.AreEqual('m4', GetCorpo(LQ, 2), 'seguinte');
    Assert.AreEqual('m5', GetCorpo(LQ, 3), 'ultima');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.MaxLengthZero_SemLimite;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LStats: TAMQPQueueStats;
  I: Integer;
begin
  LQ := TAMQPServerQueue.Create('q', 0);
  try
    for I := 1 to 50 do
    begin
      LMsg := NovaMensagem('m' + IntToStr(I));
      try
        LQ.PostMessage(LMsg);
      finally
        LMsg.Release;
      end;
    end;
    LStats := LQ.Stats;
    Assert.AreEqual(50, LStats.MessageCount, 'guardou todas');
    Assert.AreEqual(0, Integer(LStats.DroppedCount), 'nada descartado');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Purge_LimpaProntasENaoAsNaoConfirmadas;
var
  LQ: TAMQPServerQueue;
  LMsg, LSaida: TAMQPMessage;
  LRedelivered: Boolean;
  LStats: TAMQPQueueStats;
  I: Integer;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    for I := 1 to 4 do
    begin
      LMsg := NovaMensagem('m' + IntToStr(I));
      try
        LQ.PostMessage(LMsg);
      finally
        LMsg.Release;
      end;
    end;
    Assert.IsTrue(LQ.Get(1, 1, False, LSaida, LRedelivered), 'get');
    try
      Assert.AreEqual(3, LQ.Purge, 'purgou so as prontas');
      LStats := LQ.Stats;
      Assert.AreEqual(0, LStats.MessageCount, 'vazia');
      Assert.AreEqual(1, LStats.UnackedCount, 'nao-confirmada intacta');
      Assert.AreEqual(0, Integer(LStats.DroppedCount), 'purge nao e descarte');
    finally
      LSaida.Release;
    end;
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Delete_IfUnused_ComConsumidor_Recusa;
var
  LQ: TAMQPServerQueue;
  LCount: Integer;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', False, False, AlvoQueRecusa(1)));
    Assert.IsTrue(LQ.Delete(True, False, LCount) = amqqdEmUso, 'if-unused com consumidor');
    // A fila continua viva.
    Assert.AreEqual(1, LQ.Stats.ConsumerCount, 'fila segue atendendo');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Delete_IfEmpty_ComMensagem_Recusa;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LCount: Integer;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;
    Assert.IsTrue(LQ.Delete(False, True, LCount) = amqqdNaoVazia, 'if-empty com mensagem');
    Assert.AreEqual(1, LQ.Stats.MessageCount, 'fila segue atendendo');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Delete_DevolveContagemEParaAFila;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LCount, I: Integer;
  LLevantou: Boolean;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    for I := 1 to 3 do
    begin
      LMsg := NovaMensagem('m' + IntToStr(I));
      try
        LQ.PostMessage(LMsg);
      finally
        LMsg.Release;
      end;
    end;
    Assert.IsTrue(LQ.Delete(False, False, LCount) = amqqdOk, 'delete');
    Assert.AreEqual(3, LCount, 'contagem do Delete-Ok');

    // Fila apagada nao volta a atender.
    LLevantou := False;
    try
      LQ.Stats;
    except
      on E: EAMQPQueueActor do
        LLevantou := True;
    end;
    Assert.IsTrue(LLevantou, 'sincrono numa fila apagada levanta');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Stop_DescartaPostsNovosSemVazar;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LQ.Stop;
    LMsg := NovaMensagem('a');
    try
      // Publish nao pode falhar por corrida de teardown: descarta em silencio.
      LQ.PostMessage(LMsg);
      Assert.AreEqual(1, LMsg.RefCount,
        'fila parada nao reteve referencia nenhuma');
    finally
      LMsg.Release;
    end;
    LQ.Stop; // idempotente
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Stop_SincronoDepois_Levanta;
var
  LQ: TAMQPServerQueue;
  LLevantou: Boolean;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LQ.Stop;
    LLevantou := False;
    try
      LQ.Purge;
    except
      on E: EAMQPQueueActor do
        LLevantou := True;
    end;
    Assert.IsTrue(LLevantou, 'sincrono numa fila parada levanta');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.RefCount_FilaSeguraUmaReferencia;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      Assert.AreEqual(1, LMsg.RefCount, 'so o criador');
      LQ.PostMessage(LMsg);
      LQ.Stats; // barreira: o enqueue ja rodou
      Assert.AreEqual(2, LMsg.RefCount, 'a fila tirou a dela');
      Assert.AreEqual(1, LQ.Purge, 'purgou');
      Assert.AreEqual(1, LMsg.RefCount, 'a fila devolveu a dela');
    finally
      LMsg.Release;
    end;
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Sincrono_DeDentroDoAtor_Levanta;
var
  LQ: TQueueEspia;
  LMsg: TAMQPMessage;
begin
  LQ := TQueueEspia.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg);
    finally
      LMsg.Release;
    end;
    LQ.Stats; // barreira
    Assert.IsTrue(LQ.Tentou, 'a espia rodou dentro do ator');
    Assert.IsTrue(LQ.GuardBarrou,
      'sincrono de dentro do ator tem de levantar EAMQPQueueActor');
    Assert.AreEqual(1, LQ.Stats.MessageCount, 'o comando seguiu normalmente');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Estresse_ProdutoresConcorrentesESincronos;
const
  PRODUTORES = 8;
  POR_PRODUTOR = 2000;
var
  LQ: TAMQPServerQueue;
  LConcluidos: Integer;
  I: Integer;
  LStats: TAMQPQueueStats;
  LDeadline: UInt64;
  LVoltas: Integer;
begin
  LConcluidos := 0;
  LVoltas := 0;
  LQ := TAMQPServerQueue.Create('q');
  try
    for I := 1 to PRODUTORES do
      AmqpPool.Queue(TProdutorWork.Create(LQ, POR_PRODUTOR, @LConcluidos));

    // Enquanto os produtores despejam, comandos SINCRONOS concorrentes.
    LDeadline := AmqpTickMs + 30000;
    while (AmqpAtomicGet(LConcluidos) < PRODUTORES) and (AmqpTickMs < LDeadline) do
    begin
      LQ.Stats;
      Inc(LVoltas);
    end;
    Assert.AreEqual(PRODUTORES, AmqpAtomicGet(LConcluidos),
      'produtores terminaram dentro do prazo');
    Assert.IsTrue(LVoltas > 0,
      'os comandos sincronos concorreram de fato com os produtores');

    LStats := LQ.Stats;
    Assert.AreEqual(PRODUTORES * POR_PRODUTOR, LStats.MessageCount,
      'nenhum comando perdido nem duplicado');
    Assert.AreEqual(0, Integer(LStats.DroppedCount), 'nada descartado');
    Assert.AreEqual(PRODUTORES * POR_PRODUTOR, LQ.Purge, 'purge devolve o total');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.CaixaSerial_PostNaJanelaDoFimDeRodada_NaoPerdeWakeup;
var
  LQ: TQueueCorrida;
  LMsg: TAMQPMessage;
  LDeadline: UInt64;
begin
  LQ := TQueueCorrida.Create('q');
  try
    LMsg := NovaMensagem('a');
    try
      LQ.PostMessage(LMsg); // dispara a rodada que vai injetar na janela
    finally
      LMsg.Release;
    end;

    // Ninguem mais posta nada: se o wakeup se perder, nada resgata o comando
    // do injetor (e' exatamente o caso "ultimo post antes da ociosidade").
    LDeadline := AmqpTickMs + 5000;
    while (AmqpAtomicGet(LQ.FConcluiu) = 0) and (AmqpTickMs < LDeadline) do
      Sleep(10);

    Assert.AreEqual(1, AmqpAtomicGet(LQ.FConcluiu),
      'o comando postado na janela do fim de rodada tem de ser atendido');
    Assert.AreEqual(1, AmqpAtomicGet(LQ.FOk), 'e sem levantar');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.Delete_AvisaConsumidoresComCancel;
var
  LQ: TAMQPServerQueue;
  LA, LB: TAlvoFalso;
  LRefA, LRefB: IAMQPDeliveryTarget;
  LCount: Integer;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    LA := TAlvoFalso.Create(1);
    LA.Recusar := True;
    LRefA := LA;
    LB := TAlvoFalso.Create(2);
    LB.Recusar := True;
    LRefB := LB;
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct-a', False, False, LRefA));
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct-b', False, False, LRefB));

    Assert.IsTrue(LQ.Delete(False, False, LCount) = amqqdOk, 'delete');

    // A fila sumiu debaixo dos consumidores: os dois tem de saber.
    Assert.AreEqual('ct-a', LA.Cancelados, 'o primeiro foi avisado');
    Assert.AreEqual('ct-b', LB.Cancelados, 'o segundo tambem');
  finally
    LQ.Free;
  end;
end;

procedure TQueueActorTests.EverHadConsumer_SoViraVerdadeComConsumidor;
var
  LQ: TAMQPServerQueue;
begin
  LQ := TAMQPServerQueue.Create('q');
  try
    Assert.IsFalse(LQ.Stats.EverHadConsumer, 'fila nova nunca teve consumidor');
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', False, False,
      AlvoQueRecusa(1)));
    Assert.IsTrue(LQ.Stats.EverHadConsumer, 'agora teve');
    LQ.PostRemoveConsumer(1, 'ct');
    // Continua verdade DEPOIS de o consumidor sair -- e' o que distingue
    // "auto-delete que perdeu o ultimo" de "auto-delete que nunca teve".
    Assert.IsTrue(LQ.Stats.EverHadConsumer, 'e continua tendo tido');
    Assert.AreEqual(0, LQ.Stats.ConsumerCount, 'sem consumidor agora');
  finally
    LQ.Free;
  end;
end;

// Wrappers de assercao: mantem o CORPO da fixture identico nos dois arquivos.
procedure ChecaInt(const AMsg: string; AEsperado, AReal: Int64);
begin
  Assert.AreEqual(AEsperado, AReal, AMsg);
end;

procedure ChecaBool(const AMsg: string; AEsperado, AReal: Boolean);
begin
  Assert.IsTrue(AReal = AEsperado, AMsg);
end;

procedure ChecaStr(const AMsg, AEsperado, AReal: string);
begin
  Assert.AreEqual(AEsperado, AReal, AMsg);
end;

{ TQueuePriorityTtlTests }

// Fila com relogio na mao: o teste faz o tempo andar em vez de dormir (D13).
// Sem isto, testar TTL significaria Sleep de verdade -- lento e instavel sob
// carga, que e' o defeito que a suite de integracao ja pagou uma vez.
type
  TQueueRelogio = class(TAMQPServerQueue)
  private
    FAgora: UInt64;
  protected
    function NowTick: UInt64; override;
  public
    procedure Avanca(AMs: UInt64);
  end;

function TQueueRelogio.NowTick: UInt64;
begin
  Result := FAgora;
end;

procedure TQueueRelogio.Avanca(AMs: UInt64);
begin
  // Escrita de fora do ator, mas segura: o campo e' lido so' pelo ator e o
  // teste nao tem comando em voo quando mexe no relogio (todo Post anterior
  // ja foi drenado por um comando SINCRONO -- ver o comentario de Estabiliza).
  Inc(FAgora, AMs);
end;

// A caixa e' serial e FIFO, entao um comando SINCRONO posto depois de um
// assincrono so volta quando o assincrono ja rodou. E' a barreira do projeto
// para nao precisar de Sleep em teste.
function Estabiliza(AQ: TAMQPServerQueue): TAMQPQueueStats;
begin
  Result := AQ.Stats;
end;

function PolPrioridade(AMax: Integer): TAMQPQueuePolicy;
begin
  Result := TAMQPQueuePolicy.Empty;
  Result.MaxPriority := AMax;
end;

function PolTtl(AMs: Int64): TAMQPQueuePolicy;
begin
  Result := TAMQPQueuePolicy.Empty;
  Result.MessageTtlMs := AMs;
end;

// Publica com prioridade e TTL proprio, soltando a referencia do publicador.
procedure Publica(AQ: TAMQPServerQueue; const ATexto: string;
  APrio: Byte = 0; ATtlMs: Int64 = -1);
var
  LMsg: TAMQPMessage;
begin
  LMsg := NovaMensagem(ATexto);
  try
    AQ.PostMessage(LMsg, APrio, ATtlMs);
  finally
    LMsg.Release;
  end;
end;

// Tira em no-ack e devolve o corpo ('' se a fila estava vazia).
function TiraCorpo(AQ: TAMQPServerQueue; ATag: UInt64): string;
var
  LMsg: TAMQPMessage;
  LRedel: Boolean;
begin
  if not AQ.Get(1, ATag, True, LMsg, LRedel) then
    Exit('');
  try
    Result := CorpoDe(LMsg);
  finally
    LMsg.Release;
  end;
end;

procedure TQueuePriorityTtlTests.Prioridade_MaiorSaiPrimeiro;
var
  LQ: TAMQPServerQueue;
begin
  LQ := TAMQPServerQueue.Create('q', PolPrioridade(9));
  try
    Publica(LQ, 'baixa', 0);
    Publica(LQ, 'alta', 5);
    Publica(LQ, 'media', 2);
    ChecaStr('maior prioridade primeiro', 'alta', TiraCorpo(LQ, 1));
    ChecaStr('depois a media', 'media', TiraCorpo(LQ, 2));
    ChecaStr('a baixa por ultimo', 'baixa', TiraCorpo(LQ, 3));
  finally
    LQ.Free;
  end;
end;

// Prioridade reordena ENTRE niveis, nunca DENTRO de um.
procedure TQueuePriorityTtlTests.Prioridade_FifoDentroDoNivel;
var
  LQ: TAMQPServerQueue;
begin
  LQ := TAMQPServerQueue.Create('q', PolPrioridade(9));
  try
    Publica(LQ, 'a', 3);
    Publica(LQ, 'b', 3);
    Publica(LQ, 'c', 3);
    ChecaStr('1a', 'a', TiraCorpo(LQ, 1));
    ChecaStr('2a', 'b', TiraCorpo(LQ, 2));
    ChecaStr('3a', 'c', TiraCorpo(LQ, 3));
  finally
    LQ.Free;
  end;
end;

// Publicar acima do teto da fila nao cria balde novo nem estoura indice: a
// prioridade e limitada ao teto e a mensagem entra no balde de cima.
procedure TQueuePriorityTtlTests.Prioridade_AcimaDoTetoEhLimitada;
var
  LQ: TAMQPServerQueue;
begin
  LQ := TAMQPServerQueue.Create('q', PolPrioridade(2));
  try
    Publica(LQ, 'teto', 2);
    Publica(LQ, 'acima', 9);   // vira 2
    Publica(LQ, 'baixa', 0);
    ChecaInt('teto em vigor', 2, LQ.MaxPriority);
    // 'teto' e 'acima' estao no MESMO balde, entao valem FIFO entre si.
    ChecaStr('1a', 'teto', TiraCorpo(LQ, 1));
    ChecaStr('2a', 'acima', TiraCorpo(LQ, 2));
    ChecaStr('3a', 'baixa', TiraCorpo(LQ, 3));
  finally
    LQ.Free;
  end;
end;

// O requeue devolve a mensagem a' cabeca do PROPRIO BALDE. Se voltasse a'
// cabeca absoluta, uma mensagem de prioridade baixa devolvida furaria a fila
// das altas -- e o teste abaixo e montado exatamente para pegar isso: depois
// de devolver uma p0, uma p5 NOVA tem de sair na frente dela.
procedure TQueuePriorityTtlTests.Prioridade_RequeueVoltaAoProprioBalde;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
begin
  LQ := TAMQPServerQueue.Create('q', PolPrioridade(9));
  try
    Publica(LQ, 'baixa', 0);
    // Tira a baixa em modo ACK, para poder devolve-la.
    ChecaBool('tirou a baixa', True, LQ.Get(1, 10, False, LMsg, LRedel));
    LMsg.Release;
    LQ.PostNack(1, 10, False, True); // requeue
    Publica(LQ, 'alta', 5);
    Estabiliza(LQ);
    ChecaStr('a alta nova passa na frente da baixa devolvida', 'alta',
      TiraCorpo(LQ, 2));
    ChecaStr('so entao a baixa', 'baixa', TiraCorpo(LQ, 3));
  finally
    LQ.Free;
  end;
end;

// D12: o teto de memoria descarta do balde MENOS prioritario. Descartar da
// cabeca absoluta jogaria fora justamente o que o cliente marcou como mais
// importante.
procedure TQueuePriorityTtlTests.MaxLength_DescartaDoBaldeMenosPrioritario;
var
  LQ: TAMQPServerQueue;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q', PolPrioridade(9), 2);
  try
    Publica(LQ, 'alta1', 9);
    Publica(LQ, 'baixa', 0);
    Publica(LQ, 'alta2', 9); // estoura o teto: a BAIXA e que tem de sair
    LStats := Estabiliza(LQ);
    ChecaInt('duas restantes', 2, LStats.MessageCount);
    ChecaInt('uma descartada', 1, LStats.DroppedCount);
    ChecaStr('sobrou alta1', 'alta1', TiraCorpo(LQ, 1));
    ChecaStr('sobrou alta2', 'alta2', TiraCorpo(LQ, 2));
  finally
    LQ.Free;
  end;
end;

procedure TQueuePriorityTtlTests.Ttl_DaFila_VenceNoPrazo;
var
  LQ: TQueueRelogio;
  LStats: TAMQPQueueStats;
begin
  LQ := TQueueRelogio.Create('q', PolTtl(1000));
  try
    Publica(LQ, 'm');
    Estabiliza(LQ);
    LQ.Avanca(1001);
    // Nenhum consumidor: e o tick da monitora que acorda a fila (D13).
    LQ.PostDeliverTick;
    LStats := Estabiliza(LQ);
    ChecaInt('venceu', 0, LStats.MessageCount);
    ChecaInt('contada como expirada', 1, LStats.ExpiredCount);
    ChecaInt('nao conta como descartada por limite', 0, LStats.DroppedCount);
  finally
    LQ.Free;
  end;
end;

procedure TQueuePriorityTtlTests.Ttl_NaoVenceAntesDoPrazo;
var
  LQ: TQueueRelogio;
  LStats: TAMQPQueueStats;
begin
  LQ := TQueueRelogio.Create('q', PolTtl(1000));
  try
    Publica(LQ, 'm');
    Estabiliza(LQ);
    LQ.Avanca(999);
    LQ.PostDeliverTick;
    LStats := Estabiliza(LQ);
    ChecaInt('ainda viva', 1, LStats.MessageCount);
    ChecaInt('nada expirado', 0, LStats.ExpiredCount);
  finally
    LQ.Free;
  end;
end;

// O MENOR dos dois TTL vence, venha ele da fila ou da mensagem.
procedure TQueuePriorityTtlTests.Ttl_MenorEntreFilaEMensagemVence;
var
  LQ: TQueueRelogio;
  LStats: TAMQPQueueStats;
begin
  // TTL da MENSAGEM menor que o da fila.
  LQ := TQueueRelogio.Create('q', PolTtl(10000));
  try
    Publica(LQ, 'curta', 0, 100);
    Estabiliza(LQ);
    LQ.Avanca(200);
    LQ.PostDeliverTick;
    LStats := Estabiliza(LQ);
    ChecaInt('a da mensagem venceu', 0, LStats.MessageCount);
  finally
    LQ.Free;
  end;

  // TTL da FILA menor que o da mensagem.
  LQ := TQueueRelogio.Create('q', PolTtl(100));
  try
    Publica(LQ, 'longa', 0, 10000);
    Estabiliza(LQ);
    LQ.Avanca(200);
    LQ.PostDeliverTick;
    LStats := Estabiliza(LQ);
    ChecaInt('a da fila venceu', 0, LStats.MessageCount);
  finally
    LQ.Free;
  end;
end;

// O Basic.Get e o outro caminho de saida: nao pode devolver mensagem vencida.
procedure TQueuePriorityTtlTests.Ttl_GetNaoDevolveVencida;
var
  LQ: TQueueRelogio;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
begin
  LQ := TQueueRelogio.Create('q', PolTtl(500));
  try
    Publica(LQ, 'm');
    Estabiliza(LQ);
    LQ.Avanca(501);
    ChecaBool('Get nao devolve vencida', False,
      LQ.Get(1, 1, True, LMsg, LRedel));
    ChecaInt('e ela foi contada', 1, LQ.Stats.ExpiredCount);
  finally
    LQ.Free;
  end;
end;

// Devolver ao estoque nao pode dar sobrevida: o prazo e o ORIGINAL, nao um
// novo contado a partir do requeue.
procedure TQueuePriorityTtlTests.Ttl_RequeuePreservaOPrazo;
var
  LQ: TQueueRelogio;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
  LStats: TAMQPQueueStats;
begin
  LQ := TQueueRelogio.Create('q', PolTtl(1000));
  try
    Publica(LQ, 'm');
    Estabiliza(LQ);
    ChecaBool('tirou em modo ack', True, LQ.Get(1, 7, False, LMsg, LRedel));
    LMsg.Release;
    LQ.Avanca(1500);                 // o prazo passou enquanto estava em voo
    LQ.PostNack(1, 7, False, True);  // devolve
    LQ.PostDeliverTick;
    LStats := Estabiliza(LQ);
    ChecaInt('devolvida ja vencida, nao ressuscitada', 0, LStats.MessageCount);
    ChecaInt('contada como expirada', 1, LStats.ExpiredCount);
  finally
    LQ.Free;
  end;
end;

{ TDeadLetterTests }

// Sink duble: guarda o que foi republicado, para o teste inspecionar o
// x-death sem precisar de broker nenhum.
//
// ATENCAO ao usar: e' TInterfacedObject, e a fila guarda a referencia como
// INTERFACE. Se o teste segurar so' o ponteiro de objeto, liberar a fila
// derruba o refcount a zero e destroi o duble -- o teste do ciclo, que reusa o
// mesmo sink em varias filas, morreu com Access violation exatamente assim.
// Por isso todo teste aqui guarda tambem um LSinkRef de interface.
type
  TMortaRegistrada = record
    Dlx: string;
    RoutingKey: string;
    HeaderPayload: TBytes;
    Corpo: string;
    Priority: Byte;
  end;

  TSinkEspia = class(TInterfacedObject, IAMQPDeadLetterSink)
  private
    FMortas: array of TMortaRegistrada;
  public
    procedure DeadLetter(const ADlx, ARoutingKey: string;
      AMessage: TAMQPMessage; APriority: Byte);
    function Count: Integer;
    function Item(AIdx: Integer): TMortaRegistrada;
  end;

procedure TSinkEspia.DeadLetter(const ADlx, ARoutingKey: string;
  AMessage: TAMQPMessage; APriority: Byte);
var
  N: Integer;
begin
  N := Length(FMortas);
  SetLength(FMortas, N + 1);
  FMortas[N].Dlx := ADlx;
  FMortas[N].RoutingKey := ARoutingKey;
  // COPIA: o contrato diz que a referencia continua sendo do chamador, entao
  // guardar o objeto seria errado -- e guardar so' o buffer prova, de quebra,
  // que a mensagem derivada esta completa no momento da chamada.
  FMortas[N].HeaderPayload := Copy(AMessage.HeaderPayload, 0,
    Length(AMessage.HeaderPayload));
  FMortas[N].Corpo := CorpoDe(AMessage);
  FMortas[N].Priority := APriority;
end;

function TSinkEspia.Count: Integer;
begin
  Result := Length(FMortas);
end;

function TSinkEspia.Item(AIdx: Integer): TMortaRegistrada;
begin
  Result := FMortas[AIdx];
end;

function PolDlx(const ADlx: string; ATtlMs: Int64 = -1): TAMQPQueuePolicy;
begin
  Result := TAMQPQueuePolicy.Empty;
  Result.DeadLetterExchange := ADlx;
  Result.HasDeadLetterExchange := True;
  Result.MessageTtlMs := ATtlMs;
end;

// Le uma entrada do x-death do payload registrado pelo sink.
function LeMorte(const APayload: TBytes; AIdx: Integer;
  const ACampo: string): string;
var
  LEd: TAMQPHeaderEditor;
  LArr, LElem, LVal: TValue;
  LTab: TAMQPFieldTable;
begin
  Result := '';
  LEd := TAMQPHeaderEditor.Create(APayload);
  try
    if not LEd.Headers.TryGetValue('x-death', LArr) then
      Exit;
    LArr := AmqpUnwrapValue(LArr);
    if (not LArr.IsArray) or (AIdx >= LArr.GetArrayLength) then
      Exit;
    LElem := AmqpUnwrapValue(LArr.GetArrayElement(AIdx));
    if not (LElem.IsObject and (LElem.AsObject is TAMQPFieldTable)) then
      Exit;
    LTab := TAMQPFieldTable(LElem.AsObject);
    if LTab.TryGetValue(ACampo, LVal) then
    begin
      LVal := AmqpUnwrapValue(LVal);
      if LVal.Kind = tkInt64 then
        Result := IntToStr(LVal.AsInt64)
      else if LVal.Kind = tkInteger then
        Result := IntToStr(LVal.AsInteger)
      else if not LVal.IsObject then
        Result := LVal.AsString;
    end;
  finally
    LEd.Free;
  end;
end;

function ContaMortes(const APayload: TBytes): Integer;
var
  LEd: TAMQPHeaderEditor;
  LArr: TValue;
begin
  Result := 0;
  LEd := TAMQPHeaderEditor.Create(APayload);
  try
    if not LEd.Headers.TryGetValue('x-death', LArr) then
      Exit;
    LArr := AmqpUnwrapValue(LArr);
    if LArr.IsArray then
      Result := LArr.GetArrayLength;
  finally
    LEd.Free;
  end;
end;

// Publica com um content-header de verdade (o dead-lettering reescreve o
// header, entao um payload vazio nao serviria).
procedure PublicaComHeader(AQ: TAMQPServerQueue; const ATexto: string;
  const AExpiration: string = ''; APrio: Byte = 0);
var
  LProps: TAMQPBasicProperties;
  LMsg: TAMQPMessage;
  LCorpo, LHdr: TBytes;
begin
  LCorpo := AmqpUtf8Encode(ATexto);
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  if AExpiration <> '' then
    LProps.SetExpiration(AExpiration);
  LHdr := BuildContentHeader(Length(LCorpo), LProps);
  LMsg := TAMQPMessage.Create('ex.origem', 'rk.origem', 'guest', LHdr, LCorpo);
  try
    // No caminho real quem le 'expiration' e' a engine, na thread do
    // publicador (D1). Aqui o teste e' de nivel de fila, entao passa na mao.
    if AExpiration <> '' then
      AQ.PostMessage(LMsg, APrio, StrToInt64(AExpiration))
    else
      AQ.PostMessage(LMsg, APrio, -1);
  finally
    LMsg.Release;
  end;
end;

procedure TDeadLetterTests.Rejeitada_VaiParaODlx;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
  LStats: TAMQPQueueStats;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TAMQPServerQueue.Create('q.origem', PolDlx('dlx'));
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'm');
    ChecaBool('tirou em modo ack', True, LQ.Get(1, 5, False, LMsg, LRedel));
    LMsg.Release;
    LQ.PostNack(1, 5, False, False); // reject sem requeue = morte
    LStats := Estabiliza(LQ);
    ChecaInt('uma republicada', 1, LSink.Count);
    ChecaInt('contada', 1, LStats.DeadLetteredCount);
    ChecaStr('foi para o dlx', 'dlx', LSink.Item(0).Dlx);
    ChecaStr('corpo intacto', 'm', LSink.Item(0).Corpo);
    ChecaStr('razao', 'rejected',
      LeMorte(LSink.Item(0).HeaderPayload, 0, 'reason'));
    ChecaStr('fila de origem', 'q.origem',
      LeMorte(LSink.Item(0).HeaderPayload, 0, 'queue'));
    ChecaStr('exchange original', 'ex.origem',
      LeMorte(LSink.Item(0).HeaderPayload, 0, 'exchange'));
  finally
    LQ.Free;
  end;
end;

// O ACK nao pode virar morte. Os dois chegam ao ResolveUnacked com
// requeue=false, e confundi-los mandaria TODA mensagem confirmada para o DLX.
procedure TDeadLetterTests.Ack_NaoVaiParaODlx;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TAMQPServerQueue.Create('q', PolDlx('dlx'));
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'm');
    ChecaBool('tirou', True, LQ.Get(1, 5, False, LMsg, LRedel));
    LMsg.Release;
    LQ.PostAck(1, 5, False);
    Estabiliza(LQ);
    ChecaInt('ack nao republica nada', 0, LSink.Count);
  finally
    LQ.Free;
  end;
end;

// Nack COM requeue tambem nao e' morte: volta para a fila.
procedure TDeadLetterTests.NackComRequeue_NaoVaiParaODlx;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
  LStats: TAMQPQueueStats;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TAMQPServerQueue.Create('q', PolDlx('dlx'));
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'm');
    ChecaBool('tirou', True, LQ.Get(1, 5, False, LMsg, LRedel));
    LMsg.Release;
    LQ.PostNack(1, 5, False, True);
    LStats := Estabiliza(LQ);
    ChecaInt('nada republicado', 0, LSink.Count);
    ChecaInt('voltou para a fila', 1, LStats.MessageCount);
  finally
    LQ.Free;
  end;
end;

procedure TDeadLetterTests.Expirada_VaiParaODlxComRazaoExpired;
var
  LQ: TQueueRelogio;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TQueueRelogio.Create('q.ttl', PolDlx('dlx', 500));
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'm');
    Estabiliza(LQ);
    LQ.Avanca(501);
    LQ.PostDeliverTick;
    Estabiliza(LQ);
    ChecaInt('uma republicada', 1, LSink.Count);
    ChecaStr('razao', 'expired',
      LeMorte(LSink.Item(0).HeaderPayload, 0, 'reason'));
  finally
    LQ.Free;
  end;
end;

procedure TDeadLetterTests.EstourouOTeto_VaiParaODlxComRazaoMaxlen;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TAMQPServerQueue.Create('q', PolDlx('dlx'), 1);
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'primeira');
    PublicaComHeader(LQ, 'segunda'); // estoura o teto de 1
    Estabiliza(LQ);
    ChecaInt('a que saiu foi republicada', 1, LSink.Count);
    ChecaStr('razao', 'maxlen',
      LeMorte(LSink.Item(0).HeaderPayload, 0, 'reason'));
    ChecaStr('foi a mais velha', 'primeira', LSink.Item(0).Corpo);
  finally
    LQ.Free;
  end;
end;

// Sem x-dead-letter-routing-key, a chave ORIGINAL e mantida -- e' o que faz um
// DLX topic continuar roteando pelo mesmo criterio.
procedure TDeadLetterTests.SemRoutingKeyPropria_MantemAOriginal;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LPol: TAMQPQueuePolicy;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TAMQPServerQueue.Create('q', PolDlx('dlx'));
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'm');
    LQ.Get(1, 5, False, LMsg, LRedel);
    LMsg.Release;
    LQ.PostNack(1, 5, False, False);
    Estabiliza(LQ);
    ChecaStr('chave original preservada', 'rk.origem', LSink.Item(0).RoutingKey);
  finally
    LQ.Free;
  end;

  // Com chave propria, ela substitui.
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LPol := PolDlx('dlx');
  LPol.DeadLetterRoutingKey := 'rk.morta';
  LPol.HasDeadLetterRoutingKey := True;
  LQ := TAMQPServerQueue.Create('q', LPol);
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'm');
    LQ.Get(1, 5, False, LMsg, LRedel);
    LMsg.Release;
    LQ.PostNack(1, 5, False, False);
    Estabiliza(LQ);
    ChecaStr('chave trocada', 'rk.morta', LSink.Item(0).RoutingKey);
  finally
    LQ.Free;
  end;
end;

// Morrer DUAS VEZES na mesma fila pela mesma razao nao pode criar duas
// entradas: o campo count existe justamente para isso, e sem o colapso um laco
// de retry infla o header a cada volta ate' estourar o frame-max.
procedure TDeadLetterTests.MesmaFilaEMesmaRazao_ColapsaNoCount;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
  LPayload: TBytes;
  I: Integer;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TAMQPServerQueue.Create('q.retry', PolDlx('dlx'));
  try
    LQ.DeadLetterSink := LSink;
    // Primeira morte.
    PublicaComHeader(LQ, 'm');
    LQ.Get(1, 1, False, LMsg, LRedel);
    LMsg.Release;
    LQ.PostNack(1, 1, False, False);
    Estabiliza(LQ);
    ChecaInt('uma entrada apos a 1a morte', 1,
      ContaMortes(LSink.Item(0).HeaderPayload));
    ChecaStr('count 1', '1', LeMorte(LSink.Item(0).HeaderPayload, 0, 'count'));

    // Devolve a MESMA mensagem morta para a fila e mata de novo, simulando a
    // volta do laco de retry.
    LPayload := LSink.Item(0).HeaderPayload;
    for I := 2 to 3 do
    begin
      LMsg := TAMQPMessage.Create('ex.origem', 'rk.origem', 'guest', LPayload,
        AmqpUtf8Encode('m'));
      try
        LQ.PostMessage(LMsg, 0, -1);
      finally
        LMsg.Release;
      end;
      LQ.Get(1, UInt64(I), False, LMsg, LRedel);
      LMsg.Release;
      LQ.PostNack(1, UInt64(I), False, False);
      Estabiliza(LQ);
      LPayload := LSink.Item(LSink.Count - 1).HeaderPayload;
      ChecaInt('continua UMA entrada', 1, ContaMortes(LPayload));
      ChecaStr('count acumulou', IntToStr(I),
        LeMorte(LPayload, 0, 'count'));
    end;
  finally
    LQ.Free;
  end;
end;

// A mensagem morta por TTL nao pode levar o MESMO 'expiration' para a DLQ:
// morreria no ato e o retry-com-espera nao funcionaria. O valor antigo vira
// 'original-expiration' na entrada.
procedure TDeadLetterTests.ExpirationEhRetiradaEVaiParaOriginalExpiration;
var
  LQ: TQueueRelogio;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LEd: TAMQPHeaderEditor;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TQueueRelogio.Create('q', PolDlx('dlx'));
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'm', '300');
    Estabiliza(LQ);
    LQ.Avanca(301);
    LQ.PostDeliverTick;
    Estabiliza(LQ);
    ChecaInt('republicada', 1, LSink.Count);
    ChecaStr('registrada na entrada', '300',
      LeMorte(LSink.Item(0).HeaderPayload, 0, 'original-expiration'));
    LEd := TAMQPHeaderEditor.Create(LSink.Item(0).HeaderPayload);
    try
      ChecaBool('propriedade expiration removida', False,
        LEd.Properties.Has(bpExpiration));
    finally
      LEd.Free;
    end;
  finally
    LQ.Free;
  end;
end;

// x-first-death-* registra a PRIMEIRA morte e nunca e' sobrescrito.
procedure TDeadLetterTests.PrimeiraMorte_RegistraXFirstDeath;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LEd: TAMQPHeaderEditor;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
  LVal: TValue;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LQ := TAMQPServerQueue.Create('q.primeira', PolDlx('dlx'));
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'm');
    LQ.Get(1, 1, False, LMsg, LRedel);
    LMsg.Release;
    LQ.PostNack(1, 1, False, False);
    Estabiliza(LQ);
    LEd := TAMQPHeaderEditor.Create(LSink.Item(0).HeaderPayload);
    try
      ChecaBool('tem x-first-death-queue', True,
        LEd.Headers.TryGetValue('x-first-death-queue', LVal));
      ChecaStr('fila da primeira morte', 'q.primeira', LVal.AsString);
      ChecaBool('tem x-first-death-reason', True,
        LEd.Headers.TryGetValue('x-first-death-reason', LVal));
      ChecaStr('razao da primeira morte', 'rejected', LVal.AsString);
    finally
      LEd.Free;
    end;
  finally
    LQ.Free;
  end;
end;

// D14: o grafo de DLX pode ter ciclo. Depois do teto de saltos a mensagem
// morre de vez, em vez de circular para sempre.
procedure TDeadLetterTests.GuardaDeCiclo_ParaDepoisDoTeto;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LMsg: TAMQPMessage;
  LRedel: Boolean;
  LPayload: TBytes;
  I, LAntes: Integer;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  // Cada volta usa uma fila de nome DIFERENTE, senao o colapso por (fila,
  // razao) manteria uma entrada so' e o teto (que soma os counts) seria o
  // mesmo -- mas o teste ficaria menos claro sobre o que esta contando.
  LPayload := nil;
  for I := 1 to AMQP_MAX_DEADLETTER_HOPS + 2 do
  begin
    LQ := TAMQPServerQueue.Create('q' + IntToStr(I), PolDlx('dlx'));
    try
      LQ.DeadLetterSink := LSink;
      if LPayload = nil then
        PublicaComHeader(LQ, 'm')
      else
      begin
        LMsg := TAMQPMessage.Create('ex.origem', 'rk.origem', 'guest',
          LPayload, AmqpUtf8Encode('m'));
        try
          LQ.PostMessage(LMsg, 0, -1);
        finally
          LMsg.Release;
        end;
      end;
      LAntes := LSink.Count;
      LQ.Get(1, 1, False, LMsg, LRedel);
      LMsg.Release;
      LQ.PostNack(1, 1, False, False);
      Estabiliza(LQ);
      if LSink.Count = LAntes then
      begin
        // Parou de republicar: e' a guarda agindo.
        ChecaBool('parou no teto, nao antes', True,
          I > AMQP_MAX_DEADLETTER_HOPS);
        Exit;
      end;
      LPayload := LSink.Item(LSink.Count - 1).HeaderPayload;
    finally
      LQ.Free;
    end;
  end;
  ChecaBool('a guarda de ciclo tinha de ter agido', False, True);
end;

// Sem DLX configurado, morte e simples descarte -- o comportamento da WS4.
procedure TDeadLetterTests.SemDlx_MorteEhDescarte;
var
  LQ: TQueueRelogio;
  LStats: TAMQPQueueStats;
begin
  LQ := TQueueRelogio.Create('q', PolTtl(100));
  try
    PublicaComHeader(LQ, 'm');
    Estabiliza(LQ);
    LQ.Avanca(101);
    LQ.PostDeliverTick;
    LStats := Estabiliza(LQ);
    ChecaInt('sumiu', 0, LStats.MessageCount);
    ChecaInt('contada como expirada', 1, LStats.ExpiredCount);
    ChecaInt('nao contada como republicada', 0, LStats.DeadLetteredCount);
  finally
    LQ.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TQueueActorTests);
  TDUnitX.RegisterTestFixture(TQueuePriorityTtlTests);
  TDUnitX.RegisterTestFixture(TDeadLetterTests);

end.

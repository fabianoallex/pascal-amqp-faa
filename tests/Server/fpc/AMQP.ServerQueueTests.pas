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

  Espelho DUnitX de tests\Server\AMQP.ServerQueueTests.pas -- mantenha os dois
  em sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  AMQP.Wire,
  AMQP.Threading,
  AMQP.Server.Message,
  AMQP.Server.Resources,
  AMQP.Server.Header,
  Rtti,
  AMQP.Basic.Methods,
  AMQP.Server.Queue,
  AMQP.ServerTestDoubles;

type
  TQueueActorTests = class(TTestCase)
  published
    procedure Enqueue_ContaNasStats;
    procedure Get_OrdemFifo;
    procedure Get_FilaVazia_DevolveFalse;
    procedure Get_NoAck_NaoDeixaNaoConfirmada;
    procedure Get_ComAck_DeixaNaoConfirmadaEAckLimpa;
    procedure Ack_Multiple_LimpaAteATag;
    procedure Nack_Requeue_VoltaNaCabecaComRedelivered;
    procedure Nack_SemRequeue_Descarta;
    procedure Reject_Requeue_VoltaComRedelivered;
    procedure RemoveChannel_TiraConsumidoresEDevolveNaoConfirmadas;
    procedure Consumidores_AddERemove;
    procedure MaxLength_DescartaDaCabecaEConta;
    procedure MaxLengthZero_SemLimite;
    procedure Purge_LimpaProntasENaoAsNaoConfirmadas;
    procedure Delete_IfUnused_ComConsumidor_Recusa;
    procedure Delete_IfEmpty_ComMensagem_Recusa;
    procedure Delete_DevolveContagemEParaAFila;
    procedure Stop_DescartaPostsNovosSemVazar;
    procedure Stop_SincronoDepois_Levanta;
    procedure RefCount_FilaSeguraUmaReferencia;
    procedure Sincrono_DeDentroDoAtor_Levanta;
    procedure Estresse_ProdutoresConcorrentesESincronos;
    procedure Delete_AvisaConsumidoresComCancel;
    procedure EverHadConsumer_SoViraVerdadeComConsumidor;
    procedure CaixaSerial_PostNaJanelaDoFimDeRodada_NaoPerdeWakeup;
  end;

  TQueuePriorityTtlTests = class(TTestCase)
  published
    procedure Prioridade_MaiorSaiPrimeiro;
    procedure Prioridade_FifoDentroDoNivel;
    procedure Prioridade_AcimaDoTetoEhLimitada;
    procedure Prioridade_RequeueVoltaAoProprioBalde;
    procedure MaxLength_DescartaDoBaldeMenosPrioritario;
    procedure Ttl_DaFila_VenceNoPrazo;
    procedure Ttl_NaoVenceAntesDoPrazo;
    procedure Ttl_MenorEntreFilaEMensagemVence;
    procedure Ttl_GetNaoDevolveVencida;
    procedure Ttl_RequeuePreservaOPrazo;
    procedure Ttl_RequeueAposEntregaAConsumidor_PreservaOPrazo;
    procedure Prioridade_RequeueAposEntregaAConsumidor_VoltaAoProprioBalde;
  end;

  TDeadLetterTests = class(TTestCase)
  published
    procedure Rejeitada_VaiParaODlx;
    procedure Ack_NaoVaiParaODlx;
    procedure NackComRequeue_NaoVaiParaODlx;
    procedure Expirada_VaiParaODlxComRazaoExpired;
    procedure EstourouOTeto_VaiParaODlxComRazaoMaxlen;
    procedure SemRoutingKeyPropria_MantemAOriginal;
    procedure MesmaFilaEMesmaRazao_ColapsaNoCount;
    procedure ExpirationEhRetiradaEVaiParaOriginalExpiration;
    procedure PrimeiraMorte_RegistraXFirstDeath;
    procedure GuardaDeCiclo_ParaDepoisDoTeto;
    procedure SemDlx_MorteEhDescarte;
  end;

  TOverflowTests = class(TTestCase)
  published
    procedure DropHead_DescartaAMaisVelha;
    procedure MaxLengthBytes_ContaOCorpo;
    procedure MaxLengthBytes_AcumuladorVoltaAoConsumir;
    procedure RejectPublish_RecusaQuandoCheia;
    procedure RejectPublish_VoltaAAceitarAoEsvaziar;
    procedure RejectPublish_VencidaNaoOcupaVaga;
    procedure TetoGlobalEDaFila_OMenorVence;
    procedure DropHead_ContinuaIndoParaODlx;
  end;

  TIdleExpiryTests = class(TTestCase)
  published
    procedure NasceUsadaAgora;
    procedure OciosidadeCresceComORelogio;
    procedure PublicarNaoContaComoUso;
    procedure GetContaComoUso;
    procedure ComConsumidor_OciosidadeEhZero;
    procedure RelogioComecaQuandoOUltimoConsumidorSai;
    procedure TouchReiniciaORelogio;
    procedure SemXExpires_NaoEhCandidata;
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
    AssertEquals('prontas', 1, LStats.MessageCount);
    AssertEquals('nao-confirmadas', 0, LStats.UnackedCount);
    AssertEquals('consumidores', 0, LStats.ConsumerCount);
    AssertEquals('descartadas', 0, Integer(LStats.DroppedCount));
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
    AssertEquals('primeira', 'm1', GetCorpo(LQ, 1));
    AssertEquals('segunda', 'm2', GetCorpo(LQ, 2));
    AssertEquals('terceira', 'm3', GetCorpo(LQ, 3));
    AssertEquals('esvaziou', 0, LQ.Stats.MessageCount);
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
    AssertFalse('fila vazia', LQ.Get(1, 1, True, LMsg, LRedelivered));
    AssertTrue('sem mensagem', LMsg = nil);
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
    AssertEquals('corpo', 'a', GetCorpo(LQ, 7));
    AssertEquals('no-ack nao gera nao-confirmada', 0, LQ.Stats.UnackedCount);
    AssertEquals('saiu do estoque', 0, LQ.Stats.MessageCount);
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

    AssertTrue('get', LQ.Get(5, 100, False, LSaida, LRedelivered));
    try
      AssertFalse('primeira entrega nao e reentrega', LRedelivered);
      AssertEquals('ficou nao-confirmada', 1, LQ.Stats.UnackedCount);
      AssertEquals('saiu das prontas', 0, LQ.Stats.MessageCount);
      // Ack de outro canal com a mesma tag NAO pode limpar.
      LQ.PostAck(6, 100, False);
      AssertEquals('ack de outro canal nao conta', 1, LQ.Stats.UnackedCount);
      LQ.PostAck(5, 100, False);
      AssertEquals('ack limpou', 0, LQ.Stats.UnackedCount);
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
      AssertTrue('get ' + IntToStr(I),
        LQ.Get(9, I, False, LSaida, LRedelivered));
      LSaida.Release;
    end;
    AssertEquals('tres nao-confirmadas', 3, LQ.Stats.UnackedCount);

    LQ.PostAck(9, 2, True); // multiple: tags 1 e 2
    AssertEquals('sobrou a tag 3', 1, LQ.Stats.UnackedCount);
    LQ.PostAck(9, 3, False);
    AssertEquals('limpou', 0, LQ.Stats.UnackedCount);
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

    AssertTrue('get', LQ.Get(1, 10, False, LSaida, LRedelivered));
    AssertEquals('tirou a primeira', 'primeira', CorpoDe(LSaida));
    LSaida.Release;

    LQ.PostNack(1, 10, False, True); // requeue
    AssertEquals('voltou para as prontas', 2, LQ.Stats.MessageCount);
    AssertEquals('saiu das nao-confirmadas', 0, LQ.Stats.UnackedCount);

    // Volta na CABECA (antes da 'segunda') e marcada como reentrega.
    AssertTrue('get de novo', LQ.Get(1, 11, True, LSaida, LRedelivered));
    try
      AssertEquals('voltou na cabeca', 'primeira', CorpoDe(LSaida));
      AssertTrue('redelivered', LRedelivered);
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
      AssertTrue('get', LQ.Get(1, 1, False, LSaida, LRedelivered));
      LSaida.Release;
      LQ.PostNack(1, 1, False, False); // sem requeue: descarta (DLX e Fase 3)
      AssertEquals('nao voltou', 0, LQ.Stats.MessageCount);
      AssertEquals('saiu das nao-confirmadas', 0, LQ.Stats.UnackedCount);
      // So' a referencia do teste sobrou.
      AssertEquals('fila soltou a referencia dela', 1, LMsg.RefCount);
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
    AssertTrue('get', LQ.Get(3, 42, False, LSaida, LRedelivered));
    LSaida.Release;

    LQ.PostReject(3, 42, True);
    AssertEquals('voltou', 1, LQ.Stats.MessageCount);

    AssertTrue('get de novo', LQ.Get(3, 43, True, LSaida, LRedelivered));
    try
      AssertTrue('redelivered', LRedelivered);
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
    AssertTrue('get', LQ.Get(77, 1, False, LSaida, LRedelivered));
    LSaida.Release;

    LQ.PostRemoveChannel(77);
    LStats := LQ.Stats;
    AssertEquals('sobrou o consumidor do canal 88', 1, LStats.ConsumerCount);
    AssertEquals('nao-confirmada do canal morto saiu', 0, LStats.UnackedCount);
    AssertEquals('e voltou para as prontas', 1, LStats.MessageCount);

    AssertTrue('get de novo', LQ.Get(88, 1, True, LSaida, LRedelivered));
    try
      AssertTrue('redelivered depois da queda do canal', LRedelivered);
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
    AssertEquals('dois consumidores', 2, LQ.Stats.ConsumerCount);

    // Tag certa, canal errado: nao remove.
    LQ.PostRemoveConsumer(2, 'ct-a');
    AssertEquals('canal errado nao remove', 2, LQ.Stats.ConsumerCount);

    LQ.PostRemoveConsumer(1, 'ct-a');
    AssertEquals('removeu o ct-a', 1, LQ.Stats.ConsumerCount);
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
    AssertEquals('segurou o teto', 3, LStats.MessageCount);
    AssertEquals('contou os descartes', 2, Integer(LStats.DroppedCount));
    // Sobraram as MAIS NOVAS: descarte e' da cabeca.
    AssertEquals('cabeca depois do descarte', 'm3', GetCorpo(LQ, 1));
    AssertEquals('seguinte', 'm4', GetCorpo(LQ, 2));
    AssertEquals('ultima', 'm5', GetCorpo(LQ, 3));
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
    AssertEquals('guardou todas', 50, LStats.MessageCount);
    AssertEquals('nada descartado', 0, Integer(LStats.DroppedCount));
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
    AssertTrue('get', LQ.Get(1, 1, False, LSaida, LRedelivered));
    try
      AssertEquals('purgou so as prontas', 3, LQ.Purge);
      LStats := LQ.Stats;
      AssertEquals('vazia', 0, LStats.MessageCount);
      AssertEquals('nao-confirmada intacta', 1, LStats.UnackedCount);
      AssertEquals('purge nao e descarte', 0, Integer(LStats.DroppedCount));
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
    AssertTrue('if-unused com consumidor',
      LQ.Delete(True, False, LCount) = amqqdEmUso);
    // A fila continua viva.
    AssertEquals('fila segue atendendo', 1, LQ.Stats.ConsumerCount);
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
    AssertTrue('if-empty com mensagem',
      LQ.Delete(False, True, LCount) = amqqdNaoVazia);
    AssertEquals('fila segue atendendo', 1, LQ.Stats.MessageCount);
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
    AssertTrue('delete', LQ.Delete(False, False, LCount) = amqqdOk);
    AssertEquals('contagem do Delete-Ok', 3, LCount);

    // Fila apagada nao volta a atender.
    LLevantou := False;
    try
      LQ.Stats;
    except
      on E: EAMQPQueueActor do
        LLevantou := True;
    end;
    AssertTrue('sincrono numa fila apagada levanta', LLevantou);
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
      AssertEquals('fila parada nao reteve referencia nenhuma', 1,
        LMsg.RefCount);
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
    AssertTrue('sincrono numa fila parada levanta', LLevantou);
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
      AssertEquals('so o criador', 1, LMsg.RefCount);
      LQ.PostMessage(LMsg);
      LQ.Stats; // barreira: o enqueue ja rodou
      AssertEquals('a fila tirou a dela', 2, LMsg.RefCount);
      AssertEquals('purgou', 1, LQ.Purge);
      AssertEquals('a fila devolveu a dela', 1, LMsg.RefCount);
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
    AssertTrue('a espia rodou dentro do ator', LQ.Tentou);
    AssertTrue('sincrono de dentro do ator tem de levantar EAMQPQueueActor',
      LQ.GuardBarrou);
    AssertEquals('o comando seguiu normalmente', 1, LQ.Stats.MessageCount);
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
    AssertEquals('produtores terminaram dentro do prazo', PRODUTORES,
      AmqpAtomicGet(LConcluidos));
    AssertTrue('os comandos sincronos concorreram de fato com os produtores',
      LVoltas > 0);

    LStats := LQ.Stats;
    AssertEquals('nenhum comando perdido nem duplicado',
      PRODUTORES * POR_PRODUTOR, LStats.MessageCount);
    AssertEquals('nada descartado', 0, Integer(LStats.DroppedCount));
    AssertEquals('purge devolve o total', PRODUTORES * POR_PRODUTOR, LQ.Purge);
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

    AssertEquals('o comando postado na janela do fim de rodada tem de ser atendido',
      1, AmqpAtomicGet(LQ.FConcluiu));
    AssertEquals('e sem levantar', 1, AmqpAtomicGet(LQ.FOk));
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

    AssertTrue('delete', LQ.Delete(False, False, LCount) = amqqdOk);

    // A fila sumiu debaixo dos consumidores: os dois tem de saber.
    AssertEquals('o primeiro foi avisado', 'ct-a', LA.Cancelados);
    AssertEquals('o segundo tambem', 'ct-b', LB.Cancelados);
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
    AssertFalse('fila nova nunca teve consumidor', LQ.Stats.EverHadConsumer);
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', False, False,
      AlvoQueRecusa(1)));
    AssertTrue('agora teve', LQ.Stats.EverHadConsumer);
    LQ.PostRemoveConsumer(1, 'ct');
    // Continua verdade DEPOIS de o consumidor sair -- e' o que distingue
    // "auto-delete que perdeu o ultimo" de "auto-delete que nunca teve".
    AssertTrue('e continua tendo tido', LQ.Stats.EverHadConsumer);
    AssertEquals('sem consumidor agora', 0, LQ.Stats.ConsumerCount);
  finally
    LQ.Free;
  end;
end;

// Wrappers de assercao: mantem o CORPO da fixture identico nos dois arquivos.
procedure ChecaInt(const AMsg: string; AEsperado, AReal: Int64);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AReal);
end;

procedure ChecaBool(const AMsg: string; AEsperado, AReal: Boolean);
begin
  TAssert.AssertTrue(AMsg, AReal = AEsperado);
end;

procedure ChecaStr(const AMsg, AEsperado, AReal: string);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AReal);
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
procedure TQueuePriorityTtlTests.Prioridade_RequeueAposEntregaAConsumidor_VoltaAoProprioBalde;
var
  LQ: TAMQPServerQueue;
  LAlvo: TAlvoFalso;
  LAlvoRef: IAMQPDeliveryTarget;
begin
  // A METADE DE PRIORIDADE do mesmo defeito do
  // Ttl_RequeueAposEntregaAConsumidor_PreservaOPrazo: sem copiar Priority para
  // a nao-confirmada, a devolucao cai num balde arbitrario.
  //
  // O 'media', publicado DEPOIS da devolucao, e' o que torna o teste
  // discriminante: se a 'alta' voltasse para o balde 0, ela sairia atras dele.
  // Sem esse terceiro nivel, a devolucao a' CABECA do balde errado ainda
  // pareceria certa.
  LQ := TAMQPServerQueue.Create('q', PolPrioridade(9));
  LAlvo := TAlvoFalso.Create(1);
  LAlvoRef := LAlvo;
  try
    // As duas ANTES do consumidor: com consumidor ja' anexado, o proprio
    // enqueue entrega na hora, e a primeira publicada sairia primeiro
    // independentemente da prioridade. (A primeira versao deste teste supos o
    // contrario e falhou -- a premissa errada era do TESTE, nao do codigo.)
    Publica(LQ, 'baixa', 0);
    Publica(LQ, 'alta', 5);
    Estabiliza(LQ);
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', False, False,
      LAlvoRef));
    LQ.PostDeliverTick;
    Estabiliza(LQ);
    ChecaInt('as duas foram entregues', 2, LAlvo.Count);
    ChecaStr('a de maior prioridade saiu primeiro', 'alta',
      LAlvo.Entrega(0).Corpo);

    LQ.PostRemoveConsumer(1, 'ct');
    Estabiliza(LQ);

    // Devolve a 'alta' pela tag que ELA recebeu -- nada de supor numero.
    LQ.PostNack(1, LAlvo.Entrega(0).DeliveryTag, False, True);
    Estabiliza(LQ);
    Publica(LQ, 'media', 3);
    Estabiliza(LQ);

    ChecaStr('a devolvida volta ao balde 5 e sai primeiro', 'alta',
      TiraCorpo(LQ, 10));
    ChecaStr('depois a media do balde 3', 'media', TiraCorpo(LQ, 11));
  finally
    LQ.Free;
  end;
end;

procedure TQueuePriorityTtlTests.Ttl_RequeueAposEntregaAConsumidor_PreservaOPrazo;
var
  LQ: TQueueRelogio;
  LAlvo: TAlvoFalso;
  LAlvoRef: IAMQPDeliveryTarget;
  LStats: TAMQPQueueStats;
begin
  // O irmao do Ttl_RequeuePreservaOPrazo, mas pelo caminho do CONSUMIDOR em
  // vez do Basic.Get -- e e' esse o ponto. O caminho do Get sempre copiou
  // Priority/ExpiraEm para a nao-confirmada; o da entrega a consumidor NAO
  // copiava, e o registro ficava com lixo de pilha. Como todo teste de requeue
  // usava Get, o defeito atravessou as Fases 2 e 3 sem aparecer.
  //
  // As duas metades do teste sao o que fecha o cerco: com prazo lixo PEQUENO a
  // mensagem morre cedo e a primeira metade cai; com prazo lixo GRANDE (ou
  // zero, que aqui significa "nunca vence") ela nao morre nunca e a segunda
  // metade cai.
  LQ := TQueueRelogio.Create('q', PolTtl(1000));
  LAlvo := TAlvoFalso.Create(1);
  LAlvoRef := LAlvo; // o teste segura a referencia (licao da WS5)
  try
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', False, False,
      LAlvoRef));
    Publica(LQ, 'm');
    LQ.PostDeliverTick;
    Estabiliza(LQ);
    ChecaInt('entregue ao consumidor', 1, LAlvo.Count);

    // Tira o consumidor para a devolucao nao ser reentregue na hora.
    LQ.PostRemoveConsumer(1, 'ct');
    Estabiliza(LQ);

    LQ.Avanca(500);
    LQ.PostNack(1, 1, False, True); // requeue da tag 1
    LQ.PostDeliverTick;
    LStats := Estabiliza(LQ);
    ChecaInt('a meio prazo continua viva', 1, LStats.MessageCount);
    ChecaInt('e nao foi contada como expirada', 0, LStats.ExpiredCount);

    LQ.Avanca(600); // 1100 no total, acima do TTL de 1000
    LQ.PostDeliverTick;
    LStats := Estabiliza(LQ);
    ChecaInt('passado o prazo ORIGINAL, vence', 0, LStats.MessageCount);
    ChecaInt('contada como expirada', 1, LStats.ExpiredCount);
  finally
    LQ.Free;
  end;
end;

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

{ TOverflowTests }

function PolMaxLen(AMax: Int64; AOverflow: TAMQPOverflow = amqovDropHead;
  AMaxBytes: Int64 = -1): TAMQPQueuePolicy;
begin
  Result := TAMQPQueuePolicy.Empty;
  Result.MaxLength := AMax;
  Result.MaxLengthBytes := AMaxBytes;
  Result.Overflow := AOverflow;
end;

// drop-head e o default: ao estourar, a MAIS VELHA sai.
procedure TOverflowTests.DropHead_DescartaAMaisVelha;
var
  LQ: TAMQPServerQueue;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q', PolMaxLen(2));
  try
    Publica(LQ, 'a');
    Publica(LQ, 'b');
    Publica(LQ, 'c');
    LStats := Estabiliza(LQ);
    ChecaInt('teto respeitado', 2, LStats.MessageCount);
    ChecaInt('uma descartada', 1, LStats.DroppedCount);
    ChecaStr('a mais velha saiu', 'b', TiraCorpo(LQ, 1));
    ChecaStr('e a seguinte', 'c', TiraCorpo(LQ, 2));
  finally
    LQ.Free;
  end;
end;

// x-max-length-bytes conta o CORPO, nao a quantidade.
procedure TOverflowTests.MaxLengthBytes_ContaOCorpo;
var
  LQ: TAMQPServerQueue;
  LStats: TAMQPQueueStats;
begin
  // Teto de 10 bytes; cada corpo abaixo tem 4.
  LQ := TAMQPServerQueue.Create('q', PolMaxLen(-1, amqovDropHead, 10));
  try
    Publica(LQ, 'aaaa');
    Publica(LQ, 'bbbb');
    LStats := Estabiliza(LQ);
    ChecaInt('8 bytes cabem', 2, LStats.MessageCount);
    Publica(LQ, 'cccc'); // 12 bytes: estoura
    LStats := Estabiliza(LQ);
    ChecaInt('estourou, uma saiu', 2, LStats.MessageCount);
    ChecaInt('contada', 1, LStats.DroppedCount);
    ChecaStr('a mais velha saiu', 'bbbb', TiraCorpo(LQ, 1));
  finally
    LQ.Free;
  end;
end;

// O acumulador de bytes tem de VOLTAR ao tirar mensagem -- senao a fila fica
// "cheia" para sempre depois de um pico. E' o risco real de manter um total
// incremental em vez de somar na hora.
procedure TOverflowTests.MaxLengthBytes_AcumuladorVoltaAoConsumir;
var
  LQ: TAMQPServerQueue;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q', PolMaxLen(-1, amqovDropHead, 10));
  try
    Publica(LQ, 'aaaa');
    Publica(LQ, 'bbbb');
    ChecaStr('consome uma', 'aaaa', TiraCorpo(LQ, 1));
    // Sobraram 4 bytes; se o acumulador nao tivesse voltado, esta publicacao
    // pareceria estourar o teto e derrubaria a anterior.
    Publica(LQ, 'cccc');
    LStats := Estabiliza(LQ);
    ChecaInt('as duas cabem', 2, LStats.MessageCount);
    ChecaInt('nada descartado', 0, LStats.DroppedCount);
  finally
    LQ.Free;
  end;
end;

// D15: com reject-publish, o enqueue e SINCRONO e devolve False quando cheia.
procedure TOverflowTests.RejectPublish_RecusaQuandoCheia;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
  LStats: TAMQPQueueStats;
begin
  LQ := TAMQPServerQueue.Create('q', PolMaxLen(1, amqovRejectPublish));
  try
    LMsg := NovaMensagem('primeira');
    try
      ChecaBool('a primeira entra', True, LQ.TryPostMessage(LMsg));
    finally
      LMsg.Release;
    end;
    LMsg := NovaMensagem('segunda');
    try
      ChecaBool('a segunda e RECUSADA', False, LQ.TryPostMessage(LMsg));
    finally
      LMsg.Release;
    end;
    LStats := Estabiliza(LQ);
    ChecaInt('so a primeira ficou', 1, LStats.MessageCount);
    // Recusar NAO e descartar: a mensagem nunca entrou, entao nao conta como
    // dropped -- quem foi avisado foi o publicador.
    ChecaInt('recusa nao conta como descarte', 0, LStats.DroppedCount);
    ChecaStr('e e a primeira', 'primeira', TiraCorpo(LQ, 1));
  finally
    LQ.Free;
  end;
end;

// Depois de esvaziar, a fila volta a aceitar.
procedure TOverflowTests.RejectPublish_VoltaAAceitarAoEsvaziar;
var
  LQ: TAMQPServerQueue;
  LMsg: TAMQPMessage;
begin
  LQ := TAMQPServerQueue.Create('q', PolMaxLen(1, amqovRejectPublish));
  try
    LMsg := NovaMensagem('a');
    try
      LQ.TryPostMessage(LMsg);
    finally
      LMsg.Release;
    end;
    LMsg := NovaMensagem('b');
    try
      ChecaBool('cheia recusa', False, LQ.TryPostMessage(LMsg));
    finally
      LMsg.Release;
    end;
    ChecaStr('esvazia', 'a', TiraCorpo(LQ, 1));
    LMsg := NovaMensagem('c');
    try
      ChecaBool('vazia aceita de novo', True, LQ.TryPostMessage(LMsg));
    finally
      LMsg.Release;
    end;
  finally
    LQ.Free;
  end;
end;

// Uma mensagem VENCIDA nao pode ocupar vaga e fazer recusar um publish vivo.
procedure TOverflowTests.RejectPublish_VencidaNaoOcupaVaga;
var
  LQ: TQueueRelogio;
  LPol: TAMQPQueuePolicy;
  LMsg: TAMQPMessage;
begin
  LPol := PolMaxLen(1, amqovRejectPublish);
  LPol.MessageTtlMs := 100;
  LQ := TQueueRelogio.Create('q', LPol);
  try
    LMsg := NovaMensagem('velha');
    try
      ChecaBool('entra', True, LQ.TryPostMessage(LMsg));
    finally
      LMsg.Release;
    end;
    LQ.Avanca(101); // a velha venceu
    LMsg := NovaMensagem('nova');
    try
      ChecaBool('a vaga da vencida e liberada', True, LQ.TryPostMessage(LMsg));
    finally
      LMsg.Release;
    end;
    ChecaStr('e a nova esta la', 'nova', TiraCorpo(LQ, 1));
  finally
    LQ.Free;
  end;
end;

// O teto GLOBAL (D7) e o x-max-length da fila sao os dois um limite superior,
// entao o MENOR vence -- e o global e uma rede de seguranca do processo
// hospedeiro, que um argumento do cliente nao pode afrouxar.
procedure TOverflowTests.TetoGlobalEDaFila_OMenorVence;
var
  LQ: TAMQPServerQueue;
begin
  // Global 2, fila pede 5: vale 2.
  LQ := TAMQPServerQueue.Create('q', PolMaxLen(5), 2);
  try
    ChecaInt('o global aperta', 2, LQ.MaxLength);
  finally
    LQ.Free;
  end;

  // Global 5, fila pede 2: vale 2.
  LQ := TAMQPServerQueue.Create('q', PolMaxLen(2), 5);
  try
    ChecaInt('a fila aperta', 2, LQ.MaxLength);
  finally
    LQ.Free;
  end;

  // Sem global (0 = ilimitado), vale o da fila.
  LQ := TAMQPServerQueue.Create('q', PolMaxLen(3), 0);
  try
    ChecaInt('so a fila', 3, LQ.MaxLength);
  finally
    LQ.Free;
  end;

  // Sem nenhum dos dois: ilimitado.
  LQ := TAMQPServerQueue.Create('q', TAMQPQueuePolicy.Empty, 0);
  try
    ChecaInt('ilimitado', 0, LQ.MaxLength);
  finally
    LQ.Free;
  end;
end;

// O descarte por teto continua sendo morte com razao 'maxlen' (WS5).
procedure TOverflowTests.DropHead_ContinuaIndoParaODlx;
var
  LQ: TAMQPServerQueue;
  LSink: TSinkEspia;
  LSinkRef: IAMQPDeadLetterSink;
  LPol: TAMQPQueuePolicy;
begin
  LSink := TSinkEspia.Create;
  LSinkRef := LSink;
  LPol := PolMaxLen(1);
  LPol.DeadLetterExchange := 'dlx';
  LPol.HasDeadLetterExchange := True;
  LQ := TAMQPServerQueue.Create('q', LPol);
  try
    LQ.DeadLetterSink := LSink;
    PublicaComHeader(LQ, 'velha');
    PublicaComHeader(LQ, 'nova');
    Estabiliza(LQ);
    ChecaInt('a descartada foi republicada', 1, LSink.Count);
    ChecaStr('razao', 'maxlen',
      LeMorte(LSink.Item(0).HeaderPayload, 0, 'reason'));
    ChecaStr('e foi a mais velha', 'velha', LSink.Item(0).Corpo);
  finally
    LQ.Free;
  end;
end;

{ TIdleExpiryTests }

function PolExpires(AMs: Int64): TAMQPQueuePolicy;
begin
  Result := TAMQPQueuePolicy.Empty;
  Result.ExpiresMs := AMs;
end;

// Fila recem-criada nasce "usada agora": nao pode estar ociosa de saida.
procedure TIdleExpiryTests.NasceUsadaAgora;
var
  LQ: TQueueRelogio;
begin
  LQ := TQueueRelogio.Create('q', PolExpires(1000));
  try
    ChecaInt('ociosidade zero ao nascer', 0, Estabiliza(LQ).IdleMs);
  finally
    LQ.Free;
  end;
end;

procedure TIdleExpiryTests.OciosidadeCresceComORelogio;
var
  LQ: TQueueRelogio;
begin
  LQ := TQueueRelogio.Create('q', PolExpires(1000));
  try
    Estabiliza(LQ);
    LQ.Avanca(700);
    ChecaInt('700 ms sem uso', 700, Estabiliza(LQ).IdleMs);
  finally
    LQ.Free;
  end;
end;

// PUBLICAR NAO E USO -- e' a regra do RabbitMQ, e a razao dela e' o proposito
// do argumento: uma fila que so' recebe e ninguem le e' exatamente a que se
// quer recolher.
procedure TIdleExpiryTests.PublicarNaoContaComoUso;
var
  LQ: TQueueRelogio;
begin
  LQ := TQueueRelogio.Create('q', PolExpires(1000));
  try
    Estabiliza(LQ);
    LQ.Avanca(500);
    Publica(LQ, 'm');
    LQ.Avanca(300);
    ChecaInt('o publish nao reiniciou o relogio', 800, Estabiliza(LQ).IdleMs);
  finally
    LQ.Free;
  end;
end;

// Basic.Get E' uso.
procedure TIdleExpiryTests.GetContaComoUso;
var
  LQ: TQueueRelogio;
begin
  LQ := TQueueRelogio.Create('q', PolExpires(1000));
  try
    Publica(LQ, 'm');
    Estabiliza(LQ);
    LQ.Avanca(500);
    TiraCorpo(LQ, 1);
    LQ.Avanca(200);
    ChecaInt('o Get reiniciou o relogio', 200, Estabiliza(LQ).IdleMs);
  finally
    LQ.Free;
  end;
end;

// Com consumidor a fila esta em uso POR DEFINICAO: a ociosidade e' zero, por
// mais tempo que passe.
procedure TIdleExpiryTests.ComConsumidor_OciosidadeEhZero;
var
  LQ: TQueueRelogio;
  LAlvo: TAlvoFalso;
  LAlvoRef: IAMQPDeliveryTarget;
begin
  LQ := TQueueRelogio.Create('q', PolExpires(1000));
  LAlvo := TAlvoFalso.Create(1);
  LAlvoRef := LAlvo; // o teste segura a referencia (licao da WS5)
  try
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', True, False, LAlvo));
    Estabiliza(LQ);
    LQ.Avanca(999999);
    ChecaInt('consumidor mantem a fila em uso', 0, Estabiliza(LQ).IdleMs);
  finally
    LQ.Free;
  end;
end;

// O relogio comeca quando o ULTIMO consumidor sai -- nao do instante em que
// ele entrou.
procedure TIdleExpiryTests.RelogioComecaQuandoOUltimoConsumidorSai;
var
  LQ: TQueueRelogio;
  LAlvo: TAlvoFalso;
  LAlvoRef: IAMQPDeliveryTarget;
begin
  LQ := TQueueRelogio.Create('q', PolExpires(1000));
  LAlvo := TAlvoFalso.Create(1);
  LAlvoRef := LAlvo; // o teste segura a referencia (licao da WS5)
  try
    LQ.PostAddConsumer(TAMQPServerConsumer.Create('ct', True, False, LAlvo));
    Estabiliza(LQ);
    LQ.Avanca(5000); // muito tempo COM consumidor
    LQ.PostRemoveConsumer(1, 'ct');
    Estabiliza(LQ);
    ChecaInt('zera na saida do consumidor', 0, Estabiliza(LQ).IdleMs);
    LQ.Avanca(300);
    ChecaInt('e passa a contar dali', 300, Estabiliza(LQ).IdleMs);
  finally
    LQ.Free;
  end;
end;

// Redeclare conta como uso (o PostTouch da engine).
procedure TIdleExpiryTests.TouchReiniciaORelogio;
var
  LQ: TQueueRelogio;
begin
  LQ := TQueueRelogio.Create('q', PolExpires(1000));
  try
    Estabiliza(LQ);
    LQ.Avanca(800);
    LQ.PostTouch;
    // PostTouch e' ASSINCRONO: sem a barreira, ele rodaria depois do Avanca
    // abaixo e leria o relogio ja adiantado. Um sincrono posto depois de um
    // assincrono e' a barreira do projeto -- a caixa e' serial e FIFO.
    Estabiliza(LQ);
    LQ.Avanca(100);
    ChecaInt('o touch reiniciou', 100, Estabiliza(LQ).IdleMs);
  finally
    LQ.Free;
  end;
end;

// Fila SEM x-expires nao paga nada: a varredura da engine nem pede Stats dela,
// e o IdleMs continua sendo so' informativo.
procedure TIdleExpiryTests.SemXExpires_NaoEhCandidata;
var
  LQ: TQueueRelogio;
begin
  LQ := TQueueRelogio.Create('q', TAMQPQueuePolicy.Empty);
  try
    ChecaInt('sem x-expires', -1, LQ.Policy.ExpiresMs);
    Estabiliza(LQ);
    LQ.Avanca(999999);
    // O IdleMs continua sendo calculado, mas ninguem age sobre ele.
    ChecaBool('ociosidade so informativa', True, Estabiliza(LQ).IdleMs > 0);
  finally
    LQ.Free;
  end;
end;

initialization
  RegisterTest(TQueueActorTests);
  RegisterTest(TQueuePriorityTtlTests);
  RegisterTest(TDeadLetterTests);
  RegisterTest(TOverflowTests);
  RegisterTest(TIdleExpiryTests);

end.

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

initialization
  RegisterTest(TQueueActorTests);

end.

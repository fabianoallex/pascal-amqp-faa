unit AMQP.ServerConfirmTests;

{ Confirm assincrono preso a marca d'agua -- WS5 da Fase 4 (durabilidade,
  ver CLAUDE.md, decisoes D19-D28; esta e' a D24).

  E' a camada de baixo, e o teste-ancora da WS5: o rastreador com um
  TAMQPFrameWriter DE VERDADE sobre um stream que registra, para as assercoes
  serem sobre os FRAMES QUE SAIRAM NO WIRE e nao sobre estado interno. E'
  determinista de ponta a ponta -- a marca d'agua entra como PARAMETRO, e nao
  vem de um fsync -- e por isso nada aqui depende de tempo.

  O que estes testes prendem, e que nenhuma suite de ponta a ponta prende:

  1. O confirm NAO sai antes de o LSN estar duravel. E' a razao de a WS5
     existir; sem isto o broker prometeria durabilidade que ainda nao tem.

  2. O corolario (a) da D24 -- confirm sai em ordem de seq POR CANAL. Um
     publish TRANSIENTE atras de um persistente pendente ESPERA, e sai
     colapsado junto. Um teste de cliente nao pega isso: ele espera por todos
     os seqs e ficaria verde com a ordem trocada.

  3. O colapso da D9 -- N confirms viram UM Basic.Ack com multiple=true. Nao
     basta "o confirm chegou": uma implementacao que emitisse um frame por seq
     passaria em qualquer teste que so' contasse confirms recebidos.

  4. O caminho rapido continua rapido: sem LSN e sem pendente, nao adia nada --
     o broker sem DataDir e' letra por letra o da Fase 3 (D19).

  As tres funcoes Checa* existem para os corpos dos testes serem IDENTICOS nos
  dois dialetos: so' a implementacao delas muda de lado. }

{ Espelho DUnitX de tests\Server\AMQP.ServerConfirmTests.pas -- mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  AMQP.Protocol,
  AMQP.Frame,
  AMQP.Wire,
  AMQP.Basic.Methods,
  AMQP.Server.Types,
  AMQP.Server.FrameIO,
  AMQP.Server.Journal,
  AMQP.Server.Confirm,
  AMQP.ServerTestDoubles;

type
  { Um Basic.Ack/Nack lido do wire. }
  TConfirmLido = record
    Seq: UInt64;
    Multiple: Boolean;
    Nack: Boolean;
  end;

  { A camada de baixo do confirm assincrono: um rastreador de canal. }
  TConfirmTrackerTests = class(TTestCase)
  private
    FStream: TStreamGravador;
    FWriter: TAMQPFrameWriter;
    FTracker: IAMQPConfirmTracker;
    /// Confirms escritos ate agora, decodificados do wire. PARA a escritora --
    /// chame uma vez so', no fim do teste.
    function Confirms: TArray<TConfirmLido>;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
     procedure SemLsnESemPendente_NaoAdia;
     procedure ComLsn_Adia_ESoSaiDepoisDaMarca;
     procedure TransienteAtrasDePersistente_Espera;
     procedure VariosPersistentes_SaemNumAckSoComMultiple;
     procedure MarcaParcial_LiberaSoOPrefixo;
     procedure Nack_SaiSozinho_ECortaOColapso;
     procedure JournalFalho_TudoPendenteViraNack;
     procedure CanalMorto_NaoEscreveNada;
     procedure SaidaCheia_NaoBloqueiaQuemLibera;
  end;

  { O registro: a ponte entre a thread do journal e os canais. }
  TConfirmRegistryTests = class(TTestCase)
  published
     procedure Durable_LiberaTodosOsRastreadores;
     procedure Unregister_ParaDeAvisar;
     procedure Watermark_AcompanhaOUltimoDurable;
  end;

implementation

// As tres pontes para as assercoes do dialeto. Existem para os corpos dos
// testes serem IDENTICOS nos dois espelhos: so' estas tres mudam de lado.
procedure ChecaInt(const AMsg: string; AEsperado, AObtido: Integer);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertTrue(AMsg, ACond);
end;

procedure ChecaNao(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertFalse(AMsg, ACond);
end;

function LeConfirms(const AFrames: TArray<TAMQPFrame>): TArray<TConfirmLido>;
var
  I: Integer;
  LReader: TAMQPReader;
  LClasse, LMetodo: Word;
  LAck: TAMQPBasicAck;
  LNack: TAMQPBasicNack;
begin
  SetLength(Result, Length(AFrames));
  for I := 0 to High(AFrames) do
  begin
    LReader := TAMQPReader.Create(AFrames[I].Payload);
    try
      LClasse := LReader.ReadShortUInt;
      LMetodo := LReader.ReadShortUInt;
      Result[I].Nack := (LClasse = AMQP_CLASS_BASIC)
        and (LMetodo = AMQP_BASIC_NACK);
      if Result[I].Nack then
      begin
        LNack := DecodeBasicNack(LReader);
        Result[I].Seq := LNack.DeliveryTag;
        Result[I].Multiple := LNack.Multiple;
      end
      else
      begin
        LAck := DecodeBasicAck(LReader);
        Result[I].Seq := LAck.DeliveryTag;
        Result[I].Multiple := LAck.Multiple;
      end;
    finally
      LReader.Free;
    end;
  end;
end;

{ --- TConfirmTrackerTests --- }

procedure TConfirmTrackerTests.SetUp;
begin
  inherited SetUp;
  FStream := TStreamGravador.Create;
  FWriter := TAMQPFrameWriter.Create(FStream);
  FTracker := TAMQPConfirmTracker.Create(FWriter, 7);
end;

procedure TConfirmTrackerTests.TearDown;
begin
  FTracker := nil;
  FWriter.Free;
  FStream.Free;
  inherited TearDown;
end;

function TConfirmTrackerTests.Confirms: TArray<TConfirmLido>;
begin
  Result := LeConfirms(FramesEscritos(FWriter, FStream));
end;

procedure TConfirmTrackerTests.SemLsnESemPendente_NaoAdia;
begin
  // O caminho rapido da Fase 3 continua existindo: publish transiente num
  // canal sem nada pendente confirma NA HORA, sem passar pelo rastreador.
  ChecaNao('nao adia', FTracker.TryDefer(1, 0, False));
  ChecaInt('nada ficou pendente', 0, FTracker.PendingCount);
  ChecaInt('e nada foi escrito por nossa conta', 0, Length(Confirms));
end;

procedure TConfirmTrackerTests.ComLsn_Adia_ESoSaiDepoisDaMarca;
var
  LC: TArray<TConfirmLido>;
begin
  // A razao de a WS5 existir: com LSN a esperar, o frame NAO sai enquanto o
  // disco nao responder.
  ChecaOk('adiou', FTracker.TryDefer(1, 10, False));
  FTracker.Release(9); // a marca ainda nao alcancou
  ChecaInt('segue pendente com a marca em 9', 1, FTracker.PendingCount);
  FTracker.Release(10);
  ChecaInt('liberou com a marca em 10', 0, FTracker.PendingCount);
  LC := Confirms;
  ChecaInt('um confirm', 1, Length(LC));
  ChecaInt('do seq 1', 1, Integer(LC[0].Seq));
  ChecaNao('e nao e nack', LC[0].Nack);
end;

procedure TConfirmTrackerTests.TransienteAtrasDePersistente_Espera;
var
  LC: TArray<TConfirmLido>;
begin
  // O corolario (a) da D24. O seq 2 nao escreveu nada no disco e poderia sair
  // agora -- mas sair agora furaria a ordem de confirms do canal, e o cliente
  // veria o ack do 2 antes do do 1. Ele espera, e sai COLAPSADO com o 1.
  ChecaOk('o persistente adia', FTracker.TryDefer(1, 10, False));
  ChecaOk('e o transiente atras dele TAMBEM adia', FTracker.TryDefer(2, 0, False));
  FTracker.Release(9);
  ChecaInt('nenhum dos dois saiu', 2, FTracker.PendingCount);
  FTracker.Release(10);
  ChecaInt('os dois sairam juntos', 0, FTracker.PendingCount);
  LC := Confirms;
  ChecaInt('num frame so', 1, Length(LC));
  ChecaInt('com o MAIOR seq', 2, Integer(LC[0].Seq));
  ChecaOk('e multiple=true, que e o que resolve o seq 1', LC[0].Multiple);
end;

procedure TConfirmTrackerTests.VariosPersistentes_SaemNumAckSoComMultiple;
var
  LC: TArray<TConfirmLido>;
  I: Integer;
begin
  // A letra da D9: o prefixo inteiro vira UM frame.
  for I := 1 to 5 do
    ChecaOk('adiou ' + IntToStr(I), FTracker.TryDefer(I, 100 + I, False));
  FTracker.Release(105);
  LC := Confirms;
  ChecaInt('cinco confirms num frame so', 1, Length(LC));
  ChecaInt('seq do ultimo', 5, Integer(LC[0].Seq));
  ChecaOk('multiple', LC[0].Multiple);
end;

procedure TConfirmTrackerTests.MarcaParcial_LiberaSoOPrefixo;
var
  LC: TArray<TConfirmLido>;
begin
  // Marca no meio: sai o prefixo, o resto fica. Um lote de fsync nao precisa
  // cobrir tudo o que esta pendente.
  FTracker.TryDefer(1, 10, False);
  FTracker.TryDefer(2, 20, False);
  FTracker.TryDefer(3, 30, False);
  FTracker.Release(20);
  ChecaInt('sobrou o seq 3', 1, FTracker.PendingCount);
  LC := Confirms;
  ChecaInt('um frame', 1, Length(LC));
  ChecaInt('ate o seq 2', 2, Integer(LC[0].Seq));
  ChecaOk('multiple', LC[0].Multiple);
end;

procedure TConfirmTrackerTests.Nack_SaiSozinho_ECortaOColapso;
var
  LC: TArray<TConfirmLido>;
begin
  // Nack marca UM seq: nao da para colapsa-lo com os vizinhos. Entao o ack
  // acumulado sai ANTES dele, e a acumulacao recomeca depois.
  FTracker.TryDefer(1, 10, False);
  FTracker.TryDefer(2, 0, True);  // reject-publish: nack, e nao espera disco
  FTracker.TryDefer(3, 0, False);
  FTracker.Release(10);
  ChecaInt('tudo saiu', 0, FTracker.PendingCount);
  LC := Confirms;
  ChecaInt('tres frames e nao um', 3, Length(LC));
  ChecaInt('1o: ack ate o seq 1', 1, Integer(LC[0].Seq));
  ChecaNao('1o nao e nack', LC[0].Nack);
  ChecaInt('2o: o nack do seq 2', 2, Integer(LC[1].Seq));
  ChecaOk('2o e nack', LC[1].Nack);
  ChecaNao('e nack nunca vai com multiple', LC[1].Multiple);
  ChecaInt('3o: ack do seq 3', 3, Integer(LC[2].Seq));
  ChecaNao('3o nao e nack', LC[2].Nack);
end;

procedure TConfirmTrackerTests.JournalFalho_TudoPendenteViraNack;
var
  LC: TArray<TConfirmLido>;
begin
  // A marca nao vai mais andar. Deixar o publicador pendurado seria o pior dos
  // mundos: ele nao veria nem sucesso nem falha.
  FTracker.TryDefer(1, 10, False);
  FTracker.TryDefer(2, 20, False);
  FTracker.FailAll;
  ChecaInt('nada ficou preso', 0, FTracker.PendingCount);
  LC := Confirms;
  ChecaInt('um nack por seq', 2, Length(LC));
  ChecaOk('o 1o e nack', LC[0].Nack);
  ChecaOk('o 2o e nack', LC[1].Nack);
  ChecaInt('seq 1', 1, Integer(LC[0].Seq));
  ChecaInt('seq 2', 2, Integer(LC[1].Seq));
end;

procedure TConfirmTrackerTests.CanalMorto_NaoEscreveNada;
begin
  // Quem libera e' a thread do JOURNAL, e o canal pode ter morrido no meio.
  // Depois do Detach nada e' escrito -- e nada e' adiado, porque nao ha para
  // quem confirmar.
  FTracker.TryDefer(1, 10, False);
  FTracker.Detach;
  ChecaInt('o pendente foi descartado', 0, FTracker.PendingCount);
  FTracker.Release(10);
  ChecaNao('e nem adia mais', FTracker.TryDefer(2, 10, False));
  ChecaInt('nenhum frame', 0, Length(Confirms));
end;

procedure TConfirmTrackerTests.SaidaCheia_NaoBloqueiaQuemLibera;
var
  LC: TArray<TConfirmLido>;
begin
  // A forma da D3 aplicada ao confirm: quem chama Release e' a thread do
  // JOURNAL, e ela nao pode esperar a fila de escrita de um canal esvaziar.
  // Com o stream travado, se a liberacao usasse PostFrames em vez de
  // TryPostFrames este teste NAO VOLTARIA -- a suite travaria em vez de
  // falhar, que e' o sintoma a reconhecer se ele um dia pendurar.
  FStream.Bloquear;
  try
    FTracker.TryDefer(1, 10, False);
    FTracker.Release(10);
    ChecaInt('a liberacao voltou sem esperar o stream', 0,
      FTracker.PendingCount);
  finally
    FStream.Liberar;
  end;
  LC := Confirms;
  ChecaInt('e o frame saiu quando o stream destravou', 1, Length(LC));
  ChecaInt('seq 1', 1, Integer(LC[0].Seq));
end;

{ --- TConfirmRegistryTests --- }

procedure TConfirmRegistryTests.Durable_LiberaTodosOsRastreadores;
var
  LReg: IAMQPConfirmRegistry;
  LSink: IAMQPDurabilitySink;
  LStreamA, LStreamB: TStreamGravador;
  LWriterA, LWriterB: TAMQPFrameWriter;
  LA, LB: IAMQPConfirmTracker;
begin
  LStreamA := TStreamGravador.Create;
  LStreamB := TStreamGravador.Create;
  LWriterA := TAMQPFrameWriter.Create(LStreamA);
  LWriterB := TAMQPFrameWriter.Create(LStreamB);
  try
    LReg := TAMQPConfirmRegistry.Create;
    LSink := LReg as IAMQPDurabilitySink;
    LA := LReg.CreateTracker(LWriterA, 1);
    LB := LReg.CreateTracker(LWriterB, 2);
    LA.TryDefer(1, 10, False);
    LB.TryDefer(1, 10, False);
    // UM fsync destrava os DOIS canais: e' o que "marca d'agua do broker
    // inteiro" quer dizer.
    LSink.Durable(10);
    ChecaInt('canal A liberou', 0, LA.PendingCount);
    ChecaInt('canal B liberou', 0, LB.PendingCount);
    ChecaInt('um confirm em A', 1, Length(LeConfirms(FramesEscritos(LWriterA, LStreamA))));
    ChecaInt('um confirm em B', 1, Length(LeConfirms(FramesEscritos(LWriterB, LStreamB))));
    LA := nil;
    LB := nil;
    LSink := nil;
    LReg := nil;
  finally
    LWriterA.Free;
    LWriterB.Free;
    LStreamA.Free;
    LStreamB.Free;
  end;
end;

procedure TConfirmRegistryTests.Unregister_ParaDeAvisar;
var
  LReg: IAMQPConfirmRegistry;
  LSink: IAMQPDurabilitySink;
  LStream: TStreamGravador;
  LWriter: TAMQPFrameWriter;
  LT: IAMQPConfirmTracker;
begin
  LStream := TStreamGravador.Create;
  LWriter := TAMQPFrameWriter.Create(LStream);
  try
    LReg := TAMQPConfirmRegistry.Create;
    LSink := LReg as IAMQPDurabilitySink;
    LT := LReg.CreateTracker(LWriter, 1);
    LT.TryDefer(1, 10, False);
    LReg.Unregister(LT);
    LSink.Durable(10);
    // Sem o Unregister, a thread do journal percorreria rastreadores de canais
    // mortos a cada fsync, para sempre.
    ChecaInt('o desregistrado nao foi avisado', 1, LT.PendingCount);
    LT.Detach; // senao o TearDown do writer o encontraria vivo
    LT := nil;
    LSink := nil;
    LReg := nil;
  finally
    LWriter.Free;
    LStream.Free;
  end;
end;

procedure TConfirmRegistryTests.Watermark_AcompanhaOUltimoDurable;
var
  LReg: IAMQPConfirmRegistry;
  LSink: IAMQPDurabilitySink;
begin
  LReg := TAMQPConfirmRegistry.Create;
  LSink := LReg as IAMQPDurabilitySink;
  ChecaInt('comeca em zero', 0, Integer(LReg.Watermark));
  LSink.Durable(42);
  ChecaInt('anda com o fsync', 42, Integer(LReg.Watermark));
  // O RetryPending recobra o que ficou preso sem INVENTAR durabilidade.
  LReg.RetryPending;
  ChecaInt('o retry nao mexe na marca', 42, Integer(LReg.Watermark));
  LSink := nil;
  LReg := nil;
end;

initialization
  RegisterTest(TConfirmTrackerTests);
  RegisterTest(TConfirmRegistryTests);

end.

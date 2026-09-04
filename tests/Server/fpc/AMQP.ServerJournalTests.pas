unit AMQP.ServerJournalTests;

{ Testes da thread do journal (AMQP.Server.Journal) -- WS2 da Fase 4
  (durabilidade, ver CLAUDE.md, decisoes D19-D28).

  O QUE ESTA SUITE PRECISA PROVAR, e que nao e' obvio:

  1. Que o group commit AGRUPA. Nao basta "gravou tudo": a D25 inteira e' a
     aposta de que o lote cresce sozinho sob carga, e o numero que mostra isso
     e' Registros/Syncs. Sem esta medida, uma implementacao que da' um fsync
     por registro passaria em todos os outros testes.

  2. Que o lote e' INDIVISIVEL. Quatro threads submetendo pares (aN, bN) em uma
     chamada cada: no arquivo os pares tem de sair ADJACENTES e com LSNs
     consecutivos. Sem esta checagem, atribuir os LSNs sob o lock e enfileirar
     FORA dele passaria despercebido -- foi exatamente a mutacao que motivou o
     teste. (Ela e' pega em dobro, alias: a invariante de LSN crescente do
     segmento, da WS1, recusa a escrita intercalada na camada de arquivo.)

  3. Que a marca d'agua NAO anda quando o fsync falha. E' a diferenca entre
     prometer durabilidade e mentir sobre ela, e so' e' observavel com o duble
     de arquivo (camada 2 da D28).

  4. Que uma falha de ESCRITA fecha a porta em definitivo, enquanto uma falha
     de FSYNC e' recuperavel. Sao tratamentos deliberadamente diferentes: no
     primeiro caso o registro nao esta' em lugar nenhum; no segundo os bytes ja'
     estao no arquivo e um fsync posterior os cobre.

  O duble de arquivo (TAMQPWalFileFalso) vive em AMQP.ServerTestDoubles, junto
  com o alvo de entrega -- e' a mesma unit e a mesma razao: dois arquivos de
  teste precisam do mesmo objeto, e duplicar andaime e' a superficie onde esta
  codebase ja' errou (corrigir um lado e esquecer o gemeo).

  Espelho DUnitX de tests\Server\AMQP.ServerJournalTests.pas -- mantenha os dois
  em sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  AMQP.Threading,
  AMQP.Server.Wal,
  AMQP.Server.Journal,
  AMQP.ServerTestDoubles;

type
  { Journal sobre o duble de arquivo, para contar fsyncs e injetar falha. }
  TJournalSobreDuble = class(TAMQPJournal)
  private
    FArq: TAMQPWalFileFalso;
    FIface: IAMQPWalFile;
  protected
    function CriaArquivo(const APath: string;
      ACriar: Boolean): IAMQPWalFile; override;
  public
    /// So' e' valido depois do Start.
    property Arq: TAMQPWalFileFalso read FArq;
  end;

  { Injeta UM registro na janela entre "tirei o lote da fila" e "escrevi".
    Mesma ideia do ActorRoundEnding da fila-ator: a janela e' inalcancavel por
    estresse comum, porque um registro perdido ali seria ressuscitado pelo
    submit seguinte -- o bug so' e' fatal no ULTIMO. }
  TJournalComJanela = class(TAMQPJournal)
  private
    FInjetou: Boolean;
    FLsnInjetado: UInt64;
  protected
    procedure LoteTomado; override;
  public
    property LsnInjetado: UInt64 read FLsnInjetado;
  end;

  TSinkFalso = class(TInterfacedObject, IAMQPDurabilitySink)
  private
    FUltimo: UInt64;
    FAvisos: Integer;
    FFalhas: Integer;
    FMensagem: string;
  public
    procedure Durable(ALsn: UInt64);
    procedure JournalFailed(const AMessage: string);
    property Ultimo: UInt64 read FUltimo;
    property Avisos: Integer read FAvisos;
    property Falhas: Integer read FFalhas;
    property Mensagem: string read FMensagem;
  end;

  { Submete pares (aN, bN) numa chamada so' -- o material do teste de
    indivisibilidade. }
  TProdutorFalso = class(TThread)
  private
    FJournal: TAMQPJournal;
    FQuantos: Integer;
    FErros: Integer;
  protected
    procedure Execute; override;
  public
    constructor Create(AJournal: TAMQPJournal; AQuantos: Integer);
    property Erros: Integer read FErros;
  end;

  { Base: nome de diretorio proprio por teste e limpeza. }
  TJournalFixture = class(TTestCase)
  protected
    FDir: string;
    procedure SetUp; override;
    procedure TearDown; override;
    function Rec(AKind: Byte; const S: string): TAMQPJournalRecord;
    function Um(AKind: Byte; const S: string): TAMQPJournalRecords;
  end;

  TJournalLsnTests = class(TJournalFixture)
  published
    procedure Submit_DevolveOLsnDoUltimo;
    procedure LsnsSaoContiguosEntreLotes;
    procedure SubmitVazio_Levanta;
    procedure SubmitAntesDoStart_Levanta;
    procedure SubmitDepoisDoStop_Levanta;
  end;

  TJournalGroupCommitTests = class(TJournalFixture)
  published
    procedure CargaConcorrente_AgrupaEmMenosFsyncsQueRegistros;
    procedure LoteEhIndivisivel_ParesSaemAdjacentes;
    procedure SubmitNaJanelaDoLote_NaoSePerde;
  end;

  TJournalDurabilidadeTests = class(TJournalFixture)
  published
    procedure FsyncOk_AvancaAMarcaDagua;
    procedure FsyncFalho_NaoAvancaAMarcaDagua;
    procedure FsyncFalho_NaoDerrubaOJournal;
    procedure FsyncSeguinte_CobreOLoteAnterior;
    procedure Sink_EhAvisadoComOMaiorLsn;
  end;

  TJournalFalhaTests = class(TJournalFixture)
  published
    procedure FalhaDeEscrita_FechaAPorta;
    procedure FalhaDeEscrita_AvisaOSinkUmaVez;
    procedure DepoisDaFalha_SubmitLevanta;
    procedure DepoisDaFalha_WaitDurableDevolveFalse;
  end;

  TJournalCicloDeVidaTests = class(TJournalFixture)
  published
    procedure SubmitNoWait_NuncaBloqueia;
    procedure Reinicio_LsnContinuaDeOndeParou;
    procedure Reinicio_MarcaDaguaNasceNoQueJaEstava;
    procedure Stop_DrenaOQueFoiAceito;
    procedure StartDuasVezes_EhIdempotente;
    procedure DoisJournalsNoMesmoDir_OSegundoEhRecusado;
  end;

implementation

var
  GSeq: Integer = 0;

function TextoDe(const B: TBytes): string;
var
  I: Integer;
begin
  SetLength(Result, Length(B));
  for I := 0 to High(B) do
    Result[I + 1] := Char(B[I]);
end;

{ TJournalSobreDuble }

function TJournalSobreDuble.CriaArquivo(const APath: string;
  ACriar: Boolean): IAMQPWalFile;
begin
  if FArq = nil then
  begin
    FArq := TAMQPWalFileFalso.Create;
    FIface := FArq; // a referencia de INTERFACE e quem o mantem vivo
  end;
  Result := FIface;
end;

{ TJournalComJanela }

procedure TJournalComJanela.LoteTomado;
const
  MARCA = 'injetado na janela';
var
  LRecs: TAMQPJournalRecords;
  I: Integer;
begin
  if FInjetou then
    Exit;
  FInjetou := True;
  SetLength(LRecs, 1);
  LRecs[0].Kind := 7;
  SetLength(LRecs[0].Payload, Length(MARCA));
  for I := 1 to Length(MARCA) do
    LRecs[0].Payload[I - 1] := Byte(MARCA[I]);
  // SubmitNoWait, e nao Submit: estamos NA thread do journal, e esperar vaga
  // aqui seria esperar por nos mesmos.
  FLsnInjetado := SubmitNoWait(LRecs);
end;

{ TSinkFalso }

procedure TSinkFalso.Durable(ALsn: UInt64);
begin
  FUltimo := ALsn;
  Inc(FAvisos);
end;

procedure TSinkFalso.JournalFailed(const AMessage: string);
begin
  Inc(FFalhas);
  FMensagem := AMessage;
end;

{ TProdutorFalso }

constructor TProdutorFalso.Create(AJournal: TAMQPJournal; AQuantos: Integer);
begin
  FJournal := AJournal;
  FQuantos := AQuantos;
  inherited Create(False);
end;

procedure TProdutorFalso.Execute;
var
  I, K: Integer;
  LRecs: TAMQPJournalRecords;
  LTxt: string;
begin
  SetLength(LRecs, 2);
  for I := 1 to FQuantos do
  begin
    LTxt := 'a' + IntToStr(I);
    LRecs[0].Kind := 1;
    SetLength(LRecs[0].Payload, Length(LTxt));
    for K := 1 to Length(LTxt) do
      LRecs[0].Payload[K - 1] := Byte(LTxt[K]);
    LTxt := 'b' + IntToStr(I);
    LRecs[1].Kind := 2;
    SetLength(LRecs[1].Payload, Length(LTxt));
    for K := 1 to Length(LTxt) do
      LRecs[1].Payload[K - 1] := Byte(LTxt[K]);
    try
      FJournal.Submit(LRecs);
    except
      Inc(FErros);
    end;
  end;
end;

{ TJournalFixture }

procedure TJournalFixture.SetUp;
begin
  inherited SetUp;
  Inc(GSeq);
  FDir := IncludeTrailingPathDelimiter(GetTempDir) + 'amqpjournal-'
    + IntToStr(GSeq);
  TearDown; // garante diretorio limpo mesmo apos uma execucao interrompida
  ForceDirectories(FDir);
end;

procedure TJournalFixture.TearDown;
var
  LRec: TSearchRec;
begin
  if FindFirst(IncludeTrailingPathDelimiter(FDir) + '*', faAnyFile, LRec) = 0 then
  begin
    try
      repeat
        if (LRec.Attr and faDirectory) = 0 then
          SysUtils.DeleteFile(IncludeTrailingPathDelimiter(FDir) + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      SysUtils.FindClose(LRec);
    end;
  end;
end;

function TJournalFixture.Rec(AKind: Byte;
  const S: string): TAMQPJournalRecord;
var
  I: Integer;
begin
  Result.Kind := AKind;
  SetLength(Result.Payload, Length(S));
  for I := 1 to Length(S) do
    Result.Payload[I - 1] := Byte(S[I]);
end;

function TJournalFixture.Um(AKind: Byte;
  const S: string): TAMQPJournalRecords;
begin
  SetLength(Result, 1);
  Result[0] := Rec(AKind, S);
end;

{ TJournalLsnTests }

procedure TJournalLsnTests.Submit_DevolveOLsnDoUltimo;
var
  J: TAMQPJournal;
  LRecs: TAMQPJournalRecords;
begin
  // O publicador guarda ESTE numero no confirm pendente (D24): e' o LSN que
  // precisa estar duravel para o lote inteiro estar seguro.
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    SetLength(LRecs, 3);
    LRecs[0] := Rec(1, 'um');
    LRecs[1] := Rec(1, 'dois');
    LRecs[2] := Rec(1, 'tres');
    AssertEquals('LSN do ultimo do lote', Int64(3), Int64(J.Submit(LRecs)));
    AssertTrue('e ele fica duravel', J.WaitDurable(3, 5000));
  finally
    J.Free;
  end;
end;

procedure TJournalLsnTests.LsnsSaoContiguosEntreLotes;
var
  J: TAMQPJournal;
begin
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    AssertEquals('1o lote', Int64(1), Int64(J.Submit(Um(1, 'a'))));
    AssertEquals('2o lote continua', Int64(2), Int64(J.Submit(Um(1, 'b'))));
    AssertEquals('3o lote continua', Int64(3), Int64(J.Submit(Um(1, 'c'))));
    AssertTrue('tudo duravel', J.WaitDurable(3, 5000));
  finally
    J.Free;
  end;
end;

procedure TJournalLsnTests.SubmitVazio_Levanta;
var
  J: TAMQPJournal;
  LRecs: TAMQPJournalRecords;
  LLevantou: Boolean;
begin
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    LRecs := nil;
    LLevantou := False;
    try
      J.Submit(LRecs);
    except
      on E: EAMQPJournal do
        LLevantou := True;
    end;
    AssertTrue('submit sem registro tem de levantar', LLevantou);
  finally
    J.Free;
  end;
end;

procedure TJournalLsnTests.SubmitAntesDoStart_Levanta;
var
  J: TAMQPJournal;
  LLevantou: Boolean;
begin
  J := TAMQPJournal.Create(FDir);
  try
    LLevantou := False;
    try
      J.Submit(Um(1, 'x'));
    except
      on E: EAMQPJournal do
        LLevantou := True;
    end;
    AssertTrue('journal parado nao aceita submit', LLevantou);
  finally
    J.Free;
  end;
end;

procedure TJournalLsnTests.SubmitDepoisDoStop_Levanta;
var
  J: TAMQPJournal;
  LLevantou: Boolean;
begin
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    J.Submit(Um(1, 'x'));
    J.Stop;
    LLevantou := False;
    try
      J.Submit(Um(1, 'y'));
    except
      on E: EAMQPJournal do
        LLevantou := True;
    end;
    AssertTrue('depois do Stop nao aceita mais', LLevantou);
  finally
    J.Free;
  end;
end;

{ TJournalGroupCommitTests }

procedure TJournalGroupCommitTests.CargaConcorrente_AgrupaEmMenosFsyncsQueRegistros;
var
  J: TAMQPJournal;
  P: array[0..3] of TProdutorFalso;
  I: Integer;
  LS: TAMQPJournalStats;
begin
  // ---- A D25 INTEIRA ESTA' NESTE NUMERO ----
  // Sem esta medida, uma implementacao que da' um fsync por registro passaria
  // em todos os outros testes desta suite.
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    for I := 0 to 3 do
      P[I] := TProdutorFalso.Create(J, 250);
    for I := 0 to 3 do
    begin
      P[I].WaitFor;
      AssertEquals('produtor sem erro', 0, P[I].Erros);
      P[I].Free;
    end;
    AssertTrue('tudo duravel', J.WaitDurable(2000, 30000));
    LS := J.Stats;
    AssertEquals('gravou tudo', Int64(2000), LS.Registros);
    AssertTrue('agrupou: menos fsyncs que registros ('
      + IntToStr(LS.Syncs) + ' x ' + IntToStr(LS.Registros) + ')',
      LS.Syncs < LS.Registros);
    AssertTrue('lote medio bem acima de 1 ('
      + IntToStr(LS.Registros div LS.Syncs) + ')',
      (LS.Registros div LS.Syncs) >= 3);
  finally
    J.Free;
  end;
end;

procedure TJournalGroupCommitTests.LoteEhIndivisivel_ParesSaemAdjacentes;
var
  J: TAMQPJournal;
  P: array[0..3] of TProdutorFalso;
  I, LN, LPares, LQuebras: Integer;
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LRegs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LA, LB: string;
begin
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    for I := 0 to 3 do
      P[I] := TProdutorFalso.Create(J, 250);
    for I := 0 to 3 do
    begin
      P[I].WaitFor;
      P[I].Free;
    end;
    AssertTrue('tudo duravel', J.WaitDurable(2000, 30000));
  finally
    J.Free;
  end;

  LArq := TAMQPWalOsFile.Create(
    IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(1), False);
  LSeg := TAMQPWalSegment.OpenExisting(LArq, False);
  try
    LN := LSeg.ReadPrefix(LRegs, LStop);
    AssertEquals('leu tudo do arquivo', 2000, LN);
    LPares := 0;
    LQuebras := 0;
    I := 0;
    while I < LN - 1 do
    begin
      if LRegs[I].Kind = 1 then
      begin
        LA := TextoDe(LRegs[I].Payload);
        LB := TextoDe(LRegs[I + 1].Payload);
        if (LRegs[I + 1].Kind <> 2)
          or (Copy(LA, 2, MaxInt) <> Copy(LB, 2, MaxInt))
          or (LRegs[I + 1].Lsn <> LRegs[I].Lsn + 1) then
          Inc(LQuebras)
        else
          Inc(LPares);
        Inc(I, 2);
      end
      else
        Inc(I);
    end;
    AssertEquals('nenhum par quebrado', 0, LQuebras);
    AssertEquals('os 1000 pares adjacentes', 1000, LPares);
  finally
    LSeg.Free;
    LArq := nil;
  end;
end;

procedure TJournalGroupCommitTests.SubmitNaJanelaDoLote_NaoSePerde;
var
  J: TJournalComJanela;
begin
  J := TJournalComJanela.Create(FDir);
  try
    J.Start;
    AssertTrue('o gatilho fica duravel', J.WaitDurable(J.Submit(Um(1, 'gatilho')),
      5000));
    AssertTrue('o injetado NA JANELA tambem',
      J.WaitDurable(J.LsnInjetado, 5000));
    AssertEquals('e nada se perdeu', Int64(2), J.Stats.Registros);
  finally
    J.Free;
  end;
end;

{ TJournalDurabilidadeTests }

procedure TJournalDurabilidadeTests.FsyncOk_AvancaAMarcaDagua;
var
  J: TJournalSobreDuble;
  LSN: UInt64;
begin
  J := TJournalSobreDuble.Create(FDir);
  try
    J.Start;
    LSN := J.Submit(Um(1, 'a'));
    AssertTrue('ficou duravel', J.WaitDurable(LSN, 5000));
    AssertTrue('marca d agua alcancou o LSN', J.DurableLsn >= LSN);
  finally
    J.Free;
  end;
end;

procedure TJournalDurabilidadeTests.FsyncFalho_NaoAvancaAMarcaDagua;
var
  J: TJournalSobreDuble;
  LAntes, LSN: UInt64;
begin
  // A diferenca entre prometer durabilidade e mentir sobre ela.
  J := TJournalSobreDuble.Create(FDir);
  try
    J.Start;
    LSN := J.Submit(Um(1, 'antes'));
    J.WaitDurable(LSN, 5000);
    LAntes := J.DurableLsn;

    J.Arq.SetSyncOk(False);
    LSN := J.Submit(Um(1, 'com fsync falhando'));
    AssertFalse('nao pode ficar duravel', J.WaitDurable(LSN, 800));
    AssertEquals('marca d agua nao andou', Int64(LAntes), Int64(J.DurableLsn));
    AssertTrue('e o fsync falho foi contado', J.Stats.SyncsFalhos > 0);
  finally
    J.Free;
  end;
end;

procedure TJournalDurabilidadeTests.FsyncFalho_NaoDerrubaOJournal;
var
  J: TJournalSobreDuble;
begin
  // fsync que falha e' RECUPERAVEL: os bytes ja' estao no arquivo.
  J := TJournalSobreDuble.Create(FDir);
  try
    J.Start;
    J.Arq.SetSyncOk(False);
    J.Submit(Um(1, 'x'));
    Sleep(300);
    AssertFalse('journal continua de pe', J.Failed);
  finally
    J.Free;
  end;
end;

procedure TJournalDurabilidadeTests.FsyncSeguinte_CobreOLoteAnterior;
var
  J: TJournalSobreDuble;
  LSN: UInt64;
begin
  J := TJournalSobreDuble.Create(FDir);
  try
    J.Start;
    J.Arq.SetSyncOk(False);
    LSN := J.Submit(Um(1, 'no escuro'));
    AssertFalse('ainda nao', J.WaitDurable(LSN, 500));
    J.Arq.SetSyncOk(True);
    J.Submit(Um(1, 'com fsync de novo'));
    AssertTrue('o fsync seguinte cobre o lote anterior',
      J.WaitDurable(LSN, 5000));
  finally
    J.Free;
  end;
end;

procedure TJournalDurabilidadeTests.Sink_EhAvisadoComOMaiorLsn;
var
  J: TJournalSobreDuble;
  S: TSinkFalso;
  ISink: IAMQPDurabilitySink;
  LSN, LPrazo: UInt64;
begin
  J := TJournalSobreDuble.Create(FDir);
  S := TSinkFalso.Create;
  ISink := S;
  try
    J.DurabilitySink := ISink;
    J.Start;
    LSN := J.Submit(Um(1, 'a'));
    AssertTrue('duravel', J.WaitDurable(LSN, 5000));
    // ESPERA o aviso, em vez de exigi-lo ja' na volta do WaitDurable: a marca
    // d'agua avanca ANTES de o sink ser chamado, e essa ordem e' deliberada --
    // inverte-la poria um callback de fora na frente de acordar os waiters. Sao
    // dois sinais independentes, e o teste que os tratou como um so' falhava
    // uma vez a cada oito execucoes.
    LPrazo := AmqpTickMs + 5000;
    while (S.Avisos = 0) and (AmqpTickMs < LPrazo) do
      Sleep(2);
    AssertTrue('sink foi avisado', S.Avisos > 0);
    AssertEquals('com o MAIOR LSN do lote', Int64(LSN), Int64(S.Ultimo));
  finally
    J.Free;
  end;
end;

{ TJournalFalhaTests }

procedure TJournalFalhaTests.FalhaDeEscrita_FechaAPorta;
var
  J: TJournalSobreDuble;
  LPrazo: UInt64;
begin
  // Escrita que falha nao tem como ser reposta: o registro nao esta' em lugar
  // nenhum. Um broker que nao consegue mais escrever tem de DIZER isso.
  J := TJournalSobreDuble.Create(FDir);
  try
    J.Start;
    J.Submit(Um(1, 'ok'));
    J.WaitDurable(1, 5000);
    J.Arq.SetFalharEscritaNa(J.Arq.Escritas + 1);
    J.Submit(Um(1, 'vai falhar'));
    LPrazo := AmqpTickMs + 5000;
    while (not J.Failed) and (AmqpTickMs < LPrazo) do
      Sleep(5);
    AssertTrue('journal em falha', J.Failed);
    AssertTrue('com mensagem', J.LastError <> '');
  finally
    J.Free;
  end;
end;

procedure TJournalFalhaTests.FalhaDeEscrita_AvisaOSinkUmaVez;
var
  J: TJournalSobreDuble;
  S: TSinkFalso;
  ISink: IAMQPDurabilitySink;
  LPrazo: UInt64;
begin
  J := TJournalSobreDuble.Create(FDir);
  S := TSinkFalso.Create;
  ISink := S;
  try
    J.DurabilitySink := ISink;
    J.Start;
    J.Arq.SetFalharEscritaNa(J.Arq.Escritas + 1);
    J.Submit(Um(1, 'vai falhar'));
    LPrazo := AmqpTickMs + 5000;
    while (not J.Failed) and (AmqpTickMs < LPrazo) do
      Sleep(5);
    AssertEquals('avisado exatamente uma vez', 1, S.Falhas);
  finally
    J.Free;
  end;
end;

procedure TJournalFalhaTests.DepoisDaFalha_SubmitLevanta;
var
  J: TJournalSobreDuble;
  LPrazo: UInt64;
  LLevantou: Boolean;
begin
  J := TJournalSobreDuble.Create(FDir);
  try
    J.Start;
    J.Arq.SetFalharEscritaNa(J.Arq.Escritas + 1);
    J.Submit(Um(1, 'vai falhar'));
    LPrazo := AmqpTickMs + 5000;
    while (not J.Failed) and (AmqpTickMs < LPrazo) do
      Sleep(5);
    LLevantou := False;
    try
      J.Submit(Um(1, 'depois'));
    except
      on E: EAMQPJournal do
        LLevantou := True;
    end;
    AssertTrue('submit depois da falha tem de levantar', LLevantou);
  finally
    J.Free;
  end;
end;

procedure TJournalFalhaTests.DepoisDaFalha_WaitDurableDevolveFalse;
var
  J: TJournalSobreDuble;
  LPrazo: UInt64;
begin
  J := TJournalSobreDuble.Create(FDir);
  try
    J.Start;
    J.Arq.SetFalharEscritaNa(J.Arq.Escritas + 1);
    J.Submit(Um(1, 'vai falhar'));
    LPrazo := AmqpTickMs + 5000;
    while (not J.Failed) and (AmqpTickMs < LPrazo) do
      Sleep(5);
    AssertFalse('quem esperava durabilidade e' + ' liberado com False',
      J.WaitDurable(99, 300));
  finally
    J.Free;
  end;
end;

{ TJournalCicloDeVidaTests }

procedure TJournalCicloDeVidaTests.SubmitNoWait_NuncaBloqueia;
var
  J: TAMQPJournal;
  I: Integer;
  LSN: UInt64;
begin
  // E' por aqui que o ATOR da fila escreve (D24, corolario c): a D2 proibe o
  // ator de esperar I/O, entao o caminho dele nao pode ter teto.
  J := TAMQPJournal.Create(FDir);
  try
    J.MaxPendingBytes := 512; // teto ridiculo de proposito
    J.Start;
    LSN := 0;
    for I := 1 to 200 do
      LSN := J.SubmitNoWait(Um(9, StringOfChar('z', 64)));
    AssertEquals('os 200 foram aceitos', Int64(200), Int64(LSN));
    AssertTrue('e todos ficam duraveis', J.WaitDurable(LSN, 20000));
  finally
    J.Free;
  end;
end;

procedure TJournalCicloDeVidaTests.Reinicio_LsnContinuaDeOndeParou;
var
  J: TAMQPJournal;
  LSN: UInt64;
begin
  // Recomecar do 1 faria o proprio Append da WS1 levantar (LSN nao crescente).
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    LSN := J.Submit(Um(1, 'antes'));
    J.WaitDurable(LSN, 5000);
  finally
    J.Free;
  end;

  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    AssertEquals('proximo LSN continua', Int64(LSN + 1),
      Int64(J.Stats.ProximoLsn));
    AssertTrue('e grava depois do restart',
      J.WaitDurable(J.Submit(Um(1, 'depois')), 5000));
  finally
    J.Free;
  end;
end;

procedure TJournalCicloDeVidaTests.Reinicio_MarcaDaguaNasceNoQueJaEstava;
var
  J: TAMQPJournal;
  LSN: UInt64;
begin
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    LSN := J.Submit(Um(1, 'antes'));
    J.WaitDurable(LSN, 5000);
  finally
    J.Free;
  end;

  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    // O que ja' estava no arquivo esta' no disco por definicao: a marca d'agua
    // nao pode nascer ATRAS dele.
    AssertEquals('marca d agua nasce no que ja estava', Int64(LSN),
      Int64(J.DurableLsn));
  finally
    J.Free;
  end;
end;

procedure TJournalCicloDeVidaTests.Stop_DrenaOQueFoiAceito;
var
  J: TAMQPJournal;
  I: Integer;
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LRegs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
begin
  // Nao ha "comando de parada" na fila justamente por isto: o que ja' foi
  // aceito ainda vai ao disco.
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    for I := 1 to 300 do
      J.SubmitNoWait(Um(1, 'r' + IntToStr(I)));
    J.Stop; // sem esperar durabilidade, de proposito
  finally
    J.Free;
  end;

  LArq := TAMQPWalOsFile.Create(
    IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(1), False);
  LSeg := TAMQPWalSegment.OpenExisting(LArq, False);
  try
    AssertEquals('os 300 aceitos foram ao disco', 300,
      LSeg.ReadPrefix(LRegs, LStop));
    AssertTrue('integro', LStop = awsFim);
  finally
    LSeg.Free;
    LArq := nil;
  end;
end;

procedure TJournalCicloDeVidaTests.StartDuasVezes_EhIdempotente;
var
  J: TAMQPJournal;
begin
  J := TAMQPJournal.Create(FDir);
  try
    J.Start;
    J.Start; // nao pode roubar o proprio lock nem subir duas threads
    AssertTrue('continua rodando', J.Running);
    AssertTrue('e grava', J.WaitDurable(J.Submit(Um(1, 'x')), 5000));
  finally
    J.Free;
  end;
end;

procedure TJournalCicloDeVidaTests.DoisJournalsNoMesmoDir_OSegundoEhRecusado;
var
  J1, J2: TAMQPJournal;
  LLevantou: Boolean;
begin
  // Dois brokers sobre o mesmo DataDir escreveriam LSNs proprios nos mesmos
  // arquivos e corromperiam o log em silencio (o lock e' da WS1).
  J1 := TAMQPJournal.Create(FDir);
  J2 := TAMQPJournal.Create(FDir);
  try
    J1.Start;
    LLevantou := False;
    try
      J2.Start;
    except
      on E: Exception do
        LLevantou := True;
    end;
    AssertTrue('o segundo journal tem de ser recusado', LLevantou);
  finally
    J2.Free;
    J1.Free;
  end;
end;

initialization
  RegisterTest(TJournalLsnTests);
  RegisterTest(TJournalGroupCommitTests);
  RegisterTest(TJournalDurabilidadeTests);
  RegisterTest(TJournalFalhaTests);
  RegisterTest(TJournalCicloDeVidaTests);

end.

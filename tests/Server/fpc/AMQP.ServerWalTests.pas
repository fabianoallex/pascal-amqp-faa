unit AMQP.ServerWalTests;

{ Testes da camada de arquivo do WAL (AMQP.Server.Wal) -- WS1 da Fase 4
  (durabilidade, ver CLAUDE.md, decisoes D19-D28).

  Sao testes unitarios puros: nenhum sobe broker nenhum.

  DUAS COISAS AQUI NAO SAO TESTE COMUM, e valem explicacao:

  1. A CAMADA 1 da D28 sao DUAS varreduras exaustivas, e sao os testes-ancora
     da fase. Nenhuma escolhe pontos "representativos": a queda escolhe o dela.

       TruncamentoExaustivo  corta o log em CADA offset possivel e exige que a
                             recuperacao devolva exatamente os registros que
                             cabiam inteiros ali. Modela a escrita CURTA.
       CorrupcaoExaustiva    troca um bit em CADA byte do log e exige que a
                             recuperacao pare no registro anterior ao ferido.
                             Modela o setor escrito ERRADO.

     Precisam ser as duas, e isso foi MEDIDO, nao suposto: com a checagem de
     CRC removida, a varredura de truncamento continua verde. E' que uma cauda
     cortada e' pega antes, pela checagem de comprimento -- o CRC so' importa
     quando os bytes ESTAO la' e estao errados, que e' o caso que so' a
     varredura de corrupcao produz. Ate' descobrir isso, o comentario aqui
     afirmava que a primeira cobria o CRC; nao cobria.

  2. TAMQPWalFileFalso e' a CAMADA 2 da D28. Como o segmento fala com IAMQPWalFile
     e nao com o sistema de arquivos, da' para contar os fsyncs e falhar na
     hora escolhida -- e assim provar (a) que o cabecalho de um segmento novo
     vai ao disco na hora e (b) que uma falha de escrita nao deixa o segmento
     com estado adiantado em relacao ao arquivo. Nenhuma das duas seria
     observavel por fora.

  Espelho DUnitX de tests\Server\AMQP.ServerWalTests.pas -- mantenha os dois em
  sincronia (mesmos nomes de teste, mesmas assercoes). }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  AMQP.Threading,
  AMQP.Server.Wal,
  AMQP.ServerTestDoubles;

type

  TWalCrc32Tests = class(TTestCase)
  published
    procedure Vetores_Publicos;
    procedure Parcial_EquivaleAoInteiro;
    procedure ByteTrocado_MudaOCrc;
  end;

  TWalSegmentNameTests = class(TTestCase)
  published
    procedure Formato_ComZerosAEsquerda;
    procedure RoundTrip_CobreTodoOCardinal;
    procedure RecusaNomeNaoNumerico;
    procedure RecusaEspaco;
    procedure RecusaOutraExtensao;
  end;

  { Base com o par (duble, interface) montado. O duble e' TInterfacedObject: a
    referencia de CLASSE existe para o teste espiar os contadores, e a de
    INTERFACE e' quem o mantem vivo -- guardar so' a de classe seria o erro de
    duble que ja' mordeu esta codebase. }
  TWalFixture = class(TTestCase)
  protected
    FDuble: TAMQPWalFileFalso;
    FArq: IAMQPWalFile;
    procedure SetUp; override;
    procedure TearDown; override;
    function Bytes(const S: string): TBytes;
    function ComoTexto(const B: TBytes): string;
  end;

  TWalSegmentTests = class(TWalFixture)
  published
    procedure SegmentoNovo_TemSoOCabecalho;
    procedure RoundTrip_CinquentaRegistros;
    procedure PayloadVazio_Sobrevive;
    procedure Reabrir_RecuperaLastLsnEContinuaEscrevendo;
    procedure LsnRepetido_Levanta;
    procedure LsnMenor_Levanta;
    procedure CreateNewSobreArquivoNaoVazio_Levanta;
    procedure AssinaturaErrada_Levanta;
    procedure VersaoErrada_Levanta;
    procedure CabecalhoTruncado_Levanta;
  end;

  TWalRecoveryTests = class(TWalFixture)
  published
    procedure TruncamentoExaustivo_TodoOffsetDaPrefixoConsistente;
    procedure CorrupcaoExaustiva_TodoBitTrocadoDaPrefixoConsistente;
    procedure ByteTrocado_PrefixoParaNoRegistroAnterior;
    procedure ByteTrocado_MotivoEhCrc;
    procedure CaudaTorta_EhContadaETruncada;
    procedure CaudaTorta_AposTrimOAppendEhAlcancavel;
    procedure LenAbsurdo_ParaComTamanho;
    procedure LsnForaDeOrdem_ParaComLsn;
  end;

  TWalFileSeamTests = class(TWalFixture)
  published
    procedure CreateNew_SincronizaOCabecalho;
    procedure Sync_ChegaAoArquivo;
    procedure FalhaDeSync_ViraFalseSemLevantar;
    procedure FalhaDeEscrita_NaoAvancaOEstadoDoSegmento;
  end;

  TWalDirLockTests = class(TTestCase)
  private
    function DirTeste: string;
  published
    procedure SegundoDono_EhRecusado;
    procedure AposSoltar_NovoDonoConsegue;
  end;

  TWalOsFileTests = class(TTestCase)
  private
    function DirTeste: string;
    procedure LimpaDir;
    function Bytes(const S: string): TBytes;
  published
    procedure RoundTrip_EmDiscoDeVerdade;
    procedure ListaSegmentos_OrdenadosPorNumero;
    procedure CriarSobreExistente_Levanta;
    procedure AbrirInexistente_Levanta;
  end;

  TWalClockTests = class(TTestCase)
  published
    procedure WallMs_Plausivel;
    procedure WallMs_NaoAndaParaTras;
    procedure WallMs_DifereDoMonotonico;
  end;

implementation


{ TWalFixture }

procedure TWalFixture.SetUp;
begin
  inherited SetUp;
  FDuble := TAMQPWalFileFalso.Create;
  FArq := FDuble;
end;

procedure TWalFixture.TearDown;
begin
  FArq := nil;
  FDuble := nil;
  inherited TearDown;
end;

function TWalFixture.Bytes(const S: string): TBytes;
var
  I: Integer;
begin
  SetLength(Result, Length(S));
  for I := 1 to Length(S) do
    Result[I - 1] := Byte(S[I]);
end;

function TWalFixture.ComoTexto(const B: TBytes): string;
var
  I: Integer;
begin
  SetLength(Result, Length(B));
  for I := 0 to High(B) do
    Result[I + 1] := Char(B[I]);
end;

{ TWalCrc32Tests }

procedure TWalCrc32Tests.Vetores_Publicos;
var
  LB: TBytes;

  function Bs(const S: string): TBytes;
  var
    I: Integer;
  begin
    SetLength(Result, Length(S));
    for I := 1 to Length(S) do
      Result[I - 1] := Byte(S[I]);
  end;

begin
  // Vetores publicos do CRC-32 IEEE (os mesmos do zip/png/gzip). Sao eles que
  // provam que isto e' um CRC-32 de verdade, e nao um checksum caseiro que
  // ninguem de fora consegue conferir.
  LB := nil;
  AssertEquals('vazio', Int64($00000000), Int64(AmqpCrc32(LB)));
  AssertEquals('"a"', Int64($E8B7BE43), Int64(AmqpCrc32(Bs('a'))));
  AssertEquals('"123456789"', Int64($CBF43926),
    Int64(AmqpCrc32(Bs('123456789'))));
  AssertEquals('pangrama', Int64($414FA339),
    Int64(AmqpCrc32(Bs('The quick brown fox jumps over the lazy dog'))));
end;

procedure TWalCrc32Tests.Parcial_EquivaleAoInteiro;
var
  LB: TBytes;
  LParcial: Cardinal;
  I: Integer;
begin
  SetLength(LB, 64);
  for I := 0 to 63 do
    LB[I] := Byte(I * 7);
  // A forma incremental, aplicada em dois pedacos, tem de dar o mesmo que a
  // forma de uma tacada -- e' dela que o Append depende para checar a moldura
  // e o payload numa passada so'.
  LParcial := AmqpCrc32($FFFFFFFF, LB, 0, 30);
  LParcial := AmqpCrc32(LParcial, LB, 30, 34) xor $FFFFFFFF;
  AssertEquals('dois pedacos == inteiro', Int64(AmqpCrc32(LB)), Int64(LParcial));
end;

procedure TWalCrc32Tests.ByteTrocado_MudaOCrc;
var
  LB: TBytes;
  LAntes: Cardinal;
  I: Integer;
begin
  SetLength(LB, 32);
  for I := 0 to 31 do
    LB[I] := Byte(I);
  LAntes := AmqpCrc32(LB);
  LB[17] := LB[17] xor $01; // um unico bit
  AssertTrue('um bit trocado muda o CRC', AmqpCrc32(LB) <> LAntes);
end;

{ TWalSegmentNameTests }

procedure TWalSegmentNameTests.Formato_ComZerosAEsquerda;
begin
  AssertEquals('seg-0000000001.wal', AmqpWalSegmentName(1));
  AssertEquals('seg-0000000042.wal', AmqpWalSegmentName(42));
end;

procedure TWalSegmentNameTests.RoundTrip_CobreTodoOCardinal;
var
  LNo: Cardinal;
begin
  // O maior Cardinal tem de sobreviver ao round-trip. Com menos digitos no
  // nome, ele geraria um arquivo que o proprio parser nao reconheceria de
  // volta -- e o segmento sumiria da listagem em silencio.
  AssertTrue('parseou o maior Cardinal',
    AmqpWalParseSegmentName(AmqpWalSegmentName(High(Cardinal)), LNo));
  AssertEquals('valor de volta', Int64(High(Cardinal)), Int64(LNo));
  AssertTrue('parseou o 1',
    AmqpWalParseSegmentName(AmqpWalSegmentName(1), LNo));
  AssertEquals('valor de volta', Int64(1), Int64(LNo));
end;

procedure TWalSegmentNameTests.RecusaNomeNaoNumerico;
var
  LNo: Cardinal;
begin
  AssertFalse('letra no meio',
    AmqpWalParseSegmentName('seg-000000001a.wal', LNo));
end;

procedure TWalSegmentNameTests.RecusaEspaco;
var
  LNo: Cardinal;
begin
  // StrToInt64Def sozinho aceitaria ' 12', e um nome desses viraria segmento
  // fantasma com o numero de outro.
  AssertFalse('espaco a esquerda',
    AmqpWalParseSegmentName('seg- 000000001.wal', LNo));
end;

procedure TWalSegmentNameTests.RecusaOutraExtensao;
var
  LNo: Cardinal;
begin
  AssertFalse('extensao errada',
    AmqpWalParseSegmentName('seg-0000000001.log', LNo));
end;

{ TWalSegmentTests }

procedure TWalSegmentTests.SegmentoNovo_TemSoOCabecalho;
var
  LSeg: TAMQPWalSegment;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 3);
  try
    AssertEquals('fim logo apos o cabecalho',
      Int64(AMQP_WAL_SEG_HEADER_SIZE), LSeg.EndOffset);
    AssertEquals('tamanho do arquivo',
      Int64(AMQP_WAL_SEG_HEADER_SIZE), FDuble.Size);
    AssertEquals('sem LSN', Int64(0), Int64(LSeg.LastLsn));
    AssertEquals('numero do segmento', Int64(3), Int64(LSeg.SegNo));
  finally
    LSeg.Free;
  end;
end;

procedure TWalSegmentTests.RoundTrip_CinquentaRegistros;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  I, LN: Integer;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    for I := 1 to 50 do
      LSeg.Append(I, Byte(I mod 7), Bytes(StringOfChar('x', I)));
    LN := LSeg.ReadPrefix(LRecs, LStop);
    AssertEquals('leu todos', 50, LN);
    AssertTrue('parou no fim', LStop = awsFim);
    AssertEquals('primeiro lsn', Int64(1), Int64(LRecs[0].Lsn));
    AssertEquals('ultimo lsn', Int64(50), Int64(LRecs[49].Lsn));
    AssertEquals('kind do 5o', Int64(5 mod 7), Int64(LRecs[4].Kind));
    AssertEquals('tamanho do payload do 50o', 50, Length(LRecs[49].Payload));
    AssertEquals('conteudo do 50o', StringOfChar('x', 50),
      ComoTexto(LRecs[49].Payload));
    AssertEquals('LastLsn', Int64(50), Int64(LSeg.LastLsn));
    AssertEquals('sem cauda', Int64(0), LSeg.BytesDescartados);
  finally
    LSeg.Free;
  end;
end;

procedure TWalSegmentTests.PayloadVazio_Sobrevive;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
begin
  // Um registro so' de moldura e' legitimo (um DEQ pode nao precisar de mais
  // que o proprio Kind, adiante). Zero bytes de payload nao pode virar cauda.
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(1, 9, nil);
    LSeg.Append(2, 9, Bytes('depois'));
    AssertEquals('dois registros', 2, LSeg.ReadPrefix(LRecs, LStop));
    AssertTrue('integro ate o fim', LStop = awsFim);
    AssertEquals('payload vazio', 0, Length(LRecs[0].Payload));
    AssertEquals('o seguinte continua legivel', 'depois',
      ComoTexto(LRecs[1].Payload));
  finally
    LSeg.Free;
  end;
end;

procedure TWalSegmentTests.Reabrir_RecuperaLastLsnEContinuaEscrevendo;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 5);
  try
    LSeg.Append(1, 0, Bytes('um'));
    LSeg.Append(2, 0, Bytes('dois'));
  finally
    LSeg.Free;
  end;

  LSeg := TAMQPWalSegment.OpenExisting(FArq);
  try
    AssertEquals('segno veio do cabecalho', Int64(5), Int64(LSeg.SegNo));
    AssertEquals('LastLsn recuperado', Int64(2), Int64(LSeg.LastLsn));
    LSeg.Append(3, 0, Bytes('tres'));
    AssertEquals('tres registros', 3, LSeg.ReadPrefix(LRecs, LStop));
    AssertEquals('o novo esta la', 'tres', ComoTexto(LRecs[2].Payload));
  finally
    LSeg.Free;
  end;
end;

procedure TWalSegmentTests.LsnRepetido_Levanta;
var
  LSeg: TAMQPWalSegment;
  LLevantou: Boolean;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(10, 0, Bytes('a'));
    LLevantou := False;
    try
      LSeg.Append(10, 0, Bytes('b'));
    except
      on E: EAMQPWal do
        LLevantou := True;
    end;
    AssertTrue('LSN repetido tem de levantar', LLevantou);
  finally
    LSeg.Free;
  end;
end;

procedure TWalSegmentTests.LsnMenor_Levanta;
var
  LSeg: TAMQPWalSegment;
  LLevantou: Boolean;
begin
  // Nao e' preciosismo: a leitura para no primeiro LSN que nao cresce, entao
  // gravar um menor produziria um arquivo que a propria recuperacao truncaria
  // ali -- perda silenciosa de tudo que viesse depois.
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(10, 0, Bytes('a'));
    LLevantou := False;
    try
      LSeg.Append(9, 0, Bytes('b'));
    except
      on E: EAMQPWal do
        LLevantou := True;
    end;
    AssertTrue('LSN menor tem de levantar', LLevantou);
  finally
    LSeg.Free;
  end;
end;

procedure TWalSegmentTests.CreateNewSobreArquivoNaoVazio_Levanta;
var
  LSeg: TAMQPWalSegment;
  LLevantou: Boolean;
begin
  FDuble.PoeBytes(Bytes('ja tem coisa aqui'));
  LLevantou := False;
  LSeg := nil;
  try
    LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  except
    on E: EAMQPWal do
      LLevantou := True;
  end;
  LSeg.Free;
  AssertTrue('CreateNew sobre arquivo com bytes tem de levantar', LLevantou);
end;

procedure TWalSegmentTests.AssinaturaErrada_Levanta;
var
  LSeg: TAMQPWalSegment;
  LBuf: TBytes;
  LLevantou: Boolean;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  LSeg.Append(1, 0, Bytes('x'));
  LSeg.Free;

  LBuf := FDuble.Bytes;
  LBuf[2] := Byte('Z'); // estraga o magic
  FDuble.PoeBytes(LBuf);

  // Assinatura errada NAO e' cauda torta: e' arquivo de outra origem no
  // DataDir, que e' erro de operacao e tem de aparecer, nao ser engolido.
  LLevantou := False;
  LSeg := nil;
  try
    LSeg := TAMQPWalSegment.OpenExisting(FArq);
  except
    on E: EAMQPWal do
      LLevantou := True;
  end;
  LSeg.Free;
  AssertTrue('assinatura errada tem de levantar', LLevantou);
end;

procedure TWalSegmentTests.VersaoErrada_Levanta;
var
  LSeg: TAMQPWalSegment;
  LBuf: TBytes;
  LLevantou: Boolean;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  LSeg.Free;

  LBuf := FDuble.Bytes;
  LBuf[8] := 99; // versao
  FDuble.PoeBytes(LBuf);

  LLevantou := False;
  LSeg := nil;
  try
    LSeg := TAMQPWalSegment.OpenExisting(FArq);
  except
    on E: EAMQPWal do
      LLevantou := True;
  end;
  LSeg.Free;
  AssertTrue('versao desconhecida tem de levantar', LLevantou);
end;

procedure TWalSegmentTests.CabecalhoTruncado_Levanta;
var
  LSeg: TAMQPWalSegment;
  LLevantou: Boolean;
begin
  FDuble.PoeBytes(Bytes('AMQPW')); // menos que o cabecalho
  LLevantou := False;
  LSeg := nil;
  try
    LSeg := TAMQPWalSegment.OpenExisting(FArq);
  except
    on E: EAMQPWal do
      LLevantou := True;
  end;
  LSeg.Free;
  AssertTrue('cabecalho incompleto tem de levantar', LLevantou);
end;

{ TWalRecoveryTests }

procedure TWalRecoveryTests.TruncamentoExaustivo_TodoOffsetDaPrefixoConsistente;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LOrig, LCortado: TBytes;
  LFim: array of Integer;
  I, LCorte, LEsperado, LN: Integer;
  LDuble2: TAMQPWalFileFalso;
  LArq2: IAMQPWalFile;
begin
  // ---- CAMADA 1 DA D28: o teste-ancora da fase ----
  // Monta um log pequeno, corta em CADA offset possivel e exige que a
  // recuperacao devolva exatamente os registros que cabiam inteiros. Nao ha
  // ponto de corte "representativo": a queda escolhe o dela.
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  SetLength(LFim, 21);
  LFim[0] := AMQP_WAL_SEG_HEADER_SIZE;
  try
    for I := 1 to 20 do
    begin
      LSeg.Append(I, 1, Bytes(StringOfChar('p', I * 3)));
      LFim[I] := LFim[I - 1] + AmqpWalRecordSize(I * 3);
    end;
  finally
    LSeg.Free;
  end;
  LOrig := FDuble.Bytes;
  AssertEquals('tamanho previsto', LFim[20], Length(LOrig));

  for LCorte := AMQP_WAL_SEG_HEADER_SIZE to Length(LOrig) do
  begin
    LCortado := Copy(LOrig, 0, LCorte);
    LDuble2 := TAMQPWalFileFalso.Create;
    LArq2 := LDuble2;
    try
      LDuble2.PoeBytes(LCortado);
      LEsperado := 0;
      for I := 1 to 20 do
        if LFim[I] <= LCorte then
          LEsperado := I;

      LSeg := TAMQPWalSegment.OpenExisting(LArq2, False);
      try
        LN := LSeg.ReadPrefix(LRecs, LStop);
        AssertEquals('corte em ' + IntToStr(LCorte) + ': numero de registros',
          LEsperado, LN);
        if LN > 0 then
          AssertEquals('corte em ' + IntToStr(LCorte) + ': ultimo LSN',
            Int64(LEsperado), Int64(LRecs[LN - 1].Lsn));
      finally
        LSeg.Free;
      end;
    finally
      LArq2 := nil;
    end;
  end;
end;

procedure TWalRecoveryTests.CorrupcaoExaustiva_TodoBitTrocadoDaPrefixoConsistente;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LOrig, LMutado: TBytes;
  LIni, LFim: array of Integer;
  I, LPos, LEsperado, LN, K: Integer;
  LDuble2: TAMQPWalFileFalso;
  LArq2: IAMQPWalFile;
begin
  // ---- CAMADA 1 DA D28, segunda metade: o setor escrito ERRADO ----
  // Todo byte de um registro esta' sob o CRC (LSN, kind, flags, len e
  // payload), entao trocar UM BIT em qualquer lugar dele tem de fazer a
  // recuperacao parar exatamente no registro anterior -- nem antes (perda), nem
  // depois (dado corrompido entregue como bom).
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  SetLength(LIni, 11);
  SetLength(LFim, 11);
  LFim[0] := AMQP_WAL_SEG_HEADER_SIZE;
  try
    for I := 1 to 10 do
    begin
      LIni[I] := LFim[I - 1];
      LSeg.Append(I, 1, Bytes(StringOfChar('c', I * 2)));
      LFim[I] := LIni[I] + AmqpWalRecordSize(I * 2);
    end;
  finally
    LSeg.Free;
  end;
  LOrig := FDuble.Bytes;

  for LPos := AMQP_WAL_SEG_HEADER_SIZE to High(LOrig) do
  begin
    K := 0;
    for I := 1 to 10 do
      if (LPos >= LIni[I]) and (LPos < LFim[I]) then
        K := I;
    LEsperado := K - 1;

    LMutado := Copy(LOrig, 0, Length(LOrig));
    LMutado[LPos] := LMutado[LPos] xor $01;

    LDuble2 := TAMQPWalFileFalso.Create;
    LArq2 := LDuble2;
    try
      LDuble2.PoeBytes(LMutado);
      LSeg := TAMQPWalSegment.OpenExisting(LArq2, False);
      try
        LN := LSeg.ReadPrefix(LRecs, LStop);
        AssertEquals('bit trocado no byte ' + IntToStr(LPos) + ' (registro '
          + IntToStr(K) + ')', LEsperado, LN);
        if LN > 0 then
          AssertEquals('bit trocado no byte ' + IntToStr(LPos)
            + ': ultimo LSN', Int64(LEsperado), Int64(LRecs[LN - 1].Lsn));
      finally
        LSeg.Free;
      end;
    finally
      LArq2 := nil;
    end;
  end;
end;

procedure TWalRecoveryTests.ByteTrocado_PrefixoParaNoRegistroAnterior;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LBuf: TBytes;
  LFim5: Integer;
  I: Integer;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    for I := 1 to 10 do
      LSeg.Append(I, 1, Bytes('carga'));
  finally
    LSeg.Free;
  end;
  // inicio do 5o registro
  LFim5 := AMQP_WAL_SEG_HEADER_SIZE + 4 * AmqpWalRecordSize(5);
  LBuf := FDuble.Bytes;
  LBuf[LFim5 + AMQP_WAL_REC_HEADER_SIZE] :=
    LBuf[LFim5 + AMQP_WAL_REC_HEADER_SIZE] xor $FF;
  FDuble.PoeBytes(LBuf);

  LSeg := TAMQPWalSegment.OpenExisting(FArq, False);
  try
    AssertEquals('para nos 4 anteriores', 4, LSeg.ReadPrefix(LRecs, LStop));
  finally
    LSeg.Free;
  end;
end;

procedure TWalRecoveryTests.ByteTrocado_MotivoEhCrc;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LBuf: TBytes;
  I: Integer;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    for I := 1 to 10 do
      LSeg.Append(I, 1, Bytes('carga'));
  finally
    LSeg.Free;
  end;
  LBuf := FDuble.Bytes;
  I := AMQP_WAL_SEG_HEADER_SIZE + 4 * AmqpWalRecordSize(5)
    + AMQP_WAL_REC_HEADER_SIZE;
  LBuf[I] := LBuf[I] xor $FF;
  FDuble.PoeBytes(LBuf);

  LSeg := TAMQPWalSegment.OpenExisting(FArq, False);
  try
    LSeg.ReadPrefix(LRecs, LStop);
    AssertTrue('corrupcao no meio e CRC, nao cauda', LStop = awsCrc);
  finally
    LSeg.Free;
  end;
end;

procedure TWalRecoveryTests.CaudaTorta_EhContadaETruncada;
var
  LSeg: TAMQPWalSegment;
  LBuf, LLixo: TBytes;
  I, LAntes: Integer;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(1, 0, Bytes('um'));
    LSeg.Append(2, 0, Bytes('dois'));
  finally
    LSeg.Free;
  end;
  LAntes := Integer(FDuble.Size);

  // 7 bytes de escrita interrompida no fim
  SetLength(LLixo, 7);
  for I := 0 to 6 do
    LLixo[I] := $AB;
  LBuf := FDuble.Bytes;
  SetLength(LBuf, LAntes + 7);
  for I := 0 to 6 do
    LBuf[LAntes + I] := LLixo[I];
  FDuble.PoeBytes(LBuf);

  LSeg := TAMQPWalSegment.OpenExisting(FArq, True);
  try
    AssertEquals('contou os bytes descartados', Int64(7), LSeg.BytesDescartados);
    AssertTrue('motivo foi cauda', LSeg.Stop = awsCauda);
    AssertEquals('e o arquivo encolheu de verdade', Int64(LAntes), FDuble.Size);
  finally
    LSeg.Free;
  end;
end;

procedure TWalRecoveryTests.CaudaTorta_AposTrimOAppendEhAlcancavel;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LBuf: TBytes;
  I, LAntes: Integer;
begin
  // O motivo de o trim existir: sem ele, o registro novo entraria DEPOIS de
  // bytes que a leitura descarta, e ficaria inalcancavel para sempre -- pior
  // que a queda original, porque o broker acharia que gravou.
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(1, 0, Bytes('um'));
    LSeg.Append(2, 0, Bytes('dois'));
  finally
    LSeg.Free;
  end;
  LAntes := Integer(FDuble.Size);
  LBuf := FDuble.Bytes;
  SetLength(LBuf, LAntes + 7);
  for I := 0 to 6 do
    LBuf[LAntes + I] := $AB;
  FDuble.PoeBytes(LBuf);

  LSeg := TAMQPWalSegment.OpenExisting(FArq, True);
  try
    LSeg.Append(3, 0, Bytes('tres'));
    AssertEquals('os tres estao la', 3, LSeg.ReadPrefix(LRecs, LStop));
    AssertTrue('integro ate o fim', LStop = awsFim);
    AssertEquals('o terceiro e alcancavel', Int64(3), Int64(LRecs[2].Lsn));
  finally
    LSeg.Free;
  end;
end;

procedure TWalRecoveryTests.LenAbsurdo_ParaComTamanho;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LBuf: TBytes;
  P: Integer;
begin
  // Um `len` corrompido tem de ser recusado ANTES de virar alocacao: e' o
  // unico campo do registro que o proprio leitor usa para decidir quanto ler.
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(1, 0, Bytes('um'));
  finally
    LSeg.Free;
  end;
  LBuf := FDuble.Bytes;
  SetLength(LBuf, Length(LBuf) + AMQP_WAL_REC_HEADER_SIZE);
  P := Length(LBuf) - AMQP_WAL_REC_HEADER_SIZE;
  // lsn = 2, kind = 0, flags = 0, len = $7FFFFFFF
  LBuf[P] := 2;
  LBuf[P + 10] := $FF;
  LBuf[P + 11] := $FF;
  LBuf[P + 12] := $FF;
  LBuf[P + 13] := $7F;
  FDuble.PoeBytes(LBuf);

  LSeg := TAMQPWalSegment.OpenExisting(FArq, False);
  try
    AssertEquals('so o primeiro', 1, LSeg.ReadPrefix(LRecs, LStop));
    AssertTrue('motivo foi tamanho', LStop = awsTamanho);
  finally
    LSeg.Free;
  end;
end;

procedure TWalRecoveryTests.LsnForaDeOrdem_ParaComLsn;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LBufA, LBufB, LJunto: TBytes;
  I, LCab: Integer;
  LDuble2: TAMQPWalFileFalso;
  LArq2: IAMQPWalFile;
begin
  // Um registro INTEGRO com LSN fora de ordem so' aparece em arquivo remontado
  // a mao (ou por bug de quem escreveu) -- e vale distinguir de lixo, porque
  // as duas causas se investigam de forma diferente.
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(5, 0, Bytes('cinco'));
  finally
    LSeg.Free;
  end;
  LBufA := FDuble.Bytes;

  LDuble2 := TAMQPWalFileFalso.Create;
  LArq2 := LDuble2;
  try
    LSeg := TAMQPWalSegment.CreateNew(LArq2, 1);
    try
      LSeg.Append(3, 0, Bytes('tres'));
    finally
      LSeg.Free;
    end;
    LBufB := LDuble2.Bytes;
  finally
    LArq2 := nil;
  end;

  // cola o registro de LSN 3 depois do de LSN 5, os dois com CRC bom
  LCab := AMQP_WAL_SEG_HEADER_SIZE;
  SetLength(LJunto, Length(LBufA) + Length(LBufB) - LCab);
  for I := 0 to High(LBufA) do
    LJunto[I] := LBufA[I];
  for I := LCab to High(LBufB) do
    LJunto[Length(LBufA) + I - LCab] := LBufB[I];
  FDuble.PoeBytes(LJunto);

  LSeg := TAMQPWalSegment.OpenExisting(FArq, False);
  try
    AssertEquals('para no primeiro', 1, LSeg.ReadPrefix(LRecs, LStop));
    AssertTrue('motivo foi LSN, nao CRC', LStop = awsLsn);
  finally
    LSeg.Free;
  end;
end;

{ TWalFileSeamTests }

procedure TWalFileSeamTests.CreateNew_SincronizaOCabecalho;
var
  LSeg: TAMQPWalSegment;
begin
  // O cabecalho e' a unica parte do arquivo que replay nenhum reconstroi: sem
  // ele o segmento inteiro fica irreconhecivel. Por isso vai ao disco na hora,
  // e nao no primeiro lote -- e isto so' e' observavel pela costura.
  AssertEquals('nenhum sync antes', 0, FDuble.Syncs);
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    AssertEquals('cabecalho sincronizado na criacao', 1, FDuble.Syncs);
  finally
    LSeg.Free;
  end;
end;

procedure TWalFileSeamTests.Sync_ChegaAoArquivo;
var
  LSeg: TAMQPWalSegment;
  LAntes: Integer;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LAntes := FDuble.Syncs;
    LSeg.Append(1, 0, Bytes('a'));
    AssertEquals('append sozinho NAO sincroniza', LAntes, FDuble.Syncs);
    LSeg.Sync;
    AssertEquals('e o Sync sincroniza uma vez', LAntes + 1, FDuble.Syncs);
  finally
    LSeg.Free;
  end;
end;

procedure TWalFileSeamTests.FalhaDeSync_ViraFalseSemLevantar;
var
  LSeg: TAMQPWalSegment;
begin
  // Falha de fsync nao e' excecao: e' um False que a thread do journal (WS2)
  // tem de olhar para NAO liberar os confirms do lote.
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(1, 0, Bytes('a'));
    FDuble.SetSyncOk(False);
    AssertFalse('Sync devolve False', LSeg.Sync);
  finally
    LSeg.Free;
  end;
end;

procedure TWalFileSeamTests.FalhaDeEscrita_NaoAvancaOEstadoDoSegmento;
var
  LSeg: TAMQPWalSegment;
  LLevantou: Boolean;
  LFimAntes: Int64;
  LLsnAntes: UInt64;
begin
  // Se a escrita falha, o segmento em MEMORIA nao pode achar que gravou: o
  // LSN seguinte seria aceito, deixando um buraco que a recuperacao nunca
  // reconciliaria.
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    LSeg.Append(1, 0, Bytes('a'));
    LFimAntes := LSeg.EndOffset;
    LLsnAntes := LSeg.LastLsn;

    FDuble.SetFalharEscritaNa(FDuble.Escritas + 1);
    LLevantou := False;
    try
      LSeg.Append(2, 0, Bytes('b'));
    except
      on E: EAMQPWal do
        LLevantou := True;
    end;
    AssertTrue('a falha chega ao chamador', LLevantou);
    AssertEquals('fim nao andou', LFimAntes, LSeg.EndOffset);
    AssertEquals('LastLsn nao andou', Int64(LLsnAntes), Int64(LSeg.LastLsn));

    // e o LSN 2 continua disponivel
    FDuble.SetFalharEscritaNa(-1);
    LSeg.Append(2, 0, Bytes('b'));
    AssertEquals('LSN 2 aceito na retentativa', Int64(2), Int64(LSeg.LastLsn));
  finally
    LSeg.Free;
  end;
end;

{ TWalDirLockTests }

function TWalDirLockTests.DirTeste: string;
begin
  Result := IncludeTrailingPathDelimiter(GetTempDir) + 'amqpwal-lock-teste';
end;

procedure TWalDirLockTests.SegundoDono_EhRecusado;
var
  L1, L2: TAMQPWalDirLock;
  LLevantou: Boolean;
begin
  // Dois brokers sobre o mesmo DataDir escreveriam LSNs proprios nos mesmos
  // arquivos e corromperiam o log em silencio.
  L1 := TAMQPWalDirLock.Create(DirTeste);
  try
    LLevantou := False;
    L2 := nil;
    try
      L2 := TAMQPWalDirLock.Create(DirTeste);
    except
      on E: EAMQPWal do
        LLevantou := True;
    end;
    L2.Free;
    AssertTrue('o segundo dono tem de ser recusado', LLevantou);
  finally
    L1.Free;
  end;
end;

procedure TWalDirLockTests.AposSoltar_NovoDonoConsegue;
var
  L: TAMQPWalDirLock;
begin
  L := TAMQPWalDirLock.Create(DirTeste);
  L.Free;
  L := TAMQPWalDirLock.Create(DirTeste);
  try
    AssertTrue('o lock e liberado no destrutor', L.Path <> '');
  finally
    L.Free;
  end;
end;

{ TWalOsFileTests }

function TWalOsFileTests.DirTeste: string;
begin
  Result := IncludeTrailingPathDelimiter(GetTempDir) + 'amqpwal-os-teste';
end;

function TWalOsFileTests.Bytes(const S: string): TBytes;
var
  I: Integer;
begin
  SetLength(Result, Length(S));
  for I := 1 to Length(S) do
    Result[I - 1] := Byte(S[I]);
end;

procedure TWalOsFileTests.LimpaDir;
var
  LRec: TSearchRec;
begin
  ForceDirectories(DirTeste);
  if FindFirst(IncludeTrailingPathDelimiter(DirTeste) + '*', faAnyFile,
    LRec) = 0 then
  begin
    try
      repeat
        if (LRec.Attr and faDirectory) = 0 then
          SysUtils.DeleteFile(IncludeTrailingPathDelimiter(DirTeste)
            + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      FindClose(LRec);
    end;
  end;
end;

procedure TWalOsFileTests.RoundTrip_EmDiscoDeVerdade;
var
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LPath: string;
  I: Integer;
begin
  // O resto da suite roda sobre o duble em memoria, que e' deterministico e
  // rapido. ESTE passa pelo sistema de arquivos de verdade -- e' o que prova
  // que o TAMQPWalOsFile (seek/write/read/truncate) casa com o formato.
  LimpaDir;
  LPath := IncludeTrailingPathDelimiter(DirTeste) + AmqpWalSegmentName(1);
  LArq := TAMQPWalOsFile.Create(LPath, True);
  LSeg := TAMQPWalSegment.CreateNew(LArq, 1);
  try
    for I := 1 to 30 do
      LSeg.Append(I, Byte(I), Bytes(StringOfChar('d', I * 2)));
    AssertTrue('fsync de verdade funciona', LSeg.Sync);
  finally
    LSeg.Free;
    LArq := nil;
  end;

  LArq := TAMQPWalOsFile.Create(LPath, False);
  LSeg := TAMQPWalSegment.OpenExisting(LArq);
  try
    AssertEquals('leu do disco', 30, LSeg.ReadPrefix(LRecs, LStop));
    AssertTrue('integro', LStop = awsFim);
    AssertEquals('ultimo payload', 60, Length(LRecs[29].Payload));
  finally
    LSeg.Free;
    LArq := nil;
  end;
end;

procedure TWalOsFileTests.ListaSegmentos_OrdenadosPorNumero;
var
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LSegs: TArray<Cardinal>;
  LNos: array[0..3] of Cardinal;
  I: Integer;
begin
  LimpaDir;
  LNos[0] := 10;
  LNos[1] := 2;
  LNos[2] := 33;
  LNos[3] := 1;
  for I := 0 to 3 do
  begin
    LArq := TAMQPWalOsFile.Create(
      IncludeTrailingPathDelimiter(DirTeste) + AmqpWalSegmentName(LNos[I]),
      True);
    LSeg := TAMQPWalSegment.CreateNew(LArq, LNos[I]);
    LSeg.Free;
    LArq := nil;
  end;
  LSegs := AmqpWalListSegments(DirTeste);
  AssertEquals('achou os quatro', 4, Length(LSegs));
  AssertEquals('1o', Int64(1), Int64(LSegs[0]));
  AssertEquals('2o', Int64(2), Int64(LSegs[1]));
  AssertEquals('3o', Int64(10), Int64(LSegs[2]));
  AssertEquals('4o', Int64(33), Int64(LSegs[3]));
end;

procedure TWalOsFileTests.CriarSobreExistente_Levanta;
var
  LArq: IAMQPWalFile;
  LPath: string;
  LLevantou: Boolean;
begin
  LimpaDir;
  LPath := IncludeTrailingPathDelimiter(DirTeste) + AmqpWalSegmentName(1);
  LArq := TAMQPWalOsFile.Create(LPath, True);
  LArq := nil;
  LLevantou := False;
  try
    LArq := TAMQPWalOsFile.Create(LPath, True);
  except
    on E: EAMQPWal do
      LLevantou := True;
  end;
  LArq := nil;
  AssertTrue('criar por cima tem de levantar', LLevantou);
end;

procedure TWalOsFileTests.AbrirInexistente_Levanta;
var
  LArq: IAMQPWalFile;
  LLevantou: Boolean;
begin
  LimpaDir;
  LLevantou := False;
  try
    LArq := TAMQPWalOsFile.Create(
      IncludeTrailingPathDelimiter(DirTeste) + AmqpWalSegmentName(999), False);
  except
    on E: EAMQPWal do
      LLevantou := True;
  end;
  LArq := nil;
  AssertTrue('abrir o que nao existe tem de levantar', LLevantou);
end;

{ TWalClockTests }

procedure TWalClockTests.WallMs_Plausivel;
var
  LMs: Int64;
begin
  // A forma de AmqpWallMs erra em horas inteiras se a conversao para UTC for
  // feita duas vezes -- e' o defeito que a WS0 mediu. Um teste de faixa nao
  // pega isso sozinho (o erro e' de horas, nao de anos); pega o caso grosseiro
  // de a expressao estar em segundos, ou na epoch errada.
  LMs := AmqpWallMs;
  AssertTrue('depois de 2020-01-01', LMs > Int64(1577836800000));
  AssertTrue('antes de 2100-01-01', LMs < Int64(4102444800000));
end;

procedure TWalClockTests.WallMs_NaoAndaParaTras;
var
  A, B: Int64;
begin
  A := AmqpWallMs;
  B := AmqpWallMs;
  AssertTrue('duas leituras seguidas nao voltam no tempo', B >= A);
end;

procedure TWalClockTests.WallMs_DifereDoMonotonico;
begin
  // Sao relogios de origens diferentes (epoch x boot). Se um dia alguem
  // "simplificar" AmqpWallMs para devolver AmqpTickMs, a D21 inteira cai --
  // e este e o teste que grita.
  AssertTrue('parede e monotonico sao relogios diferentes',
    AmqpWallMs <> Int64(AmqpTickMs));
end;

initialization
  RegisterTest(TWalCrc32Tests);
  RegisterTest(TWalSegmentNameTests);
  RegisterTest(TWalSegmentTests);
  RegisterTest(TWalRecoveryTests);
  RegisterTest(TWalFileSeamTests);
  RegisterTest(TWalDirLockTests);
  RegisterTest(TWalOsFileTests);
  RegisterTest(TWalClockTests);

end.

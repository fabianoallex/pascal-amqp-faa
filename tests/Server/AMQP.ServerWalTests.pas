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

  2. TArquivoFalso e' a CAMADA 2 da D28. Como o segmento fala com IAMQPWalFile
     e nao com o sistema de arquivos, da' para contar os fsyncs e falhar na
     hora escolhida -- e assim provar (a) que o cabecalho de um segmento novo
     vai ao disco na hora e (b) que uma falha de escrita nao deixa o segmento
     com estado adiantado em relacao ao arquivo. Nenhuma das duas seria
     observavel por fora.

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerWalTests.pas -- mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas assercoes). Como nas outras
  suites do server, o lado Delphi ACHATA a fixture-base do lado FPC (cada
  fixture declara o proprio par duble/interface) e ica os helpers para funcoes
  de unit -- e' a mesma forma que o TServerFixture ja' tem. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.IOUtils,
  AMQP.Threading,
  AMQP.Server.Wal;

type
  { Arquivo em memoria: conta fsyncs, sabe falhar na escrita e deixa o teste
    mexer nos bytes crus (para simular queda e corrupcao sem tocar em disco). }
  TArquivoFalso = class(TInterfacedObject, IAMQPWalFile)
  private
    FBuf: TBytes;
    FSyncs: Integer;
    FSyncOk: Boolean;
    FEscritas: Integer;
    FFalharEscritaNa: Integer; // -1 = nunca
  public
    constructor Create;
    function Append(const ABytes: TBytes): Int64;
    function ReadAll: TBytes;
    function Sync: Boolean;
    function Size: Int64;
    procedure TruncateTo(ASize: Int64);

    function Bytes: TBytes;
    procedure PoeBytes(const ABytes: TBytes);
    property Syncs: Integer read FSyncs;
    property Escritas: Integer read FEscritas;
    property SyncOk: Boolean read FSyncOk write FSyncOk;
    /// Numero da escrita (1-based) em que Append vai levantar. -1 = nunca.
    property FalharEscritaNa: Integer read FFalharEscritaNa
      write FFalharEscritaNa;
  end;

  [TestFixture]
  TWalCrc32Tests = class
  public
    [Test] procedure Vetores_Publicos;
    [Test] procedure Parcial_EquivaleAoInteiro;
    [Test] procedure ByteTrocado_MudaOCrc;
  end;

  [TestFixture]
  TWalSegmentNameTests = class
  public
    [Test] procedure Formato_ComZerosAEsquerda;
    [Test] procedure RoundTrip_CobreTodoOCardinal;
    [Test] procedure RecusaNomeNaoNumerico;
    [Test] procedure RecusaEspaco;
    [Test] procedure RecusaOutraExtensao;
  end;

  { O duble e' TInterfacedObject: a referencia de CLASSE existe para o teste
    espiar os contadores, e a de INTERFACE e' quem o mantem vivo -- guardar so'
    a de classe seria o erro de duble que ja' mordeu esta codebase. }
  [TestFixture]
  TWalSegmentTests = class
  private
    FDuble: TArquivoFalso;
    FArq: IAMQPWalFile;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure SegmentoNovo_TemSoOCabecalho;
    [Test] procedure RoundTrip_CinquentaRegistros;
    [Test] procedure PayloadVazio_Sobrevive;
    [Test] procedure Reabrir_RecuperaLastLsnEContinuaEscrevendo;
    [Test] procedure LsnRepetido_Levanta;
    [Test] procedure LsnMenor_Levanta;
    [Test] procedure CreateNewSobreArquivoNaoVazio_Levanta;
    [Test] procedure AssinaturaErrada_Levanta;
    [Test] procedure VersaoErrada_Levanta;
    [Test] procedure CabecalhoTruncado_Levanta;
  end;

  [TestFixture]
  TWalRecoveryTests = class
  private
    FDuble: TArquivoFalso;
    FArq: IAMQPWalFile;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure TruncamentoExaustivo_TodoOffsetDaPrefixoConsistente;
    [Test] procedure CorrupcaoExaustiva_TodoBitTrocadoDaPrefixoConsistente;
    [Test] procedure ByteTrocado_PrefixoParaNoRegistroAnterior;
    [Test] procedure ByteTrocado_MotivoEhCrc;
    [Test] procedure CaudaTorta_EhContadaETruncada;
    [Test] procedure CaudaTorta_AposTrimOAppendEhAlcancavel;
    [Test] procedure LenAbsurdo_ParaComTamanho;
    [Test] procedure LsnForaDeOrdem_ParaComLsn;
  end;

  [TestFixture]
  TWalFileSeamTests = class
  private
    FDuble: TArquivoFalso;
    FArq: IAMQPWalFile;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure CreateNew_SincronizaOCabecalho;
    [Test] procedure Sync_ChegaAoArquivo;
    [Test] procedure FalhaDeSync_ViraFalseSemLevantar;
    [Test] procedure FalhaDeEscrita_NaoAvancaOEstadoDoSegmento;
  end;

  [TestFixture]
  TWalDirLockTests = class
  private
    function DirTeste: string;
  public
    [Test] procedure SegundoDono_EhRecusado;
    [Test] procedure AposSoltar_NovoDonoConsegue;
  end;

  [TestFixture]
  TWalOsFileTests = class
  private
    function DirTeste: string;
    procedure LimpaDir;
  public
    [Test] procedure RoundTrip_EmDiscoDeVerdade;
    [Test] procedure ListaSegmentos_OrdenadosPorNumero;
    [Test] procedure CriarSobreExistente_Levanta;
    [Test] procedure AbrirInexistente_Levanta;
  end;

  [TestFixture]
  TWalClockTests = class
  public
    [Test] procedure WallMs_Plausivel;
    [Test] procedure WallMs_NaoAndaParaTras;
    [Test] procedure WallMs_DifereDoMonotonico;
  end;

implementation

// --- helpers de unit (o lado FPC os tem como metodos da fixture-base) -------

function Bytes(const S: string): TBytes;
var
  I: Integer;
begin
  SetLength(Result, Length(S));
  for I := 1 to Length(S) do
    Result[I - 1] := Byte(S[I]);
end;

function ComoTexto(const B: TBytes): string;
var
  I: Integer;
begin
  SetLength(Result, Length(B));
  for I := 0 to High(B) do
    Result[I + 1] := Char(B[I]);
end;

{ TArquivoFalso }

constructor TArquivoFalso.Create;
begin
  inherited Create;
  FSyncOk := True;
  FFalharEscritaNa := -1;
end;

function TArquivoFalso.Append(const ABytes: TBytes): Int64;
var
  LAntes: Integer;
begin
  Inc(FEscritas);
  if FEscritas = FFalharEscritaNa then
    raise EAMQPWal.Create('falha de escrita injetada');
  LAntes := Length(FBuf);
  SetLength(FBuf, LAntes + Length(ABytes));
  if Length(ABytes) > 0 then
    Move(ABytes[0], FBuf[LAntes], Length(ABytes));
  Result := LAntes;
end;

function TArquivoFalso.ReadAll: TBytes;
begin
  Result := Copy(FBuf, 0, Length(FBuf));
end;

function TArquivoFalso.Sync: Boolean;
begin
  Inc(FSyncs);
  Result := FSyncOk;
end;

function TArquivoFalso.Size: Int64;
begin
  Result := Length(FBuf);
end;

procedure TArquivoFalso.TruncateTo(ASize: Int64);
begin
  SetLength(FBuf, Integer(ASize));
end;

function TArquivoFalso.Bytes: TBytes;
begin
  Result := Copy(FBuf, 0, Length(FBuf));
end;

procedure TArquivoFalso.PoeBytes(const ABytes: TBytes);
begin
  FBuf := Copy(ABytes, 0, Length(ABytes));
end;

{ TWalCrc32Tests }

procedure TWalCrc32Tests.Vetores_Publicos;
var
  LB: TBytes;
begin
  // Vetores publicos do CRC-32 IEEE (os mesmos do zip/png/gzip). Sao eles que
  // provam que isto e' um CRC-32 de verdade, e nao um checksum caseiro que
  // ninguem de fora consegue conferir.
  LB := nil;
  Assert.AreEqual(Int64($00000000), Int64(AmqpCrc32(LB)), 'vazio');
  Assert.AreEqual(Int64($E8B7BE43), Int64(AmqpCrc32(Bytes('a'))), '"a"');
  Assert.AreEqual(Int64($CBF43926), Int64(AmqpCrc32(Bytes('123456789'))),
    '"123456789"');
  Assert.AreEqual(Int64($414FA339),
    Int64(AmqpCrc32(Bytes('The quick brown fox jumps over the lazy dog'))),
    'pangrama');
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
  Assert.AreEqual(Int64(AmqpCrc32(LB)), Int64(LParcial),
    'dois pedacos == inteiro');
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
  Assert.IsTrue(AmqpCrc32(LB) <> LAntes, 'um bit trocado muda o CRC');
end;

{ TWalSegmentNameTests }

procedure TWalSegmentNameTests.Formato_ComZerosAEsquerda;
begin
  Assert.AreEqual('seg-0000000001.wal', AmqpWalSegmentName(1));
  Assert.AreEqual('seg-0000000042.wal', AmqpWalSegmentName(42));
end;

procedure TWalSegmentNameTests.RoundTrip_CobreTodoOCardinal;
var
  LNo: Cardinal;
begin
  // O maior Cardinal tem de sobreviver ao round-trip. Com menos digitos no
  // nome, ele geraria um arquivo que o proprio parser nao reconheceria de
  // volta -- e o segmento sumiria da listagem em silencio.
  Assert.IsTrue(AmqpWalParseSegmentName(AmqpWalSegmentName(High(Cardinal)),
    LNo), 'parseou o maior Cardinal');
  Assert.AreEqual(Int64(High(Cardinal)), Int64(LNo), 'valor de volta');
  Assert.IsTrue(AmqpWalParseSegmentName(AmqpWalSegmentName(1), LNo),
    'parseou o 1');
  Assert.AreEqual(Int64(1), Int64(LNo), 'valor de volta');
end;

procedure TWalSegmentNameTests.RecusaNomeNaoNumerico;
var
  LNo: Cardinal;
begin
  Assert.IsFalse(AmqpWalParseSegmentName('seg-000000001a.wal', LNo),
    'letra no meio');
end;

procedure TWalSegmentNameTests.RecusaEspaco;
var
  LNo: Cardinal;
begin
  // StrToInt64Def sozinho aceitaria ' 12', e um nome desses viraria segmento
  // fantasma com o numero de outro.
  Assert.IsFalse(AmqpWalParseSegmentName('seg- 000000001.wal', LNo),
    'espaco a esquerda');
end;

procedure TWalSegmentNameTests.RecusaOutraExtensao;
var
  LNo: Cardinal;
begin
  Assert.IsFalse(AmqpWalParseSegmentName('seg-0000000001.log', LNo),
    'extensao errada');
end;

{ TWalSegmentTests }

procedure TWalSegmentTests.Setup;
begin
  FDuble := TArquivoFalso.Create;
  FArq := FDuble;
end;

procedure TWalSegmentTests.TearDown;
begin
  FArq := nil;
  FDuble := nil;
end;

procedure TWalSegmentTests.SegmentoNovo_TemSoOCabecalho;
var
  LSeg: TAMQPWalSegment;
begin
  LSeg := TAMQPWalSegment.CreateNew(FArq, 3);
  try
    Assert.AreEqual(Int64(AMQP_WAL_SEG_HEADER_SIZE), LSeg.EndOffset,
      'fim logo apos o cabecalho');
    Assert.AreEqual(Int64(AMQP_WAL_SEG_HEADER_SIZE), FDuble.Size,
      'tamanho do arquivo');
    Assert.AreEqual(Int64(0), Int64(LSeg.LastLsn), 'sem LSN');
    Assert.AreEqual(Int64(3), Int64(LSeg.SegNo), 'numero do segmento');
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
    Assert.AreEqual(50, LN, 'leu todos');
    Assert.IsTrue(LStop = awsFim, 'parou no fim');
    Assert.AreEqual(Int64(1), Int64(LRecs[0].Lsn), 'primeiro lsn');
    Assert.AreEqual(Int64(50), Int64(LRecs[49].Lsn), 'ultimo lsn');
    Assert.AreEqual(Int64(5 mod 7), Int64(LRecs[4].Kind), 'kind do 5o');
    Assert.AreEqual(50, Length(LRecs[49].Payload),
      'tamanho do payload do 50o');
    Assert.AreEqual(StringOfChar('x', 50), ComoTexto(LRecs[49].Payload),
      'conteudo do 50o');
    Assert.AreEqual(Int64(50), Int64(LSeg.LastLsn), 'LastLsn');
    Assert.AreEqual(Int64(0), LSeg.BytesDescartados, 'sem cauda');
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
    Assert.AreEqual(2, LSeg.ReadPrefix(LRecs, LStop), 'dois registros');
    Assert.IsTrue(LStop = awsFim, 'integro ate o fim');
    Assert.AreEqual(0, Length(LRecs[0].Payload), 'payload vazio');
    Assert.AreEqual('depois', ComoTexto(LRecs[1].Payload),
      'o seguinte continua legivel');
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
    Assert.AreEqual(Int64(5), Int64(LSeg.SegNo), 'segno veio do cabecalho');
    Assert.AreEqual(Int64(2), Int64(LSeg.LastLsn), 'LastLsn recuperado');
    LSeg.Append(3, 0, Bytes('tres'));
    Assert.AreEqual(3, LSeg.ReadPrefix(LRecs, LStop), 'tres registros');
    Assert.AreEqual('tres', ComoTexto(LRecs[2].Payload), 'o novo esta la');
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
    Assert.IsTrue(LLevantou, 'LSN repetido tem de levantar');
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
    Assert.IsTrue(LLevantou, 'LSN menor tem de levantar');
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
  Assert.IsTrue(LLevantou, 'CreateNew sobre arquivo com bytes tem de levantar');
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
  Assert.IsTrue(LLevantou, 'assinatura errada tem de levantar');
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
  Assert.IsTrue(LLevantou, 'versao desconhecida tem de levantar');
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
  Assert.IsTrue(LLevantou, 'cabecalho incompleto tem de levantar');
end;

{ TWalRecoveryTests }

procedure TWalRecoveryTests.Setup;
begin
  FDuble := TArquivoFalso.Create;
  FArq := FDuble;
end;

procedure TWalRecoveryTests.TearDown;
begin
  FArq := nil;
  FDuble := nil;
end;

procedure TWalRecoveryTests.TruncamentoExaustivo_TodoOffsetDaPrefixoConsistente;
var
  LSeg: TAMQPWalSegment;
  LRecs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LOrig, LCortado: TBytes;
  LFim: array of Integer;
  I, LCorte, LEsperado, LN: Integer;
  LDuble2: TArquivoFalso;
  LArq2: IAMQPWalFile;
begin
  // ---- CAMADA 1 DA D28: a escrita CURTA ----
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
  Assert.AreEqual(LFim[20], Length(LOrig), 'tamanho previsto');

  for LCorte := AMQP_WAL_SEG_HEADER_SIZE to Length(LOrig) do
  begin
    LCortado := Copy(LOrig, 0, LCorte);
    LDuble2 := TArquivoFalso.Create;
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
        Assert.AreEqual(LEsperado, LN,
          'corte em ' + IntToStr(LCorte) + ': numero de registros');
        if LN > 0 then
          Assert.AreEqual(Int64(LEsperado), Int64(LRecs[LN - 1].Lsn),
            'corte em ' + IntToStr(LCorte) + ': ultimo LSN');
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
  LDuble2: TArquivoFalso;
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

    LDuble2 := TArquivoFalso.Create;
    LArq2 := LDuble2;
    try
      LDuble2.PoeBytes(LMutado);
      LSeg := TAMQPWalSegment.OpenExisting(LArq2, False);
      try
        LN := LSeg.ReadPrefix(LRecs, LStop);
        Assert.AreEqual(LEsperado, LN, 'bit trocado no byte '
          + IntToStr(LPos) + ' (registro ' + IntToStr(K) + ')');
        if LN > 0 then
          Assert.AreEqual(Int64(LEsperado), Int64(LRecs[LN - 1].Lsn),
            'bit trocado no byte ' + IntToStr(LPos) + ': ultimo LSN');
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
    Assert.AreEqual(4, LSeg.ReadPrefix(LRecs, LStop), 'para nos 4 anteriores');
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
    Assert.IsTrue(LStop = awsCrc, 'corrupcao no meio e CRC, nao cauda');
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
    Assert.AreEqual(Int64(7), LSeg.BytesDescartados,
      'contou os bytes descartados');
    Assert.IsTrue(LSeg.Stop = awsCauda, 'motivo foi cauda');
    Assert.AreEqual(Int64(LAntes), FDuble.Size,
      'e o arquivo encolheu de verdade');
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
    Assert.AreEqual(3, LSeg.ReadPrefix(LRecs, LStop), 'os tres estao la');
    Assert.IsTrue(LStop = awsFim, 'integro ate o fim');
    Assert.AreEqual(Int64(3), Int64(LRecs[2].Lsn), 'o terceiro e alcancavel');
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
    Assert.AreEqual(1, LSeg.ReadPrefix(LRecs, LStop), 'so o primeiro');
    Assert.IsTrue(LStop = awsTamanho, 'motivo foi tamanho');
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
  LDuble2: TArquivoFalso;
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

  LDuble2 := TArquivoFalso.Create;
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
    Assert.AreEqual(1, LSeg.ReadPrefix(LRecs, LStop), 'para no primeiro');
    Assert.IsTrue(LStop = awsLsn, 'motivo foi LSN, nao CRC');
  finally
    LSeg.Free;
  end;
end;

{ TWalFileSeamTests }

procedure TWalFileSeamTests.Setup;
begin
  FDuble := TArquivoFalso.Create;
  FArq := FDuble;
end;

procedure TWalFileSeamTests.TearDown;
begin
  FArq := nil;
  FDuble := nil;
end;

procedure TWalFileSeamTests.CreateNew_SincronizaOCabecalho;
var
  LSeg: TAMQPWalSegment;
begin
  // O cabecalho e' a unica parte do arquivo que replay nenhum reconstroi: sem
  // ele o segmento inteiro fica irreconhecivel. Por isso vai ao disco na hora,
  // e nao no primeiro lote -- e isto so' e' observavel pela costura.
  Assert.AreEqual(0, FDuble.Syncs, 'nenhum sync antes');
  LSeg := TAMQPWalSegment.CreateNew(FArq, 1);
  try
    Assert.AreEqual(1, FDuble.Syncs, 'cabecalho sincronizado na criacao');
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
    Assert.AreEqual(LAntes, FDuble.Syncs, 'append sozinho NAO sincroniza');
    LSeg.Sync;
    Assert.AreEqual(LAntes + 1, FDuble.Syncs, 'e o Sync sincroniza uma vez');
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
    FDuble.SyncOk := False;
    Assert.IsFalse(LSeg.Sync, 'Sync devolve False');
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

    FDuble.FalharEscritaNa := FDuble.Escritas + 1;
    LLevantou := False;
    try
      LSeg.Append(2, 0, Bytes('b'));
    except
      on E: EAMQPWal do
        LLevantou := True;
    end;
    Assert.IsTrue(LLevantou, 'a falha chega ao chamador');
    Assert.AreEqual(LFimAntes, LSeg.EndOffset, 'fim nao andou');
    Assert.AreEqual(Int64(LLsnAntes), Int64(LSeg.LastLsn),
      'LastLsn nao andou');

    // e o LSN 2 continua disponivel
    FDuble.FalharEscritaNa := -1;
    LSeg.Append(2, 0, Bytes('b'));
    Assert.AreEqual(Int64(2), Int64(LSeg.LastLsn),
      'LSN 2 aceito na retentativa');
  finally
    LSeg.Free;
  end;
end;

{ TWalDirLockTests }

function TWalDirLockTests.DirTeste: string;
begin
  Result := IncludeTrailingPathDelimiter(TPath.GetTempPath)
    + 'amqpwal-lock-teste';
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
    Assert.IsTrue(LLevantou, 'o segundo dono tem de ser recusado');
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
    Assert.IsTrue(L.Path <> '', 'o lock e liberado no destrutor');
  finally
    L.Free;
  end;
end;

{ TWalOsFileTests }

function TWalOsFileTests.DirTeste: string;
begin
  Result := IncludeTrailingPathDelimiter(TPath.GetTempPath)
    + 'amqpwal-os-teste';
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
          System.SysUtils.DeleteFile(IncludeTrailingPathDelimiter(DirTeste)
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
    Assert.IsTrue(LSeg.Sync, 'fsync de verdade funciona');
  finally
    LSeg.Free;
    LArq := nil;
  end;

  LArq := TAMQPWalOsFile.Create(LPath, False);
  LSeg := TAMQPWalSegment.OpenExisting(LArq);
  try
    Assert.AreEqual(30, LSeg.ReadPrefix(LRecs, LStop), 'leu do disco');
    Assert.IsTrue(LStop = awsFim, 'integro');
    Assert.AreEqual(60, Length(LRecs[29].Payload), 'ultimo payload');
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
  Assert.AreEqual(4, Length(LSegs), 'achou os quatro');
  Assert.AreEqual(Int64(1), Int64(LSegs[0]), '1o');
  Assert.AreEqual(Int64(2), Int64(LSegs[1]), '2o');
  Assert.AreEqual(Int64(10), Int64(LSegs[2]), '3o');
  Assert.AreEqual(Int64(33), Int64(LSegs[3]), '4o');
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
  Assert.IsTrue(LLevantou, 'criar por cima tem de levantar');
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
  Assert.IsTrue(LLevantou, 'abrir o que nao existe tem de levantar');
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
  Assert.IsTrue(LMs > Int64(1577836800000), 'depois de 2020-01-01');
  Assert.IsTrue(LMs < Int64(4102444800000), 'antes de 2100-01-01');
end;

procedure TWalClockTests.WallMs_NaoAndaParaTras;
var
  A, B: Int64;
begin
  A := AmqpWallMs;
  B := AmqpWallMs;
  Assert.IsTrue(B >= A, 'duas leituras seguidas nao voltam no tempo');
end;

procedure TWalClockTests.WallMs_DifereDoMonotonico;
begin
  // Sao relogios de origens diferentes (epoch x boot). Se um dia alguem
  // "simplificar" AmqpWallMs para devolver AmqpTickMs, a D21 inteira cai --
  // e este e o teste que grita.
  Assert.IsTrue(AmqpWallMs <> Int64(AmqpTickMs),
    'parede e monotonico sao relogios diferentes');
end;

initialization
  TDUnitX.RegisterTestFixture(TWalCrc32Tests);
  TDUnitX.RegisterTestFixture(TWalSegmentNameTests);
  TDUnitX.RegisterTestFixture(TWalSegmentTests);
  TDUnitX.RegisterTestFixture(TWalRecoveryTests);
  TDUnitX.RegisterTestFixture(TWalFileSeamTests);
  TDUnitX.RegisterTestFixture(TWalDirLockTests);
  TDUnitX.RegisterTestFixture(TWalOsFileTests);
  TDUnitX.RegisterTestFixture(TWalClockTests);

end.

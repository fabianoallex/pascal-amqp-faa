unit AMQP.ServerTopologyTests;

{ Testes da topologia duravel (AMQP.Server.Records + a fiacao na engine e no
  TAMQPServer) -- WS3 da Fase 4 (durabilidade, ver CLAUDE.md, D19-D28).

  A suite tem tres camadas, e a do meio e' a que importa:

  1. O CODEC (TTopoRecordTests): round-trip puro dos quatro registros. Usa o
     codec da AMQP.Wire de proposito -- os campos de topologia sao os mesmos que
     vem no fio (shortstr e field-table), e a AMQP.Wire ja' e' a parte da lib
     com mais teste em cima. Inventar um segundo formato para o mesmo dado seria
     criar uma segunda chance de errar, com metade da cobertura.

  2. O QUE VAI (e o que NAO VAI) PARA O DISCO: broker de verdade com DataDir,
     cliente de verdade declarando topologia, e depois a leitura do WAL. E' aqui
     que a D20 vira teste -- e repare que metade das assercoes e' NEGATIVA:
     fila transiente, fila exclusiva, redeclare idempotente e binding com uma
     ponta transiente NAO podem gerar registro. Uma implementacao que gravasse
     tudo passaria em qualquer teste que so' olhasse o que foi gravado.

  3. A D19 (TTopoSemDataDirTests): com DataDir vazio o broker nao escreve
     arquivo NENHUM -- nem sequer cria o diretorio. E' o que garante que o
     caminho da Fase 3 continua intacto para quem nao pediu durabilidade.

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerTopologyTests.pas -- mantenha
  os dois em sincronia (mesmos nomes de teste, mesmas assercoes). Como nas
  outras suites do server, o lado Delphi ACHATA a fixture-base do lado FPC e
  ica os helpers para funcoes de unit. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.Rtti,
  System.IOUtils,
  AMQP.Wire,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Connection,
  AMQP.Threading,
  AMQP.Server.Wal,
  AMQP.Server.Journal,
  AMQP.Server.Records,
  AMQP.Server.Engine,
  AMQP.Server.Broker;

type
  [TestFixture]
  TTopoRecordTests = class
  public
    [Test] procedure Exchange_RoundTrip;
    [Test] procedure Queue_RoundTrip;
    [Test] procedure Binding_RoundTrip;
    [Test] procedure Name_RoundTrip;
    [Test] procedure ArgumentosVazios_VoltamComoNil;
    [Test] procedure ArgumentosNil_CodificamIgualATabelaVazia;
    [Test] procedure KindsDeTopologia_SaoReconhecidos;
  end;

  { Sobe um broker com DataDir proprio, deixa o teste declarar topologia com o
    cliente de verdade, e depois le' o WAL. }
  [TestFixture]
  TTopoPersistTests = class
  private
    FBroker: TAMQPServer;
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    FDir: string;
    /// Fecha o cliente, para o broker e devolve os registros do WAL, um por
    /// linha, no formato 'kind:detalhe'.
    function FechaELeWal: TStringList;
    function DeclaraFila(const ANome: string; ADurable, AExclusive,
      AAutoDelete: Boolean; AArgs: TAMQPFieldTable = nil): string;
    procedure DeclaraExchange(const ANome, ATipo: string;
      ADurable, AAutoDelete: Boolean);
    procedure Liga(const AFila, AExchange, AChave: string);
    function Conta(L: TStringList; const APrefixo: string): Integer;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure ExchangeDuravel_EhGravado;
    [Test] procedure ExchangeTransiente_NaoEhGravado;
    [Test] procedure FilaDuravel_EhGravada;
    [Test] procedure FilaTransiente_NaoEhGravada;
    [Test] procedure FilaExclusiva_NaoEhGravadaMesmoDuravel;
    [Test] procedure Redeclare_NaoGeraSegundoRegistro;
    [Test] procedure Bind_SoQuandoAsDuasPontasSaoDuraveis;
    [Test] procedure ArgumentosDaFila_SobrevivemNoRegistro;
    [Test] procedure DeleteDeFilaDuravel_EhGravado;
    [Test] procedure AutoDeleteDeFilaDuravel_EhGravado;
    [Test] procedure AutoDeleteDeExchangeDuravel_EhGravado;
  end;

  [TestFixture]
  TTopoSemDataDirTests = class
  public
    [Test] procedure SemDataDir_NaoHaJournal;
    [Test] procedure SemDataDir_NenhumArquivoEscrito;
    [Test] procedure DataDir_NaoMudaComBrokerNoAr;
  end;

  [TestFixture]
  TTopoFalhaTests = class
  public
    [Test] procedure JournalParado_DeclareDuravel_DaSemDurabilidade;
    [Test] procedure JournalParado_DeclareTransiente_ContinuaFuncionando;
  end;

implementation

var
  GSeq: Integer = 0;

function DirNovo(const APrefixo: string): string;
begin
  Inc(GSeq);
  Result := IncludeTrailingPathDelimiter(TPath.GetTempPath) + APrefixo + '-'
    + IntToStr(GSeq);
end;

procedure LimpaDir(const ADir: string);
var
  LRec: TSearchRec;
begin
  if FindFirst(IncludeTrailingPathDelimiter(ADir) + '*', faAnyFile, LRec) = 0 then
  begin
    try
      repeat
        if (LRec.Attr and faDirectory) = 0 then
          System.SysUtils.DeleteFile(IncludeTrailingPathDelimiter(ADir)
            + LRec.Name);
      until FindNext(LRec) <> 0;
    finally
      System.SysUtils.FindClose(LRec);
    end;
  end;
end;

function ContaArquivos(const ADir: string): Integer;
var
  LRec: TSearchRec;
begin
  Result := 0;
  if FindFirst(IncludeTrailingPathDelimiter(ADir) + '*', faAnyFile, LRec) = 0 then
  begin
    try
      repeat
        if (LRec.Attr and faDirectory) = 0 then
          Inc(Result);
      until FindNext(LRec) <> 0;
    finally
      System.SysUtils.FindClose(LRec);
    end;
  end;
end;

// Uma linha por registro, num formato que o teste consegue comparar por
// prefixo -- deliberadamente legivel, para a falha dizer o que apareceu.
function MontaLinha(const ARec: TAMQPWalRecord): string;
var
  LQ: TAMQPRecQueue;
  LE: TAMQPRecExchange;
  LB: TAMQPRecBinding;
  LN: TAMQPRecName;
begin
  case ARec.Kind of
    AMQP_REC_QUEUE_DECLARE:
      begin
        LQ := AmqpDecodeRecQueue(ARec.Payload);
        try
          Result := 'queue.declare:' + LQ.Name + ':dur='
            + BoolToStr(LQ.Durable, True) + ':ad='
            + BoolToStr(LQ.AutoDelete, True) + ':args='
            + IntToStr(Ord(LQ.Arguments <> nil));
        finally
          LQ.Arguments.Free;
        end;
      end;
    AMQP_REC_EXCHANGE_DECLARE:
      begin
        LE := AmqpDecodeRecExchange(ARec.Payload);
        try
          Result := 'exchange.declare:' + LE.Name + ':' + LE.ExchangeType;
        finally
          LE.Arguments.Free;
        end;
      end;
    AMQP_REC_QUEUE_BIND, AMQP_REC_QUEUE_UNBIND,
    AMQP_REC_EXCHANGE_BIND, AMQP_REC_EXCHANGE_UNBIND:
      begin
        LB := AmqpDecodeRecBinding(ARec.Payload);
        try
          Result := AmqpRecordKindName(ARec.Kind) + ':' + LB.Source + '->'
            + LB.Destination + ':' + LB.RoutingKey;
        finally
          LB.Arguments.Free;
        end;
      end;
    AMQP_REC_QUEUE_DELETE, AMQP_REC_EXCHANGE_DELETE:
      begin
        LN := AmqpDecodeRecName(ARec.Payload);
        Result := AmqpRecordKindName(ARec.Kind) + ':' + LN.Name;
      end;
  else
    Result := AmqpRecordKindName(ARec.Kind);
  end;
end;

function LeWal(const ADir: string): TStringList;
var
  LArq: IAMQPWalFile;
  LSeg: TAMQPWalSegment;
  LRegs: TAMQPWalRecords;
  LStop: TAMQPWalStop;
  LSegs: TArray<Cardinal>;
  I, LN: Integer;
begin
  Result := TStringList.Create;
  LSegs := AmqpWalListSegments(ADir);
  if Length(LSegs) = 0 then
    Exit;
  LArq := TAMQPWalOsFile.Create(
    IncludeTrailingPathDelimiter(ADir) + AmqpWalSegmentName(LSegs[0]), False);
  LSeg := TAMQPWalSegment.OpenExisting(LArq, False);
  try
    LN := LSeg.ReadPrefix(LRegs, LStop);
    for I := 0 to LN - 1 do
      Result.Add(MontaLinha(LRegs[I]));
  finally
    LSeg.Free;
    LArq := nil;
  end;
end;

{ TTopoRecordTests }

procedure TTopoRecordTests.Exchange_RoundTrip;
var
  R, R2: TAMQPRecExchange;
begin
  R.VHost := '/';
  R.Name := 'ex.um';
  R.ExchangeType := 'topic';
  R.Durable := True;
  R.AutoDelete := False;
  R.Internal := True;
  R.Arguments := TAMQPFieldTable.Create;
  R.Arguments.Put('alternate-exchange', TValue.From<string>('ae'));
  try
    R2 := AmqpDecodeRecExchange(AmqpEncodeRecExchange(R));
  finally
    R.Arguments.Free;
  end;
  try
    Assert.AreEqual('/', R2.VHost, 'vhost');
    Assert.AreEqual('ex.um', R2.Name, 'nome');
    Assert.AreEqual('topic', R2.ExchangeType, 'tipo');
    Assert.IsTrue(R2.Durable, 'durable');
    Assert.IsFalse(R2.AutoDelete, 'auto-delete');
    Assert.IsTrue(R2.Internal, 'internal');
    Assert.IsTrue(R2.Arguments <> nil, 'argumentos vieram');
    Assert.AreEqual(1, R2.Arguments.Count, 'uma chave');
  finally
    R2.Arguments.Free;
  end;
end;

procedure TTopoRecordTests.Queue_RoundTrip;
var
  R, R2: TAMQPRecQueue;
begin
  R.VHost := '/vh';
  R.Name := 'q.um';
  R.Durable := True;
  R.AutoDelete := True;
  R.Arguments := TAMQPFieldTable.Create;
  R.Arguments.Put('x-message-ttl', TValue.From<Integer>(60000));
  try
    R2 := AmqpDecodeRecQueue(AmqpEncodeRecQueue(R));
  finally
    R.Arguments.Free;
  end;
  try
    Assert.AreEqual('/vh', R2.VHost, 'vhost');
    Assert.AreEqual('q.um', R2.Name, 'nome');
    Assert.IsTrue(R2.Durable, 'durable');
    Assert.IsTrue(R2.AutoDelete, 'auto-delete');
    Assert.IsTrue(R2.Arguments <> nil, 'argumentos vieram');
  finally
    R2.Arguments.Free;
  end;
end;

procedure TTopoRecordTests.Binding_RoundTrip;
var
  R, R2: TAMQPRecBinding;
begin
  R.VHost := '/';
  R.Source := 'ex';
  R.Destination := 'q';
  R.RoutingKey := 'a.b.c';
  R.Arguments := nil;
  R2 := AmqpDecodeRecBinding(AmqpEncodeRecBinding(R));
  try
    Assert.AreEqual('ex', R2.Source, 'source');
    Assert.AreEqual('q', R2.Destination, 'destination');
    Assert.AreEqual('a.b.c', R2.RoutingKey, 'routing-key');
  finally
    R2.Arguments.Free;
  end;
end;

procedure TTopoRecordTests.Name_RoundTrip;
var
  R, R2: TAMQPRecName;
begin
  R.VHost := '/outro';
  R.Name := 'q.apagada';
  R2 := AmqpDecodeRecName(AmqpEncodeRecName(R));
  Assert.AreEqual('/outro', R2.VHost, 'vhost');
  Assert.AreEqual('q.apagada', R2.Name, 'nome');
end;

procedure TTopoRecordTests.ArgumentosVazios_VoltamComoNil;
var
  R, R2: TAMQPRecQueue;
begin
  // Tabela sem chaves e ausencia de tabela sao a MESMA coisa para a topologia,
  // e o decode devolve nil nos dois casos -- assim quem le nao precisa
  // distinguir dois jeitos de dizer "sem argumentos".
  R.VHost := '/';
  R.Name := 'q';
  R.Durable := True;
  R.AutoDelete := False;
  R.Arguments := TAMQPFieldTable.Create;
  try
    R2 := AmqpDecodeRecQueue(AmqpEncodeRecQueue(R));
  finally
    R.Arguments.Free;
  end;
  Assert.IsTrue(R2.Arguments = nil, 'tabela vazia volta como nil');
end;

procedure TTopoRecordTests.ArgumentosNil_CodificamIgualATabelaVazia;
var
  R: TAMQPRecQueue;
  LComNil, LComVazia: TBytes;
  LTab: TAMQPFieldTable;
  I: Integer;
  LIgual: Boolean;
begin
  R.VHost := '/';
  R.Name := 'q';
  R.Durable := True;
  R.AutoDelete := False;
  R.Arguments := nil;
  LComNil := AmqpEncodeRecQueue(R);
  LTab := TAMQPFieldTable.Create;
  try
    R.Arguments := LTab;
    LComVazia := AmqpEncodeRecQueue(R);
  finally
    LTab.Free;
  end;
  LIgual := Length(LComNil) = Length(LComVazia);
  if LIgual then
    for I := 0 to High(LComNil) do
      if LComNil[I] <> LComVazia[I] then
        LIgual := False;
  Assert.IsTrue(LIgual, 'nil e tabela vazia dao o MESMO byte');
end;

procedure TTopoRecordTests.KindsDeTopologia_SaoReconhecidos;
begin
  Assert.IsTrue(AmqpIsTopoRecord(AMQP_REC_EXCHANGE_DECLARE),
    'declare de exchange');
  Assert.IsTrue(AmqpIsTopoRecord(AMQP_REC_EXCHANGE_UNBIND),
    'unbind de exchange');
  // A faixa 20-29 e' de conteudo/colocacao (D22) e NAO e' topologia -- e' o
  // que a recuperacao usa para separar os dois passes.
  Assert.IsFalse(AmqpIsTopoRecord(20), '20 ja e conteudo');
  Assert.IsFalse(AmqpIsTopoRecord(0), '0 nao e nada');
end;

{ TTopoPersistTests }

procedure TTopoPersistTests.Setup;
var
  LParams: TAMQPConnectionParams;
begin
  FDir := DirNovo('amqptopo');
  ForceDirectories(FDir);
  LimpaDir(FDir);
  FBroker := TAMQPServer.Create;
  FBroker.BindAddress := '127.0.0.1';
  FBroker.Port := 0;
  FBroker.DataDir := FDir;
  FBroker.Start;

  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := FBroker.Port;
  FConn := TAMQPConnection.Create(LParams);
  FConn.Open;
  FChan := FConn.CreateChannel;
end;

procedure TTopoPersistTests.TearDown;
begin
  if FConn <> nil then
  begin
    try
      FConn.Close;
    except
    end;
    FreeAndNil(FConn);
  end;
  if FBroker <> nil then
  begin
    try
      FBroker.Stop;
    except
    end;
    FreeAndNil(FBroker);
  end;
  LimpaDir(FDir);
end;

function TTopoPersistTests.FechaELeWal: TStringList;
begin
  FConn.Close;
  FreeAndNil(FConn);
  FBroker.Stop;
  FreeAndNil(FBroker);
  Result := LeWal(FDir);
end;

function TTopoPersistTests.DeclaraFila(const ANome: string; ADurable,
  AExclusive, AAutoDelete: Boolean; AArgs: TAMQPFieldTable): string;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := TAMQPQueueDeclare.Create(ANome, ADurable);
  LDecl.Exclusive := AExclusive;
  LDecl.AutoDelete := AAutoDelete;
  LDecl.Arguments := AArgs;
  Result := FChan.DeclareQueue(LDecl).QueueName;
end;

procedure TTopoPersistTests.DeclaraExchange(const ANome, ATipo: string;
  ADurable, AAutoDelete: Boolean);
var
  LDecl: TAMQPExchangeDeclare;
begin
  LDecl := TAMQPExchangeDeclare.Create(ANome, ATipo, ADurable);
  LDecl.AutoDelete := AAutoDelete;
  FChan.DeclareExchange(LDecl);
end;

procedure TTopoPersistTests.Liga(const AFila, AExchange, AChave: string);
var
  LBind: TAMQPQueueBind;
begin
  LBind.QueueName := AFila;
  LBind.ExchangeName := AExchange;
  LBind.RoutingKey := AChave;
  LBind.NoWait := False;
  LBind.Arguments := nil;
  FChan.BindQueue(LBind);
end;

function TTopoPersistTests.Conta(L: TStringList;
  const APrefixo: string): Integer;
var
  I: Integer;
begin
  Result := 0;
  for I := 0 to L.Count - 1 do
    if Pos(APrefixo, L[I]) = 1 then
      Inc(Result);
end;

procedure TTopoPersistTests.ExchangeDuravel_EhGravado;
var
  L: TStringList;
begin
  DeclaraExchange('ex.dur', 'topic', True, False);
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'exchange.declare:ex.dur'),
      'um registro do exchange');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.ExchangeTransiente_NaoEhGravado;
var
  L: TStringList;
begin
  DeclaraExchange('ex.tra', 'direct', False, False);
  L := FechaELeWal;
  try
    Assert.AreEqual(0, Conta(L, 'exchange.declare:ex.tra'),
      'exchange transiente nao vai para o disco');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.FilaDuravel_EhGravada;
var
  L: TStringList;
begin
  DeclaraFila('q.dur', True, False, False);
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'queue.declare:q.dur'), 'um registro da fila');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.FilaTransiente_NaoEhGravada;
var
  L: TStringList;
begin
  DeclaraFila('q.tra', False, False, False);
  L := FechaELeWal;
  try
    Assert.AreEqual(0, Conta(L, 'queue.declare:q.tra'),
      'fila transiente nao vai para o disco');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.FilaExclusiva_NaoEhGravadaMesmoDuravel;
var
  L: TStringList;
begin
  // D20: fila exclusiva morre com a conexao dona, e depois de um restart nao
  // existe conexao dona. O registro nem CARREGA o flag exclusive -- o formato
  // nao consegue expressar uma coisa que nao deveria existir.
  DeclaraFila('q.exc', True, True, False);
  L := FechaELeWal;
  try
    Assert.AreEqual(0, Conta(L, 'queue.declare:q.exc'),
      'exclusiva nao vai, mesmo declarada duravel');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.Redeclare_NaoGeraSegundoRegistro;
var
  L: TStringList;
begin
  // Redeclare identico e' idempotente: gravar de novo so' encheria o log de
  // registros que a recuperacao teria trabalho de reaplicar por cima de si.
  DeclaraFila('q.dur', True, False, False);
  DeclaraFila('q.dur', True, False, False);
  DeclaraFila('q.dur', True, False, False);
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'queue.declare:q.dur'),
      'tres declares, UM registro');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.Bind_SoQuandoAsDuasPontasSaoDuraveis;
var
  L: TStringList;
begin
  // Um binding e' tao duravel quanto o mais fragil dos dois lados.
  DeclaraExchange('ex.dur', 'direct', True, False);
  DeclaraExchange('ex.tra', 'direct', False, False);
  DeclaraFila('q.dur', True, False, False);
  DeclaraFila('q.tra', False, False, False);
  Liga('q.dur', 'ex.dur', 'k');  // duravel x duravel   -> grava
  Liga('q.tra', 'ex.dur', 'k');  // fila transiente     -> nao grava
  Liga('q.dur', 'ex.tra', 'k');  // exchange transiente -> nao grava
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'queue.bind:ex.dur->q.dur'),
      'duravel x duravel');
    Assert.AreEqual(0, Conta(L, 'queue.bind:ex.dur->q.tra'), 'fila transiente');
    Assert.AreEqual(0, Conta(L, 'queue.bind:ex.tra->q.dur'),
      'exchange transiente');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.ArgumentosDaFila_SobrevivemNoRegistro;
var
  L: TStringList;
  LArgs: TAMQPFieldTable;
begin
  LArgs := TAMQPFieldTable.Create;
  try
    LArgs.Put('x-message-ttl', TValue.From<Integer>(60000));
    // A posse da tabela e' de QUEM CHAMA -- o cliente so' a le' para montar o
    // frame. Foi o heaptrc que cobrou: a primeira versao deste teste vazava
    // dois blocos, e o vazamento era do teste, nao do codigo.
    DeclaraFila('q.args', True, False, False, LArgs);
  finally
    LArgs.Free;
  end;
  L := FechaELeWal;
  try
    Assert.AreEqual(1,
      Conta(L, 'queue.declare:q.args:dur=True:ad=False:args=1'),
      'registro com argumentos');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.DeleteDeFilaDuravel_EhGravado;
var
  L: TStringList;
  LDel: TAMQPQueueDelete;
begin
  DeclaraFila('q.dur', True, False, False);
  LDel.QueueName := 'q.dur';
  LDel.IfUnused := False;
  LDel.IfEmpty := False;
  LDel.NoWait := False;
  FChan.DeleteQueue(LDel);
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'queue.delete:q.dur'),
      'o sumico foi registrado');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.AutoDeleteDeFilaDuravel_EhGravado;
var
  L: TStringList;
  LTag: string;
begin
  // Caminho AUTOMATICO: ninguem chamou Queue.Delete. Sem registro, a
  // recuperacao ressuscitaria uma fila que o broker ja' tinha recolhido.
  DeclaraFila('q.ad', True, False, True);
  LTag := FChan.Consume('q.ad', nil);
  Sleep(150);
  FChan.Cancel(LTag);
  Sleep(400);
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'queue.delete:q.ad'),
      'auto-delete de fila duravel registrado');
  finally
    L.Free;
  end;
end;

procedure TTopoPersistTests.AutoDeleteDeExchangeDuravel_EhGravado;
var
  L: TStringList;
  LTag: string;
begin
  // O exchange auto-delete perde o ultimo binding quando a fila some. Este
  // caminho passava pelo vhost DIRETO e nao gravava nada -- foi o buraco que a
  // WS3 achou ao seguir o rastro do auto-delete.
  DeclaraExchange('ex.ad', 'direct', True, True);
  DeclaraFila('q.ad', True, False, True);
  Liga('q.ad', 'ex.ad', 'k');
  LTag := FChan.Consume('q.ad', nil);
  Sleep(150);
  FChan.Cancel(LTag);
  Sleep(400);
  L := FechaELeWal;
  try
    Assert.AreEqual(1, Conta(L, 'exchange.delete:ex.ad'),
      'auto-delete de exchange duravel registrado');
  finally
    L.Free;
  end;
end;

{ TTopoSemDataDirTests }

procedure TTopoSemDataDirTests.SemDataDir_NaoHaJournal;
var
  B: TAMQPServer;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;
    Assert.IsTrue(B.Journal = nil, 'DataDir vazio nao cria journal');
    B.Stop;
  finally
    B.Free;
  end;
end;

procedure TTopoSemDataDirTests.SemDataDir_NenhumArquivoEscrito;
var
  B: TAMQPServer;
  C: TAMQPConnection;
  Ch: TAMQPChannel;
  LParams: TAMQPConnectionParams;
  LDir: string;
  LDecl: TAMQPQueueDeclare;
begin
  // D19: com DataDir vazio o broker e' EXATAMENTE o da Fase 3. Declarar
  // topologia duravel continua funcionando e nao escreve nada.
  LDir := DirNovo('amqpsemdd');
  ForceDirectories(LDir);
  LimpaDir(LDir);
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;
    LParams := TAMQPConnectionParams.Localhost;
    LParams.Host := '127.0.0.1';
    LParams.Port := B.Port;
    C := TAMQPConnection.Create(LParams);
    try
      C.Open;
      Ch := C.CreateChannel;
      LDecl := TAMQPQueueDeclare.Create('q.dur', True);
      Ch.DeclareQueue(LDecl);
      C.Close;
    finally
      C.Free;
    end;
    B.Stop;
  finally
    B.Free;
  end;
  Assert.AreEqual(0, ContaArquivos(LDir), 'nenhum arquivo no diretorio');
end;

procedure TTopoSemDataDirTests.DataDir_NaoMudaComBrokerNoAr;
var
  B: TAMQPServer;
  LLevantou: Boolean;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;
    LLevantou := False;
    try
      B.DataDir := DirNovo('amqpnaomuda');
    except
      on E: Exception do
        LLevantou := True;
    end;
    Assert.IsTrue(LLevantou,
      'trocar DataDir com o broker no ar tem de levantar');
    B.Stop;
  finally
    B.Free;
  end;
end;

{ TTopoFalhaTests }

procedure TTopoFalhaTests.JournalParado_DeclareDuravel_DaSemDurabilidade;
var
  LEngine: TAMQPEngine;
  LJournal: TAMQPJournal;
  LDir: string;
  LMsg, LCons: Integer;
begin
  // Journal que nao aceita mais submit e' a mesma situacao de journal em falha
  // do ponto de vista de quem declara: nao ha como registrar, e responder
  // Declare-Ok seria mentir sobre durabilidade.
  LDir := DirNovo('amqpfalha');
  LEngine := TAMQPEngine.Create;
  LJournal := TAMQPJournal.Create(LDir);
  try
    LJournal.Start;
    LEngine.Journal := LJournal;
    LJournal.Stop;
    Assert.IsTrue(
      LEngine.DeclareQueue('/', 'q.dur', False, True, False, False, nil, 0,
        LMsg, LCons) = amqerSemDurabilidade,
      'declare duravel avisa que nao da');
  finally
    LEngine.Free;
    LJournal.Free;
    LimpaDir(LDir);
  end;
end;

procedure TTopoFalhaTests.JournalParado_DeclareTransiente_ContinuaFuncionando;
var
  LEngine: TAMQPEngine;
  LJournal: TAMQPJournal;
  LDir: string;
  LMsg, LCons: Integer;
begin
  // Quem nao pediu durabilidade nao pode ser punido por ela estar quebrada.
  LDir := DirNovo('amqpfalha');
  LEngine := TAMQPEngine.Create;
  LJournal := TAMQPJournal.Create(LDir);
  try
    LJournal.Start;
    LEngine.Journal := LJournal;
    LJournal.Stop;
    Assert.IsTrue(
      LEngine.DeclareQueue('/', 'q.tra', False, False, False, False, nil, 0,
        LMsg, LCons) = amqerOk,
      'declare transiente segue normal');
  finally
    LEngine.Free;
    LJournal.Free;
    LimpaDir(LDir);
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TTopoRecordTests);
  TDUnitX.RegisterTestFixture(TTopoPersistTests);
  TDUnitX.RegisterTestFixture(TTopoSemDataDirTests);
  TDUnitX.RegisterTestFixture(TTopoFalhaTests);

end.

unit AMQP.ServerSkeletonTests;

{ Testes do esqueleto do broker (WS6): virtual-hosts, TAMQPFrameWriter,
  TAMQPTcpListener e o ciclo accept/registro/reap do TAMQPServer. Os "clientes"
  aqui são sockets crus: só o protocol-header e frames soltos, sem handshake.
  A FSM de handshake é exercitada em AMQP.ServerHandshakeTests (WS4).

  Espelho DUnitX de tests\Server\fpc\AMQP.ServerSkeletonTests.pas — mantenha os
  dois em sincronia (mesmos nomes de teste, mesmas asserções). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  AMQP.Protocol,
  AMQP.Frame,
  AMQP.Threading,
  AMQP.Transport,
  AMQP.Server.FrameIO,
  AMQP.Server.Types,
  AMQP.Server.Broker;

type
  [TestFixture]
  TVHostRegistryTests = class
  public
    [Test] procedure Default_TemRaiz;
    [Test] procedure Add_Contains_Remove;
  end;

  [TestFixture]
  TFrameWriterTests = class
  public
    [Test] procedure OrdemFIFO;
    [Test] procedure PostFrames_Atomico;
    [Test] procedure FalhaDeEscrita_MarcaFailed;
  end;

  [TestFixture]
  TListenerTests = class
  public
    [Test] procedure PortaZero_DevolveEfetiva;
    [Test] procedure Accept_RecebeConexaoEDados;
    [Test] procedure Close_DesbloqueiaAccept;
  end;

  [TestFixture]
  TServerSkeletonTests = class
  public
    [Test] procedure StartStop_Idempotente;
    [Test] procedure ConexaoRegistraEReap;
    [Test] procedure ProtocolHeaderInvalido_Fecha;
    [Test] procedure VariasConexoesConcorrentes;
    [Test] procedure StopComConexoesAbertas_NaoTrava;
    [Test] procedure HeartbeatEcoado;
  end;

implementation

// --- helpers --------------------------------------------------------------

function MkFrame(AType: Byte; ACh: Word; APayloadByte: Byte): TAMQPFrame;
var
  P: TBytes;
begin
  SetLength(P, 1);
  P[0] := APayloadByte;
  Result := TAMQPFrame.Create(AType, ACh, P);
end;

// Espera ACount conexões vivas por até ATimeoutMs.
function WaitConnCount(AServer: TAMQPServer; ACount, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  while AmqpTickMs < LDeadline do
  begin
    if AServer.ConnectionCount = ACount then
      Exit(True);
    TThread.Sleep(10);
  end;
  Result := AServer.ConnectionCount = ACount;
end;

// Espera o total de aceitas chegar a pelo menos AN.
function WaitAccepted(AServer: TAMQPServer; AN, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + UInt64(ATimeoutMs);
  while AmqpTickMs < LDeadline do
  begin
    if AServer.TotalAccepted >= AN then
      Exit(True);
    TThread.Sleep(10);
  end;
  Result := AServer.TotalAccepted >= AN;
end;

type
  { Stream de teste que registra os bytes escritos. }
  TRecStream = class(TStream)
  public
    Log: TBytes;
    LogLen: Integer;
    FailAfterCalls: Integer;
    Calls: Integer;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Read(var Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
  end;

function TRecStream.Read(var Buffer; Count: Longint): Longint;
begin
  Result := 0;
end;

function TRecStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  Result := 0;
end;

function TRecStream.Write(const Buffer; Count: Longint): Longint;
var
  P: PByte;
  I: Integer;
begin
  Inc(Calls);
  if (FailAfterCalls > 0) and (Calls > FailAfterCalls) then
    raise EAMQPTransport.Create('stream morto (teste)');
  if LogLen + Count > Length(Log) then
    SetLength(Log, (LogLen + Count) * 2 + 64);
  P := @Buffer;
  for I := 0 to Count - 1 do
  begin
    Log[LogLen] := P[I];
    Inc(LogLen);
  end;
  Result := Count;
end;

type
  TBatchProd = class(TThread)
  public
    W: TAMQPFrameWriter;
    Tag: Byte;
    Reps: Integer;
    procedure Execute; override;
  end;

procedure TBatchProd.Execute;
var
  I: Integer;
  B: array[0..2] of TAMQPFrame;
begin
  for I := 1 to Reps do
  begin
    B[0] := MkFrame(AMQP_FRAME_METHOD, 1, Tag);
    B[1] := MkFrame(AMQP_FRAME_HEADER, 1, Tag);
    B[2] := MkFrame(AMQP_FRAME_BODY, 1, Tag);
    W.PostFrames(B);
  end;
end;

type
  { Sonda: chama Accept uma vez numa thread e guarda o resultado. }
  TAMQPAcceptProbe = class(TThread)
  public
    Listener: TAMQPTcpListener;
    Accepted: TAMQPTcpSocket;
    GotNil: Boolean;
    constructor Create(AListener: TAMQPTcpListener);
    procedure Execute; override;
  end;

constructor TAMQPAcceptProbe.Create(AListener: TAMQPTcpListener);
begin
  Listener := AListener;
  inherited Create(False);
end;

procedure TAMQPAcceptProbe.Execute;
begin
  Accepted := Listener.Accept;
  GotNil := Accepted = nil;
end;

// Cliente cru: abre socket, manda o protocol-header (opcionalmente errado).
function RawConnect(APort: Word; ASendHeader: Boolean;
  ABadHeader: Boolean = False): TAMQPTcpSocket;
var
  LHdr: array[0..7] of Byte;
  I: Integer;
begin
  Result := TAMQPTcpSocket.Create;
  Result.Connect('127.0.0.1', APort);
  if ASendHeader then
  begin
    for I := 0 to 7 do
      LHdr[I] := AMQP_PROTOCOL_HEADER[I];
    if ABadHeader then
      LHdr[5] := 99;
    Result.Send(LHdr[0], 8);
  end;
end;

{ TVHostRegistryTests }

procedure TVHostRegistryTests.Default_TemRaiz;
var
  R: TAMQPVirtualHostRegistry;
begin
  R := TAMQPVirtualHostRegistry.Create;
  try
    Assert.IsTrue(R.Contains('/'), 'raiz presente por default');
    Assert.IsFalse(R.Contains('/inexistente'));
  finally
    R.Free;
  end;
end;

procedure TVHostRegistryTests.Add_Contains_Remove;
var
  R: TAMQPVirtualHostRegistry;
begin
  R := TAMQPVirtualHostRegistry.Create;
  try
    R.Add('/app');
    Assert.IsTrue(R.Contains('/app'));
    R.Add('/app'); // idempotente
    Assert.AreEqual(2, Length(R.ToArray));
    R.Remove('/app');
    Assert.IsFalse(R.Contains('/app'));
    Assert.IsTrue(R.Contains('/'), 'raiz intacta');
  finally
    R.Free;
  end;
end;

{ TFrameWriterTests }

procedure TFrameWriterTests.OrdemFIFO;
var
  S: TRecStream;
  W: TAMQPFrameWriter;
  I, LPos, Seen: Integer;
  F: TAMQPFrame;
begin
  S := TRecStream.Create;
  try
    W := TAMQPFrameWriter.Create(S, 8);
    for I := 1 to 20 do
    begin
      F := MkFrame(AMQP_FRAME_METHOD, 1, I);
      W.PostFrame(F);
    end;
    W.Stop;
    W.Free;
    // cada frame: 7 header + 1 payload + 1 frame-end = 9 bytes; payload em +7
    Seen := 0;
    LPos := 0;
    while LPos + 9 <= S.LogLen do
    begin
      Inc(Seen);
      Assert.AreEqual(Seen, Integer(S.Log[LPos + 7]),
        'ordem no frame ' + IntToStr(Seen));
      Inc(LPos, 9);
    end;
    Assert.AreEqual(20, Seen);
  finally
    S.Free;
  end;
end;

procedure TFrameWriterTests.PostFrames_Atomico;
var
  S: TRecStream;
  W: TAMQPFrameWriter;
  pA, pB: TBatchProd;
  LPos: Integer;
begin
  S := TRecStream.Create;
  try
    W := TAMQPFrameWriter.Create(S, 4); // fila rasa força backpressure
    pA := TBatchProd.Create(True); pA.W := W; pA.Tag := 111; pA.Reps := 40;
    pB := TBatchProd.Create(True); pB.W := W; pB.Tag := 222; pB.Reps := 40;
    pA.Start; pB.Start;
    pA.WaitFor; pB.WaitFor;
    pA.Free; pB.Free;
    W.Stop; W.Free;

    // 80 trincas de 27 bytes; em cada trinca os 3 payloads (idx +7, +16, +25)
    // têm de ser do MESMO tag.
    LPos := 0;
    while LPos + 27 <= S.LogLen do
    begin
      Assert.AreEqual(Integer(S.Log[LPos + 7]), Integer(S.Log[LPos + 16]),
        'trinca coesa (h)');
      Assert.AreEqual(Integer(S.Log[LPos + 7]), Integer(S.Log[LPos + 25]),
        'trinca coesa (b)');
      Inc(LPos, 27);
    end;
    Assert.AreEqual(80 * 27, S.LogLen, 'todos os 80 lotes sairam');
  finally
    S.Free;
  end;
end;

procedure TFrameWriterTests.FalhaDeEscrita_MarcaFailed;
var
  S: TRecStream;
  W: TAMQPFrameWriter;
  F: TAMQPFrame;
  I: Integer;
  LRaised: Boolean;
begin
  S := TRecStream.Create;
  try
    S.FailAfterCalls := 5;
    W := TAMQPFrameWriter.Create(S, 4);
    F := MkFrame(AMQP_FRAME_METHOD, 1, 1);
    LRaised := False;
    try
      for I := 1 to 400 do
      begin
        W.PostFrame(F);
        TThread.Sleep(1);
      end;
    except
      on EAMQPTransport do
        LRaised := True;
    end;
    Assert.IsTrue(LRaised or W.Failed, 'Post* deve levantar apos a falha');
    Assert.IsTrue(W.Failed, 'Failed=True');
    Assert.IsTrue(W.LastError <> '', 'LastError preenchido');
    W.Stop;
    W.Free;
  finally
    S.Free;
  end;
end;

{ TListenerTests }

procedure TListenerTests.PortaZero_DevolveEfetiva;
var
  L: TAMQPTcpListener;
begin
  L := TAMQPTcpListener.Create;
  try
    L.Listen('127.0.0.1', 0);
    Assert.IsTrue(L.Port > 0, 'porta efetiva > 0');
  finally
    L.Free;
  end;
end;

procedure TListenerTests.Accept_RecebeConexaoEDados;
var
  L: TAMQPTcpListener;
  Probe: TAMQPAcceptProbe;
  Cli: TAMQPTcpSocket;
  Buf: TBytes;
  N: Integer;
  S: string;
begin
  L := TAMQPTcpListener.Create;
  try
    L.Listen('127.0.0.1', 0);
    Probe := TAMQPAcceptProbe.Create(L);
    try
      TThread.Sleep(100);
      Cli := TAMQPTcpSocket.Create;
      try
        Cli.Connect('127.0.0.1', L.Port);
        S := 'oi';
        Cli.Send(S[1], 2);
        Probe.WaitFor;
        Assert.IsNotNull(Probe.Accepted, 'Accept devolveu um socket');
        Assert.IsTrue(Pos('127.0.0.1:', Probe.Accepted.PeerAddress) = 1,
          'peer preenchido');
        SetLength(Buf, 16);
        N := Probe.Accepted.Receive(Buf[0], 16);
        Assert.AreEqual(2, N);
        Probe.Accepted.Free;
      finally
        Cli.Free;
      end;
    finally
      Probe.Free;
    end;
  finally
    L.Free;
  end;
end;

procedure TListenerTests.Close_DesbloqueiaAccept;
var
  L: TAMQPTcpListener;
  Probe: TAMQPAcceptProbe;
begin
  L := TAMQPTcpListener.Create;
  try
    L.Listen('127.0.0.1', 0);
    Probe := TAMQPAcceptProbe.Create(L);
    try
      TThread.Sleep(100);
      L.Close;
      Probe.WaitFor;
      Assert.IsTrue(Probe.GotNil, 'Accept devolveu nil apos Close');
    finally
      Probe.Free;
    end;
  finally
    L.Free;
  end;
end;

{ TServerSkeletonTests }

procedure TServerSkeletonTests.StartStop_Idempotente;
var
  B: TAMQPServer;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;
    Assert.IsTrue(B.Running, 'rodando');
    Assert.IsTrue(B.Port > 0, 'porta efetiva');
    B.Start; // no-op
    B.Stop;
    Assert.IsFalse(B.Running);
    B.Stop; // no-op
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.ConexaoRegistraEReap;
var
  B: TAMQPServer;
  C: TAMQPTcpSocket;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;

    C := RawConnect(B.Port, True);
    Assert.IsTrue(WaitConnCount(B, 1, 2000), 'conexao registrada');
    Assert.AreEqual(1, B.TotalAccepted);

    C.Free; // desconecta
    Assert.IsTrue(WaitConnCount(B, 0, 2000), 'conexao reapada apos desconexao');

    B.Stop;
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.ProtocolHeaderInvalido_Fecha;
var
  B: TAMQPServer;
  C: TAMQPTcpSocket;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;

    C := RawConnect(B.Port, True, True); // header errado
    try
      Assert.IsTrue(WaitAccepted(B, 1, 2000), 'conexao foi aceita');
      // o servidor fecha sozinho -> a conexao some do registro
      Assert.IsTrue(WaitConnCount(B, 0, 2000), 'conexao fechada pelo servidor');
    finally
      C.Free;
    end;

    B.Stop;
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.VariasConexoesConcorrentes;
var
  B: TAMQPServer;
  Conns: array[0..9] of TAMQPTcpSocket;
  I: Integer;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;

    for I := 0 to High(Conns) do
      Conns[I] := RawConnect(B.Port, True);
    Assert.IsTrue(WaitConnCount(B, 10, 3000), '10 conexoes vivas');

    for I := 0 to High(Conns) do
      Conns[I].Free;
    Assert.IsTrue(WaitConnCount(B, 0, 3000), 'todas reapadas');
    Assert.AreEqual(10, B.TotalAccepted);

    B.Stop;
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.StopComConexoesAbertas_NaoTrava;
var
  B: TAMQPServer;
  Conns: array[0..4] of TAMQPTcpSocket;
  I: Integer;
  LStart: UInt64;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;
    for I := 0 to High(Conns) do
      Conns[I] := RawConnect(B.Port, True);
    Assert.IsTrue(WaitConnCount(B, 5, 3000));

    LStart := AmqpTickMs;
    B.Stop; // deve derrubar as 5 e voltar rapido
    Assert.IsTrue(AmqpTickMs - LStart < 5000, 'Stop nao travou');
    Assert.IsFalse(B.Running);

    for I := 0 to High(Conns) do
      Conns[I].Free;
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.HeartbeatEcoado;
var
  B: TAMQPServer;
  C: TAMQPTcpSocket;
  Strm: TAMQPSocketStream;
  HB: TAMQPFrame;
  Reply: TAMQPFrame;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;

    C := RawConnect(B.Port, True);
    try
      Strm := TAMQPSocketStream.Create(C);
      try
        // Desde o WS4 o broker manda Connection.Start assim que o
        // protocol-header e' aceito: drena esse frame antes.
        Reply := TAMQPFrame.ReadFrom(Strm, 131072);
        Assert.IsTrue(Reply.IsMethod, 'primeiro frame e Connection.Start');

        HB := TAMQPFrame.Heartbeat;
        HB.WriteTo(Strm);
        Reply := TAMQPFrame.ReadFrom(Strm, 4096);
        Assert.IsTrue(Reply.IsHeartbeat, 'resposta e heartbeat');
      finally
        Strm.Free;
      end;
    finally
      C.Free;
    end;

    B.Stop;
  finally
    B.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TVHostRegistryTests);
  TDUnitX.RegisterTestFixture(TFrameWriterTests);
  TDUnitX.RegisterTestFixture(TListenerTests);
  TDUnitX.RegisterTestFixture(TServerSkeletonTests);

end.

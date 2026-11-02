unit AMQP.ServerSkeletonTests;

{ Testes do esqueleto do broker (WS6): virtual-hosts, TAMQPFrameWriter,
  o listener do broker e o ciclo accept/registro/reap do TAMQPServer. Os "clientes"
  aqui são sockets crus: só o protocol-header e frames soltos, sem handshake.
  A FSM de handshake é exercitada em AMQP.ServerHandshakeTests (WS4). }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Classes,
  AMQP.Protocol,
  AMQP.Frame,
  PascalCommon.Threading,
  PascalCommon.Transport,
  PascalCommon.Transport.Tcp,
  AMQP.Transport,
  AMQP.Server.FrameIO,
  AMQP.Server.Types,
  AMQP.Server.Connection,
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

  { The broker's listener, which comes from pascal-common-faa since 0.6.0.
    The listener itself (accept, Close unblocking Accept, port 0) is tested
    there; these check what this library asks of it. }
  [TestFixture]
  TListenerTests = class
  public
    [Test] procedure PortaZero_BrokerInformaAEfetiva;
    [Test] procedure Accept_PeerPreenchido;
    [Test] procedure PortaOcupada_StartLevantaEAMQPTransport;
    [Test] procedure TcpOptions_NagleDesligado;
    [Test] procedure PedidoResposta_SemAtrasoDeNagle;
  end;

  [TestFixture]
  TServerSkeletonTests = class
  public
    [Test] procedure StartStop_Idempotente;
    [Test] procedure ConexaoRegistraEReap;
    [Test] procedure ProtocolHeaderInvalido_Fecha;
    [Test] procedure VariasConexoesConcorrentes;
    [Test] procedure StopComConexoesAbertas_NaoTrava;
    [Test] procedure HeartbeatRecebido_NaoDerrubaNemResponde;
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
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  while PcTickMs < LDeadline do
  begin
    if AServer.ConnectionCount = ACount then
      Exit(True);
    Sleep(10);
  end;
  Result := AServer.ConnectionCount = ACount;
end;

// Espera o total de aceitas chegar a pelo menos AN.
function WaitAccepted(AServer: TAMQPServer; AN, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  while PcTickMs < LDeadline do
  begin
    if AServer.TotalAccepted >= AN then
      Exit(True);
    Sleep(10);
  end;
  Result := AServer.TotalAccepted >= AN;
end;

// Stream de teste que registra os bytes escritos.
type
  TRecStream = class(TStream)
  public
    Log: TBytes;
    LogLen: Integer;
    FailAfterCalls: Integer;
    Calls: Integer;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Read(var Buffer; Count: Longint): Longint; override;
    function Seek(const AOffset: Int64; AOrigin: TSeekOrigin): Int64; override;
  end;

function TRecStream.Read(var Buffer; Count: Longint): Longint;
begin
  Result := 0;
end;

function TRecStream.Seek(const AOffset: Int64; AOrigin: TSeekOrigin): Int64;
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
  { Peer of the Nagle test: accepts one connection and, ACount times, reads
    two bytes and answers with two separate one-byte writes. }
  TNagleEcho = class(TThread)
  public
    Listener: TPcListener;
    Count: Integer;
    Error: string;
    constructor Create(AListener: TPcListener; ACount: Integer);
    procedure Execute; override;
  end;

constructor TNagleEcho.Create(AListener: TPcListener; ACount: Integer);
begin
  Listener := AListener;
  Count := ACount;
  inherited Create(False);
end;

procedure ReadExactly(AEndpoint: TPcEndpoint; var ABuffer; ACount: Integer);
var
  LDone: Integer;
  P: PByte;
begin
  P := @ABuffer;
  LDone := 0;
  while LDone < ACount do
    Inc(LDone, AEndpoint.Read(P[LDone], ACount - LDone));
end;

procedure TNagleEcho.Execute;
var
  E: TPcEndpoint;
  I: Integer;
  B: array[0..1] of Byte;
begin
  try
    E := Listener.Accept;
    try
      for I := 1 to Count do
      begin
        ReadExactly(E, B[0], 2);
        E.WriteExactly(B[0], 1);
        E.WriteExactly(B[1], 1);
      end;
    finally
      E.Free;
    end;
  except
    on X: Exception do
      Error := X.ClassName + ': ' + X.Message;
  end;
end;

// Cliente cru: abre socket, manda o protocol-header (opcionalmente errado).
function RawConnect(APort: Word; ASendHeader: Boolean;
  ABadHeader: Boolean = False): TPcEndpoint;
var
  LHdr: array[0..7] of Byte;
  I: Integer;
begin
  Result := PcTcpConnect('127.0.0.1', APort, AmqpTcpOptions);
  if ASendHeader then
  begin
    for I := 0 to 7 do
      LHdr[I] := AMQP_PROTOCOL_HEADER[I];
    if ABadHeader then
      LHdr[5] := 99;
    Result.WriteExactly(LHdr[0], 8);
  end;
end;

{ TVHostRegistryTests }

procedure TVHostRegistryTests.Default_TemRaiz;
var
  R: TAMQPVirtualHostRegistry;
begin
  R := TAMQPVirtualHostRegistry.Create;
  try
    TAssert.AssertTrue('raiz presente por default', R.Contains('/'));
    TAssert.AssertFalse(R.Contains('/inexistente'));
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
    TAssert.AssertTrue(R.Contains('/app'));
    R.Add('/app'); // idempotente
    TAssert.AssertEquals(2, Length(R.ToArray));
    R.Remove('/app');
    TAssert.AssertFalse(R.Contains('/app'));
    TAssert.AssertTrue('raiz intacta', R.Contains('/'));
  finally
    R.Free;
  end;
end;

{ TFrameWriterTests }

procedure TFrameWriterTests.OrdemFIFO;
var
  S: TRecStream;
  W: TAMQPFrameWriter;
  I, Pos, Seen: Integer;
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
    Pos := 0;
    while Pos + 9 <= S.LogLen do
    begin
      Inc(Seen);
      TAssert.AssertEquals('ordem no frame ' + IntToStr(Seen), Seen, Integer(S.Log[Pos + 7]));
      Inc(Pos, 9);
    end;
    TAssert.AssertEquals(20, Seen);
  finally
    S.Free;
  end;
end;

procedure TFrameWriterTests.PostFrames_Atomico;
var
  S: TRecStream;
  W: TAMQPFrameWriter;
  pA, pB: TBatchProd;
  Pos: Integer;
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
    Pos := 0;
    while Pos + 27 <= S.LogLen do
    begin
      TAssert.AssertEquals('trinca coesa (h)', Integer(S.Log[Pos + 7]), Integer(S.Log[Pos + 16]));
      TAssert.AssertEquals('trinca coesa (b)', Integer(S.Log[Pos + 7]), Integer(S.Log[Pos + 25]));
      Inc(Pos, 27);
    end;
    TAssert.AssertEquals('todos os 80 lotes sairam', 80 * 27, S.LogLen);
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
        Sleep(1);
      end;
    except
      on EAMQPTransport do
        LRaised := True;
    end;
    TAssert.AssertTrue('Post* deve levantar apos a falha', LRaised or W.Failed);
    TAssert.AssertTrue('Failed=True', W.Failed);
    TAssert.AssertTrue('LastError preenchido', W.LastError <> '');
    W.Stop;
    W.Free;
  finally
    S.Free;
  end;
end;

{ TListenerTests }

procedure TListenerTests.PortaZero_BrokerInformaAEfetiva;
var
  B: TAMQPServer;
  C: TPcEndpoint;
begin
  // T1 of the common's transport-design: a listener on port 0 reports the
  // port it got, and the suites start every broker that way.
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;
    TAssert.AssertTrue('porta efetiva > 0', B.Port > 0);
    C := RawConnect(B.Port, True);
    try
      TAssert.AssertTrue('cliente chegou pela porta informada',
        WaitConnCount(B, 1, 2000));
    finally
      C.Free;
    end;
    B.Stop;
  finally
    B.Free;
  end;
end;

procedure TListenerTests.Accept_PeerPreenchido;
var
  B: TAMQPServer;
  C: TPcEndpoint;
  Conns: TArray<TAMQPServerConnection>;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;
    C := RawConnect(B.Port, True);
    try
      TAssert.AssertTrue(WaitConnCount(B, 1, 2000));
      Conns := B.Connections;
      TAssert.AssertEquals(1, Length(Conns));
      // The authenticator receives it (APeerAddress), so it can't go empty.
      TAssert.AssertTrue('peer preenchido: ' + Conns[0].Peer,
        Pos('127.0.0.1:', Conns[0].Peer) = 1);
    finally
      C.Free;
    end;
    B.Stop;
  finally
    B.Free;
  end;
end;

procedure TListenerTests.PortaOcupada_StartLevantaEAMQPTransport;
var
  A, B: TAMQPServer;
  LRaised: string;
begin
  A := TAMQPServer.Create;
  B := TAMQPServer.Create;
  try
    A.BindAddress := '127.0.0.1';
    A.Port := 0;
    A.Start;
    B.BindAddress := '127.0.0.1';
    B.Port := A.Port;
    LRaised := '';
    try
      B.Start;
    except
      on E: Exception do
        LRaised := E.ClassName;
    end;
    // T6: the library's class, never the common's EPcTransport.
    TAssert.AssertEquals('classe da excecao', 'EAMQPTransport', LRaised);
    TAssert.AssertFalse('broker que nao escutou fica parado', B.Running);
    // ...and a later Start on a free port works (the failed one undid itself).
    B.Port := 0;
    B.Start;
    TAssert.AssertTrue(B.Running);
    B.Stop;
    A.Stop;
  finally
    B.Free;
    A.Free;
  end;
end;

procedure TListenerTests.TcpOptions_NagleDesligado;
var
  O: TPcTcpOptions;
begin
  // Every socket the library opens (client and accepted) gets these
  // options. Nagle on costs ~40 ms per round trip on Linux: the server suite
  // went from 50 s to 13 min without TCP_NODELAY (CLAUDE.md, gotchas).
  O := AmqpTcpOptions;
  TAssert.AssertFalse('TCP_NODELAY (Nagle desligado)', O.Nagle);
  TAssert.AssertEquals('sem keep-alive: o AMQP tem heartbeat', 0,
    Integer(O.KeepAliveSeconds));
  TAssert.AssertEquals('connect: o padrao da common', 0,
    Integer(O.ConnectTimeoutMs));
  TAssert.AssertEquals('connect: o valor pedido', 1234,
    Integer(AmqpTcpOptions(1234).ConnectTimeoutMs));
end;

procedure TListenerTests.PedidoResposta_SemAtrasoDeNagle;
const
  ROUNDS = 20;
var
  L: TPcListener;
  Echo: TNagleEcho;
  C: TPcEndpoint;
  I: Integer;
  B: array[0..1] of Byte;
  LStart, LElapsed: UInt64;
begin
  // Measures what TcpOptions_NagleDesligado only states. Each side writes
  // two small pieces and then waits for the other's two: with Nagle on, the
  // second piece waits for the ACK of the first, and the peer delays that
  // ACK (it has nothing to send yet) -- ~40 ms per round on Linux, so 20
  // rounds take ~1 s or more. With TCP_NODELAY on both sockets, a few ms.
  // Windows' loopback hides Nagle, so there the test passes either way;
  // Linux (tools/test_fpc_docker.sh) is where it guards.
  LElapsed := 0;
  L := PcTcpListen('127.0.0.1', 0, AmqpTcpOptions);
  try
    Echo := TNagleEcho.Create(L, ROUNDS);
    try
      C := PcTcpConnect('127.0.0.1', L.Port, AmqpTcpOptions);
      try
        B[0] := 1;
        B[1] := 2;
        LStart := PcTickMs;
        for I := 1 to ROUNDS do
        begin
          C.WriteExactly(B[0], 1);
          C.WriteExactly(B[1], 1);
          ReadExactly(C, B[0], 2);
        end;
        LElapsed := PcTickMs - LStart;
      finally
        C.Free;
      end;
      Echo.WaitFor;
      TAssert.AssertEquals('eco sem erro', '', Echo.Error);
    finally
      Echo.Free;
    end;
  finally
    L.Free;
  end;
  TAssert.AssertTrue(Format('%d rodadas em %d ms (Nagle ligado: ~40 ms cada)',
    [ROUNDS, Integer(LElapsed)]), LElapsed < 400);
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
    TAssert.AssertTrue('rodando', B.Running);
    TAssert.AssertTrue('porta efetiva', B.Port > 0);
    B.Start; // no-op
    B.Stop;
    TAssert.AssertFalse(B.Running);
    B.Stop; // no-op
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.ConexaoRegistraEReap;
var
  B: TAMQPServer;
  C: TPcEndpoint;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;

    C := RawConnect(B.Port, True);
    TAssert.AssertTrue('conexao registrada', WaitConnCount(B, 1, 2000));
    TAssert.AssertEquals(1, B.TotalAccepted);

    C.Free; // desconecta
    TAssert.AssertTrue('conexao reapada apos desconexao', WaitConnCount(B, 0, 2000));

    B.Stop;
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.ProtocolHeaderInvalido_Fecha;
var
  B: TAMQPServer;
  C: TPcEndpoint;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;

    C := RawConnect(B.Port, True, True); // header errado
    try
      TAssert.AssertTrue('conexao foi aceita', WaitAccepted(B, 1, 2000));
      // o servidor fecha sozinho -> a conexao some do registro
      TAssert.AssertTrue('conexao fechada pelo servidor', WaitConnCount(B, 0, 2000));
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
  Conns: array[0..9] of TPcEndpoint;
  I: Integer;
begin
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;

    for I := 0 to High(Conns) do
      Conns[I] := RawConnect(B.Port, True);
    TAssert.AssertTrue('10 conexoes vivas', WaitConnCount(B, 10, 3000));

    for I := 0 to High(Conns) do
      Conns[I].Free;
    TAssert.AssertTrue('todas reapadas', WaitConnCount(B, 0, 3000));
    TAssert.AssertEquals(10, B.TotalAccepted);

    B.Stop;
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.StopComConexoesAbertas_NaoTrava;
var
  B: TAMQPServer;
  Conns: array[0..4] of TPcEndpoint;
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
    TAssert.AssertTrue(WaitConnCount(B, 5, 3000));

    LStart := PcTickMs;
    B.Stop; // deve derrubar as 5 e voltar rapido
    TAssert.AssertTrue('Stop nao travou', PcTickMs - LStart < 5000);
    TAssert.AssertFalse(B.Running);

    for I := 0 to High(Conns) do
      Conns[I].Free;
  finally
    B.Free;
  end;
end;

procedure TServerSkeletonTests.HeartbeatRecebido_NaoDerrubaNemResponde;
var
  B: TAMQPServer;
  C: TPcEndpoint;
  Strm: TAMQPEndpointStream;
  HB: TAMQPFrame;
  Reply: TAMQPFrame;
  Conns: TArray<TAMQPServerConnection>;
  Deadline: UInt64;
begin
  // Heartbeat NAO se ecoa (spec 4.2.7): cada lado emite pelo proprio timer.
  // Receber um so conta como sinal de vida. Aqui a conexao nem chegou ao
  // Tune-Ok, entao o broker tambem nao tem intervalo para emitir -- o teste
  // verifica so que o frame e' aceito e nao derruba nada. A emissao de verdade
  // esta em THeartbeatTests (AMQP.ServerHandshakeTests).
  B := TAMQPServer.Create;
  try
    B.BindAddress := '127.0.0.1';
    B.Port := 0;
    B.Start;

    C := RawConnect(B.Port, True);
    try
      Strm := TAMQPEndpointStream.Create(C);
      try
        // Desde o WS4 o broker manda Connection.Start assim que o
        // protocol-header e' aceito: drena esse frame antes.
        Reply := TAMQPFrame.ReadFrom(Strm, 131072);
        TAssert.AssertTrue('primeiro frame e Connection.Start', Reply.IsMethod);

        HB := TAMQPFrame.Heartbeat;
        HB.WriteTo(Strm);

        // O broker contabiliza o frame recebido e segue vivo.
        Deadline := PcTickMs + 2000;
        Conns := B.Connections;
        while (Length(Conns) = 1) and (Conns[0].FramesRead < 1) and
              (PcTickMs < Deadline) do
        begin
          Sleep(10);
          Conns := B.Connections;
        end;
        TAssert.AssertEquals('conexao viva', 1, Length(Conns));
        TAssert.AssertTrue('heartbeat contabilizado', Conns[0].FramesRead >= 1);
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

unit AMQP.Server.Broker;

{$I amqp.inc}

{ O broker: ciclo de vida do listener, loop de accept e registro de conexões
  (sub-módulo server).

  Start liga o listener e uma thread de accept que cria uma
  TAMQPServerConnection por conexão, entregando a cada uma a config corrente
  (autenticador, autorizador, vhosts e os limites do Tune); Stop derruba tudo
  sem leak. A FSM de handshake e o mapa de canais vivem na conexão (WS4); a
  engine de roteamento (WS5/Fase 2) e os heartbeats (WS7) vêm depois.

  Uso:
    B := TAMQPServer.Create;
    B.BindAddress := '0.0.0.0';
    B.Port := 5672;
    // B.Authenticator := TAMQPStaticAuthenticator.Create(['admin','s3cr3t']);
    B.VirtualHosts.Add('/app');
    B.Start;
    ...
    B.Stop;  // idempotente; Destroy também para
}

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Threading,
  AMQP.Transport,
  AMQP.Server.Auth,
  AMQP.Server.Types,
  AMQP.Server.Engine,
  AMQP.Server.Journal,
  AMQP.Server.Confirm,
  AMQP.Server.Recovery,
  AMQP.Server.Connection;

// TAMQPVirtualHostRegistry mudou-se para AMQP.Server.Types no WS4 (a FSM de
// handshake precisa consultá-lo, e ela não pode usar esta unit — que a usa).
// Quem mexe em vhosts precisa listar AMQP.Server.Types no próprio uses.

type
  TAMQPServer = class;

  TAMQPAcceptThread = class(TThread)
  private
    FServer: TAMQPServer;
  protected
    procedure Execute; override;
  public
    constructor Create(AServer: TAMQPServer);
  end;

  { Thread monitora: uma só para o broker inteiro (não uma por conexão). A cada
    AMQP_SERVER_MONITOR_TICK_MS percorre as conexões vivas aplicando o passo de
    heartbeat e o prazo do Connection.Close-Ok, e recolhe as mortas. É a ÚNICA
    thread que libera conexões enquanto o broker roda — por isso o snapshot que
    ela tira não pode ser liberado debaixo dela. }
  TAMQPMonitorThread = class(TThread)
  private
    FServer: TAMQPServer;
  protected
    procedure Execute; override;
  public
    constructor Create(AServer: TAMQPServer);
  end;

  TAMQPServer = class
  private
    FListener: TAMQPTcpListener;
    FAccept: TAMQPAcceptThread;
    FMonitor: TAMQPMonitorThread;
    FMonitorStop: TEvent;
    FLock: TCriticalSection;
    FActive: TList<TAMQPServerConnection>;
    FDead: TList<TAMQPServerConnection>;
    FAuth: IAMQPAuthenticator;
    FAuthorizer: IAMQPAuthorizer;
    FSink: IAMQPMessageSink;
    FEngine: TAMQPEngine; // WS5: a engine de verdade; tambem e' o Sink
    FJournal: TAMQPJournal; // Fase 4: nil enquanto DataDir estiver vazio
    // Onde os canais penduram confirms adiados. Existe junto com o journal e
    // pelo mesmo motivo: sem DataDir nao ha o que esperar, e o confirm sai
    // inline como na Fase 3.
    FConfirms: IAMQPConfirmRegistry;
    // O que a ultima recuperacao encontrou. Zerado enquanto DataDir estiver
    // vazio. Existe pela mesma razao das contagens do journal: um broker que
    // subiu com o log truncado tem de conseguir DIZER isso.
    FRecoveryStats: TAMQPRecoveryStats;
    FMaxJournalBytes: Int64;
    FDataDir: string;
    FMaxQueueLength: Integer;
    FVHosts: TAMQPVirtualHostRegistry;
    FBindAddress: string;
    FPort: Word;
    FBacklog: Integer;
    FChannelMax: Word;
    FFrameMax: Cardinal;
    FHeartbeat: Word;
    FCloseTimeoutMs: Cardinal;
    FUseTls: Boolean;
    FTlsCertFile: string;
    FTlsKeyFile: string;
    FRunning: Boolean;
    FStopping: Boolean;
    FTotalAccepted: Integer; // atômico
    function ConnConfig: TAMQPServerConnConfig;
    procedure AcceptLoop;
    procedure MonitorLoop;
    procedure StopMonitor;
    procedure HandleConnClosed(AConn: TAMQPServerConnection);
    procedure ReapDead;
    procedure SetMaxQueueLength(AValue: Integer);
    procedure SetDataDir(const AValue: string);
  public
    constructor Create;
    destructor Destroy; override;

    procedure Start;
    procedure Stop;

    /// Nº de conexões vivas neste momento.
    function ConnectionCount: Integer;
    /// Snapshot das conexões vivas. Os objetos podem ser reapados logo em
    /// seguida — use só para inspeção imediata (log, testes, métricas).
    function Connections: TArray<TAMQPServerConnection>;
    /// Total de conexões aceitas desde o Start (inclui as já fechadas).
    property TotalAccepted: Integer read FTotalAccepted;

    /// A engine de roteamento (Fase 2). Criada pelo broker e usada como
    /// MessageSink; exposta para testes e diagnóstico.
    property Engine: TAMQPEngine read FEngine;
    /// Journal de durabilidade, ou nil quando DataDir esta' vazio. Existe so'
    /// entre o Start e o Stop.
    property Journal: TAMQPJournal read FJournal;
    /// Registro de confirms adiados, ou nil quando DataDir esta' vazio.
    /// Existe pela mesma razao do Journal acima: diagnostico e teste.
    property Confirms: IAMQPConfirmRegistry read FConfirms;
    /// O que o replay do WAL encontrou no ultimo Start. Tudo zero quando
    /// DataDir esta' vazio ou o log estava vazio.
    property RecoveryStats: TAMQPRecoveryStats read FRecoveryStats;

    /// Diretorio de dados. VAZIO (default) = broker exatamente igual ao da
    /// Fase 3: `durable` e `delivery-mode 2` continuam aceitos e ignorados, e
    /// nada e' escrito em disco. Preenchido = durabilidade ligada (decisao
    /// D19).
    ///
    /// E' opt-in pela mesma razao do UseTls: o broker roda DENTRO da app do
    /// usuario, e ninguem deve ganhar arquivos em disco por ter chamado Start.
    /// So' pode mudar com o broker parado.
    property DataDir: string read FDataDir write SetDataDir;
    /// Teto duro do WAL em bytes. 0 (default) = ilimitado, a forma do
    /// MaxQueueLength. Ao estourar, o publish PERSISTENTE leva Basic.Nack --
    /// teto de memoria descarta, teto de disco recusa. Aplicado a partir do
    /// proximo Start.
    property MaxJournalBytes: Int64 read FMaxJournalBytes
      write FMaxJournalBytes;
    /// Teto de mensagens prontas POR FILA (decisão D7 do CLAUDE.md).
    /// 0 (default) = ilimitado. O broker roda dentro da app do usuário: uma
    /// fila sem consumidor não pode derrubar o processo hospedeiro. Vale para
    /// as filas criadas depois de atribuído. `x-max-length` por fila é Fase 3.
    property MaxQueueLength: Integer read FMaxQueueLength
      write SetMaxQueueLength;

    property BindAddress: string read FBindAddress write FBindAddress;
    /// Porta a ligar; 0 = o SO escolhe. Depois do Start, reflete a efetiva.
    property Port: Word read FPort write FPort;
    property Backlog: Integer read FBacklog write FBacklog;
    property Running: Boolean read FRunning;

    // --- limites propostos no Connection.Tune (0 = sem limite). Lidos a cada
    //     conexão aceita, que recebe uma cópia da config.
    property ChannelMax: Word read FChannelMax write FChannelMax;
    property FrameMax: Cardinal read FFrameMax write FFrameMax;
    /// Heartbeat proposto no Tune. Quem decide é o cliente no Tune-Ok (spec):
    /// o broker adota o valor que ele devolver. 0 = sem heartbeat.
    property Heartbeat: Word read FHeartbeat write FHeartbeat;
    /// Quanto esperar pelo Connection.Close-Ok antes de derrubar o socket.
    property CloseTimeoutMs: Cardinal read FCloseTimeoutMs write FCloseTimeoutMs;

    // --- TLS (WS3). Exige um build com -dAMQP_OPENSSL: o backend SChannel
    //     desta lib só implementa o lado cliente. Com UseTls=True cada conexão
    //     aceita faz o handshake TLS na PRÓPRIA thread, antes do
    //     protocol-header do AMQP -- é a porta 5671 "AMQPS", não STARTTLS.
    /// Liga o TLS. Sem CertFile/KeyFile válidos a conexão morre no handshake.
    property UseTls: Boolean read FUseTls write FUseTls;
    /// Cadeia de certificados em PEM (o do servidor primeiro).
    property TlsCertFile: string read FTlsCertFile write FTlsCertFile;
    /// Chave privada em PEM, sem senha.
    property TlsKeyFile: string read FTlsKeyFile write FTlsKeyFile;

    /// Default: TAMQPStaticAuthenticator com guest/guest. Só antes do Start.
    property Authenticator: IAMQPAuthenticator read FAuth write FAuth;
    property Authorizer: IAMQPAuthorizer read FAuthorizer write FAuthorizer;
    /// Para onde vão as mensagens publicadas. Default: TAMQPNullMessageSink,
    /// que descarta — é o que faz da Fase 1 um "null broker". A engine de
    /// roteamento da Fase 2 entra por aqui.
    property MessageSink: IAMQPMessageSink read FSink write FSink;
    property VirtualHosts: TAMQPVirtualHostRegistry read FVHosts;
  end;

implementation

{ TAMQPAcceptThread }

constructor TAMQPAcceptThread.Create(AServer: TAMQPServer);
begin
  FServer := AServer;
  inherited Create(True);
end;

procedure TAMQPAcceptThread.Execute;
begin
  FServer.AcceptLoop;
end;

{ TAMQPMonitorThread }

constructor TAMQPMonitorThread.Create(AServer: TAMQPServer);
begin
  FServer := AServer;
  inherited Create(True);
end;

procedure TAMQPMonitorThread.Execute;
begin
  FServer.MonitorLoop;
end;

{ TAMQPServer }

constructor TAMQPServer.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FActive := TList<TAMQPServerConnection>.Create;
  FDead := TList<TAMQPServerConnection>.Create;
  FVHosts := TAMQPVirtualHostRegistry.Create;
  FAuth := TAMQPStaticAuthenticator.Create; // guest/guest
  FAuthorizer := TAMQPAllowAllAuthorizer.Create;
  // WS5: a engine e' o sink. O caminho de conteudo da Fase 1 (canal remonta
  // -> Sink.RouteMessage) nao mudou -- so' quem esta' plugado nele.
  FEngine := TAMQPEngine.Create;
  FSink := FEngine;
  FMonitorStop := TEvent.Create(nil, True, False, '');
  FBindAddress := '0.0.0.0';
  FPort := 5672;
  FBacklog := 64;
  FChannelMax := AMQP_SERVER_DEFAULT_CHANNEL_MAX;
  FFrameMax := AMQP_SERVER_DEFAULT_FRAME_MAX;
  FHeartbeat := AMQP_SERVER_DEFAULT_HEARTBEAT;
  FCloseTimeoutMs := AMQP_SERVER_CLOSE_TIMEOUT_MS;
end;

function TAMQPServer.ConnConfig: TAMQPServerConnConfig;
begin
  Result := TAMQPServerConnConfig.Defaults;
  Result.Auth := FAuth;
  Result.Authorizer := FAuthorizer;
  Result.VHosts := FVHosts;
  Result.ChannelMax := FChannelMax;
  Result.FrameMax := FFrameMax;
  Result.Heartbeat := FHeartbeat;
  Result.CloseTimeoutMs := FCloseTimeoutMs;
  Result.Sink := FSink;
  Result.Confirms := FConfirms;
  Result.Tls := FUseTls;
  Result.TlsCertFile := FTlsCertFile;
  Result.TlsKeyFile := FTlsKeyFile;
end;

destructor TAMQPServer.Destroy;
begin
  Stop;
  FActive.Free;
  FDead.Free;
  FVHosts.Free;
  FMonitorStop.Free;
  FLock.Free;
  FAuth := nil;
  FAuthorizer := nil;
  FSink := nil;
  // Depois do Stop: nenhuma conexao viva, entao nenhum ator de fila esta'
  // sendo alimentado. O destrutor da engine para os atores antes de liberar.
  FreeAndNil(FJournal);
  FEngine.Free;
  inherited;
end;

procedure TAMQPServer.SetDataDir(const AValue: string);
begin
  if FRunning then
    raise EAMQPServerAbort.Create(
      'DataDir nao pode mudar com o broker no ar');
  FDataDir := AValue;
end;

procedure TAMQPServer.Start;
var
  LEstado: TAMQPRecoveredState;
begin
  if FRunning then
    Exit;
  FStopping := False;
  FTotalAccepted := 0;

  // O journal sobe ANTES do socket de escuta: nenhum cliente pode chegar num
  // broker meio recuperado (a recuperacao propriamente dita e' a WS6, e vai
  // entrar exatamente aqui). Se ele nao subir -- diretorio ja tem dono, disco
  // sem permissao --, o Start levanta e nada foi prometido a ninguem.
  if FDataDir <> '' then
  begin
    FJournal := TAMQPJournal.Create(FDataDir);
    FJournal.MaxJournalBytes := FMaxJournalBytes;
    // O registro de confirms E' o IAMQPDurabilitySink do journal: e' assim que
    // "a marca d'agua andou" vira "estes canais podem confirmar" (D24).
    // Injetado ANTES do Start, como a propriedade pede.
    FConfirms := TAMQPConfirmRegistry.Create;
    FJournal.DurabilitySink := FConfirms as IAMQPDurabilitySink;
    try
      FJournal.Start;
    except
      FConfirms := nil;
      FreeAndNil(FJournal);
      raise;
    end;
    FEngine.Journal := FJournal;

    // A RECUPERACAO RODA AQUI: depois de o journal estar de pe' -- e com o
    // lock do diretorio ja' na mao, o que garante que nenhum outro broker
    // esta' mexendo no mesmo WAL -- e ANTES de o socket de escuta abrir.
    // Ninguem pode falar com um broker meio recuperado.
    LEstado := AmqpReplayWal(FDataDir);
    try
      FEngine.Recupera(LEstado);
      FRecoveryStats := LEstado.Stats;
    finally
      LEstado.Free;
    end;

    // COMPACTACAO, e SO' AQUI: o journal esta' quiescente (a recuperacao ja'
    // terminou e o socket de escuta ainda nao abriu, entao nao ha publicador
    // nem ator submetendo). Compactar em voo exigiria quiescer a alocacao de
    // LSN, o que esbarra na D2 -- ver o cabecalho de AMQP.Server.Journal.
    // O efeito pratico: o log e' podado A CADA REINICIO.
    if (FJournal.CompactarAcimaDe > 0)
      and (FJournal.TamanhoTotal >= FJournal.CompactarAcimaDe) then
      FJournal.Compacta;
  end;
  FListener := TAMQPTcpListener.Create;
  FListener.Listen(FBindAddress, FPort, FBacklog);
  FPort := FListener.Port; // efetiva (relevante se veio 0)
  FRunning := True;
  FAccept := TAMQPAcceptThread.Create(Self);
  FAccept.Start;
  FMonitorStop.ResetEvent;
  FMonitor := TAMQPMonitorThread.Create(Self);
  FMonitor.Start;
end;

procedure TAMQPServer.Stop;
var
  LSnapshot: TArray<TAMQPServerConnection>;
  I: Integer;
begin
  LSnapshot := nil;
  if not FRunning then
  begin
    ReapDead;
    Exit;
  end;
  FStopping := True;

  // 0) para a monitora ANTES de tudo: enquanto ela vive, é ela quem libera as
  //    conexões mortas — dois reapers concorrentes dariam double-free.
  StopMonitor;

  // 1) para o accept: fecha o listener (desbloqueia o Accept pendente).
  if FListener <> nil then
    FListener.Close;
  if FAccept <> nil then
  begin
    FAccept.WaitFor;
    FreeAndNil(FAccept);
  end;
  FreeAndNil(FListener);

  // 2) derruba as conexões vivas. Snapshot sob lock, depois age sem o lock
  //    (HandleConnClosed também pega o lock).
  FLock.Enter;
  try
    SetLength(LSnapshot, FActive.Count);
    for I := 0 to FActive.Count - 1 do
      LSnapshot[I] := FActive[I];
  finally
    FLock.Leave;
  end;
  for I := 0 to High(LSnapshot) do
    LSnapshot[I].Shutdown;
  for I := 0 to High(LSnapshot) do
    LSnapshot[I].WaitFor;

  // 3) o OnClosed de cada conexão já as moveu para FDead; libera todas.
  ReapDead;

  // resto que por acaso ainda esteja em FActive (corrida rara): move e libera.
  FLock.Enter;
  try
    for I := 0 to FActive.Count - 1 do
      FDead.Add(FActive[I]);
    FActive.Clear;
  finally
    FLock.Leave;
  end;
  ReapDead;

  // O journal desce por ULTIMO: as conexoes ja' morreram, entao ninguem mais
  // submete, e o Stop dele drena o que ja' foi aceito antes de fechar.
  if FJournal <> nil then
  begin
    FEngine.Journal := nil;
    try
      FJournal.Stop;
    except
    end;
    FreeAndNil(FJournal);
  end;
  // O registro so cai depois do journal: enquanto a thread do journal vive,
  // ela pode estar dentro de um Release.
  FConfirms := nil;

  FRunning := False;
end;

procedure TAMQPServer.AcceptLoop;
var
  LSock: TAMQPTcpSocket;
  LConn: TAMQPServerConnection;
begin
  while not FStopping do
  begin
    LSock := FListener.Accept;
    if LSock = nil then
      Break; // listener fechado
    if FStopping then
    begin
      LSock.Free;
      Break;
    end;

    // (o reap fica com a thread monitora — ver MonitorLoop)

    // A conexão vira dona do socket e recebe uma cópia da config do broker.
    LConn := TAMQPServerConnection.Create(LSock, ConnConfig);
    LConn.OnClosed := HandleConnClosed;
    // Antes do Start: a FSM consulta a engine desde o Connection.Open.
    LConn.Engine := FEngine;
    FLock.Enter;
    try
      FActive.Add(LConn);
    finally
      FLock.Leave;
    end;
    AmqpAtomicInc(FTotalAccepted);
    LConn.Start;
  end;
end;

procedure TAMQPServer.MonitorLoop;
var
  LSnapshot: TArray<TAMQPServerConnection>;
  I: Integer;
begin
  while not FStopping do
  begin
    if FMonitorStop.WaitFor(AMQP_SERVER_MONITOR_TICK_MS) = wrSignaled then
      Break;
    if FStopping then
      Break;

    // Snapshot sob lock; os Tick rodam FORA do lock (HeartbeatTick pode postar
    // no writer e o Shutdown de uma conexão morta chama HandleConnClosed, que
    // também pega o lock). Seguro porque só ESTA thread libera conexões
    // enquanto o broker roda, e o ReapDead abaixo só acontece depois dos Tick.
    // WS5: recobra confirm que ficou preso porque a fila de escrita do canal
    // estava cheia no instante do fsync. Sem este cutucao, um canal assim so'
    // confirmaria no PROXIMO avanco da marca -- que pode nunca vir, se ninguem
    // mais publicar. Mesma ideia do PostDeliverTick para a entrega (D3).
    if FConfirms <> nil then
      try
        FConfirms.RetryPending;
      except
      end;

    LSnapshot := Connections;
    for I := 0 to High(LSnapshot) do
    begin
      try
        LSnapshot[I].HeartbeatTick;
      except
      end;
      try
        LSnapshot[I].EnforceCloseDeadline;
      except
      end;
    end;

    // Caso degradado da D3: consumidor que recusou e nenhum ack chegando --
    // sem este empurrao a fila so' voltaria a entregar no proximo publish.
    if FEngine <> nil then
      try
        FEngine.DeliverTick;
      except
      end;

    // WS8: fila com x-expires que passou do prazo sem uso e' recolhida aqui.
    // Mesmo lugar do DeliverTick porque e' o mesmo papel: a thread da conexao
    // esta' bloqueada no read, entao quem tem de acordar para olhar o relogio
    // e' esta monitora.
    if FEngine <> nil then
      try
        FEngine.ExpireIdleQueues;
      except
      end;

    ReapDead;
  end;
end;

procedure TAMQPServer.SetMaxQueueLength(AValue: Integer);
begin
  if AValue < 0 then
    AValue := 0;
  FMaxQueueLength := AValue;
  if FEngine <> nil then
    FEngine.MaxQueueLength := AValue;
end;

procedure TAMQPServer.StopMonitor;
begin
  if FMonitor = nil then
    Exit;
  FMonitorStop.SetEvent;
  FMonitor.WaitFor;
  FreeAndNil(FMonitor);
end;

procedure TAMQPServer.HandleConnClosed(AConn: TAMQPServerConnection);
begin
  // Disparado NA thread da conexão: só move para a lista de reap.
  FLock.Enter;
  try
    FActive.Remove(AConn);
    if FDead.IndexOf(AConn) < 0 then
      FDead.Add(AConn);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPServer.ReapDead;
var
  LToFree: TArray<TAMQPServerConnection>;
  I: Integer;
begin
  LToFree := nil;
  FLock.Enter;
  try
    SetLength(LToFree, FDead.Count);
    for I := 0 to FDead.Count - 1 do
      LToFree[I] := FDead[I];
    FDead.Clear;
  finally
    FLock.Leave;
  end;
  // Free fora do lock: o destrutor da conexão faz join da thread de leitura.
  for I := 0 to High(LToFree) do
    LToFree[I].Free;
end;

function TAMQPServer.ConnectionCount: Integer;
begin
  FLock.Enter;
  try
    Result := FActive.Count;
  finally
    FLock.Leave;
  end;
end;

function TAMQPServer.Connections: TArray<TAMQPServerConnection>;
var
  I: Integer;
begin
  Result := nil;
  FLock.Enter;
  try
    SetLength(Result, FActive.Count);
    for I := 0 to FActive.Count - 1 do
      Result[I] := FActive[I];
  finally
    FLock.Leave;
  end;
end;

end.

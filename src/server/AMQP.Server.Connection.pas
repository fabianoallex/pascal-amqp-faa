unit AMQP.Server.Connection;

{$I amqp.inc}

{ Uma conexão do broker: a thread de leitura, a FSM de handshake, o mapa de
  canais e o writer serial (sub-módulo server, WS4).

  Modelo de execução (ver CLAUDE.md "Sub-módulo broker"): thread-por-conexão,
  I/O bloqueante. SÓ esta thread lê o socket e SÓ ela toca a FSM e o mapa de
  canais; tudo o que sai vai pelo TAMQPFrameWriter (uma thread escritora por
  conexão). Shutdown (chamado pelo broker de outra thread) fecha o socket para
  desbloquear um ReadFrom pendente. A conexão NUNCA se auto-libera (sem
  FreeOnTerminate) — o broker faz o reap.

  FSM (spec 0-9-1, 1.4.2.1 / 4.2.5 / 4.2.7):

    cliente: "AMQP" 0 0 9 1
    S->C: Connection.Start   (server-properties, mechanisms, locales)
    C->S: Connection.Start-Ok(client-properties, mechanism, response, locale)
          -> IAMQPAuthenticator.Authenticate
    S->C: Connection.Tune    (channel-max, frame-max, heartbeat propostos)
    C->S: Connection.Tune-Ok (o que o cliente aceita; nunca acima do proposto)
    C->S: Connection.Open    (virtual-host)
    S->C: Connection.Open-Ok -> conexão utilizável (amqssOpen)

  Fora dessa ordem, qualquer método é COMMAND_INVALID (503) e derruba a
  conexão — é o que impede um cliente de publicar antes de autenticar.

  Modelo de erro:
  - EAMQPChannelError    -> Channel.Close no canal ofensor; a conexão segue.
  - EAMQPConnectionError -> Connection.Close no canal 0; esperamos o Close-Ok
                            (descartando todo o resto) e encerramos.
  - EAMQPServerAbort     -> derruba o socket sem Close.

  WS5 pendura o despacho de Exchange/Queue/Basic/Confirm e a montagem de
  conteúdo nos dois hooks virtuais (DispatchChannelMethod / DispatchContent);
  no WS4 eles recusam e o canal morre com NOT_IMPLEMENTED (540). }

interface

uses
  SysUtils,
  Classes,
  Rtti,
  SyncObjs,
  Generics.Collections,
  AMQP.Threading,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Frame,
  AMQP.Transport,
  {$IFDEF AMQP_OPENSSL}
  AMQP.Transport.OpenSSL,
  {$ENDIF}
  AMQP.Connection.Methods,
  AMQP.Channel.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods,
  AMQP.Server.Auth,
  AMQP.Server.Types,
  AMQP.Server.Channel,
  AMQP.Server.FrameIO;

const
  /// frame-max aceito enquanto o Tune-Ok não chega (piso de segurança contra
  /// um cliente que anuncie um frame gigante antes de negociar). Depois do
  /// Tune-Ok vale o valor negociado.
  AMQP_SERVER_FRAME_MAX_BOOT = 131072;

  /// Octetos de um frame que NÃO são payload: 7 de cabeçalho + 1 de frame-end.
  /// frame-max da spec conta o frame inteiro; TAMQPFrame.ReadFrom limita só o
  /// payload.
  AMQP_FRAME_OVERHEAD = 8;

type
  TAMQPServerConnection = class;

  /// Notificação de que a conexão terminou (thread de leitura saiu). Disparada
  /// NA thread da conexão; o handler (o broker) deve só mover a conexão para a
  /// lista de reap, sem bloquear.
  TAMQPConnectionClosedEvent = procedure(AConn: TAMQPServerConnection) of object;

  /// Estados da FSM acima. Escrito só pela thread de leitura.
  TAMQPServerConnState = (
    amqssHeader,       // esperando o protocol-header do cliente
    amqssAwaitStartOk, // Connection.Start enviado
    amqssAwaitTuneOk,  // Connection.Tune enviado
    amqssAwaitOpen,    // Tune-Ok recebido, esperando Connection.Open
    amqssOpen,         // Open-Ok enviado — conexão utilizável
    amqssClosing,      // mandamos Connection.Close, esperando o Close-Ok
    amqssClosed        // acabou (a thread de leitura sai)
  );

  TAMQPServerConnThread = class(TThread)
  private
    FOwner: TAMQPServerConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TAMQPServerConnection);
  end;

  TAMQPServerConnection = class
  private
    FSocket: TAMQPTcpSocket;
    FRawStream: TAMQPSocketStream; // bytes crus do socket
    // Stream efetivo do AMQP: = FRawStream em plain; o wrapper TLS quando
    // cifrado (e aí o wrapper é dono do FRawStream). Só existe depois que a
    // thread da conexão monta o transporte — ver SetupTransport.
    FStream: TStream;
    FWriter: TAMQPFrameWriter;
    FThread: TAMQPServerConnThread;
    FConfig: TAMQPServerConnConfig;
    FPeer: string;
    FFramesRead: Integer;      // atômico
    FProtocolOk: Boolean;
    FError: string;
    FClosing: Integer;         // atômico (0/1)
    FOnClosed: TAMQPConnectionClosedEvent;

    // --- estado da FSM (só a thread de leitura escreve) ---
    FState: TAMQPServerConnState;
    FNegotiated: TAMQPConnectionTune;
    FUserId: string;
    FVirtualHost: string;
    FAuthFailureClose: Boolean; // cliente anunciou a capability
    FCloseDeadline: UInt64;     // tick-limite do Close-Ok (0 = sem prazo)
    FLastReadTick: UInt64;      // atomico; ultimo frame CHEGADO do peer
    FNameSeq: Integer;          // gera nomes de fila / consumer-tag unicos

    // --- canais ---
    FChLock: TCriticalSection;
    FChannels: TDictionary<Word, TAMQPServerChannel>;

    procedure SetupTransport;
    function ReadProtocolHeader: Boolean;
    procedure RunReadLoop;
    function CurrentMaxPayload: Cardinal;

    procedure HandleFrame(const AFrame: TAMQPFrame);
    procedure HandleMethodFrame(const AFrame: TAMQPFrame);
    procedure HandleConnectionMethod(const AId: TAMQPMethodId;
      const AReader: TAMQPReader);
    procedure HandleChannelFrame(const AFrame: TAMQPFrame;
      const AId: TAMQPMethodId; const AReader: TAMQPReader);
    procedure HandleContentFrame(const AFrame: TAMQPFrame);

    procedure DoStartOk(const AReader: TAMQPReader);
    procedure DoTuneOk(const AReader: TAMQPReader);
    procedure DoOpen(const AReader: TAMQPReader);

    procedure PostMethod(AChannel: Word; const APayload: TBytes);
    procedure SendConnectionStart;
    procedure SendTune;
    procedure SendConnectionClose(ACode: Word; const AText: string;
      AClassId, AMethodId: Word);
    procedure SendChannelClose(AChannel, ACode: Word; const AText: string;
      AClassId, AMethodId: Word);

    function FindChannel(AId: Word): TAMQPServerChannel;
    function AddChannel(AId: Word): TAMQPServerChannel;
    procedure DropChannel(AId: Word);
    procedure ClearChannels;
    procedure DetachAllDelivery;

    function ClientWantsAuthFailureClose(AProps: TAMQPFieldTable): Boolean;
    function MechanismOffered(const AMechanism: string): Boolean;

    function GeneratedName(const APrefix: string): string;
    function DispatchExchange(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
    function DispatchQueue(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
    function DispatchBasic(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
    function DispatchConfirm(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
    procedure CompleteContent(AChannel: TAMQPServerChannel);
  protected
    /// WS5/Fase 2: despacha um método das classes Exchange/Queue/Basic/
    /// Confirm/Tx num canal aberto. Devolver False = método não tratado, e a
    /// FSM fecha o canal com NOT_IMPLEMENTED (540). Levante EAMQPChannelError
    /// / EAMQPConnectionError para os demais erros do protocolo.
    function DispatchChannelMethod(AChannel: TAMQPServerChannel;
      const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean; virtual;
    /// WS5: content-header / content-body de um Basic.Publish em curso.
    /// False = não havia publish em andamento -> UNEXPECTED_FRAME (505).
    function DispatchContent(AChannel: TAMQPServerChannel;
      const AFrame: TAMQPFrame): Boolean; virtual;
  public
    constructor Create(ASocket: TAMQPTcpSocket;
      const AConfig: TAMQPServerConnConfig);
    destructor Destroy; override;

    /// Arranca a thread de leitura.
    procedure Start;
    /// Fecha o socket (desbloqueia o ReadFrom) e para o writer. Chamável de
    /// outra thread e mais de uma vez. Não faz join — use WaitFor.
    procedure Shutdown;
    /// Bloqueia até a thread de leitura sair.
    procedure WaitFor;

    /// Derruba a conexão se ela está esperando um Connection.Close-Ok que não
    /// veio dentro de FConfig.CloseTimeoutMs. Chamável de outra thread.
    procedure EnforceCloseDeadline;

    /// Um passo do heartbeat (spec 4.2.7), chamado periodicamente pela thread
    /// monitora do broker — NÃO pela thread desta conexão, que está bloqueada
    /// no read. Faz duas coisas, ambas sobre o intervalo negociado no Tune-Ok:
    ///  - envio ocioso há >= metade do intervalo -> posta um heartbeat;
    ///  - nada recebido há > 2x o intervalo -> considera o peer morto e derruba
    ///    a conexão (fecha o socket, o que desbloqueia o read).
    /// No-op enquanto o Tune-Ok não chegou ou se o intervalo negociado é 0.
    procedure HeartbeatTick;

    /// Tick do último frame recebido do peer (0 se nenhum). Diagnóstico/testes.
    function LastReadTick: UInt64;

    /// Nº de canais abertos agora (thread-safe).
    function ChannelCount: Integer;

    property Peer: string read FPeer;
    property Writer: TAMQPFrameWriter read FWriter;
    property FramesRead: Integer read FFramesRead;
    property ProtocolOk: Boolean read FProtocolOk;
    property LastError: string read FError;
    /// Snapshot da FSM (escrito pela thread da conexão; leitura de fora é
    /// informativa — para log/testes).
    property State: TAMQPServerConnState read FState;
    /// Preenchidos no handshake; válidos a partir de amqssOpen.
    property UserId: string read FUserId;
    property VirtualHost: string read FVirtualHost;
    property Negotiated: TAMQPConnectionTune read FNegotiated;
    property OnClosed: TAMQPConnectionClosedEvent read FOnClosed write FOnClosed;
  end;

implementation

{ TAMQPServerConnThread }

constructor TAMQPServerConnThread.Create(AOwner: TAMQPServerConnection);
begin
  FOwner := AOwner;
  inherited Create(True); // suspenso; Start explícito
end;

procedure TAMQPServerConnThread.Execute;
begin
  FOwner.RunReadLoop;
end;

{ TAMQPServerConnection }

constructor TAMQPServerConnection.Create(ASocket: TAMQPTcpSocket;
  const AConfig: TAMQPServerConnConfig);
begin
  inherited Create;
  FSocket := ASocket;
  FConfig := AConfig;
  FPeer := ASocket.PeerAddress;
  FState := amqssHeader;
  FRawStream := TAMQPSocketStream.Create(FSocket); // não é dono do socket
  // FStream e FWriter só nascem em SetupTransport, na thread desta conexão: o
  // handshake TLS é bloqueante e não pode rodar na thread de accept, senão um
  // cliente lento (ou hostil) trava o broker inteiro.
  FChLock := TCriticalSection.Create;
  FChannels := TDictionary<Word, TAMQPServerChannel>.Create;
  FThread := TAMQPServerConnThread.Create(Self);
end;

destructor TAMQPServerConnection.Destroy;
begin
  Shutdown;
  if FThread <> nil then
  begin
    FThread.WaitFor;
    FThread.Free;
  end;
  FWriter.Free;   // Stop já foi chamado no Shutdown (nil-safe)
  // Em TLS o wrapper é dono do stream cru e libera os dois; em plain os dois
  // ponteiros são o mesmo objeto.
  if (FStream <> nil) and (FStream <> TStream(FRawStream)) then
  begin
    FStream.Free;
    FRawStream := nil;
  end;
  FRawStream.Free;
  FSocket.Free;   // a conexão É dona do socket aceito
  ClearChannels;
  FChannels.Free;
  FChLock.Free;
  inherited;
end;

procedure TAMQPServerConnection.Start;
begin
  FThread.Start;
end;

procedure TAMQPServerConnection.Shutdown;
begin
  if AmqpAtomicCompareExchange(FClosing, 1, 0) <> 0 then
    Exit; // já em curso
  // Fecha o socket: desbloqueia um ReadFrom pendente na thread de leitura.
  // Guardado: Shutdown é chamado pelo Stop do broker e pelo destrutor, e uma
  // falha aqui (peer já sumiu) não pode derrubar o teardown do broker inteiro.
  if FSocket <> nil then
    try
      FSocket.Close;
    except
    end;
  DetachAllDelivery; // nenhuma entrega pode alcancar o writer depois daqui
  if FWriter <> nil then
    FWriter.Stop;
end;

procedure TAMQPServerConnection.WaitFor;
begin
  if FThread <> nil then
    FThread.WaitFor;
end;

function TAMQPServerConnection.LastReadTick: UInt64;
begin
  Result := AmqpAtomicRead64(FLastReadTick);
end;

procedure TAMQPServerConnection.HeartbeatTick;
var
  LIntervalMs, LNow, LLastRead, LLastWrite: UInt64;
begin
  // Antes do Tune-Ok não há intervalo negociado; depois do Closing não vale a
  // pena mandar heartbeat (já estamos encerrando).
  if not (FState in [amqssAwaitOpen, amqssOpen]) then
    Exit;
  LIntervalMs := UInt64(FNegotiated.Heartbeat) * 1000;
  if LIntervalMs = 0 then
    Exit; // heartbeat desabilitado pelo cliente no Tune-Ok

  LNow := AmqpTickMs;

  // Peer morto: a spec manda derrubar após dois intervalos sem NADA chegar
  // (qualquer frame conta, não só heartbeat). Fechar o socket desbloqueia o
  // ReadFrom da thread desta conexão, que então encerra pelo caminho normal.
  LLastRead := AmqpAtomicRead64(FLastReadTick);
  if (LLastRead <> 0) and ((LNow - LLastRead) > (2 * LIntervalMs)) then
  begin
    if FError = '' then
      FError := Format('peer sem enviar frames há mais de %d ms (heartbeat)',
        [2 * LIntervalMs]);
    Shutdown;
    Exit;
  end;

  // Envio ocioso: manda um heartbeat para o peer não nos julgar mortos. Meio
  // intervalo dá folga para o frame chegar antes do prazo dele.
  LLastWrite := FWriter.LastWriteTick;
  if (LLastWrite = 0) or ((LNow - LLastWrite) >= (LIntervalMs div 2)) then
    try
      FWriter.PostFrame(TAMQPFrame.Heartbeat);
    except
      // writer parado/falho: a thread de leitura já vai perceber
    end;
end;

procedure TAMQPServerConnection.EnforceCloseDeadline;
begin
  if (FState = amqssClosing) and (FCloseDeadline <> 0) and
     (AmqpTickMs > FCloseDeadline) then
  begin
    if FError = '' then
      FError := 'cliente não respondeu ao Connection.Close no prazo';
    Shutdown;
  end;
end;

{ --- canais --------------------------------------------------------------- }

function TAMQPServerConnection.FindChannel(AId: Word): TAMQPServerChannel;
begin
  FChLock.Enter;
  try
    if not FChannels.TryGetValue(AId, Result) then
      Result := nil;
  finally
    FChLock.Leave;
  end;
end;

function TAMQPServerConnection.AddChannel(AId: Word): TAMQPServerChannel;
begin
  Result := TAMQPServerChannel.Create(AId);
  // WS4: o canal ja nasce com alvo de entrega. O DropChannel/ClearChannels
  // (e, em ultimo caso, o destrutor do canal) fazem o Detach -- sem isso uma
  // fila entregando numa thread do pool escreveria num writer ja liberado.
  Result.AttachDelivery(FWriter, CurrentMaxPayload);
  FChLock.Enter;
  try
    FChannels.Add(AId, Result);
  finally
    FChLock.Leave;
  end;
end;

procedure TAMQPServerConnection.DropChannel(AId: Word);
var
  LCh: TAMQPServerChannel;
begin
  LCh := nil;
  FChLock.Enter;
  try
    if FChannels.TryGetValue(AId, LCh) then
      FChannels.Remove(AId);
  finally
    FChLock.Leave;
  end;
  LCh.Free;
end;

// Solta o alvo de entrega de todos os canais. Chamado no Shutdown ANTES de
// parar o writer: a partir daqui toda entrega em voo (numa thread do pool)
// falha limpo em vez de escrever num writer que esta sendo derrubado.
procedure TAMQPServerConnection.DetachAllDelivery;
var
  LCh: TAMQPServerChannel;
begin
  FChLock.Enter;
  try
    for LCh in FChannels.Values do
      LCh.DetachDelivery;
  finally
    FChLock.Leave;
  end;
end;

procedure TAMQPServerConnection.ClearChannels;
var
  LAll: TArray<TAMQPServerChannel>;
  LCh: TAMQPServerChannel;
  I: Integer;
begin
  LAll := nil;
  FChLock.Enter;
  try
    SetLength(LAll, FChannels.Count);
    I := 0;
    for LCh in FChannels.Values do
    begin
      LAll[I] := LCh;
      Inc(I);
    end;
    FChannels.Clear;
  finally
    FChLock.Leave;
  end;
  for I := 0 to High(LAll) do
    LAll[I].Free;
end;

function TAMQPServerConnection.ChannelCount: Integer;
begin
  FChLock.Enter;
  try
    Result := FChannels.Count;
  finally
    FChLock.Leave;
  end;
end;

{ --- saída ---------------------------------------------------------------- }

procedure TAMQPServerConnection.PostMethod(AChannel: Word;
  const APayload: TBytes);
begin
  FWriter.PostFrame(TAMQPFrame.Create(AMQP_FRAME_METHOD, AChannel, APayload));
end;

procedure TAMQPServerConnection.SendConnectionStart;
var
  LProps: TAMQPFieldTable;
  LMechs: string;
  LList: TArray<string>;
  I: Integer;
begin
  LMechs := '';
  if FConfig.Auth <> nil then
  begin
    LList := FConfig.Auth.Mechanisms;
    for I := 0 to High(LList) do
      if LMechs = '' then
        LMechs := LList[I]
      else
        LMechs := LMechs + ' ' + LList[I];
  end;
  if LMechs = '' then
    LMechs := AMQP_MECH_PLAIN;

  LProps := DefaultServerProperties;
  try
    PostMethod(AMQP_CHANNEL_CONNECTION,
      BuildStart(AMQP_VERSION_MAJOR, AMQP_VERSION_MINOR, LProps,
        LMechs, AMQP_LOCALE_DEFAULT));
  finally
    LProps.Free;
  end;
end;

procedure TAMQPServerConnection.SendTune;
var
  LTune: TAMQPConnectionTune;
begin
  LTune.ChannelMax := FConfig.ChannelMax;
  LTune.FrameMax := FConfig.FrameMax;
  LTune.Heartbeat := FConfig.Heartbeat;
  PostMethod(AMQP_CHANNEL_CONNECTION, BuildTune(LTune));
end;

procedure TAMQPServerConnection.SendConnectionClose(ACode: Word;
  const AText: string; AClassId, AMethodId: Word);
var
  LClose: TAMQPConnectionClose;
begin
  LClose.ReplyCode := ACode;
  LClose.ReplyText := AText;
  LClose.ClassId := AClassId;
  LClose.MethodId := AMethodId;
  if FError = '' then
    FError := Format('%d %s', [ACode, AText]);
  PostMethod(AMQP_CHANNEL_CONNECTION, BuildClose(LClose));
  FCloseDeadline := AmqpTickMs + FConfig.CloseTimeoutMs;
end;

procedure TAMQPServerConnection.SendChannelClose(AChannel, ACode: Word;
  const AText: string; AClassId, AMethodId: Word);
var
  LClose: TAMQPCloseInfo;
  LCh: TAMQPServerChannel;
begin
  LClose.ReplyCode := ACode;
  LClose.ReplyText := AText;
  LClose.ClassId := AClassId;
  LClose.MethodId := AMethodId;
  PostMethod(AChannel, BuildChannelClose(LClose));
  // O canal fica vivo até o Channel.Close-Ok; nesse meio-tempo tudo o que
  // chegar nele é descartado (spec 1.4.2.2).
  LCh := FindChannel(AChannel);
  if LCh <> nil then
    LCh.State := amqchClosing;
end;

{ --- handshake ------------------------------------------------------------ }

function TAMQPServerConnection.MechanismOffered(
  const AMechanism: string): Boolean;
var
  LList: TArray<string>;
  I: Integer;
begin
  Result := False;
  if FConfig.Auth = nil then
    Exit;
  LList := FConfig.Auth.Mechanisms;
  for I := 0 to High(LList) do
    if SameText(LList[I], AMechanism) then
      Exit(True);
end;

function TAMQPServerConnection.ClientWantsAuthFailureClose(
  AProps: TAMQPFieldTable): Boolean;
var
  LValue, LFlag: TValue;
  LObj: TObject;
begin
  // Leitura defensiva: qualquer coisa fora do formato esperado vira False
  // (e a spec manda, nesse caso, fechar o socket sem Connection.Close).
  Result := False;
  if AProps = nil then
    Exit;
  if not AProps.TryGetValue('capabilities', LValue) then
    Exit;
  LValue := AmqpUnwrapValue(LValue);
  if not LValue.IsObject then
    Exit;
  LObj := LValue.AsObject;
  if not (LObj is TAMQPFieldTable) then
    Exit;
  if not TAMQPFieldTable(LObj).TryGetValue('authentication_failure_close',
    LFlag) then
    Exit;
  try
    Result := LFlag.AsBoolean;
  except
    Result := False;
  end;
end;

procedure TAMQPServerConnection.DoStartOk(const AReader: TAMQPReader);
var
  LStartOk: TAMQPConnectionStartOk;
  LRes: TAMQPAuthResult;
  LBadMech: Boolean;
begin
  LStartOk := DecodeStartOk(AReader);
  try
    FAuthFailureClose := ClientWantsAuthFailureClose(LStartOk.ClientProperties);
    LBadMech := not MechanismOffered(LStartOk.Mechanism);
    if LBadMech then
      // Format (e não '+'): concatenar literal UTF-8 com AnsiString dispara
      // conversão implícita no FPC.
      LRes := TAMQPAuthResult.Deny(
        Format('mecanismo não suportado: %s', [LStartOk.Mechanism]))
    else
      LRes := FConfig.Auth.Authenticate(LStartOk.Mechanism, LStartOk.Response,
        FPeer, FConfig.Tls);
  finally
    LStartOk.ClientProperties.Free;
  end;

  if not LRes.Granted then
  begin
    // Cliente que anunciou 'authentication_failure_close' recebe um
    // Connection.Close com o motivo; os demais só veem o socket cair (spec /
    // comportamento histórico dos brokers).
    if not FAuthFailureClose then
      raise EAMQPServerAbort.Create(
        Format('autenticação recusada: %s', [LRes.FailReason]));
    if LBadMech then
      raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED, LRes.FailReason,
        AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK);
    raise EAMQPConnectionError.Create(AMQP_ACCESS_REFUSED, LRes.FailReason,
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK);
  end;

  FUserId := LRes.UserId;
  SendTune;
  FState := amqssAwaitTuneOk;
end;

procedure TAMQPServerConnection.DoTuneOk(const AReader: TAMQPReader);
var
  LTune: TAMQPConnectionTune;
begin
  LTune := DecodeTuneOk(AReader);

  // O cliente não pode pedir mais do que propusemos (0 = "sem limite", que é
  // MAIS do que qualquer limite finito nosso).
  if (FConfig.ChannelMax > 0) and
     ((LTune.ChannelMax = 0) or (LTune.ChannelMax > FConfig.ChannelMax)) then
    raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
      Format('channel-max negociado inválido: %d (máximo %d)',
        [LTune.ChannelMax, FConfig.ChannelMax]),
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK);

  if (FConfig.FrameMax > 0) and
     ((LTune.FrameMax = 0) or (LTune.FrameMax > FConfig.FrameMax)) then
    raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
      Format('frame-max negociado inválido: %u (máximo %u)',
        [LTune.FrameMax, FConfig.FrameMax]),
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK);

  // Piso da spec (4.2.5): todo peer tem de aceitar 4096 octetos.
  if (LTune.FrameMax <> 0) and (LTune.FrameMax < AMQP_FRAME_MIN_SIZE) then
    raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
      Format('frame-max abaixo do mínimo da spec: %u (mínimo %d)',
        [LTune.FrameMax, AMQP_FRAME_MIN_SIZE]),
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK);

  // O heartbeat quem decide é o cliente (spec: "the delay ... that the client
  // wants"); aceitamos o que ele mandar.
  FNegotiated := LTune;
  FState := amqssAwaitOpen;
end;

procedure TAMQPServerConnection.DoOpen(const AReader: TAMQPReader);
var
  LOpen: TAMQPConnectionOpen;
begin
  LOpen := DecodeOpen(AReader);
  if (FConfig.VHosts = nil) or (not FConfig.VHosts.Contains(LOpen.VirtualHost)) then
    raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
      Format('vhost "%s" não existe neste broker', [LOpen.VirtualHost]),
      AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN);

  FVirtualHost := LOpen.VirtualHost;
  PostMethod(AMQP_CHANNEL_CONNECTION, BuildOpenOk);
  FState := amqssOpen;
end;

{ --- despacho ------------------------------------------------------------- }

procedure TAMQPServerConnection.HandleConnectionMethod(const AId: TAMQPMethodId;
  const AReader: TAMQPReader);
begin
  // Connection.Close e Close-Ok valem em QUALQUER estado: o cliente pode
  // desistir no meio do handshake.
  if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
  begin
    DecodeClose(AReader);
    PostMethod(AMQP_CHANNEL_CONNECTION, BuildCloseOk);
    FState := amqssClosed;
    Exit;
  end;
  if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK) then
  begin
    DecodeCloseOk(AReader);
    FState := amqssClosed;
    Exit;
  end;

  case FState of
    amqssAwaitStartOk:
      if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START_OK) then
      begin
        DoStartOk(AReader);
        Exit;
      end;
    amqssAwaitTuneOk:
      if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE_OK) then
      begin
        DoTuneOk(AReader);
        Exit;
      end;
    amqssAwaitOpen:
      if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN) then
      begin
        DoOpen(AReader);
        Exit;
      end;
  end;

  // Fora de ordem (ou método só-servidor, como Blocked/Unblocked, vindo do
  // cliente): a spec manda COMMAND_INVALID.
  raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
    Format('método %d/%d inesperado no canal 0 neste ponto do handshake',
      [AId.ClassId, AId.MethodId]), AId.ClassId, AId.MethodId);
end;

procedure TAMQPServerConnection.HandleChannelFrame(const AFrame: TAMQPFrame;
  const AId: TAMQPMethodId; const AReader: TAMQPReader);
var
  LCh: TAMQPServerChannel;
begin
  LCh := FindChannel(AFrame.Channel);

  // Canal fechando: só o Close-Ok interessa; o resto se descarta.
  if (LCh <> nil) and (LCh.State = amqchClosing) then
  begin
    if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE_OK) then
      DropChannel(AFrame.Channel);
    Exit;
  end;

  // Regra de interleave (spec 2.3.5): entre o método que carrega conteúdo
  // (Basic.Publish) e o último frame de body, NENHUM outro frame pode aparecer
  // NESTE canal — nem outro método, nem um Channel.Close. Vale só por canal:
  // outros canais continuam livres para intercalar, que é justamente para isso
  // que o estado de montagem vive no canal.
  if (LCh <> nil) and (LCh.AsmState <> amqasIdle) then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('método %d/%d no canal %d no meio de um conteúdo',
        [AId.ClassId, AId.MethodId, AFrame.Channel]), AId.ClassId, AId.MethodId);

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN) then
  begin
    DecodeChannelOpen(AReader);
    if LCh <> nil then
      raise EAMQPConnectionError.Create(AMQP_CHANNEL_ERROR,
        Format('segundo Channel.Open no canal %d', [AFrame.Channel]),
        AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN);
    if (FNegotiated.ChannelMax > 0) and (AFrame.Channel > FNegotiated.ChannelMax) then
      raise EAMQPConnectionError.Create(AMQP_CHANNEL_ERROR,
        Format('canal %d acima do channel-max negociado (%d)',
          [AFrame.Channel, FNegotiated.ChannelMax]),
        AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN);
    AddChannel(AFrame.Channel);
    PostMethod(AFrame.Channel, BuildChannelOpenOk);
    Exit;
  end;

  // Daqui para baixo o canal tem de existir: usar um canal não aberto é erro
  // de CONEXÃO (504), não de canal — não há canal onde mandar o Channel.Close.
  if LCh = nil then
    raise EAMQPConnectionError.Create(AMQP_CHANNEL_ERROR,
      Format('método %d/%d no canal %d, que não está aberto',
        [AId.ClassId, AId.MethodId, AFrame.Channel]), AId.ClassId, AId.MethodId);

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE) then
  begin
    DecodeChannelClose(AReader);
    PostMethod(AFrame.Channel, BuildChannelCloseOk);
    DropChannel(AFrame.Channel);
    Exit;
  end;

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE_OK) then
    // Close-Ok sem Close nosso: o canal não estava fechando.
    raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
      Format('Channel.Close-Ok inesperado no canal %d', [AFrame.Channel]),
      AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE_OK);

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW) then
  begin
    LCh.Active := DecodeChannelFlow(AReader);
    PostMethod(AFrame.Channel, BuildChannelFlowOk(LCh.Active));
    Exit;
  end;

  if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_FLOW_OK) then
  begin
    DecodeChannelFlowOk(AReader); // resposta a um Flow nosso; nada a fazer
    Exit;
  end;

  // Exchange/Queue/Basic/Confirm/Tx: WS5.
  if not DispatchChannelMethod(LCh, AId, AReader) then
    raise EAMQPChannelError.Create(AFrame.Channel, AMQP_NOT_IMPLEMENTED,
      Format('método %d/%d ainda não implementado por este broker',
        [AId.ClassId, AId.MethodId]), AId.ClassId, AId.MethodId);
end;

procedure TAMQPServerConnection.HandleMethodFrame(const AFrame: TAMQPFrame);
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
begin
  LReader := TAMQPReader.Create(AFrame.Payload);
  try
    LId := ReadMethodHeader(LReader);

    if AFrame.Channel = AMQP_CHANNEL_CONNECTION then
    begin
      if LId.ClassId <> AMQP_CLASS_CONNECTION then
        raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
          Format('classe %d não é permitida no canal 0', [LId.ClassId]),
          LId.ClassId, LId.MethodId);
      HandleConnectionMethod(LId, LReader);
      Exit;
    end;

    // Canal != 0 só depois do Open-Ok.
    if FState <> amqssOpen then
      raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
        Format('método %d/%d no canal %d antes do Connection.Open-Ok',
          [LId.ClassId, LId.MethodId, AFrame.Channel]),
        LId.ClassId, LId.MethodId);

    if LId.ClassId = AMQP_CLASS_CONNECTION then
      raise EAMQPConnectionError.Create(AMQP_COMMAND_INVALID,
        'métodos de Connection só valem no canal 0',
        LId.ClassId, LId.MethodId);

    HandleChannelFrame(AFrame, LId, LReader);
  finally
    LReader.Free;
  end;
end;

procedure TAMQPServerConnection.HandleContentFrame(const AFrame: TAMQPFrame);
var
  LCh: TAMQPServerChannel;
begin
  if (AFrame.Channel = AMQP_CHANNEL_CONNECTION) or (FState <> amqssOpen) then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('frame de conteúdo (tipo %d) no canal %d fora de contexto',
        [AFrame.FrameType, AFrame.Channel]), 0, 0);

  LCh := FindChannel(AFrame.Channel);
  if LCh = nil then
    raise EAMQPConnectionError.Create(AMQP_CHANNEL_ERROR,
      Format('conteúdo no canal %d, que não está aberto', [AFrame.Channel]),
      0, 0);
  if LCh.State = amqchClosing then
    Exit; // descartado até o Close-Ok

  if not DispatchContent(LCh, AFrame) then
    raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
      Format('frame de conteúdo (tipo %d) fora de sequência no canal %d ' +
        '(estado da montagem: %d)',
        [AFrame.FrameType, AFrame.Channel, Ord(LCh.AsmState)]), 0, 0);
end;

procedure TAMQPServerConnection.HandleFrame(const AFrame: TAMQPFrame);
begin
  case AFrame.FrameType of
    AMQP_FRAME_HEARTBEAT:
      begin
        if AFrame.Channel <> AMQP_CHANNEL_CONNECTION then
          raise EAMQPConnectionError.Create(AMQP_FRAME_ERROR,
            'heartbeat fora do canal 0', 0, 0);
        // Nada a responder: heartbeat não se ecoa. Cada lado emite pelo
        // próprio timer (ver HeartbeatTick); receber já valeu por si, porque o
        // FLastReadTick foi atualizado no loop de leitura.
      end;
    AMQP_FRAME_METHOD:
      HandleMethodFrame(AFrame);
    AMQP_FRAME_HEADER, AMQP_FRAME_BODY:
      HandleContentFrame(AFrame);
  else
    raise EAMQPConnectionError.Create(AMQP_FRAME_ERROR,
      Format('tipo de frame desconhecido: %d', [AFrame.FrameType]), 0, 0);
  end;
end;

{ --- despacho das classes de recurso (broker "nulo" da Fase 1) ------------ }

// Nomes que o servidor gera quando o cliente manda vazio (fila anonima,
// consumer-tag automatico). Unicos dentro da conexao, que e' o que a spec pede.
function TAMQPServerConnection.GeneratedName(const APrefix: string): string;
begin
  Result := Format('%s%d-%d', [APrefix, AmqpAtomicInc(FNameSeq),
    Integer(AmqpTickMs and $FFFFFF)]);
end;

function TAMQPServerConnection.DispatchExchange(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LDeclare: TAMQPExchangeDeclare;
  LDelete: TAMQPExchangeDelete;
  LBind: TAMQPExchangeBinding;
begin
  Result := True;
  case AId.MethodId of
    AMQP_EXCHANGE_DECLARE:
      begin
        LDeclare := DecodeExchangeDeclare(AReader);
        LDeclare.Arguments.Free; // a tabela decodificada e' nossa
        if not LDeclare.NoWait then
          PostMethod(AChannel.Id, BuildExchangeDeclareOk);
      end;
    AMQP_EXCHANGE_DELETE:
      begin
        LDelete := DecodeExchangeDelete(AReader);
        if not LDelete.NoWait then
          PostMethod(AChannel.Id, BuildExchangeDeleteOk);
      end;
    AMQP_EXCHANGE_BIND:
      begin
        LBind := DecodeExchangeBind(AReader);
        LBind.Arguments.Free;
        if not LBind.NoWait then
          PostMethod(AChannel.Id, BuildExchangeBindOk);
      end;
    AMQP_EXCHANGE_UNBIND:
      begin
        LBind := DecodeExchangeUnbind(AReader);
        LBind.Arguments.Free;
        if not LBind.NoWait then
          PostMethod(AChannel.Id, BuildExchangeUnbindOk);
      end;
  else
    Result := False;
  end;
end;

function TAMQPServerConnection.DispatchQueue(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LDeclare: TAMQPQueueDeclare;
  LBind: TAMQPQueueBind;
  LUnbind: TAMQPQueueUnbind;
  LPurge: TAMQPQueuePurge;
  LDelete: TAMQPQueueDelete;
  LName: string;
begin
  Result := True;
  case AId.MethodId of
    AMQP_QUEUE_DECLARE:
      begin
        LDeclare := DecodeQueueDeclare(AReader);
        LDeclare.Arguments.Free;
        LName := LDeclare.QueueName;
        if LName = '' then
          LName := GeneratedName('amq.gen-'); // fila anonima: o nome e' nosso
        if not LDeclare.NoWait then
          // Sem engine ainda: 0 mensagens, 0 consumidores.
          PostMethod(AChannel.Id, BuildQueueDeclareOk(LName, 0, 0));
      end;
    AMQP_QUEUE_BIND:
      begin
        LBind := DecodeQueueBind(AReader);
        LBind.Arguments.Free;
        if not LBind.NoWait then
          PostMethod(AChannel.Id, BuildQueueBindOk);
      end;
    AMQP_QUEUE_UNBIND:
      begin
        // queue.unbind nao tem no-wait no 0-9-1: sempre responde.
        LUnbind := DecodeQueueUnbind(AReader);
        LUnbind.Arguments.Free;
        PostMethod(AChannel.Id, BuildQueueUnbindOk);
      end;
    AMQP_QUEUE_PURGE:
      begin
        LPurge := DecodeQueuePurge(AReader);
        if not LPurge.NoWait then
          PostMethod(AChannel.Id, BuildQueuePurgeOk(0));
      end;
    AMQP_QUEUE_DELETE:
      begin
        LDelete := DecodeQueueDelete(AReader);
        if not LDelete.NoWait then
          PostMethod(AChannel.Id, BuildQueueDeleteOk(0));
      end;
  else
    Result := False;
  end;
end;

function TAMQPServerConnection.DispatchBasic(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LQos: TAMQPBasicQosArgs;
  LConsume: TAMQPBasicConsume;
  LCancel: TAMQPBasicCancelArgs;
  LPublish: TAMQPBasicPublish;
  LAck: TAMQPBasicAck;
  LNack: TAMQPBasicNack;
  LReject: TAMQPBasicReject;
  LTag: string;
begin
  Result := True;
  case AId.MethodId of
    AMQP_BASIC_QOS:
      begin
        LQos := DecodeBasicQos(AReader);
        AChannel.PrefetchCount := LQos.PrefetchCount;
        PostMethod(AChannel.Id, BuildBasicQosOk); // qos nao tem no-wait
      end;
    AMQP_BASIC_CONSUME:
      begin
        LConsume := DecodeBasicConsume(AReader);
        LConsume.Arguments.Free;
        LTag := LConsume.ConsumerTag;
        if LTag = '' then
          LTag := GeneratedName('amq.ctag-');
        if not LConsume.NoWait then
          PostMethod(AChannel.Id, BuildBasicConsumeOk(LTag));
      end;
    AMQP_BASIC_CANCEL:
      begin
        LCancel := DecodeBasicCancelArgs(AReader);
        if not LCancel.NoWait then
          PostMethod(AChannel.Id, BuildBasicCancelOk(LCancel.ConsumerTag));
      end;
    AMQP_BASIC_PUBLISH:
      begin
        // Sem resposta: o que vem a seguir e' o content-header e o body. A
        // partir daqui o canal esta "montando" e nenhum outro frame pode
        // aparecer nele (ver a regra de interleave em HandleChannelFrame).
        LPublish := DecodeBasicPublish(AReader);
        AChannel.BeginContent(LPublish);
      end;
    AMQP_BASIC_GET:
      begin
        DecodeBasicGet(AReader);
        PostMethod(AChannel.Id, BuildBasicGetEmpty); // broker nulo: sempre vazia
      end;
    // Ack/Nack/Reject não respondem nada. O que a WS4 já faz aqui é a
    // contabilidade do prefetch: a entrega INCREMENTA (no alvo, ao escrever
    // os frames) e é esta thread — a única que vê o método do cliente — que
    // DECREMENTA. O reencaminhamento para o ator da fila (devolver a mensagem
    // com requeue, tirá-la das não-confirmadas) é da WS5, que é quem sabe a
    // qual fila cada consumer-tag pertence.
    AMQP_BASIC_ACK:
      begin
        LAck := DecodeBasicAck(AReader);
        if AChannel.Delivery <> nil then
          AChannel.Delivery.NoteResolved(LAck.DeliveryTag, LAck.Multiple);
      end;
    AMQP_BASIC_NACK:
      begin
        LNack := DecodeBasicNack(AReader);
        if AChannel.Delivery <> nil then
          AChannel.Delivery.NoteResolved(LNack.DeliveryTag, LNack.Multiple);
      end;
    AMQP_BASIC_REJECT:
      begin
        LReject := DecodeBasicReject(AReader); // Reject não tem multiple
        if AChannel.Delivery <> nil then
          AChannel.Delivery.NoteResolved(LReject.DeliveryTag, False);
      end;
  else
    Result := False;
  end;
end;

function TAMQPServerConnection.DispatchConfirm(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LNoWait: Boolean;
begin
  Result := True;
  case AId.MethodId of
    AMQP_CONFIRM_SELECT:
      begin
        LNoWait := DecodeConfirmSelect(AReader);
        AChannel.ConfirmMode := True;
        if not LNoWait then
          PostMethod(AChannel.Id, BuildConfirmSelectOk);
      end;
  else
    Result := False;
  end;
end;

function TAMQPServerConnection.DispatchChannelMethod(
  AChannel: TAMQPServerChannel; const AId: TAMQPMethodId;
  const AReader: TAMQPReader): Boolean;
begin
  // Fase 1 ("null broker"): decodifica tudo o que o cliente pode mandar e
  // responde o *-Ok correto, sem estado nenhum por tras. A classe Tx (90) fica
  // de fora de proposito -- nao esta no codec e nao entra na Fase 1 (ver a
  // decisao de nao implementar transacoes no CLAUDE.md) --, entao cai no False
  // e o canal fecha com NOT_IMPLEMENTED.
  case AId.ClassId of
    AMQP_CLASS_EXCHANGE: Result := DispatchExchange(AChannel, AId, AReader);
    AMQP_CLASS_QUEUE:    Result := DispatchQueue(AChannel, AId, AReader);
    AMQP_CLASS_BASIC:    Result := DispatchBasic(AChannel, AId, AReader);
    AMQP_CLASS_CONFIRM:  Result := DispatchConfirm(AChannel, AId, AReader);
  else
    Result := False;
  end;
end;

{ --- montagem de conteudo (spec 2.3.5) ------------------------------------ }

// Conteudo completo: entrega ao sink e limpa o canal. O sink NAO fica dono da
// mensagem -- o ResetContent no finally libera a tabela de headers.
procedure TAMQPServerConnection.CompleteContent(AChannel: TAMQPServerChannel);
begin
  try
    if FConfig.Sink <> nil then
      FConfig.Sink.RouteMessage(FVirtualHost, AChannel.CurrentMessage(FUserId));
  finally
    AChannel.ResetContent;
  end;
end;

function TAMQPServerConnection.DispatchContent(AChannel: TAMQPServerChannel;
  const AFrame: TAMQPFrame): Boolean;
var
  LReader: TAMQPReader;
  LHeader: TAMQPContentHeader;
  LClassId: Word;
begin
  if AFrame.FrameType = AMQP_FRAME_HEADER then
  begin
    if AChannel.AsmState <> amqasHeader then
      Exit(False); // content-header sem Basic.Publish antes -> 505

    // O class-id do content-header tem de casar com a classe do metodo que o
    // pediu (spec 2.3.5.2). Lido direto do payload porque o DecodeContentHeader
    // consome e descarta esse campo.
    if Length(AFrame.Payload) < 2 then
      raise EAMQPConnectionError.Create(AMQP_FRAME_ERROR,
        'content-header truncado', 0, 0);
    LClassId := (Word(AFrame.Payload[0]) shl 8) or Word(AFrame.Payload[1]);
    if LClassId <> AMQP_CLASS_BASIC then
      raise EAMQPConnectionError.Create(AMQP_UNEXPECTED_FRAME,
        Format('content-header da classe %d apos Basic.Publish', [LClassId]),
        0, 0);

    LReader := TAMQPReader.Create(AFrame.Payload);
    try
      LHeader := DecodeContentHeader(LReader);
    finally
      LReader.Free;
    end;

    if AChannel.SetContentHeader(LHeader, AFrame.Payload) then
      CompleteContent(AChannel); // body-size 0: mensagem sem corpo
    Exit(True);
  end;

  if AFrame.FrameType = AMQP_FRAME_BODY then
  begin
    if AChannel.AsmState <> amqasBody then
      Exit(False); // body sem header antes -> 505
    if AChannel.AppendBody(AFrame.Payload) then
      CompleteContent(AChannel);
    Exit(True);
  end;

  Result := False;
end;

{ --- loop de leitura ------------------------------------------------------ }

// Monta o transporte na thread DESTA conexão: em TLS, o handshake é síncrono e
// pode demorar (ou nunca terminar, se o peer sumir). Só depois dele o writer
// nasce, porque tudo o que ele escreve tem de sair cifrado.
procedure TAMQPServerConnection.SetupTransport;
begin
  if FConfig.Tls then
  begin
    {$IFDEF AMQP_OPENSSL}
    try
      FStream := TAMQPOpenSslStream.CreateServer(FRawStream,
        FConfig.TlsCertFile, FConfig.TlsKeyFile);
    except
      // O wrapper e' dono do stream de baixo, e o destrutor auto-chamado de um
      // construtor que levanta JA o liberou. Soltamos a referencia para o
      // destrutor da conexao nao liberar de novo (era double-free em toda falha
      // de handshake: cert invalido, cliente em claro na porta TLS...).
      FRawStream := nil;
      raise;
    end;
    {$ELSE}
    // SChannel do lado servidor ainda não existe (ver CLAUDE.md); no Windows
    // sem OpenSSL o broker não tem como aceitar TLS.
    raise EAMQPTls.Create(
      'TLS do servidor exige um build com -dAMQP_OPENSSL ' +
      '(o backend SChannel só implementa o lado cliente)');
    {$ENDIF}
  end
  else
    FStream := FRawStream;

  FWriter := TAMQPFrameWriter.Create(FStream);
end;

function TAMQPServerConnection.ReadProtocolHeader: Boolean;
var
  LBuf: array[0..7] of Byte;
  LTotal, LRead, I: Integer;
begin
  FillChar(LBuf, SizeOf(LBuf), 0);
  LTotal := 0;
  while LTotal < 8 do
  begin
    LRead := FStream.Read(LBuf[LTotal], 8 - LTotal);
    if LRead <= 0 then
      Exit(False);
    Inc(LTotal, LRead);
  end;
  for I := 0 to 7 do
    if LBuf[I] <> AMQP_PROTOCOL_HEADER[I] then
    begin
      // spec 4.2.2: devolve o header suportado e encerra. Vai pelo FStream (e
      // não direto no socket) para sair CIFRADO quando a conexão é TLS; nada
      // foi postado no writer ainda, então não há corrida com ele.
      try
        FStream.Write(AMQP_PROTOCOL_HEADER[0], Length(AMQP_PROTOCOL_HEADER));
      except
      end;
      FError := 'protocol-header inválido';
      Exit(False);
    end;
  Result := True;
end;

function TAMQPServerConnection.CurrentMaxPayload: Cardinal;
var
  LMax: Cardinal;
begin
  if FState in [amqssHeader, amqssAwaitStartOk, amqssAwaitTuneOk] then
    LMax := AMQP_SERVER_FRAME_MAX_BOOT
  else
    LMax := FNegotiated.FrameMax;
  if LMax = 0 then
    Exit(0); // sem limite negociado
  if LMax <= AMQP_FRAME_OVERHEAD then
    Result := 1
  else
    Result := LMax - AMQP_FRAME_OVERHEAD;
end;

procedure TAMQPServerConnection.RunReadLoop;
var
  LFrame: TAMQPFrame;
begin
  try
    try
      SetupTransport; // handshake TLS aqui, se for o caso

      FProtocolOk := ReadProtocolHeader;
      if not FProtocolOk then
        Exit;

      AmqpAtomicWrite64(FLastReadTick, AmqpTickMs);
      SendConnectionStart;
      FState := amqssAwaitStartOk;

      while FState <> amqssClosed do
      begin
        try
          LFrame := TAMQPFrame.ReadFrom(FStream, CurrentMaxPayload);
        except
          on E: EAMQPFrameEOF do
            Break; // peer desconectou — encerramento normal
          on E: EAMQPFrame do
          begin
            // Frame malformado / acima do frame-max: o stream ficou
            // dessincronizado, então avisamos e saímos sem esperar Close-Ok.
            if FState <> amqssClosing then
              try
                SendConnectionClose(AMQP_FRAME_ERROR, E.Message, 0, 0);
              except
              end;
            Break;
          end;
        end;

        AmqpAtomicInc(FFramesRead);
        AmqpAtomicWrite64(FLastReadTick, AmqpTickMs);

        try
          if FState = amqssClosing then
          begin
            // Esperando o Close-Ok: só ele importa (spec 4.2.7).
            if LFrame.IsMethod and (Length(LFrame.Payload) >= 4) then
              HandleMethodFrame(LFrame);
          end
          else
            HandleFrame(LFrame);
        except
          on E: EAMQPChannelError do
            SendChannelClose(E.Channel, E.Code, E.Message, E.ClassId, E.MethodId);
          on E: EAMQPConnectionError do
            if FState <> amqssClosing then
            begin
              SendConnectionClose(E.Code, E.Message, E.ClassId, E.MethodId);
              FState := amqssClosing;
            end;
          on E: EAMQPServerAbort do
          begin
            if FError = '' then
              FError := E.Message;
            FState := amqssClosed;
          end;
          on E: EAMQPTransport do
          begin
            // O writer morreu: não há como falar com o cliente.
            if FError = '' then
              FError := E.Message;
            FState := amqssClosed;
          end;
        end;
      end;
    except
      on E: EAMQPFrame do
        ; // fim de stream fora do loop — normal
      on E: EAMQPTls do
        if FError = '' then
          FError := 'TLS: ' + E.Message;
      on E: EAMQPTransport do
        if FError = '' then
          FError := E.Message;
      on E: Exception do
        if FError = '' then
          FError := E.ClassName + ': ' + E.Message;
    end;
  finally
    FState := amqssClosed;
    // Ordem importa: Stop drena a fila (o Close-Ok que acabamos de postar, por
    // exemplo) e só então derrubamos o TCP. Fechar aqui — e não só no destrutor
    // — é o que a spec manda depois do Close-Ok, e é o que faz o peer sair do
    // recv dele: um cliente que fecha limpo fica pendurado se o socket do
    // servidor continuar aberto até o reaper passar.
    // Antes disso, o Detach: este é o caminho de fechamento NORMAL (o peer
    // mandou Connection.Close), e uma fila pode estar entregando neste exato
    // instante numa thread do pool.
    DetachAllDelivery;
    if FWriter <> nil then
      FWriter.Stop;
    if FSocket <> nil then
      try
        FSocket.Close;
      except
      end;
    ClearChannels;
    if Assigned(FOnClosed) then
      FOnClosed(Self);
  end;
end;

end.

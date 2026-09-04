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
  AMQP.Server.Delivery,
  AMQP.Server.Message,
  AMQP.Server.Queue,
  AMQP.Server.Resources,
  AMQP.Server.Engine,
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
    FEngine: TAMQPEngine;       // WS5: quem realmente declara/roteia/entrega
    FConnId: NativeUInt;        // identidade desta conexao (fila exclusiva)

    // --- canais ---
    FChLock: TCriticalSection;
    FJoinLock: TCriticalSection;
    FJoined: Boolean;
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
    /// Traduz o resultado da engine para o modelo de erro do AMQP. amqerOk
    /// nao faz nada; o resto levanta EAMQPChannelError com o reply-code da
    /// tabela (403/404/405/406) -- todos sao erros de CANAL: a conexao
    /// sobrevive e o cliente pode abrir outro canal.
    procedure CheckEngine(AChannel: TAMQPServerChannel;
      ARes: TAMQPEngineResult; const AWhat: string;
      AClassId, AMethodId: Word);
    /// Levanta 406 PRECONDITION_FAILED quando um x-argument do declare e'
    /// invalido, com o AErro que o parser produziu -- que NOMEIA o argumento
    /// ofensor. Erro de CANAL: config errada num declare nao leva a conexao
    /// junto (mesma linha do amqerTipoInvalido; ver CheckEngine).
    procedure CheckArgs(AChannel: TAMQPServerChannel; AOk: Boolean;
      const AErro: string; AClassId, AMethodId: Word);
    /// Levanta 403 se o nome for reservado ('amq.') ou vazio quando o cliente
    /// nao pode escolher -- spec 1.4: o prefixo e' do servidor.
    procedure CheckReservedName(AChannel: TAMQPServerChannel;
      const AName, AWhat: string; AClassId, AMethodId: Word);
    /// Manda a engine soltar o que este canal segurava (consumidores e
    /// nao-confirmadas voltam para as filas) e destaca o alvo de entrega.
    procedure ReleaseChannelResources(AChannel: TAMQPServerChannel);
    /// Basic.Return + content-header + body de um publish `mandatory` que não
    /// achou rota (spec 1.8.3.6, reply-code 312 NO_ROUTE).
    procedure SendBasicReturn(AChannel: TAMQPServerChannel;
      const AMessage: TAMQPServerMessage);
    procedure SendGetOk(AChannel: TAMQPServerChannel;
      ADeliveryTag: UInt64; ARedelivered: Boolean; AMessageCount: Integer;
      AMessage: TAMQPMessage);
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
    /// A engine do broker (WS5). Atribuida pelo TAMQPServer logo depois de
    /// aceitar a conexao e ANTES do Start. nil = broker sem engine (a Fase 1
    /// respondia os *-Ok sem fazer nada) -- os testes de handshake usam assim.
    property Engine: TAMQPEngine read FEngine write FEngine;
    /// Identidade desta conexao, usada como dono de fila exclusiva.
    property ConnId: NativeUInt read FConnId;
  end;

implementation

var
  // Identidade de conexao (dono de fila exclusiva). Contador monotonico e nao
  // endereco de objeto: endereco reusado casaria uma fila exclusiva orfa com
  // a conexao errada -- mesma razao da identidade de canal em
  // AMQP.Server.Delivery.
  GConnIdSeq: Integer = 0;

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
  FJoinLock := TCriticalSection.Create;
  FChannels := TDictionary<Word, TAMQPServerChannel>.Create;
  FConnId := NativeUInt(Cardinal(AmqpAtomicInc(GConnIdSeq)));
  FThread := TAMQPServerConnThread.Create(Self);
end;

destructor TAMQPServerConnection.Destroy;
begin
  Shutdown;
  if FThread <> nil then
  begin
    WaitFor; // guardado: join uma vez so'
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
  FJoinLock.Free;
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
  if FThread = nil then
    Exit;
  // JOIN UMA VEZ SO' -- mesma razao do TAMQPFrameWriter.Stop: no FPC/Unix o
  // TThread.WaitFor e' pthread_join INCONDICIONAL, e este WaitFor e' chamado
  // pelo TAMQPServer.Stop E pelo destrutor da conexao.
  FJoinLock.Enter;
  try
    if not FJoined then
    begin
      FThread.WaitFor;
      FJoined := True;
    end;
  finally
    FJoinLock.Leave;
  end;
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
  if LCh <> nil then
  begin
    // Antes de liberar: as filas precisam saber que os consumidores deste
    // canal se foram e que as nao-confirmadas dele voltam para a fila.
    ReleaseChannelResources(LCh);
    LCh.Free;
  end;
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
  begin
    ReleaseChannelResources(LAll[I]);
    LAll[I].Free;
  end;
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
  if FEngine <> nil then
    // A topologia deste vhost passa a existir (com os exchanges predefinidos)
    // no momento em que alguem abre nele.
    FEngine.EnsureVHost(FVirtualHost);
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

procedure TAMQPServerConnection.CheckArgs(AChannel: TAMQPServerChannel;
  AOk: Boolean; const AErro: string; AClassId, AMethodId: Word);
begin
  if AOk then
    Exit;
  raise EAMQPChannelError.Create(AChannel.Id, AMQP_PRECONDITION_FAILED,
    AErro, AClassId, AMethodId);
end;

procedure TAMQPServerConnection.CheckEngine(AChannel: TAMQPServerChannel;
  ARes: TAMQPEngineResult; const AWhat: string; AClassId, AMethodId: Word);
var
  LCode: Word;
  LTexto: string;
begin
  if ARes = amqerOk then
    Exit;
  // Erro de CONEXAO, e nao de canal, e por isso fora do case: um broker que
  // nao consegue mais registrar topologia duravel esta' quebrado como um todo
  // (Fase 4, WS3). Continuar respondendo Declare-Ok seria mentir sobre
  // durabilidade; 541 diz a verdade e derruba a conexao.
  if ARes = amqerSemDurabilidade then
    raise EAMQPConnectionError.Create(AMQP_INTERNAL_ERROR,
      'could not journal durable ' + AWhat + ': broker cannot guarantee '
      + 'durability', AClassId, AMethodId);
  case ARes of
    amqerNaoEncontrado:
      begin
        LCode := AMQP_NOT_FOUND;
        LTexto := 'no ' + AWhat;
      end;
    amqerDivergente:
      begin
        LCode := AMQP_PRECONDITION_FAILED;
        LTexto := 'inequivalent arg for ' + AWhat;
      end;
    amqerTipoInvalido:
      begin
        // Desvio deliberado do RabbitMQ, decidido no WS2 e registrado no
        // CLAUDE.md: ele trata tipo de exchange desconhecido como 503
        // COMMAND_INVALID, que derruba a CONEXAO. Aqui e' 406 de canal --
        // config errada de um declare nao precisa levar a conexao junto.
        LCode := AMQP_PRECONDITION_FAILED;
        LTexto := 'invalid type or match for ' + AWhat;
      end;
    amqerEmUso:
      begin
        LCode := AMQP_PRECONDITION_FAILED;
        LTexto := AWhat + ' in use';
      end;
    amqerNaoVazia:
      begin
        LCode := AMQP_PRECONDITION_FAILED;
        LTexto := AWhat + ' not empty';
      end;
    amqerNomeReservado:
      begin
        LCode := AMQP_ACCESS_REFUSED;
        LTexto := 'access to ' + AWhat + ' refused: reserved name';
      end;
    amqerExclusivaDeOutro:
      begin
        LCode := AMQP_RESOURCE_LOCKED;
        LTexto := 'cannot obtain exclusive access to ' + AWhat;
      end;
  else
    LCode := AMQP_PRECONDITION_FAILED;
    LTexto := AWhat;
  end;
  raise EAMQPChannelError.Create(AChannel.Id, LCode, LTexto, AClassId,
    AMethodId);
end;

procedure TAMQPServerConnection.CheckReservedName(
  AChannel: TAMQPServerChannel; const AName, AWhat: string;
  AClassId, AMethodId: Word);
begin
  if AmqpIsReservedName(AName) then
    CheckEngine(AChannel, amqerNomeReservado, AWhat + ' ' + AName,
      AClassId, AMethodId);
end;

procedure TAMQPServerConnection.ReleaseChannelResources(
  AChannel: TAMQPServerChannel);
var
  LNomes: TArray<string>;
  LFila: TAMQPServerQueue;
  I: Integer;
  LId: NativeUInt;
begin
  // DeliveryId e nao Delivery.ChannelId: quando este metodo roda no teardown
  // da conexao, o DetachAllDelivery ja soltou o alvo e Delivery e' nil -- foi
  // exatamente assim que as nao-confirmadas de uma conexao derrubada deixaram
  // de voltar para a fila.
  if (FEngine <> nil) and (AChannel.DeliveryId <> 0) then
  begin
    LId := AChannel.DeliveryId;
    LNomes := AChannel.TouchedQueues;
    for I := 0 to High(LNomes) do
    begin
      LFila := FEngine.FindQueue(FVirtualHost, LNomes[I]);
      if LFila <> nil then
        // Tira os consumidores deste canal e devolve as nao-confirmadas dele
        // para a fila -- o cliente nunca as confirmou.
        LFila.PostRemoveChannel(LId);
    end;
    // WS7, DEPOIS de postar todas as remocoes: uma fila auto-delete que
    // perdeu o ultimo consumidor com este canal some agora. O Stats de dentro
    // do MaybeAutoDeleteQueue e' a barreira sobre o PostRemoveChannel acima.
    for I := 0 to High(LNomes) do
      FEngine.MaybeAutoDeleteQueue(FVirtualHost, LNomes[I]);
  end;
  AChannel.DetachDelivery;
end;

// Basic.Return + content-header + body, num lote indivisivel: o cliente
// remonta a mensagem devolvida exatamente como a publicou.
procedure TAMQPServerConnection.SendBasicReturn(AChannel: TAMQPServerChannel;
  const AMessage: TAMQPServerMessage);
var
  LReturn: TAMQPBasicReturn;
  LMsg: TAMQPMessage;
  LConteudo, LTodos: TArray<TAMQPFrame>;
  I: Integer;
begin
  LReturn.ReplyCode := AMQP_NO_ROUTE;
  LReturn.ReplyText := 'NO_ROUTE';
  LReturn.Exchange := AMessage.Exchange;
  LReturn.RoutingKey := AMessage.RoutingKey;

  // So' para reusar a montagem de conteudo (que le o payload cru do header --
  // decisao D1); esta mensagem nao vai para fila nenhuma.
  LMsg := TAMQPMessage.FromServerMessage(AMessage);
  try
    LConteudo := AmqpBuildContentFrames(AChannel.Id, LMsg, CurrentMaxPayload);
    SetLength(LTodos, 1 + Length(LConteudo));
    LTodos[0] := TAMQPFrame.Create(AMQP_FRAME_METHOD, AChannel.Id,
      BuildBasicReturn(LReturn));
    for I := 0 to High(LConteudo) do
      LTodos[1 + I] := LConteudo[I];
    FWriter.PostFrames(LTodos);
  finally
    LMsg.Release;
  end;
end;

// Basic.Get-Ok + content-header + body, num lote indivisivel.
procedure TAMQPServerConnection.SendGetOk(AChannel: TAMQPServerChannel;
  ADeliveryTag: UInt64; ARedelivered: Boolean; AMessageCount: Integer;
  AMessage: TAMQPMessage);
var
  LGetOk: TAMQPBasicGetOk;
  LConteudo, LTodos: TArray<TAMQPFrame>;
  I: Integer;
begin
  LGetOk.DeliveryTag := ADeliveryTag;
  LGetOk.Redelivered := ARedelivered;
  LGetOk.Exchange := AMessage.Exchange;
  LGetOk.RoutingKey := AMessage.RoutingKey;
  LGetOk.MessageCount := Cardinal(AMessageCount);

  LConteudo := AmqpBuildContentFrames(AChannel.Id, AMessage,
    CurrentMaxPayload);
  SetLength(LTodos, 1 + Length(LConteudo));
  LTodos[0] := TAMQPFrame.Create(AMQP_FRAME_METHOD, AChannel.Id,
    BuildBasicGetOk(LGetOk));
  for I := 0 to High(LConteudo) do
    LTodos[1 + I] := LConteudo[I];
  FWriter.PostFrames(LTodos);
end;

function TAMQPServerConnection.DispatchExchange(AChannel: TAMQPServerChannel;
  const AId: TAMQPMethodId; const AReader: TAMQPReader): Boolean;
var
  LDeclare: TAMQPExchangeDeclare;
  LDelete: TAMQPExchangeDelete;
  LBind: TAMQPExchangeBinding;
  LRes: TAMQPEngineResult;
  LAltNome, LArgErro: string;
  LAltTem: Boolean;
begin
  Result := True;
  case AId.MethodId of
    AMQP_EXCHANGE_DECLARE:
      begin
        LDeclare := DecodeExchangeDeclare(AReader);
        // A tabela de argumentos decodificada e' NOSSA ate' a engine tomar
        // posse dela. Os testes de nome reservado levantam ANTES disso, entao
        // sem este try/finally a tabela vazaria -- foi o que o relatorio de
        // leaks do DUnitX pegou (o FPCUnit nao reporta leak).
        try
          if FEngine <> nil then
          begin
            if not LDeclare.Passive then
            begin
              // Nome reservado ou o exchange default: nenhum dos dois pode ser
              // declarado pelo cliente (spec 1.4). Passive escapa: consultar
              // 'amq.direct' e' legitimo e comum.
              if LDeclare.ExchangeName = '' then
                CheckEngine(AChannel, amqerNomeReservado, 'default exchange',
                  AId.ClassId, AId.MethodId);
              CheckReservedName(AChannel, LDeclare.ExchangeName, 'exchange',
                AId.ClassId, AId.MethodId);
            end;
            // WS2 da Fase 3: os x-arguments sao validados AQUI, antes de a
            // engine tomar posse da tabela -- validacao produz reply-code, e a
            // engine nao conhece reply-code nenhum (invariante da Fase 2).
            // Passive nao valida: consultar um exchange existente nao declara
            // nada, e o cliente costuma mandar a tabela vazia nesse caso.
            if not LDeclare.Passive then
              CheckArgs(AChannel, AmqpParseAlternateExchange(
                LDeclare.Arguments, LAltNome, LAltTem, LArgErro), LArgErro,
                AId.ClassId, AId.MethodId);
            LRes := FEngine.DeclareExchange(FVirtualHost,
              LDeclare.ExchangeName, LDeclare.ExchangeType, LDeclare.Passive,
              LDeclare.Durable, LDeclare.AutoDelete, LDeclare.Internal,
              LDeclare.Arguments);
            LDeclare.Arguments := nil; // a engine tomou posse (em QUALQUER resultado)
            CheckEngine(AChannel, LRes, 'exchange ' + LDeclare.ExchangeName,
              AId.ClassId, AId.MethodId);
          end;
          if not LDeclare.NoWait then
            PostMethod(AChannel.Id, BuildExchangeDeclareOk);
        finally
          LDeclare.Arguments.Free; // so' sobra se ninguem tomou posse
        end;
      end;
    AMQP_EXCHANGE_DELETE:
      begin
        LDelete := DecodeExchangeDelete(AReader);
        if FEngine <> nil then
        begin
          CheckReservedName(AChannel, LDelete.ExchangeName, 'exchange',
            AId.ClassId, AId.MethodId);
          CheckEngine(AChannel,
            FEngine.DeleteExchange(FVirtualHost, LDelete.ExchangeName,
              LDelete.IfUnused),
            'exchange ' + LDelete.ExchangeName,
            AId.ClassId, AId.MethodId);
        end;
        if not LDelete.NoWait then
          PostMethod(AChannel.Id, BuildExchangeDeleteOk);
      end;
    AMQP_EXCHANGE_BIND:
      begin
        LBind := DecodeExchangeBind(AReader);
        try
          if FEngine <> nil then
          begin
            LRes := FEngine.BindExchange(FVirtualHost, LBind.Destination,
              LBind.Source, LBind.RoutingKey, LBind.Arguments);
            LBind.Arguments := nil;
            CheckEngine(AChannel, LRes, 'exchange ' + LBind.Source,
              AId.ClassId, AId.MethodId);
          end;
          if not LBind.NoWait then
            PostMethod(AChannel.Id, BuildExchangeBindOk);
        finally
          LBind.Arguments.Free;
        end;
      end;
    AMQP_EXCHANGE_UNBIND:
      begin
        LBind := DecodeExchangeUnbind(AReader);
        try
          if FEngine <> nil then
          begin
            LRes := FEngine.UnbindExchange(FVirtualHost, LBind.Destination,
              LBind.Source, LBind.RoutingKey, LBind.Arguments);
            LBind.Arguments := nil;
            CheckEngine(AChannel, LRes, 'exchange ' + LBind.Source,
              AId.ClassId, AId.MethodId);
            FEngine.MaybeAutoDeleteExchange(FVirtualHost, LBind.Source);
          end;
          if not LBind.NoWait then
            PostMethod(AChannel.Id, BuildExchangeUnbindOk);
        finally
          LBind.Arguments.Free;
        end;
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
  LMsgs, LCons: Integer;
  LRes: TAMQPEngineResult;
  LPolicy: TAMQPQueuePolicy;
  LArgErro: string;
begin
  Result := True;
  case AId.MethodId of
    AMQP_QUEUE_DECLARE:
      begin
        LDeclare := DecodeQueueDeclare(AReader);
        LName := LDeclare.QueueName;
        LMsgs := 0;
        LCons := 0;
        // Ver o comentario de posse em AMQP_EXCHANGE_DECLARE: as checagens
        // abaixo levantam ANTES de a engine assumir a tabela.
        try
          if FEngine = nil then
          begin
            if LName = '' then
              LName := GeneratedName('amq.gen-');
          end
          else
          begin
            // O nome reservado e' checado ANTES de gerar o nosso, que tambem
            // comeca com 'amq.' e e' legitimo justamente por ser nosso.
            if (LName <> '') and (not LDeclare.Passive) then
              CheckReservedName(AChannel, LName, 'queue',
                AId.ClassId, AId.MethodId);
            if LName = '' then
            begin
              if LDeclare.Passive then
                // Passive sem nome nao tem o que verificar.
                CheckEngine(AChannel, amqerNaoEncontrado, 'queue',
                  AId.ClassId, AId.MethodId);
              LName := GeneratedName('amq.gen-');
            end;
            // WS2 da Fase 3 -- ver o comentario equivalente no Exchange.Declare.
            if not LDeclare.Passive then
              CheckArgs(AChannel, AmqpParseQueuePolicy(LDeclare.Arguments,
                LPolicy, LArgErro), LArgErro, AId.ClassId, AId.MethodId);
            LRes := FEngine.DeclareQueue(FVirtualHost, LName, LDeclare.Passive,
              LDeclare.Durable, LDeclare.Exclusive, LDeclare.AutoDelete,
              LDeclare.Arguments, FConnId, LMsgs, LCons);
            LDeclare.Arguments := nil; // posse transferida
            CheckEngine(AChannel, LRes, 'queue ' + LName,
              AId.ClassId, AId.MethodId);
            // Fila "corrente" do canal: o cliente pode omitir o nome no
            // Basic.Consume/Get e no Queue.Bind (spec 1.7.2.1).
            AChannel.LastQueue := LName;
          end;
          if not LDeclare.NoWait then
            PostMethod(AChannel.Id, BuildQueueDeclareOk(LName, LMsgs, LCons));
        finally
          LDeclare.Arguments.Free;
        end;
      end;
    AMQP_QUEUE_BIND:
      begin
        LBind := DecodeQueueBind(AReader);
        try
          if FEngine <> nil then
          begin
            LName := LBind.QueueName;
            if LName = '' then
              LName := AChannel.LastQueue;
            LRes := FEngine.BindQueue(FVirtualHost, LName, LBind.ExchangeName,
              LBind.RoutingKey, LBind.Arguments, FConnId);
            LBind.Arguments := nil;
            CheckEngine(AChannel, LRes,
              'queue ' + LName + ' or exchange ' + LBind.ExchangeName,
              AId.ClassId, AId.MethodId);
          end;
          if not LBind.NoWait then
            PostMethod(AChannel.Id, BuildQueueBindOk);
        finally
          LBind.Arguments.Free;
        end;
      end;
    AMQP_QUEUE_UNBIND:
      begin
        // queue.unbind nao tem no-wait no 0-9-1: sempre responde.
        LUnbind := DecodeQueueUnbind(AReader);
        try
          if FEngine <> nil then
          begin
            LName := LUnbind.QueueName;
            if LName = '' then
              LName := AChannel.LastQueue;
            LRes := FEngine.UnbindQueue(FVirtualHost, LName,
              LUnbind.ExchangeName, LUnbind.RoutingKey, LUnbind.Arguments,
              FConnId);
            LUnbind.Arguments := nil;
            CheckEngine(AChannel, LRes,
              'queue ' + LName + ' or exchange ' + LUnbind.ExchangeName,
              AId.ClassId, AId.MethodId);
            // WS7: pode ter sido o ultimo binding de um exchange auto-delete.
            FEngine.MaybeAutoDeleteExchange(FVirtualHost,
              LUnbind.ExchangeName);
          end;
          PostMethod(AChannel.Id, BuildQueueUnbindOk);
        finally
          LUnbind.Arguments.Free;
        end;
      end;
    AMQP_QUEUE_PURGE:
      begin
        LPurge := DecodeQueuePurge(AReader);
        LMsgs := 0;
        if FEngine <> nil then
        begin
          LName := LPurge.QueueName;
          if LName = '' then
            LName := AChannel.LastQueue;
          CheckEngine(AChannel,
            FEngine.PurgeQueue(FVirtualHost, LName, FConnId, LMsgs),
            'queue ' + LName, AId.ClassId, AId.MethodId);
        end;
        if not LPurge.NoWait then
          PostMethod(AChannel.Id, BuildQueuePurgeOk(Cardinal(LMsgs)));
      end;
    AMQP_QUEUE_DELETE:
      begin
        LDelete := DecodeQueueDelete(AReader);
        LMsgs := 0;
        if FEngine <> nil then
        begin
          LName := LDelete.QueueName;
          if LName = '' then
            LName := AChannel.LastQueue;
          CheckEngine(AChannel,
            FEngine.DeleteQueue(FVirtualHost, LName, LDelete.IfUnused,
              LDelete.IfEmpty, FConnId, LMsgs),
            'queue ' + LName, AId.ClassId, AId.MethodId);
        end;
        if not LDelete.NoWait then
          PostMethod(AChannel.Id, BuildQueueDeleteOk(Cardinal(LMsgs)));
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
  LGet: TAMQPBasicGet;
  LAck: TAMQPBasicAck;
  LNack: TAMQPBasicNack;
  LReject: TAMQPBasicReject;
  LTag, LFila: string;
  LDeliveryTag: UInt64;
  LFound, LRedelivered: Boolean;
  LMsg: TAMQPMessage;
  LMsgs: Integer;
  LRefs: TArray<TAMQPOutstanding>;
  LQueue: TAMQPServerQueue;
  I: Integer;
begin
  Result := True;
  case AId.MethodId of
    AMQP_BASIC_QOS:
      begin
        LQos := DecodeBasicQos(AReader);
        // prefetch-size (limite em OCTETOS) nao e implementado -- o RabbitMQ
        // tambem nao implementa. Silenciar seria pior que recusar: o cliente
        // acharia que ha um limite de bytes valendo.
        if LQos.PrefetchSize <> 0 then
          raise EAMQPChannelError.Create(AChannel.Id, AMQP_NOT_IMPLEMENTED,
            'prefetch-size not implemented', AId.ClassId, AId.MethodId);
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
        if FEngine <> nil then
        begin
          // no-local (nao me entregue o que EU publiquei) exigiria marcar a
          // origem em cada mensagem; o RabbitMQ tambem nao implementa.
          if LConsume.NoLocal then
            raise EAMQPChannelError.Create(AChannel.Id, AMQP_NOT_IMPLEMENTED,
              'no-local not implemented', AId.ClassId, AId.MethodId);
          // Tag repetida no mesmo canal e erro de CONEXAO pela spec
          // (1.8.3.3, not-allowed), nao de canal.
          if AChannel.ConsumerQueue(LTag, LFila) then
            raise EAMQPConnectionError.Create(AMQP_NOT_ALLOWED,
              'consumer tag already used: ' + LTag, AId.ClassId, AId.MethodId);
          LFila := LConsume.Queue;
          if LFila = '' then
            LFila := AChannel.LastQueue;
          CheckEngine(AChannel,
            FEngine.AddConsumer(FVirtualHost, LFila, LTag, LConsume.NoAck,
              LConsume.Exclusive, AChannel.DeliveryTarget, FConnId),
            'queue ' + LFila, AId.ClassId, AId.MethodId);
          AChannel.AddConsumerTag(LTag, LFila);
        end;
        if not LConsume.NoWait then
          PostMethod(AChannel.Id, BuildBasicConsumeOk(LTag));
      end;
    AMQP_BASIC_CANCEL:
      begin
        LCancel := DecodeBasicCancelArgs(AReader);
        if FEngine <> nil then
        begin
          // Tag desconhecida nao e erro: a spec manda responder Cancel-Ok do
          // mesmo jeito (o consumidor pode ter sumido junto com a fila).
          if AChannel.ConsumerQueue(LCancel.ConsumerTag, LFila) then
          begin
            FEngine.RemoveConsumer(FVirtualHost, LFila, LCancel.ConsumerTag,
              AChannel.Delivery.ChannelId);
            AChannel.RemoveConsumerTag(LCancel.ConsumerTag);
            // WS7: se a fila e' auto-delete e este era o ultimo consumidor,
            // ela some agora.
            FEngine.MaybeAutoDeleteQueue(FVirtualHost, LFila);
          end;
        end;
        if not LCancel.NoWait then
          PostMethod(AChannel.Id, BuildBasicCancelOk(LCancel.ConsumerTag));
      end;
    AMQP_BASIC_PUBLISH:
      begin
        // Sem resposta: o que vem a seguir e o content-header e o body. A
        // partir daqui o canal esta montando e nenhum outro frame pode
        // aparecer nele (ver a regra de interleave em HandleChannelFrame).
        LPublish := DecodeBasicPublish(AReader);
        if (FEngine <> nil) and (LPublish.Exchange <> '')
          and (not FEngine.ExchangeExists(FVirtualHost, LPublish.Exchange)) then
          raise EAMQPChannelError.Create(AChannel.Id, AMQP_NOT_FOUND,
            'no exchange ' + LPublish.Exchange, AId.ClassId, AId.MethodId);
        AChannel.BeginContent(LPublish);
      end;
    AMQP_BASIC_GET:
      begin
        LGet := DecodeBasicGet(AReader);
        if FEngine = nil then
        begin
          PostMethod(AChannel.Id, BuildBasicGetEmpty);
          Exit;
        end;
        LFila := LGet.Queue;
        if LFila = '' then
          LFila := AChannel.LastQueue;
        // A tag e do CANAL, mesmo no Get: sai da mesma sequencia da entrega.
        LDeliveryTag := AChannel.Delivery.NextDeliveryTag;
        CheckEngine(AChannel,
          FEngine.GetMessage(FVirtualHost, LFila, LGet.NoAck, LDeliveryTag,
            AChannel.Delivery.ChannelId, FConnId, LFound, LMsg, LRedelivered,
            LMsgs),
          'queue ' + LFila, AId.ClassId, AId.MethodId);
        if not LFound then
          PostMethod(AChannel.Id, BuildBasicGetEmpty)
        else
          try
            if not LGet.NoAck then
              // Registrado no ALVO, como as entregas a consumidor -- mas sem
              // contar para o prefetch (a spec limita entrega a consumidor).
              AChannel.Delivery.NoteGet(LDeliveryTag, LFila);
            SendGetOk(AChannel, LDeliveryTag, LRedelivered, LMsgs, LMsg);
          finally
            LMsg.Release; // a referencia que o Get entregou a nos
          end;
      end;
    // Ack/Nack/Reject nao respondem nada. Duas coisas acontecem aqui: a baixa
    // do prefetch no alvo (a entrega incrementa; so esta thread ve o metodo do
    // cliente) e o encaminhamento para a FILA que guarda a nao-confirmada -- a
    // tag e do canal, mas a mensagem e da fila.
    AMQP_BASIC_ACK:
      begin
        LAck := DecodeBasicAck(AReader);
        if AChannel.Delivery = nil then
          Exit;
        // NoteResolved faz as duas coisas de uma vez: libera o prefetch e
        // devolve as entregas resolvidas COM a fila de origem de cada uma.
        LRefs := AChannel.Delivery.NoteResolved(LAck.DeliveryTag,
          LAck.Multiple);
        if FEngine <> nil then
          for I := 0 to High(LRefs) do
          begin
            LQueue := FEngine.FindQueue(FVirtualHost, LRefs[I].Queue);
            if LQueue <> nil then
              LQueue.PostAck(AChannel.DeliveryId, LRefs[I].Tag, False);
          end;
      end;
    AMQP_BASIC_NACK:
      begin
        LNack := DecodeBasicNack(AReader);
        if AChannel.Delivery = nil then
          Exit;
        LRefs := AChannel.Delivery.NoteResolved(LNack.DeliveryTag,
          LNack.Multiple);
        if FEngine <> nil then
          for I := 0 to High(LRefs) do
          begin
            LQueue := FEngine.FindQueue(FVirtualHost, LRefs[I].Queue);
            if LQueue <> nil then
              LQueue.PostNack(AChannel.DeliveryId, LRefs[I].Tag, False,
                LNack.Requeue);
          end;
      end;
    AMQP_BASIC_REJECT:
      begin
        LReject := DecodeBasicReject(AReader); // Reject nao tem multiple
        if AChannel.Delivery = nil then
          Exit;
        LRefs := AChannel.Delivery.NoteResolved(LReject.DeliveryTag, False);
        if FEngine <> nil then
          for I := 0 to High(LRefs) do
          begin
            LQueue := FEngine.FindQueue(FVirtualHost, LRefs[I].Queue);
            if LQueue <> nil then
              LQueue.PostReject(AChannel.DeliveryId, LRefs[I].Tag,
                LReject.Requeue);
          end;
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
var
  LMsg: TAMQPServerMessage;
  LSeq: UInt64;
  LRoteada: Boolean;
  LRejeitada: Boolean;
  LTtlMs: Int64;
begin
  try
    LMsg := AChannel.CurrentMessage(FUserId);
    // A propriedade user-id, quando presente, tem de bater com o usuario
    // autenticado -- senao um cliente assinaria mensagem em nome de outro.
    if LMsg.Properties.Has(bpUserId) and (LMsg.Properties.UserId <> FUserId) then
      raise EAMQPChannelError.Create(AChannel.Id, AMQP_ACCESS_REFUSED,
        'user-id property does not match authenticated user',
        AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH);

    // WS4 da Fase 3: 'expiration' e' TTL em ms como STRING decimal (assim a
    // spec define). Malformada e' recusada com 406, e nao ignorada: uma
    // mensagem que o cliente mandou expirar e que vivesse para sempre seria um
    // erro silencioso, e ele nao teria como descobrir. Mesma linha do 406 de
    // x-argument invalido da WS2.
    if LMsg.Properties.Has(bpExpiration) then
      if not AmqpParseExpirationMs(LMsg.Properties.Expiration, LTtlMs) then
        raise EAMQPChannelError.Create(AChannel.Id, AMQP_PRECONDITION_FAILED,
          Format('invalid expiration ''%s'': must be a non-negative integer '
            + 'number of milliseconds, as a decimal string',
            [LMsg.Properties.Expiration]),
          AMQP_CLASS_BASIC, AMQP_BASIC_PUBLISH);

    // WS6: o seq-no e' consumido por TODO publish em confirm mode -- roteado
    // ou nao. E' assim que o cliente correlaciona o ack com o publish que ele
    // proprio numerou; pular numeros quebraria o WaitForConfirms dele.
    LSeq := 0;
    if AChannel.ConfirmMode then
      LSeq := AChannel.NextPublishSeq;

    LRoteada := False;
    LRejeitada := False;
    try
      if FConfig.Sink <> nil then
        LRoteada := FConfig.Sink.RouteMessage(FVirtualHost, LMsg, LRejeitada);
    except
      // Basic.Nack e' reservado a FALHA INTERNA do broker (spec da extensao:
      // "the broker could not handle the message"). Nao e' o caso de mensagem
      // sem rota, que e' sucesso e leva ack.
      on E: Exception do
      begin
        if AChannel.ConfirmMode then
          PostMethod(AChannel.Id, BuildBasicNack(LSeq, False, False));
        raise;
      end;
    end;

    // ORDEM: o Return sai ANTES do Ack. O cliente pode liberar o estado do
    // publish assim que o ack chega (o WaitForConfirms desta lib faz isso);
    // se o ack viesse primeiro, o return chegaria para um publish que o
    // cliente ja considera resolvido.
    if (not LRoteada) and LMsg.Mandatory then
      SendBasicReturn(AChannel, LMsg);

    if AChannel.ConfirmMode then
      // WS6: fila cheia declarada com x-overflow reject-publish e' o UNICO
      // caso, alem de falha interna, em que um publish leva Nack em vez de
      // Ack. Mensagem sem rota continua sendo sucesso.
      //
      // Se a mensagem ia para varias filas e so' uma recusou, o publish e'
      // nack-ado mesmo assim -- e as outras JA' RECEBERAM. E' o comportamento
      // do RabbitMQ, e a alternativa (desfazer o que ja entrou) nao existe sem
      // transacao. O publicador tem de tratar o nack como "pode ter entregue
      // em parte", que e' o contrato de confirms de qualquer jeito.
      if LRejeitada then
        PostMethod(AChannel.Id, BuildBasicNack(LSeq, False, False))
      else
        PostMethod(AChannel.Id, BuildBasicAck(LSeq, False));
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
    // WS7: fila exclusiva pertence a UMA conexao e morre com ela (spec
    // 1.7.2.1). Depois do ClearChannels, que ja soltou consumidores e
    // devolveu as nao-confirmadas.
    if FEngine <> nil then
      try
        FEngine.DeleteExclusiveQueuesOf(FVirtualHost, FConnId);
      except
        on E: Exception do
          ; // teardown nao pode falhar por causa disto
      end;
    if Assigned(FOnClosed) then
      FOnClosed(Self);
  end;
end;

end.

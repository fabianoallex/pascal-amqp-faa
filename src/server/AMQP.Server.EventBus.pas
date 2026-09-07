unit AMQP.Server.EventBus;

{$I amqp.inc}

{ Transporte dos eventos de observabilidade (Fase 4.1, D30-D33, D35).

  UM ring limitado e UMA thread notificadora, e TODO evento passa por aqui --
  inclusive os que nascem na thread de leitura da conexão. A D30 rejeitou as
  três alternativas, cada uma por um motivo próprio:

  - worker do AmqpPool: é o MESMO pool que roda os atores das filas, então um
    handler lento starva o ator. Seria a D2 violada por via indireta, que é a
    pior forma -- a que não aparece no código que a viola;
  - thread monitora: poria heartbeat, prazo de Close-Ok, varredura de TTL e
    reap de conexão morta atrás de um handler de usuário;
  - inline na thread de leitura: trava o processamento de frames daquela
    conexão, HEARTBEAT INCLUSO, até o cliente derrubá-la por timeout.

  A tentação era chamar os eventos de ciclo de vida inline (eles nascem na
  thread de leitura, parecem baratos) e marshalar só os do ator. Não existe
  metade segura: o que muda entre as três é o raio do estrago, não se há
  estrago. Uniformizar dá um contrato, uma ordem e uma política de exceção --
  e o record flat da D34 faz o assíncrono custar uma cópia por valor.

  CONTRATO DE ENTREGA (D31), em três linhas:

  - o emissor NUNCA bloqueia e NUNCA levanta. Ring cheio => descarta e conta;
  - descarta o MAIS NOVO, não a cabeça. O que entrou no ring é entregue,
    então o handler vê um PREFIXO CONTÍGUO da ordem de emissão com lacunas
    contadas em DroppedCount. Descartar a cabeça reescreveria história em
    silêncio -- e é justamente o que a D7 faz na memória, onde a mensagem
    mais nova é a que importa. Aqui é o contrário;
  - a ordem prometida é POR ORIGEM (por conexão, por fila). O consumidor
    único dá de fato uma ordem total de aceitação no ring, mas entre duas
    threads essa ordem não significa nada e não vira promessa de API.

  Fecha a trinca das três pressões desta codebase: memória descarta a cabeça
  (D7), disco RECUSA (D26), evento descarta o mais novo. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Threading,
  AMQP.Server.Events,
  AMQP.Server.Types;

type
  TAMQPEventBus = class;

  { A notificadora. Mesma forma da TAMQPJournalThread e da
    TAMQPMonitorThread: o laço vive no objeto dono, a thread só o repete. }
  TAMQPEventNotifierThread = class(TThread)
  private
    FBus: TAMQPEventBus;
  protected
    procedure Execute; override;
  public
    constructor Create(ABus: TAMQPEventBus);
  end;

  { Um assinante: o método e a máscara de tipos que ele pediu. }
  TAMQPEventSubscription = record
    Handler: TAMQPServerEventHandler;
    Mask: Cardinal;
  end;

  { O barramento. Dono: o TAMQPServer, que o cria no constructor (para o
    Subscribe funcionar antes do Start) e o destrói no destructor.

    Refcount da interface é no-op, como no TAMQPEngine -- e pela mesma razão:
    quem manda no tempo de vida é o servidor, não a contagem. }
  TAMQPEventBus = class(TInterfacedObject, IAMQPEventSink)
  private
    // --- ring + contadores (sob FMon) ---
    FMon: TAMQPMonitor;
    FRing: array of TAMQPServerEvent;
    FCapacity: Integer;
    FHead: Integer;       // próximo a sair
    FTail: Integer;       // próximo a entrar
    FCount: Integer;      // ocupação
    FStopping: Boolean;
    FDispatching: Boolean; // notificadora dentro de um handler
    FEmitted: Int64;
    FDropped: Int64;
    FFailed: Int64;
    FLastFailure: string;
    FThread: TAMQPEventNotifierThread;

    // --- assinantes (sob FSubsLock) ---
    FSubsLock: TCriticalSection;
    FSubs: TList<TAMQPEventSubscription>;
    FMask: Integer; // atômico: união das máscaras, lida sem lock por Wants

    procedure RebuildMaskLocked;
    procedure SetCapacity(AValue: Integer);
    function TakeBatch(out ABatch: TArray<TAMQPServerEvent>): Boolean;
    procedure DispatchBatch(const ABatch: TArray<TAMQPServerEvent>);
  protected
    // IAMQPEventSink -- refcount desligado (ver o cabeçalho da classe). A
    // convenção de chamada do IInterface depende da PLATAFORMA no FPC:
    // stdcall no Windows, cdecl no Unix (CLAUDE.md).
    function _AddRef: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
    function _Release: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
  public
    constructor Create;
    destructor Destroy; override;

    // --- IAMQPEventSink ---
    /// True se ALGUM assinante quer este tipo. Leitura atômica de uma
    /// máscara, sem lock: é o que faz o broker sem assinante não pagar nada.
    function Wants(AType: TAMQPServerEventType): Boolean;
    /// Enfileira. Nunca bloqueia, nunca levanta, pode descartar (D31).
    procedure Emit(const AEvent: TAMQPServerEvent);

    // --- assinatura ---
    /// Assina todos os tipos.
    procedure Subscribe(AHandler: TAMQPServerEventHandler); overload;
    /// Assina só os tipos pedidos. Assinar o mesmo método duas vezes
    /// SUBSTITUI a máscara anterior em vez de duplicar a entrega.
    procedure Subscribe(AHandler: TAMQPServerEventHandler;
      const ATypes: TAMQPServerEventTypes); overload;
    /// Remove. É OBRIGATÓRIO chamar antes de destruir o objeto dono do
    /// método (D33): um handler órfão vira AV dentro da notificadora, que a
    /// conta em FailedCount e segue -- mas o evento se perde e o sintoma
    /// aparece longe da causa.
    procedure Unsubscribe(AHandler: TAMQPServerEventHandler);
    function SubscriberCount: Integer;

    // --- ciclo de vida (chamados pelo TAMQPServer) ---
    procedure Start;
    /// Para a notificadora DEPOIS de entregar o que já está no ring. Por isso
    /// o servidor a derruba por último: os eventos de fechamento de conexão e
    /// de canal nascem durante o teardown e ainda são entregues.
    procedure Stop;

    /// Espera o ring esvaziar E o handler corrente terminar. É a barreira da
    /// D35 -- teste espera por ela, não por Sleep. False = estourou o prazo.
    /// NÃO chamar de dentro de um handler (esperaria por si mesmo).
    function Drain(ATimeoutMs: Cardinal): Boolean;

    /// Quantos eventos entraram no ring.
    function EmittedCount: Int64;
    /// Quantos foram descartados por ring cheio (D31). É a única forma de o
    /// observador saber que perdeu alguma coisa.
    function DroppedCount: Int64;
    /// Quantas exceções de handler foram capturadas (D33).
    function FailedCount: Int64;
    /// Classe e mensagem da última exceção de handler ('' se nenhuma).
    function LastFailure: string;

    /// Tamanho do ring, em eventos. Só pode mudar antes do Start.
    property Capacity: Integer read FCapacity write SetCapacity;
  end;

implementation

{ Compara dois métodos de objeto. TList<T>.IndexOf exigiria um comparer para
  record; o laço manual é mais curto que o comparer. }
function SameHandler(const A, B: TAMQPServerEventHandler): Boolean;
begin
  Result := (TMethod(A).Code = TMethod(B).Code)
        and (TMethod(A).Data = TMethod(B).Data);
end;

{ TAMQPEventNotifierThread }

constructor TAMQPEventNotifierThread.Create(ABus: TAMQPEventBus);
begin
  // O campo ANTES do inherited: com Create(False) a thread pode já estar
  // rodando quando o constructor volta.
  FBus := ABus;
  inherited Create(False);
  {$IFDEF FPC}
  NameThreadForDebugging('amqp-events');
  {$ELSE}
  NameThreadForDebugging('amqp-events', ThreadID);
  {$ENDIF}
end;

procedure TAMQPEventNotifierThread.Execute;
var
  LBatch: TArray<TAMQPServerEvent>;
begin
  while FBus.TakeBatch(LBatch) do
  begin
    if Length(LBatch) > 0 then
      FBus.DispatchBatch(LBatch);
    LBatch := nil;
  end;
end;

{ TAMQPEventBus }

constructor TAMQPEventBus.Create;
begin
  inherited Create;
  FMon := TAMQPMonitor.Create;
  FSubsLock := TCriticalSection.Create;
  FSubs := TList<TAMQPEventSubscription>.Create;
  FCapacity := AMQP_EVENT_QUEUE_CAPACITY;
  SetLength(FRing, FCapacity);
end;

destructor TAMQPEventBus.Destroy;
begin
  Stop;
  FSubs.Free;
  FSubsLock.Free;
  FMon.Free;
  FRing := nil;
  inherited Destroy;
end;

function TAMQPEventBus._AddRef: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
begin
  Result := -1; // sem refcount: o dono e' o TAMQPServer
end;

function TAMQPEventBus._Release: Integer; {$IFDEF AMQP_WINDOWS}stdcall{$ELSE}cdecl{$ENDIF};
begin
  Result := -1;
end;

procedure TAMQPEventBus.SetCapacity(AValue: Integer);
begin
  if AValue < 1 then
    AValue := 1;
  if FThread <> nil then
    raise EAMQPServerAbort.Create(
      'EventQueueCapacity so pode mudar antes do Start');
  FCapacity := AValue;
  SetLength(FRing, FCapacity);
  FHead := 0;
  FTail := 0;
  FCount := 0;
end;

{ --- assinatura ---------------------------------------------------------- }

procedure TAMQPEventBus.RebuildMaskLocked;
var
  I: Integer;
  LMask: Cardinal;
begin
  LMask := 0;
  for I := 0 to FSubs.Count - 1 do
    LMask := LMask or FSubs[I].Mask;
  // Publicacao atomica: Wants le' sem lock nenhum, no caminho quente.
  AmqpAtomicSet(FMask, Integer(LMask));
end;

procedure TAMQPEventBus.Subscribe(AHandler: TAMQPServerEventHandler);
begin
  Subscribe(AHandler, AMQP_EVENT_ALL_TYPES);
end;

procedure TAMQPEventBus.Subscribe(AHandler: TAMQPServerEventHandler;
  const ATypes: TAMQPServerEventTypes);
var
  I: Integer;
  LSub: TAMQPEventSubscription;
  LFound: Boolean;
begin
  if not Assigned(AHandler) then
    Exit;
  LSub.Handler := AHandler;
  LSub.Mask := AmqpEventMask(ATypes);
  FSubsLock.Enter;
  try
    LFound := False;
    for I := 0 to FSubs.Count - 1 do
      if SameHandler(FSubs[I].Handler, AHandler) then
      begin
        // Re-assinar SUBSTITUI a mascara. Duplicar a entrega seria a
        // surpresa: o mesmo evento chegaria duas vezes ao mesmo metodo.
        FSubs[I] := LSub;
        LFound := True;
        Break;
      end;
    if not LFound then
      FSubs.Add(LSub);
    RebuildMaskLocked;
  finally
    FSubsLock.Leave;
  end;
end;

procedure TAMQPEventBus.Unsubscribe(AHandler: TAMQPServerEventHandler);
var
  I: Integer;
begin
  FSubsLock.Enter;
  try
    for I := FSubs.Count - 1 downto 0 do
      if SameHandler(FSubs[I].Handler, AHandler) then
        FSubs.Delete(I);
    RebuildMaskLocked;
  finally
    FSubsLock.Leave;
  end;
end;

function TAMQPEventBus.SubscriberCount: Integer;
begin
  FSubsLock.Enter;
  try
    Result := FSubs.Count;
  finally
    FSubsLock.Leave;
  end;
end;

{ --- emissao ------------------------------------------------------------- }

function TAMQPEventBus.Wants(AType: TAMQPServerEventType): Boolean;
begin
  Result := (AmqpAtomicGet(FMask)
    and Integer(Cardinal(1) shl Ord(AType))) <> 0;
end;

procedure TAMQPEventBus.Emit(const AEvent: TAMQPServerEvent);
begin
  FMon.Enter;
  try
    // Depois do Stop nao ha quem drene: aceitar seria vazar o evento (e as
    // strings dele) ate' o destructor.
    if FStopping then
    begin
      Inc(FDropped);
      Exit;
    end;
    if FCount >= FCapacity then
    begin
      // D31: descarta o MAIS NOVO. Este Exit e' o contrato inteiro -- o
      // emissor volta na hora, sem bloquear e sem levantar, e a lacuna fica
      // contada. Trocar por uma espera aqui poria o ator (ou a thread de
      // leitura) atras de um handler de usuario, que e' a D2 pelo avesso.
      Inc(FDropped);
      Exit;
    end;
    FRing[FTail] := AEvent; // copia por valor; strings sobem o refcount
    FTail := (FTail + 1) mod FCapacity;
    Inc(FCount);
    Inc(FEmitted);
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
end;

{ --- consumo ------------------------------------------------------------- }

function TAMQPEventBus.TakeBatch(out ABatch: TArray<TAMQPServerEvent>): Boolean;
var
  LN, I: Integer;
begin
  ABatch := nil;
  Result := True;
  FMon.Enter;
  try
    while (FCount = 0) and (not FStopping) do
      FMon.Wait(100);

    LN := FCount;
    if LN = 0 then
    begin
      // Ring vazio E parada pedida: unico caminho de saida da thread, e ele
      // so' acontece depois de tudo que entrou ter saido. E' o que faz o
      // Stop entregar os eventos de teardown em vez de descarta-los.
      if FStopping then
        Exit(False);
      Exit(True);
    end;

    // Tira TUDO de uma vez. O que chegar durante o despacho e' o proximo
    // lote -- mesma forma do group commit do journal (D25), pelo mesmo
    // motivo: o lote se ajusta a' carga sozinho, sem parametro nenhum.
    SetLength(ABatch, LN);
    for I := 0 to LN - 1 do
    begin
      ABatch[I] := FRing[FHead];
      // Solta as strings do slot: sem isto o ring segura as ultimas
      // FCapacity mensagens vivas para sempre.
      FRing[FHead] := Default(TAMQPServerEvent);
      FHead := (FHead + 1) mod FCapacity;
    end;
    FCount := 0;
    FDispatching := True;
    FMon.PulseAll; // acorda quem esperava vaga ou dreno
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPEventBus.DispatchBatch(const ABatch: TArray<TAMQPServerEvent>);
var
  LSubs: TArray<TAMQPEventSubscription>;
  I, J: Integer;
  LBit: Cardinal;
  LFailed: Int64;
  LLastFailure: string;
begin
  LFailed := 0;
  LLastFailure := '';
  try
    // Snapshot sob lock, chamada FORA dele: o handler pode assinar,
    // desassinar, ou demorar o quanto quiser.
    FSubsLock.Enter;
    try
      SetLength(LSubs, FSubs.Count);
      for I := 0 to FSubs.Count - 1 do
        LSubs[I] := FSubs[I];
    finally
      FSubsLock.Leave;
    end;

    for I := 0 to High(ABatch) do
    begin
      LBit := Cardinal(1) shl Ord(ABatch[I].EventType);
      for J := 0 to High(LSubs) do
      begin
        if (LSubs[J].Mask and LBit) = 0 then
          Continue;
        try
          LSubs[J].Handler(ABatch[I]);
        except
          // D33: NUNCA "except end". A notificadora nao pode morrer por causa
          // de um handler, e a exceção nao pode sumir. Conta, guarda a
          // ultima, segue para o proximo handler e para o proximo evento.
          //
          // Capturar Exception pega EAccessViolation nos dois compiladores --
          // que e' o modo de falha REAL aqui: handler cujo dono foi destruido
          // sem Unsubscribe. FailedCount e' como isso se descobre.
          on E: Exception do
          begin
            Inc(LFailed);
            LLastFailure := E.ClassName + ': ' + E.Message;
          end;
        end;
      end;
    end;
  finally
    // O finally e' o que garante que FDispatching cai mesmo que algo acima
    // escape -- senao o Drain esperaria para sempre.
    FMon.Enter;
    try
      FDispatching := False;
      if LFailed > 0 then
      begin
        Inc(FFailed, LFailed);
        FLastFailure := LLastFailure;
      end;
      FMon.PulseAll;
    finally
      FMon.Leave;
    end;
  end;
end;

{ --- ciclo de vida ------------------------------------------------------- }

procedure TAMQPEventBus.Start;
begin
  if FThread <> nil then
    Exit;
  FMon.Enter;
  try
    FStopping := False;
  finally
    FMon.Leave;
  end;
  FThread := TAMQPEventNotifierThread.Create(Self);
end;

procedure TAMQPEventBus.Stop;
begin
  if FThread = nil then
    Exit;
  FMon.Enter;
  try
    FStopping := True;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
  FThread.WaitFor;
  // ANULAR logo apos o join: TThread.WaitFor NAO e' idempotente no FPC/Unix
  // (pthread_join incondicional -- CLAUDE.md). FreeAndNil fecha essa porta.
  FreeAndNil(FThread);
end;

function TAMQPEventBus.Drain(ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := AmqpTickMs + ATimeoutMs;
  FMon.Enter;
  try
    // Re-checa a condicao em laco com deadline: o TAMQPMonitor pode acordar
    // espuriamente (contrato dele).
    while ((FCount > 0) or FDispatching) and (AmqpTickMs < LDeadline) do
      FMon.Wait(20);
    Result := (FCount = 0) and (not FDispatching);
  finally
    FMon.Leave;
  end;
end;

{ --- contadores ---------------------------------------------------------- }

function TAMQPEventBus.EmittedCount: Int64;
begin
  FMon.Enter;
  try
    Result := FEmitted;
  finally
    FMon.Leave;
  end;
end;

function TAMQPEventBus.DroppedCount: Int64;
begin
  FMon.Enter;
  try
    Result := FDropped;
  finally
    FMon.Leave;
  end;
end;

function TAMQPEventBus.FailedCount: Int64;
begin
  FMon.Enter;
  try
    Result := FFailed;
  finally
    FMon.Leave;
  end;
end;

function TAMQPEventBus.LastFailure: string;
begin
  FMon.Enter;
  try
    Result := FLastFailure;
  finally
    FMon.Leave;
  end;
end;

end.

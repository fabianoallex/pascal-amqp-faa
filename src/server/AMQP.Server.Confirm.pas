unit AMQP.Server.Confirm;

{$I amqp.inc}

{ Confirms adiados pela marca d'agua de durabilidade -- sub-modulo broker,
  WS5 da Fase 4. E' a D24 virando codigo.

  O problema
  ----------
  Ate' a Fase 3 o Basic.Ack de um publish saia logo depois do RouteMessage, e
  isso estava CERTO enquanto tudo era memoria: o enfileiramento e' sincrono,
  entao quando o RouteMessage volta a mensagem ja' esta' onde tem de estar.
  Com durabilidade ligada, "onde tem de estar" passa a ser o DISCO, e o disco
  so' responde depois do fsync -- que a thread do journal faz em lote (D25).
  Confirmar antes disso seria mentir: o publicador liberaria o estado do
  publish achando que a mensagem sobrevive a uma queda, e ela nao sobreviveria.

  O desenho
  ---------
  Quem publica escreve o lote e guarda o LSN do ultimo registro (isso e' a
  engine, na thread do publicador -- ver AMQP.Server.Engine.RouteMessage). O
  canal registra AQUI o par (seq, lsn) e NAO emite nada. Depois de cada fsync,
  a thread do journal avanca a marca d'agua e chama Release em todo rastreador
  registrado; cada um solta o que ja' esta' duravel e COLAPSA num unico
  Basic.Ack(maiorSeq, multiple=true) -- a letra da D9.

  Tres coisas que o desenho obriga, e que estao neste arquivo:

  1. ORDEM DE SEQ POR CANAL (corolario (a) da D24). Um publish transiente que
     chega DEPOIS de um persistente ainda pendente nao pode passar na frente:
     ele tambem e' adiado (com Lsn=0) e sai colapsado no mesmo multiple=true.
     Por isso TryDefer adia quando ha pendente, e nao so' quando ha LSN.

  2. NUNCA BLOQUEAR A THREAD DO JOURNAL. Release usa TryPostFrames, e o que a
     fila de escrita recusar FICA pendente para a proxima rodada -- a forma da
     D3 aplicada ao confirm. Como um canal com a saida cheia poderia nunca mais
     ver a marca andar (se ninguem mais publicasse), a thread monitora chama
     RetryPending a cada tique.

  3. CICLO DE VIDA. Quem libera e' a thread do journal; o canal pode ter
     morrido no meio. Mesmo remedio do TAMQPChannelDeliveryTarget: refcount
     mantem o objeto vivo enquanto o registro o tiver na mao, e Detach zera o
     writer SOB LOCK, sem esperar I/O.

  Nack no meio da fila
  --------------------
  Um Basic.Nack nao pode ser colapsado com multiple=true junto de acks: ele
  marca UM seq. Entao a liberacao emite o ack acumulado ATE' o seq anterior,
  depois o nack sozinho, e recomeca a acumular. Nack nao espera disco (Lsn=0):
  ele nao promete durabilidade nenhuma -- so' precisa respeitar a ordem. }

interface

uses
  SysUtils,
  SyncObjs,
  Generics.Collections,
  AMQP.Protocol,
  AMQP.Frame,
  AMQP.Basic.Methods,
  AMQP.Threading,
  AMQP.Server.FrameIO,
  AMQP.Server.Types,
  AMQP.Server.Journal;

type
  { Um confirm que ainda nao saiu. }
  TAMQPPendingConfirm = record
    /// O seq-no que o cliente atribuiu a este publish.
    Seq: UInt64;
    /// O LSN que precisa estar duravel para ele poder ser confirmado. Zero =
    /// nada a esperar do disco (so' a ordem).
    Lsn: UInt64;
    /// True => sai como Basic.Nack (fila cheia com reject-publish, ou journal
    /// falho), e portanto sozinho, sem multiple.
    Nack: Boolean;
  end;

  { Rastreador de UM canal. Criado pelo registro, referenciado pelo canal. }
  TAMQPConfirmTracker = class(TInterfacedObject, IAMQPConfirmTracker)
  private
    FLock: TCriticalSection;
    FWriter: TAMQPFrameWriter; // nil depois do Detach -- sempre sob FLock
    FChannelNo: Word;
    // Pendentes em ordem CRESCENTE de seq. Como o seq-no e' monotonico por
    // canal e so' a thread de leitura da conexao registra, inserir no fim ja'
    // mantem ordenado -- nao ha ordenacao a fazer.
    FPend: TList<TAMQPPendingConfirm>;
    function Posta(const APayload: TBytes): Boolean;
  public
    constructor Create(AWriter: TAMQPFrameWriter; AChannelNo: Word);
    destructor Destroy; override;

    // --- IAMQPConfirmTracker ---
    function TryDefer(ASeq, ALsn: UInt64; ANack: Boolean): Boolean;
    procedure Release(AMarca: UInt64);
    procedure FailAll;
    procedure Detach;
    function PendingCount: Integer;
  end;

  { O registro do broker inteiro, e o sink que a thread do journal chama. }
  TAMQPConfirmRegistry = class(TInterfacedObject, IAMQPConfirmRegistry,
    IAMQPDurabilitySink)
  private
    FLock: TCriticalSection;
    FTrackers: TList<IAMQPConfirmTracker>;
    FMarca: UInt64; // atomico (Read64/Write64) -- lido sem lock
    /// Uma copia da lista, tomada sob lock, para percorrer FORA dele: a
    /// liberacao chama TryPostFrames, e segurar o lock do registro enquanto
    /// se escreve em N canais poria a thread do journal atras de todos eles.
    function Snapshot: TArray<IAMQPConfirmTracker>;
    /// Manda todo rastreador soltar o que ja' pode sair com a marca em ALsn.
    /// Separado do Durable de proposito: o Durable AVANCA a marca (e so' a
    /// thread do journal o chama), enquanto isto so' LIBERA -- e' o que a
    /// thread monitora precisa, e escrever a marca de la' a faria andar para
    /// TRAS quando as duas threads se cruzassem.
    procedure LiberaTodos(ALsn: UInt64);
  public
    constructor Create;
    destructor Destroy; override;

    // --- IAMQPConfirmRegistry ---
    function CreateTracker(AWriter: TAMQPFrameWriter;
      AChannelNo: Word): IAMQPConfirmTracker;
    procedure Unregister(const ATracker: IAMQPConfirmTracker);
    function Watermark: UInt64;
    function TrackerCount: Integer;
    procedure RetryPending;

    // --- IAMQPDurabilitySink (chamado pela thread do journal) ---
    procedure Durable(ALsn: UInt64);
    procedure JournalFailed(const AMessage: string);
  end;

implementation

{ TAMQPConfirmTracker }

constructor TAMQPConfirmTracker.Create(AWriter: TAMQPFrameWriter;
  AChannelNo: Word);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FWriter := AWriter;
  FChannelNo := AChannelNo;
  FPend := TList<TAMQPPendingConfirm>.Create;
end;

destructor TAMQPConfirmTracker.Destroy;
begin
  FPend.Free;
  FLock.Free;
  inherited;
end;

// Chamado SOB FLock. Nao bloqueia: o que a fila de escrita recusar volta como
// False e o pendente fica onde esta'.
function TAMQPConfirmTracker.Posta(const APayload: TBytes): Boolean;
begin
  Result := (FWriter <> nil)
    and FWriter.TryPostFrames([TAMQPFrame.Create(AMQP_FRAME_METHOD,
      FChannelNo, APayload)]);
end;

function TAMQPConfirmTracker.TryDefer(ASeq, ALsn: UInt64;
  ANack: Boolean): Boolean;
var
  LItem: TAMQPPendingConfirm;
begin
  FLock.Enter;
  try
    if FWriter = nil then
    begin
      // Canal morto: nao adia nada. O chamador tambem nao vai conseguir
      // emitir, e e' exatamente isso que tem de acontecer.
      Result := False;
      Exit;
    end;
    // O corolario (a) da D24: com pendente na frente, ATE' o transiente espera.
    Result := (ALsn > 0) or (FPend.Count > 0);
    if not Result then
      Exit;
    LItem.Seq := ASeq;
    LItem.Lsn := ALsn;
    LItem.Nack := ANack;
    FPend.Add(LItem);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmTracker.Release(AMarca: UInt64);
var
  I, LFeitos, LIdxAck: Integer;
  LTemAck: Boolean;
  LUltimoAck: UInt64;
begin
  FLock.Enter;
  try
    if FWriter = nil then
    begin
      FPend.Clear; // canal morto: ninguem mais espera estes acks
      Exit;
    end;
    LFeitos := 0;
    LTemAck := False;
    LUltimoAck := 0;
    LIdxAck := -1;
    I := 0;
    while (I < FPend.Count) and (FPend[I].Lsn <= AMarca) do
    begin
      if FPend[I].Nack then
      begin
        // O nack corta o colapso: o que estava acumulado sai primeiro, com o
        // seq ANTERIOR, e o nack sai sozinho.
        if LTemAck then
        begin
          if not Posta(BuildBasicAck(LUltimoAck, True)) then
            Break;
          LFeitos := LIdxAck + 1;
          LTemAck := False;
        end;
        if not Posta(BuildBasicNack(FPend[I].Seq, False, False)) then
          Break;
        LFeitos := I + 1;
      end
      else
      begin
        // Acumula: um unico Basic.Ack com multiple=true resolve todos.
        LTemAck := True;
        LUltimoAck := FPend[I].Seq;
        LIdxAck := I;
      end;
      Inc(I);
    end;
    // Aninhado, e nao 'and': o Posta tem EFEITO, e depender de curto-circuito
    // para nao o chamar seria depender de uma diretiva de compilacao.
    if LTemAck then
      if Posta(BuildBasicAck(LUltimoAck, True)) then
        LFeitos := LIdxAck + 1;
    for I := 1 to LFeitos do
      FPend.Delete(0);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmTracker.FailAll;
var
  I, LFeitos: Integer;
begin
  FLock.Enter;
  try
    if FWriter = nil then
    begin
      FPend.Clear;
      Exit;
    end;
    // Journal falho: a promessa nao pode ser cumprida, entao TUDO que estava
    // esperando disco vira nack -- inclusive o que ja' estaria duravel, porque
    // a marca nao vai mais andar e o publicador ficaria pendurado. Nack e' o
    // frame honesto aqui: "o broker nao conseguiu tratar a mensagem".
    LFeitos := 0;
    for I := 0 to FPend.Count - 1 do
    begin
      if not Posta(BuildBasicNack(FPend[I].Seq, False, False)) then
        Break;
      LFeitos := I + 1;
    end;
    for I := 1 to LFeitos do
      FPend.Delete(0);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmTracker.Detach;
begin
  FLock.Enter;
  try
    FWriter := nil;
    FPend.Clear;
  finally
    FLock.Leave;
  end;
end;

function TAMQPConfirmTracker.PendingCount: Integer;
begin
  FLock.Enter;
  try
    Result := FPend.Count;
  finally
    FLock.Leave;
  end;
end;

{ TAMQPConfirmRegistry }

constructor TAMQPConfirmRegistry.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FTrackers := TList<IAMQPConfirmTracker>.Create;
  FMarca := 0;
end;

destructor TAMQPConfirmRegistry.Destroy;
begin
  FTrackers.Free;
  FLock.Free;
  inherited;
end;

function TAMQPConfirmRegistry.Snapshot: TArray<IAMQPConfirmTracker>;
var
  I: Integer;
begin
  FLock.Enter;
  try
    SetLength(Result, FTrackers.Count);
    for I := 0 to FTrackers.Count - 1 do
      Result[I] := FTrackers[I];
  finally
    FLock.Leave;
  end;
end;

function TAMQPConfirmRegistry.CreateTracker(AWriter: TAMQPFrameWriter;
  AChannelNo: Word): IAMQPConfirmTracker;
begin
  Result := TAMQPConfirmTracker.Create(AWriter, AChannelNo);
  FLock.Enter;
  try
    FTrackers.Add(Result);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmRegistry.Unregister(const ATracker: IAMQPConfirmTracker);
var
  I: Integer;
begin
  if ATracker = nil then
    Exit;
  FLock.Enter;
  try
    for I := FTrackers.Count - 1 downto 0 do
      if FTrackers[I] = ATracker then
        FTrackers.Delete(I);
  finally
    FLock.Leave;
  end;
end;

function TAMQPConfirmRegistry.Watermark: UInt64;
begin
  Result := AmqpAtomicRead64(FMarca);
end;

function TAMQPConfirmRegistry.TrackerCount: Integer;
begin
  FLock.Enter;
  try
    Result := FTrackers.Count;
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPConfirmRegistry.LiberaTodos(ALsn: UInt64);
var
  LTrackers: TArray<IAMQPConfirmTracker>;
  I: Integer;
begin
  LTrackers := Snapshot;
  for I := 0 to High(LTrackers) do
    LTrackers[I].Release(ALsn);
end;

procedure TAMQPConfirmRegistry.RetryPending;
begin
  // NAO passa pelo Durable: recobrar nao e' avancar a marca.
  LiberaTodos(Watermark);
end;

// --- IAMQPDurabilitySink: roda na THREAD DO JOURNAL, depois de cada fsync ---

procedure TAMQPConfirmRegistry.Durable(ALsn: UInt64);
begin
  AmqpAtomicWrite64(FMarca, ALsn);
  LiberaTodos(ALsn);
end;

procedure TAMQPConfirmRegistry.JournalFailed(const AMessage: string);
var
  LTrackers: TArray<IAMQPConfirmTracker>;
  I: Integer;
begin
  LTrackers := Snapshot;
  for I := 0 to High(LTrackers) do
    LTrackers[I].FailAll;
end;

end.

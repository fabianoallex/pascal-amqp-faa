unit AMQP.ServerTestDoubles;

{ Dublês compartilhados pelas suítes do broker -- WS4 da Fase 2.

  Vive numa unit própria porque duas fixtures precisam do mesmo alvo de
  entrega falso: a da fila-ator (AMQP.ServerQueueTests, onde consumidores
  existem só para serem contados e o alvo tem de RECUSAR tudo para o estoque
  ficar onde está) e a da entrega (AMQP.ServerDeliveryTests, onde o alvo
  registra o que recebeu).

  Espelho FPCUnit de tests\Server\fpc\AMQP.ServerTestDoubles.pas -- mantenha os
  dois em sincronia. }

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  AMQP.Wire,
  AMQP.Server.Message,
  AMQP.Server.Queue,
  AMQP.Server.Wal,
  AMQP.Protocol,
  AMQP.Frame,
  AMQP.Threading,
  AMQP.Server.FrameIO;

type
  { Uma entrega registrada pelo alvo falso. }
  TEntregaReg = record
    ConsumerTag: string;
    QueueName: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Corpo: string;
  end;

  { Alvo de entrega falso: registra o que recebeu e permite mandar recusar ou
    morrer. Refcontado como o de verdade -- a fila segura a interface, então o
    objeto sobrevive enquanto ela o tiver na mão. }
  TAlvoFalso = class(TInterfacedObject, IAMQPDeliveryTarget)
  private
    FLock: TCriticalSection;
    FEntregas: TList<TEntregaReg>;
    FCancelados: TStringList;
    FId: NativeUInt;
    FNextTag: UInt64;
    FRecusar: Boolean;
    FVivo: Boolean;
  public
    constructor Create(AId: NativeUInt);
    destructor Destroy; override;

    function ChannelId: NativeUInt;
    function IsAlive: Boolean;
    function TryDeliver(const AConsumerTag, AQueueName: string;
      ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
      out ADeliveryTag: UInt64): Boolean;
    function TryCancel(const AConsumerTag: string): Boolean;

    function Count: Integer;
    function Entrega(AIndex: Integer): TEntregaReg;
    /// Corpos entregues, na ordem, separados por '|'.
    function Corpos: string;
    /// Consumer-tags que receberam Basic.Cancel (fila apagada sob eles).
    function Cancelados: string;
    function CanceladosCount: Integer;
    property Recusar: Boolean read FRecusar write FRecusar;
    property Vivo: Boolean read FVivo write FVivo;
  end;

/// Alvo VIVO que recusa toda entrega. É o que os testes da fila-ator usam
/// quando o consumidor existe só para ser contado: vivo (senão o ator o tira
/// do rodízio) mas sem consumir o estoque.
function AlvoQueRecusa(AId: NativeUInt): IAMQPDeliveryTarget;

type
  { Arquivo de WAL em memoria: conta fsyncs, sabe falhar na escrita e deixa o
    teste mexer nos bytes crus (simula queda e corrupcao sem tocar em disco).

    E' a CAMADA 2 da D28 (Fase 4): como TAMQPWalSegment e TAMQPJournal falam com
    IAMQPWalFile e nao com o sistema de arquivos, da' para provar que o fsync
    acontece na fronteira do lote e que uma falha de I/O nao corrompe estado em
    memoria -- nenhuma das duas seria observavel por fora.

    Protegido por lock porque no journal a thread do journal e a do teste o
    tocam ao mesmo tempo. }
  TAMQPWalFileFalso = class(TInterfacedObject, IAMQPWalFile)
  private
    FLock: TCriticalSection;
    FBuf: TBytes;
    FSyncs: Integer;
    FSyncOk: Boolean;
    FEscritas: Integer;
    FFalharEscritaNa: Integer; // -1 = nunca
  public
    constructor Create;
    destructor Destroy; override;

    function Append(const ABytes: TBytes): Int64;
    function ReadAll: TBytes;
    function Sync: Boolean;
    function Size: Int64;
    procedure TruncateTo(ASize: Int64);

    /// Copia dos bytes crus, para o teste corromper e devolver.
    function Bytes: TBytes;
    procedure PoeBytes(const ABytes: TBytes);
    function Syncs: Integer;
    function Escritas: Integer;
    procedure SetSyncOk(AValue: Boolean);
    /// Numero da escrita (1-based) em que Append vai levantar. -1 = nunca.
    procedure SetFalharEscritaNa(AValue: Integer);
  end;

  { Stream de teste: guarda o que foi escrito e pode BLOQUEAR a escritora
    (para encher a fila de saida do writer de proposito).

    Mora aqui, e nao na suite da entrega onde nasceu, desde a WS5 da Fase 4: a
    suite do confirm assincrono precisa exatamente do mesmo par -- um writer de
    verdade sobre um stream que registra -- para LER os frames que o rastreador
    emitiu. Duas copias divergiriam. }
  TStreamGravador = class(TStream)
  private
    FLock: TCriticalSection;
    FLog: TBytes;
    FLen: Integer;
    FPortao: TEvent; // sinalizado = passa
  public
    constructor Create;
    destructor Destroy; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Read(var Buffer; Count: Longint): Longint; override;
    function Seek(const AOffset: Int64; AOrigin: TSeekOrigin): Int64; override;
    procedure Bloquear;
    procedure Liberar;
    function Bytes: TBytes;
  end;

/// Espera a escritora esvaziar (PARANDO-A) e devolve os frames escritos ate
/// agora. Depois disso o writer nao serve mais para escrever.
function FramesEscritos(AWriter: TAMQPFrameWriter;
  AStream: TStreamGravador): TArray<TAMQPFrame>;

implementation

function AlvoQueRecusa(AId: NativeUInt): IAMQPDeliveryTarget;
var
  LAlvo: TAlvoFalso;
begin
  LAlvo := TAlvoFalso.Create(AId);
  LAlvo.Recusar := True;
  Result := LAlvo;
end;

{ TAlvoFalso }

constructor TAlvoFalso.Create(AId: NativeUInt);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FEntregas := TList<TEntregaReg>.Create;
  FCancelados := TStringList.Create;
  FId := AId;
  FNextTag := 1;
  FVivo := True;
end;

destructor TAlvoFalso.Destroy;
begin
  FEntregas.Free;
  FCancelados.Free;
  FLock.Free;
  inherited;
end;

function TAlvoFalso.ChannelId: NativeUInt;
begin
  Result := FId;
end;

function TAlvoFalso.IsAlive: Boolean;
begin
  Result := FVivo;
end;

function TAlvoFalso.TryDeliver(const AConsumerTag, AQueueName: string;
  ANoAck, ARedelivered: Boolean; AMessage: TAMQPMessage;
  out ADeliveryTag: UInt64): Boolean;
var
  LReg: TEntregaReg;
begin
  ADeliveryTag := 0;
  if FRecusar or (not FVivo) then
    Exit(False);
  FLock.Enter;
  try
    LReg.ConsumerTag := AConsumerTag;
    LReg.QueueName := AQueueName;
    LReg.DeliveryTag := FNextTag;
    LReg.Redelivered := ARedelivered;
    LReg.Corpo := AmqpUtf8Decode(AMessage.Body);
    FEntregas.Add(LReg);
    ADeliveryTag := FNextTag;
    Inc(FNextTag);
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.TryCancel(const AConsumerTag: string): Boolean;
begin
  FLock.Enter;
  try
    FCancelados.Add(AConsumerTag);
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FEntregas.Count;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Entrega(AIndex: Integer): TEntregaReg;
begin
  FLock.Enter;
  try
    Result := FEntregas[AIndex];
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Cancelados: string;
begin
  FLock.Enter;
  try
    Result := FCancelados.CommaText;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.CanceladosCount: Integer;
begin
  FLock.Enter;
  try
    Result := FCancelados.Count;
  finally
    FLock.Leave;
  end;
end;

function TAlvoFalso.Corpos: string;
var
  I: Integer;
begin
  Result := '';
  FLock.Enter;
  try
    for I := 0 to FEntregas.Count - 1 do
    begin
      if Result <> '' then
        Result := Result + '|';
      Result := Result + FEntregas[I].Corpo;
    end;
  finally
    FLock.Leave;
  end;
end;


{ TAMQPWalFileFalso }

constructor TAMQPWalFileFalso.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FSyncOk := True;
  FFalharEscritaNa := -1;
end;

destructor TAMQPWalFileFalso.Destroy;
begin
  FLock.Free;
  inherited Destroy;
end;

function TAMQPWalFileFalso.Append(const ABytes: TBytes): Int64;
var
  LAntes: Integer;
begin
  FLock.Enter;
  try
    Inc(FEscritas);
    if FEscritas = FFalharEscritaNa then
      raise EAMQPWal.Create('falha de escrita injetada');
    LAntes := Length(FBuf);
    SetLength(FBuf, LAntes + Length(ABytes));
    if Length(ABytes) > 0 then
      Move(ABytes[0], FBuf[LAntes], Length(ABytes));
    Result := LAntes;
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.ReadAll: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FBuf, 0, Length(FBuf));
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Sync: Boolean;
begin
  FLock.Enter;
  try
    Inc(FSyncs);
    Result := FSyncOk;
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Size: Int64;
begin
  FLock.Enter;
  try
    Result := Length(FBuf);
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPWalFileFalso.TruncateTo(ASize: Int64);
begin
  FLock.Enter;
  try
    SetLength(FBuf, Integer(ASize));
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Bytes: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FBuf, 0, Length(FBuf));
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPWalFileFalso.PoeBytes(const ABytes: TBytes);
begin
  FLock.Enter;
  try
    FBuf := Copy(ABytes, 0, Length(ABytes));
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Syncs: Integer;
begin
  FLock.Enter;
  try
    Result := FSyncs;
  finally
    FLock.Leave;
  end;
end;

function TAMQPWalFileFalso.Escritas: Integer;
begin
  FLock.Enter;
  try
    Result := FEscritas;
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPWalFileFalso.SetSyncOk(AValue: Boolean);
begin
  FLock.Enter;
  try
    FSyncOk := AValue;
  finally
    FLock.Leave;
  end;
end;

procedure TAMQPWalFileFalso.SetFalharEscritaNa(AValue: Integer);
begin
  FLock.Enter;
  try
    FFalharEscritaNa := AValue;
  finally
    FLock.Leave;
  end;
end;

{ TStreamGravador }

constructor TStreamGravador.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FPortao := TEvent.Create(nil, True, True, ''); // manual-reset, aberto
end;

destructor TStreamGravador.Destroy;
begin
  FPortao.Free;
  FLock.Free;
  inherited;
end;

procedure TStreamGravador.Bloquear;
begin
  FPortao.ResetEvent;
end;

procedure TStreamGravador.Liberar;
begin
  FPortao.SetEvent;
end;

function TStreamGravador.Read(var Buffer; Count: Longint): Longint;
begin
  Result := 0;
end;

function TStreamGravador.Seek(const AOffset: Int64; AOrigin: TSeekOrigin): Int64;
begin
  Result := 0;
end;

function TStreamGravador.Write(const Buffer; Count: Longint): Longint;
var
  P: PByte;
  I: Integer;
begin
  FPortao.WaitFor(AMQP_WAIT_INFINITE);
  FLock.Enter;
  try
    if FLen + Count > Length(FLog) then
      SetLength(FLog, (FLen + Count) * 2 + 64);
    P := @Buffer;
    for I := 0 to Count - 1 do
    begin
      FLog[FLen] := P[I];
      Inc(FLen);
    end;
  finally
    FLock.Leave;
  end;
  Result := Count;
end;

function TStreamGravador.Bytes: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FLog, 0, FLen);
  finally
    FLock.Leave;
  end;
end;

function LeFrames(const ABytes: TBytes): TArray<TAMQPFrame>;
var
  LMem: TBytesStream;
  LLista: TList<TAMQPFrame>;
begin
  LLista := TList<TAMQPFrame>.Create;
  try
    LMem := TBytesStream.Create(ABytes);
    try
      while LMem.Position < LMem.Size do
        LLista.Add(TAMQPFrame.ReadFrom(LMem));
    finally
      LMem.Free;
    end;
    Result := LLista.ToArray;
  finally
    LLista.Free;
  end;
end;

function FramesEscritos(AWriter: TAMQPFrameWriter;
  AStream: TStreamGravador): TArray<TAMQPFrame>;
begin
  AWriter.Stop; // drena o que estiver na fila e encerra a thread escritora
  Result := LeFrames(AStream.Bytes);
end;

end.

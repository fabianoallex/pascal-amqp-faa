unit AMQP.Server.FrameIO;

{$I amqp.inc}

{ Escrita serial de frames de UMA conexão do broker (sub-módulo server, WS4).

  Vários produtores enfileiram frames — a thread de leitura (respostas de
  método, Channel/Connection.Close), o timer de heartbeat e, na Fase 2, os
  workers de fila (Basic.Deliver) e o handler de canal (confirms). Uma única
  thread interna drena a fila para o TStream, na ordem de chegada. Assim o
  objeto SSL / o socket nunca sofrem escrita concorrente e a ordem dos frames
  no canal é determinística.

  - PostFrames enfileira um lote sob um único lock: method + content-header +
    body de uma entrega nunca são intercalados com outra sequência no mesmo
    canal (senão o cliente vê UNEXPECTED_FRAME / 505).
  - A fila é limitada (FMaxDepth): quando cheia, Post* bloqueia até a escritora
    vazar — isso propaga o backpressure do socket como flow-control natural.
  - Se uma escrita no stream falha, a escritora guarda o erro, acorda os
    produtores bloqueados e encerra; todo Post* seguinte levanta EAMQPTransport.

  Não é dono do TStream (quem cria o writer é dono do stream/socket). }

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  AMQP.Threading,
  AMQP.Frame,
  AMQP.Transport; // EAMQPTransport

type
  TAMQPFrameWriter = class;

  { Thread interna: drena a fila e escreve no stream. }
  TAMQPFrameWriterThread = class(TThread)
  private
    FOwner: TAMQPFrameWriter;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TAMQPFrameWriter);
  end;

  TAMQPFrameWriter = class
  private
    FStream: TStream;
    FQueue: TQueue<TAMQPFrame>;
    FMon: TAMQPMonitor;
    FThread: TAMQPFrameWriterThread;
    FMaxDepth: Integer;
    FStopping: Boolean;   // Stop pedido (sob FMon)
    FFailed: Boolean;     // escrita falhou (sob FMon)
    FError: string;       // motivo do FFailed (sob FMon)
    FStarted: Boolean;
    FLastWriteTick: UInt64; // atomico; ultimo flush real no stream
    procedure DrainLoop;
    procedure EnqueueOne(const AFrame: TAMQPFrame);
  public
    /// AMaxDepth = profundidade da fila antes de Post* bloquear.
    constructor Create(AStream: TStream; AMaxDepth: Integer = 256);
    destructor Destroy; override;

    /// Enfileira um frame. Bloqueia se a fila está cheia. Levanta
    /// EAMQPTransport se a escritora já parou ou falhou.
    procedure PostFrame(const AFrame: TAMQPFrame);
    /// Enfileira vários frames atomicamente (não intercalados com outro lote).
    procedure PostFrames(const AFrames: array of TAMQPFrame);
    /// Como PostFrames, mas **NÃO BLOQUEIA e NÃO LEVANTA**: devolve False se a
    /// fila está cheia agora, ou se a escritora já parou/falhou — e nesse caso
    /// nada é enfileirado (o lote é indivisível: method + header + body de uma
    /// entrega jamais entram pela metade).
    ///
    /// É o caminho da ENTREGA (WS4 da Fase 2). O ator da fila roda num worker
    /// do pool e não pode bloquear esperando I/O (decisão D2); um consumidor
    /// cujo canal não aceita mais frames sai do rodízio daquela rodada em vez
    /// de travar a fila inteira (decisão D3) — que é o que aconteceria se a
    /// entrega usasse PostFrames.
    function TryPostFrames(const AFrames: array of TAMQPFrame): Boolean;

    /// Para a escritora: drena o que já está na fila (best-effort) e encerra a
    /// thread. Idempotente. Após Stop, Post* levanta exceção.
    procedure Stop;

    /// '' enquanto sã; o motivo depois de uma falha de escrita.
    function LastError: string;
    function Failed: Boolean;
    /// Tick (AmqpTickMs) da última escrita que chegou de fato ao stream — não
    /// do último Post*. É o que o heartbeat precisa: a spec fala em ociosidade
    /// do envio no wire, e um frame parado na fila ainda não saiu.
    /// 0 enquanto nada foi escrito. Leitura atômica; chamável de outra thread.
    function LastWriteTick: UInt64;
  end;

implementation

{ TAMQPFrameWriterThread }

constructor TAMQPFrameWriterThread.Create(AOwner: TAMQPFrameWriter);
begin
  FOwner := AOwner;
  inherited Create(False); // arranca já
end;

procedure TAMQPFrameWriterThread.Execute;
begin
  FOwner.DrainLoop;
end;

{ TAMQPFrameWriter }

constructor TAMQPFrameWriter.Create(AStream: TStream; AMaxDepth: Integer);
begin
  inherited Create;
  FStream := AStream;
  FMaxDepth := AMaxDepth;
  if FMaxDepth < 1 then
    FMaxDepth := 1;
  FQueue := TQueue<TAMQPFrame>.Create;
  FMon := TAMQPMonitor.Create;
  FThread := TAMQPFrameWriterThread.Create(Self);
  FStarted := True;
end;

destructor TAMQPFrameWriter.Destroy;
begin
  Stop;
  FThread.Free; // Stop já fez WaitFor via Terminated? não — juntamos aqui
  FQueue.Free;
  FMon.Free;
  inherited;
end;

function TAMQPFrameWriter.TryPostFrames(
  const AFrames: array of TAMQPFrame): Boolean;
var
  I: Integer;
begin
  if Length(AFrames) = 0 then
    Exit(True);
  FMon.Enter;
  try
    if FStopping or FFailed or (FQueue.Count >= FMaxDepth) then
      Exit(False);
    // Uma vez decidido que cabe, o lote inteiro entra -- como no PostFrames,
    // o último lote pode passar do teto, e é isso que preserva a atomicidade.
    for I := 0 to High(AFrames) do
      FQueue.Enqueue(AFrames[I]);
    FMon.PulseAll;
    Result := True;
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPFrameWriter.Stop;
begin
  if not FStarted then
    Exit;
  FMon.Enter;
  try
    FStopping := True;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
  FThread.WaitFor; // seguro chamar mais de uma vez
end;

procedure TAMQPFrameWriter.EnqueueOne(const AFrame: TAMQPFrame);
begin
  // chamar SEGURANDO FMon
  while (FQueue.Count >= FMaxDepth) and (not FStopping) and (not FFailed) do
    FMon.Wait(AMQP_WAIT_INFINITE);
  if FStopping then
    raise EAMQPTransport.Create('frame writer parado');
  if FFailed then
    raise EAMQPTransport.Create('frame writer falhou: ' + FError);
  FQueue.Enqueue(AFrame);
end;

procedure TAMQPFrameWriter.PostFrame(const AFrame: TAMQPFrame);
begin
  FMon.Enter;
  try
    EnqueueOne(AFrame);
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPFrameWriter.PostFrames(const AFrames: array of TAMQPFrame);
var
  I: Integer;
begin
  if Length(AFrames) = 0 then
    Exit;
  FMon.Enter;
  try
    // Espera só uma vez (o lote inteiro entra junto, mesmo que passe do teto:
    // um lote indivisível não pode ficar preso pela metade).
    while (FQueue.Count >= FMaxDepth) and (not FStopping) and (not FFailed) do
      FMon.Wait(AMQP_WAIT_INFINITE);
    if FStopping then
      raise EAMQPTransport.Create('frame writer parado');
    if FFailed then
      raise EAMQPTransport.Create('frame writer falhou: ' + FError);
    for I := 0 to High(AFrames) do
      FQueue.Enqueue(AFrames[I]);
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPFrameWriter.DrainLoop;
var
  LBatch: array of TAMQPFrame;
  LCount, I: Integer;
  LDone: Boolean;
begin
  LBatch := nil;
  LDone := False;
  repeat
    // Pega tudo o que houver na fila (ou espera).
    FMon.Enter;
    try
      while (FQueue.Count = 0) and (not FStopping) and (not FFailed) do
        FMon.Wait(AMQP_WAIT_INFINITE);

      LCount := FQueue.Count;
      if LCount > 0 then
      begin
        SetLength(LBatch, LCount);
        for I := 0 to LCount - 1 do
          LBatch[I] := FQueue.Dequeue;
      end
      else
        SetLength(LBatch, 0);

      // Depois de Stop, drenamos o que restou e então saímos.
      if (FStopping or FFailed) and (FQueue.Count = 0) then
        LDone := True;

      FMon.PulseAll; // libera produtores que esperavam vaga
    finally
      FMon.Leave;
    end;

    // Escrita FORA do lock.
    if Length(LBatch) > 0 then
    begin
      try
        for I := 0 to High(LBatch) do
          LBatch[I].WriteTo(FStream);
        AmqpAtomicWrite64(FLastWriteTick, AmqpTickMs);
      except
        on E: Exception do
        begin
          FMon.Enter;
          try
            if not FFailed then
            begin
              FFailed := True;
              FError := E.Message;
            end;
            FMon.PulseAll;
          finally
            FMon.Leave;
          end;
          LDone := True;
        end;
      end;
    end;
  until LDone;
end;

function TAMQPFrameWriter.LastError: string;
begin
  FMon.Enter;
  try
    Result := FError;
  finally
    FMon.Leave;
  end;
end;

function TAMQPFrameWriter.LastWriteTick: UInt64;
begin
  Result := AmqpAtomicRead64(FLastWriteTick);
end;

function TAMQPFrameWriter.Failed: Boolean;
begin
  FMon.Enter;
  try
    Result := FFailed;
  finally
    FMon.Leave;
  end;
end;

end.

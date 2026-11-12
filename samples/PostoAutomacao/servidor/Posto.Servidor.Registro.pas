unit Posto.Servidor.Registro;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The authoritative state of the fuelings on the automation server
  (ARCHITECTURE.md §2 and §4). The whole state machine lives here; no
  other part mutates a fueling.

  Concurrency: a single TCriticalSection protects the dictionary and the version.
  The scale is tens of pumps and a handful of POS terminals, and the command
  consumer is serial (prefetch 1) -- a global lock is enough and simple.
  The "serialization per id" of the ARCHITECTURE is a consequence of that, not one
  more mechanism.

  v1: memory only. Restarting the server loses what was pending
  (ARCHITECTURE.md §12). }

interface

uses
  SysUtils, SyncObjs, Generics.Collections,
  AMQP.Threading,      // AmqpWallMs
  Posto.Abastecida;

type
  { Outcome of a transition. Versao is always the current one after the operation.
    Mudou = the state really changed (the caller publishes an event iff Ok and
    Mudou). }
  TResultadoTransicao = record
    Ok: Boolean;
    Mudou: Boolean;
    Motivo: string;             // when not Ok
    PdvAtual: string;           // refusal because already locked
    VendaAtual: string;
    Versao: Int64;
    EstadoAnterior: TEstadoAbastecida;   // liberar_forcado
    PdvAnterior: string;
  end;

  TPostoRegistro = class
  private
    FLock: TCriticalSection;
    FItens: TDictionary<string, TAbastecidaEstado>;
    FVersao: Int64;
    function Recusa(const AMotivo: string): TResultadoTransicao;
    function OkMudou(AMudou: Boolean): TResultadoTransicao;
  public
    constructor Create;
    destructor Destroy; override;

    { The pump completed a refuel. True = new fueling (publish
      EVT_NOVA); False = id already known (ignored, idempotent). }
    function Registrar(const A: TAbastecida; out AVersao: Int64): Boolean;

    function Lancar(const AId, APdv, AVenda: string): TResultadoTransicao;
    function Estornar(const AId, APdv, AVenda: string): TResultadoTransicao;
    function Finalizar(const AId, APdv, AVenda: string): TResultadoTransicao;
    function CancelarFinalizada(const AId, APdv,
      AVenda: string): TResultadoTransicao;
    function LiberarForcado(const AId: string): TResultadoTransicao;
    function Descartar(const AId: string): TResultadoTransicao;

    function Snapshot(out ADisponiveis: TArray<TAbastecida>;
      out ABloqueadas: TArray<TAbastecidaEstado>): Int64;
    { Everything, for the panel (includes available and locked ones). }
    function Todas: TArray<TAbastecidaEstado>;
    function Versao: Int64;
  end;

implementation

constructor TPostoRegistro.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FItens := TDictionary<string, TAbastecidaEstado>.Create;
end;

destructor TPostoRegistro.Destroy;
begin
  FItens.Free;
  FLock.Free;
  inherited Destroy;
end;

function TPostoRegistro.Recusa(const AMotivo: string): TResultadoTransicao;
begin
  Result := Default(TResultadoTransicao);
  Result.Ok := False;
  Result.Mudou := False;
  Result.Motivo := AMotivo;
  Result.Versao := FVersao;
end;

function TPostoRegistro.OkMudou(AMudou: Boolean): TResultadoTransicao;
begin
  Result := Default(TResultadoTransicao);
  Result.Ok := True;
  Result.Mudou := AMudou;
  if AMudou then
    Inc(FVersao);
  Result.Versao := FVersao;
end;

function TPostoRegistro.Registrar(const A: TAbastecida;
  out AVersao: Int64): Boolean;
var
  LE: TAbastecidaEstado;
begin
  FLock.Enter;
  try
    if FItens.ContainsKey(A.Id) then
    begin
      AVersao := FVersao;
      Exit(False);
    end;
    LE := Default(TAbastecidaEstado);
    LE.Dados := A;
    LE.Estado := eaDisponivel;
    LE.DesdeMs := AmqpWallMs;
    FItens.Add(A.Id, LE);
    Inc(FVersao);
    AVersao := FVersao;
    Result := True;
  finally
    FLock.Leave;
  end;
end;

function TPostoRegistro.Lancar(const AId, APdv,
  AVenda: string): TResultadoTransicao;
var
  LE: TAbastecidaEstado;
begin
  FLock.Enter;
  try
    if not FItens.TryGetValue(AId, LE) then
      Exit(Recusa(REC_NAO_EXISTE));
    case LE.Estado of
      eaDisponivel:
        begin
          LE.Estado := eaLancando;
          LE.Pdv := APdv;
          LE.Venda := AVenda;
          LE.DesdeMs := AmqpWallMs;
          FItens.AddOrSetValue(AId, LE);
          Result := OkMudou(True);
        end;
      eaLancando:
        begin
          // Idempotent for the owner/sale itself (outbox replay).
          if (LE.Pdv = APdv) and (LE.Venda = AVenda) then
            Result := OkMudou(False)
          else
          begin
            Result := Recusa(REC_JA_LANCANDO);
            Result.PdvAtual := LE.Pdv;
            Result.VendaAtual := LE.Venda;
          end;
        end;
      eaLancado:
        begin
          Result := Recusa(REC_JA_LANCADO);
          Result.PdvAtual := LE.Pdv;
          Result.VendaAtual := LE.Venda;
        end;
    end;
  finally
    FLock.Leave;
  end;
end;

function TPostoRegistro.Estornar(const AId, APdv,
  AVenda: string): TResultadoTransicao;
var
  LE: TAbastecidaEstado;
begin
  FLock.Enter;
  try
    if not FItens.TryGetValue(AId, LE) then
      Exit(Recusa(REC_NAO_EXISTE));
    if LE.Estado = eaDisponivel then
      // Already available: idempotent (outbox replay / double removal).
      Exit(OkMudou(False));
    if (LE.Estado <> eaLancando) or (LE.Pdv <> APdv) then
      Exit(Recusa(REC_NAO_E_DONO));
    LE.Estado := eaDisponivel;
    LE.Pdv := '';
    LE.Venda := '';
    LE.DesdeMs := AmqpWallMs;
    FItens.AddOrSetValue(AId, LE);
    Result := OkMudou(True);
  finally
    FLock.Leave;
  end;
end;

function TPostoRegistro.Finalizar(const AId, APdv,
  AVenda: string): TResultadoTransicao;
var
  LE: TAbastecidaEstado;
begin
  FLock.Enter;
  try
    if not FItens.TryGetValue(AId, LE) then
      Exit(Recusa(REC_NAO_EXISTE));
    if (LE.Estado = eaLancado) and (LE.Pdv = APdv) and (LE.Venda = AVenda) then
      Exit(OkMudou(False)); // idempotent (replay)
    if (LE.Estado <> eaLancando) or (LE.Pdv <> APdv) or (LE.Venda <> AVenda) then
      Exit(Recusa(REC_NAO_E_DONO));
    LE.Estado := eaLancado;
    LE.DesdeMs := AmqpWallMs;
    FItens.AddOrSetValue(AId, LE);
    Result := OkMudou(True);
  finally
    FLock.Leave;
  end;
end;

function TPostoRegistro.CancelarFinalizada(const AId, APdv,
  AVenda: string): TResultadoTransicao;
var
  LE: TAbastecidaEstado;
begin
  FLock.Enter;
  try
    if not FItens.TryGetValue(AId, LE) then
      Exit(Recusa(REC_NAO_EXISTE));
    if LE.Estado = eaDisponivel then
      Exit(OkMudou(False)); // idempotent
    if (LE.Estado <> eaLancado) or (LE.Pdv <> APdv) or (LE.Venda <> AVenda) then
      Exit(Recusa(REC_NAO_E_DONO));
    LE.Estado := eaDisponivel;
    LE.Pdv := '';
    LE.Venda := '';
    LE.DesdeMs := AmqpWallMs;
    FItens.AddOrSetValue(AId, LE);
    Result := OkMudou(True);
  finally
    FLock.Leave;
  end;
end;

function TPostoRegistro.LiberarForcado(const AId: string): TResultadoTransicao;
var
  LE: TAbastecidaEstado;
begin
  FLock.Enter;
  try
    if not FItens.TryGetValue(AId, LE) then
      Exit(Recusa(REC_NAO_EXISTE));
    if LE.Estado = eaDisponivel then
      Exit(OkMudou(False));
    Result := Default(TResultadoTransicao);
    Result.EstadoAnterior := LE.Estado;
    Result.PdvAnterior := LE.Pdv;
    LE.Estado := eaDisponivel;
    LE.Pdv := '';
    LE.Venda := '';
    LE.DesdeMs := AmqpWallMs;
    FItens.AddOrSetValue(AId, LE);
    Result.Ok := True;
    Result.Mudou := True;
    Inc(FVersao);
    Result.Versao := FVersao;
  finally
    FLock.Leave;
  end;
end;

function TPostoRegistro.Descartar(const AId: string): TResultadoTransicao;
var
  LE: TAbastecidaEstado;
begin
  FLock.Enter;
  try
    if not FItens.TryGetValue(AId, LE) then
      Exit(OkMudou(False)); // idempotent
    if LE.Estado <> eaDisponivel then
      Exit(Recusa(REC_BLOQUEADA));
    FItens.Remove(AId);
    Result := OkMudou(True);
  finally
    FLock.Leave;
  end;
end;

function TPostoRegistro.Snapshot(out ADisponiveis: TArray<TAbastecida>;
  out ABloqueadas: TArray<TAbastecidaEstado>): Int64;
var
  LE: TAbastecidaEstado;
  LD: TList<TAbastecida>;
  LB: TList<TAbastecidaEstado>;
begin
  LD := TList<TAbastecida>.Create;
  LB := TList<TAbastecidaEstado>.Create;
  FLock.Enter;
  try
    for LE in FItens.Values do
      if LE.Estado = eaDisponivel then
        LD.Add(LE.Dados)
      else
        LB.Add(LE);
    ADisponiveis := LD.ToArray;
    ABloqueadas := LB.ToArray;
    Result := FVersao;
  finally
    FLock.Leave;
    LD.Free;
    LB.Free;
  end;
end;

function TPostoRegistro.Todas: TArray<TAbastecidaEstado>;
var
  LE: TAbastecidaEstado;
  L: TList<TAbastecidaEstado>;
begin
  L := TList<TAbastecidaEstado>.Create;
  FLock.Enter;
  try
    for LE in FItens.Values do
      L.Add(LE);
    Result := L.ToArray;
  finally
    FLock.Leave;
    L.Free;
  end;
end;

function TPostoRegistro.Versao: Int64;
begin
  FLock.Enter;
  try
    Result := FVersao;
  finally
    FLock.Leave;
  end;
end;

end.

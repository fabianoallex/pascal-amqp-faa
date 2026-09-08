unit Posto.Servidor.Registro;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ O estado autoritativo das abastecidas no servidor de automacao
  (ARQUITETURA.md §2 e §4). Toda a maquina de estados vive aqui; nenhuma
  outra parte muta uma abastecida.

  Concorrencia: um TCriticalSection unico protege o dicionario e a versao.
  A escala e' de dezenas de bombas e um punhado de PDVs, e o consumidor de
  comandos e' serial (prefetch 1) -- um lock global e' suficiente e simples.
  A "serializacao por id" da ARQUITETURA e' consequencia disso, nao um
  mecanismo a mais.

  v1: so' memoria. Reiniciar o servidor perde o que estava pendente
  (ARQUITETURA.md §12). }

interface

uses
  SysUtils, SyncObjs, Generics.Collections,
  AMQP.Threading,      // AmqpWallMs
  Posto.Abastecida;

type
  { Desfecho de uma transicao. Versao e' sempre a corrente apos a operacao.
    Mudou = o estado realmente mudou (o chamador publica evento sse Ok e
    Mudou). }
  TResultadoTransicao = record
    Ok: Boolean;
    Mudou: Boolean;
    Motivo: string;             // quando nao Ok
    PdvAtual: string;           // recusa por ja-bloqueada
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

    { Bomba concluiu um abastecimento. True = abastecida nova (publicar
      EVT_NOVA); False = id ja' conhecido (ignora, idempotente). }
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
    { Tudo, para o painel (inclui disponiveis e bloqueadas). }
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
          // Idempotente para o proprio dono/venda (replay de outbox).
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
      // Ja' disponivel: idempotente (replay de outbox / dupla remocao).
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
      Exit(OkMudou(False)); // idempotente (replay)
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
      Exit(OkMudou(False)); // idempotente
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
      Exit(OkMudou(False)); // idempotente
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

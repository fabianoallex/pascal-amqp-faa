unit Posto.Abastecida;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The domain object: a fueling (a refuel completed at a pump)
  and its state on the server. One source for both compilers.

  The ID is stable and unique: pump + nozzle + final totalizer. It is the dedup
  key across the whole system (event, command, POS outbox). }

interface

uses
  SysUtils;

const
  { Refusal reasons in an RPC reply ("motivo" field, ok=false).
    They are outcomes of the state machine, so they live here, in the domain. }
  REC_JA_LANCANDO = 'JA_LANCANDO';
  REC_JA_LANCADO  = 'JA_LANCADO';
  REC_NAO_EXISTE  = 'NAO_EXISTE';
  REC_DESCARTADA  = 'DESCARTADA';
  REC_NAO_E_DONO  = 'NAO_E_DONO';
  REC_BLOQUEADA   = 'BLOQUEADA';

  { Reasons for abastecida.disponivel. }
  MOT_ESTORNO_ITEM     = 'ESTORNO_ITEM';
  MOT_CANCEL_VENDA     = 'CANCEL_VENDA';
  MOT_CANCEL_NOTA      = 'CANCEL_NOTA';
  MOT_LIBERACAO_MANUAL = 'LIBERACAO_MANUAL';

type
  { Life cycle on the server (ARCHITECTURE.md §4). Both eaLancando and
    eaLancado lock the fueling against the other POS terminals. }
  TEstadoAbastecida = (eaDisponivel, eaLancando, eaLancado);

  { taAfericao = pump test/calibration: never becomes a sale, the supervisor
    discards it from the panel. }
  TTipoAbastecida = (taVenda, taAfericao);

  TAbastecida = record
    Id: string;
    Bomba: Integer;
    Bico: Integer;
    Produto: string;            // 'GC','GA','ET','S10','DS'...
    Litros: Double;
    ValorLitro: Double;
    ValorTotal: Double;
    EncerranteInicio: Double;
    EncerranteFim: Double;
    DataHoraMs: Int64;           // epoch ms UTC (AmqpWallMs)
    Tipo: TTipoAbastecida;
  end;

  { How the server keeps a fueling: the data + the state and whose it is. }
  TAbastecidaEstado = record
    Dados: TAbastecida;
    Estado: TEstadoAbastecida;
    Pdv: string;                 // owner, when locked ('' if available)
    Venda: string;               // owner's sale ('' if available)
    DesdeMs: Int64;              // time of the last transition (epoch ms)
  end;

function EstadoParaStr(AEstado: TEstadoAbastecida): string;
function StrParaEstado(const AStr: string): TEstadoAbastecida;
function TipoParaStr(ATipo: TTipoAbastecida): string;
function StrParaTipo(const AStr: string): TTipoAbastecida;

{ Canonical ID from the pump fields. }
function MontaIdAbastecida(ABomba, ABico: Integer;
  const AEncerranteFim: Double): string;

implementation

function EstadoParaStr(AEstado: TEstadoAbastecida): string;
begin
  case AEstado of
    eaDisponivel: Result := 'disponivel';
    eaLancando:   Result := 'lancando';
    eaLancado:    Result := 'lancado';
  else
    Result := 'disponivel';
  end;
end;

function StrParaEstado(const AStr: string): TEstadoAbastecida;
begin
  if AStr = 'lancando' then
    Result := eaLancando
  else if AStr = 'lancado' then
    Result := eaLancado
  else
    Result := eaDisponivel;
end;

function TipoParaStr(ATipo: TTipoAbastecida): string;
begin
  if ATipo = taAfericao then
    Result := 'AFERICAO'
  else
    Result := 'VENDA';
end;

function StrParaTipo(const AStr: string): TTipoAbastecida;
begin
  if SameText(AStr, 'AFERICAO') then
    Result := taAfericao
  else
    Result := taVenda;
end;

function MontaIdAbastecida(ABomba, ABico: Integer;
  const AEncerranteFim: Double): string;
begin
  // E.g. "B03-BC2-0001486094": totalizer in hundredths of a liter, no dot.
  Result := Format('B%.2d-BC%d-%.10d',
    [ABomba, ABico, Round(AEncerranteFim * 100)]);
end;

end.

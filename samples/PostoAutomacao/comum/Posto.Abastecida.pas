unit Posto.Abastecida;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ O objeto de dominio: uma abastecida (um abastecimento concluido numa bomba)
  e o seu estado no servidor. Um fonte para os dois compiladores.

  O ID e' estavel e unico -- bomba + bico + encerrante final. E' a chave de
  dedup em todo o sistema (evento, comando, outbox do PDV). }

interface

uses
  SysUtils;

const
  { Motivos de recusa numa resposta RPC (campo "motivo", ok=false).
    Sao desfechos da maquina de estados -- ficam aqui, no dominio. }
  REC_JA_LANCANDO = 'JA_LANCANDO';
  REC_JA_LANCADO  = 'JA_LANCADO';
  REC_NAO_EXISTE  = 'NAO_EXISTE';
  REC_DESCARTADA  = 'DESCARTADA';
  REC_NAO_E_DONO  = 'NAO_E_DONO';
  REC_BLOQUEADA   = 'BLOQUEADA';

  { Motivos de abastecida.disponivel. }
  MOT_ESTORNO_ITEM     = 'ESTORNO_ITEM';
  MOT_CANCEL_VENDA     = 'CANCEL_VENDA';
  MOT_CANCEL_NOTA      = 'CANCEL_NOTA';
  MOT_LIBERACAO_MANUAL = 'LIBERACAO_MANUAL';

type
  { Ciclo de vida no servidor (ARQUITETURA.md §4). Tanto eaLancando quanto
    eaLancado bloqueiam a abastecida para os demais PDVs. }
  TEstadoAbastecida = (eaDisponivel, eaLancando, eaLancado);

  { taAfericao = teste/aferição de bomba: nunca vira venda, o supervisor
    descarta pelo painel. }
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

  { Como o servidor guarda a abastecida: os dados + o estado e de quem. }
  TAbastecidaEstado = record
    Dados: TAbastecida;
    Estado: TEstadoAbastecida;
    Pdv: string;                 // dono, quando bloqueada ('' se disponivel)
    Venda: string;               // venda do dono ('' se disponivel)
    DesdeMs: Int64;              // instante da ultima transicao (epoch ms)
  end;

function EstadoParaStr(AEstado: TEstadoAbastecida): string;
function StrParaEstado(const AStr: string): TEstadoAbastecida;
function TipoParaStr(ATipo: TTipoAbastecida): string;
function StrParaTipo(const AStr: string): TTipoAbastecida;

{ ID canonico a partir dos campos da bomba. }
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
  // Ex.: "B03-BC2-0001486094" -- encerrante em centesimos de litro, sem ponto.
  Result := Format('B%.2d-BC%d-%.10d',
    [ABomba, ABico, Round(AEncerranteFim * 100)]);
end;

end.

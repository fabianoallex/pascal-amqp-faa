unit Posto.Servidor.Bombas;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Simulador das bombas: uma thread nomeada que a cada intervalo aleatorio
  fabrica uma abastecida concluida e a entrega pelo callback
  (ARQUITETURA.md §1, "aqui: um simulador por timer").

  O callback roda na thread da bomba. O handler do servidor (Registrar +
  Eventos.Nova + refresh do painel) tem de ser thread-safe -- e' (Registro e
  Eventos tem lock proprio; o painel marshaliza). }

interface

uses
  SysUtils, Classes, SyncObjs,
  AMQP.Threading,     // AmqpWallMs
  Posto.Abastecida;

type
  TOnAbastecida = procedure(const A: TAbastecida) of object;

  TPostoBombas = class(TThread)
  private
    FOnAbastecida: TOnAbastecida;
    FParar: TEvent;
    FPausado: Integer;   // atomico: 1 = nao gera (continua no laco)
    FNumBombas: Integer;
    FBicosPorBomba: Integer;
    FIntervaloMinMs: Integer;
    FIntervaloMaxMs: Integer;
    FEncerrante: array of Double;   // acumulado por bico ((bomba-1)*bicos + (bico-1))
    function EncerranteRef(ABomba, ABico: Integer): Integer;
    procedure Gera;
  protected
    procedure Execute; override;
  public
    constructor Create(AOnAbastecida: TOnAbastecida);
    destructor Destroy; override;
    { Sinaliza a parada; a thread sai no proximo laco. }
    procedure Parar;
    { Liga/desliga a geracao sem parar a thread (o painel usa isto). }
    procedure DefinePausado(AValor: Boolean);
    function Pausado: Boolean;

    property NumBombas: Integer read FNumBombas write FNumBombas;
    property BicosPorBomba: Integer read FBicosPorBomba write FBicosPorBomba;
    property IntervaloMinMs: Integer read FIntervaloMinMs write FIntervaloMinMs;
    property IntervaloMaxMs: Integer read FIntervaloMaxMs write FIntervaloMaxMs;
  end;

implementation

const
  PRODUTOS: array[0..4] of string = ('GC', 'GA', 'ET', 'S10', 'DS');
  PRECOS:   array[0..4] of Double = (5.899, 7.490, 4.290, 6.190, 6.090);

constructor TPostoBombas.Create(AOnAbastecida: TOnAbastecida);
begin
  inherited Create(True);   // suspensa; o chamador ajusta as props e da Start
  FOnAbastecida := AOnAbastecida;
  FParar := TEvent.Create(nil, True, False, '');
  FNumBombas := 4;
  FBicosPorBomba := 2;
  FIntervaloMinMs := 4000;
  FIntervaloMaxMs := 12000;
end;

destructor TPostoBombas.Destroy;
begin
  Parar;
  inherited Destroy;   // Terminate + WaitFor
  FParar.Free;
end;

procedure TPostoBombas.Parar;
begin
  Terminate;
  FParar.SetEvent;
end;

procedure TPostoBombas.DefinePausado(AValor: Boolean);
begin
  if AValor then
    AmqpAtomicSet(FPausado, 1)
  else
    AmqpAtomicSet(FPausado, 0);
end;

function TPostoBombas.Pausado: Boolean;
begin
  Result := AmqpAtomicGet(FPausado) <> 0;
end;

function TPostoBombas.EncerranteRef(ABomba, ABico: Integer): Integer;
begin
  Result := (ABomba - 1) * FBicosPorBomba + (ABico - 1);
end;

procedure TPostoBombas.Gera;
var
  LBomba, LBico, LProd, LRef: Integer;
  LA: TAbastecida;
begin
  if Length(FEncerrante) <> FNumBombas * FBicosPorBomba then
  begin
    SetLength(FEncerrante, FNumBombas * FBicosPorBomba);
    for LRef := 0 to High(FEncerrante) do
      FEncerrante[LRef] := 100000 + Random(900000) + Random(1000) / 1000;
  end;

  LBomba := 1 + Random(FNumBombas);
  LBico := 1 + Random(FBicosPorBomba);
  LProd := Random(Length(PRODUTOS));
  LRef := EncerranteRef(LBomba, LBico);

  LA := Default(TAbastecida);
  LA.Bomba := LBomba;
  LA.Bico := LBico;
  LA.Produto := PRODUTOS[LProd];
  LA.ValorLitro := PRECOS[LProd];
  LA.Litros := (30 + Random(6500) / 100);   // 30,00 a 95,00 L
  LA.ValorTotal := Round(LA.Litros * LA.ValorLitro * 100) / 100;
  LA.EncerranteInicio := FEncerrante[LRef];
  FEncerrante[LRef] := FEncerrante[LRef] + LA.Litros;
  LA.EncerranteFim := FEncerrante[LRef];
  LA.DataHoraMs := AmqpWallMs;
  if Random(20) = 0 then
    LA.Tipo := taAfericao
  else
    LA.Tipo := taVenda;
  LA.Id := MontaIdAbastecida(LA.Bomba, LA.Bico, LA.EncerranteFim);

  if Assigned(FOnAbastecida) then
    FOnAbastecida(LA);
end;

procedure TPostoBombas.Execute;
var
  LEspera: Integer;
begin
  {$IFNDEF FPC}
  TThread.NameThreadForDebugging('PostoBombas');
  {$ENDIF}
  Randomize;
  while not Terminated do
  begin
    LEspera := FIntervaloMinMs +
      Random(1 + FIntervaloMaxMs - FIntervaloMinMs);
    if FParar.WaitFor(LEspera) = wrSignaled then
      Break;
    if Terminated then
      Break;
    if Pausado then
      Continue;
    try
      Gera;
    except
      // um erro no callback nao pode matar o simulador
    end;
  end;
end;

end.

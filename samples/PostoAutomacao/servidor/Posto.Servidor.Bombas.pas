unit Posto.Servidor.Bombas;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Pump simulator: a named thread that at each random interval
  makes a completed fueling and hands it over through the callback
  (ARCHITECTURE.md §1, "here: a timer-driven simulator").

  The callback runs on the pump thread. The server handler (Registrar +
  Eventos.Nova + panel refresh) must be thread-safe -- and it is (Registro and
  Eventos have their own lock; the panel marshals). }

interface

uses
  SysUtils, Classes, SyncObjs,
  PascalCommon.Threading,
  AMQP.Threading,     // AmqpWallMs
  Posto.Abastecida;

type
  TOnAbastecida = procedure(const A: TAbastecida) of object;

  TPostoBombas = class(TThread)
  private
    FOnAbastecida: TOnAbastecida;
    FParar: TEvent;
    FPausado: Integer;   // atomic: 1 = does not generate (stays in the loop)
    FNumBombas: Integer;
    FBicosPorBomba: Integer;
    FIntervaloMinMs: Integer;
    FIntervaloMaxMs: Integer;
    FEncerrante: array of Double;   // accumulated per nozzle ((pump-1)*nozzles + (nozzle-1))
    function EncerranteRef(ABomba, ABico: Integer): Integer;
    procedure Gera;
  protected
    procedure Execute; override;
  public
    constructor Create(AOnAbastecida: TOnAbastecida);
    destructor Destroy; override;
    { Signals the stop; the thread leaves on the next loop. }
    procedure Parar;
    { Turns generation on/off without stopping the thread (the panel uses this). }
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
  inherited Create(True);   // suspended; the caller sets the props and calls Start
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
    PcAtomicSet(FPausado, 1)
  else
    PcAtomicSet(FPausado, 0);
end;

function TPostoBombas.Pausado: Boolean;
begin
  Result := PcAtomicGet(FPausado) <> 0;
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
      // an error in the callback must not kill the simulator
    end;
  end;
end;

end.

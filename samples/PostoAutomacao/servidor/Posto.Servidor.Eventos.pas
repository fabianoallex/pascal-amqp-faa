unit Posto.Servidor.Eventos;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Publicacao dos eventos de abastecida no exchange posto.eventos
  (ARQUITETURA.md §6.3). Um canal dedicado; um lock interno porque a thread
  das bombas e a thread do consumidor de comandos publicam as duas. }

interface

uses
  SysUtils, SyncObjs,
  AMQP.Connection, AMQP.Basic.Methods,
  Posto.Abastecida, Posto.Contratos;

type
  TPostoEventos = class
  private
    FCanal: TAMQPChannel;
    FLock: TCriticalSection;
    procedure Publica(const ARota: string; const ABytes: TBytes);
  public
    constructor Create(ACanal: TAMQPChannel);
    destructor Destroy; override;
    procedure Nova(const A: TAbastecida; AVersao: Int64);
    procedure Lancando(const AId, APdv, AVenda: string; AVersao: Int64);
    procedure Lancado(const AId, APdv, AVenda: string; AVersao: Int64);
    procedure Disponivel(const AId, AMotivo: string; AVersao: Int64);
    procedure Descartada(const AId: string; AVersao: Int64);
  end;

implementation

constructor TPostoEventos.Create(ACanal: TAMQPChannel);
begin
  inherited Create;
  FCanal := ACanal;
  FLock := TCriticalSection.Create;
end;

destructor TPostoEventos.Destroy;
begin
  FLock.Free;
  inherited Destroy;
end;

procedure TPostoEventos.Publica(const ARota: string; const ABytes: TBytes);
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  FLock.Enter;
  try
    FCanal.Publish(EXCHANGE_EVENTOS, ARota, ABytes, LProps);
  finally
    FLock.Leave;
  end;
end;

procedure TPostoEventos.Nova(const A: TAbastecida; AVersao: Int64);
begin
  Publica(EVT_NOVA, EncEventoNova(A, AVersao));
end;

procedure TPostoEventos.Lancando(const AId, APdv, AVenda: string; AVersao: Int64);
begin
  Publica(EVT_LANCANDO, EncEventoLancando(AId, APdv, AVenda, AVersao));
end;

procedure TPostoEventos.Lancado(const AId, APdv, AVenda: string; AVersao: Int64);
begin
  Publica(EVT_LANCADO, EncEventoLancado(AId, APdv, AVenda, AVersao));
end;

procedure TPostoEventos.Disponivel(const AId, AMotivo: string; AVersao: Int64);
begin
  Publica(EVT_DISPONIVEL, EncEventoDisponivel(AId, AMotivo, AVersao));
end;

procedure TPostoEventos.Descartada(const AId: string; AVersao: Int64);
begin
  Publica(EVT_DESCARTADA, EncEventoDescartada(AId, AVersao));
end;

end.

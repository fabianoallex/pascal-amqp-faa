unit Posto.PDV.Sincronia;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ Espelho local das abastecidas no PDV (ARQUITETURA.md §7).

  Ao ligar (e a cada reconexao): assina os eventos e os BUFFERIZA, pede o
  snapshot, aplica como baseline e reprocessa o buffer descartando o que ja'
  veio na foto. Depois aplica ao vivo, com deteccao de salto de versao ->
  re-snapshot (cura evento perdido por qualquer motivo).

  O tipo do evento vem da ROUTING KEY (abastecida.nova/lancando/lancado/
  disponivel/descartada), nao do formato do corpo.

  Tudo o que muta FMapa/FUltimaVersao passa por FLock. Os callbacks de evento
  e a sincronizacao rodam em threads do pool; a UI so' le' (Pendentes,
  TryEstado) e reage ao OnMudou marcando-se suja. }

interface

uses
  SysUtils, Classes, SyncObjs, Generics.Collections,
  AMQP.Threading, AMQP.Connection,
  Posto.Json, Posto.Abastecida, Posto.Contratos,
  Posto.PDV.Cliente;

type
  TOnSincroniaMudou = procedure of object;

  TPostoSincronia = class
  private
    FCliente: TPostoPdvCliente;
    FLock: TCriticalSection;
    FMapa: TDictionary<string, TAbastecidaEstado>;
    FBuffer: TStringList;         // "rota"#9"json" recebidos durante o snapshot
    FUltimaVersao: Int64;
    FBufferando: Boolean;
    FSincronizado: Boolean;
    FOnMudou: TOnSincroniaMudou;
    procedure OnEventoAmqp(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
    procedure AplicarEvento(const ARota, ATexto: string);
    procedure PedirResync;
    procedure Notificar;
    procedure EntraEmBuffer;
  public
    constructor Create(ACliente: TPostoPdvCliente);
    destructor Destroy; override;

    { Assina os eventos e dispara a 1a sincronizacao (num worker do pool). }
    procedure Iniciar;
    { Chamar no OnReconnect: re-buffer + re-snapshot. }
    procedure Ressincronizar;
    { Roda a sincronizacao AGORA (bloqueante -- uso interno via worker). }
    procedure Sincronizar;

    function Pendentes: TArray<TAbastecida>;
    function TryEstado(const AId: string; out AEstado: TAbastecidaEstado): Boolean;
    function Sincronizado: Boolean;
    property OnMudou: TOnSincroniaMudou read FOnMudou write FOnMudou;
  end;

implementation

type
  { Worker do pool para rodar a sincronizacao fora da thread de callback. }
  TSyncWork = class(TAMQPWorkItem)
  private
    FAlvo: TPostoSincronia;
  public
    constructor Create(AAlvo: TPostoSincronia);
    procedure Execute; override;
  end;

constructor TSyncWork.Create(AAlvo: TPostoSincronia);
begin
  inherited Create;
  FAlvo := AAlvo;
end;

procedure TSyncWork.Execute;
begin
  FAlvo.Sincronizar;
end;

{ TPostoSincronia }

constructor TPostoSincronia.Create(ACliente: TPostoPdvCliente);
begin
  inherited Create;
  FCliente := ACliente;
  FLock := TCriticalSection.Create;
  FMapa := TDictionary<string, TAbastecidaEstado>.Create;
  FBuffer := TStringList.Create;
  FBufferando := True;
end;

destructor TPostoSincronia.Destroy;
begin
  FBuffer.Free;
  FMapa.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TPostoSincronia.EntraEmBuffer;
begin
  FLock.Enter;
  try
    FBufferando := True;
    FSincronizado := False;
    FBuffer.Clear;
  finally
    FLock.Leave;
  end;
end;

procedure TPostoSincronia.Iniciar;
begin
  EntraEmBuffer;
  FCliente.AssinarEventos(OnEventoAmqp);
  AmqpPool.Queue(TSyncWork.Create(Self));
end;

procedure TPostoSincronia.Ressincronizar;
begin
  EntraEmBuffer;
  AmqpPool.Queue(TSyncWork.Create(Self));
end;

procedure TPostoSincronia.OnEventoAmqp(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LRota, LTexto: string;
  LGuardar: Boolean;
begin
  LRota := ADelivery.RoutingKey;
  LTexto := ADelivery.BodyAsText;
  FLock.Enter;
  try
    LGuardar := FBufferando;
    if LGuardar then
      FBuffer.Add(LRota + #9 + LTexto);
  finally
    FLock.Leave;
  end;
  if not LGuardar then
    AplicarEvento(LRota, LTexto);
end;

procedure TPostoSincronia.Sincronizar;
var
  LResp: TRespostaRpc;
  LVersao: Int64;
  LArr, LItem, LAb: TJsonValue;
  I, LSep: Integer;
  LE: TAbastecidaEstado;
  LPend: TStringList;
  LLinha: string;
begin
  LResp := FCliente.Chamar(EncReqSnapshot, 5000);
  if not LResp.Chegou then
  begin
    if LResp.Json <> nil then
      LResp.Json.Free;
    Exit;   // sem servidor agora; a reconexao dispara de novo
  end;

  LPend := TStringList.Create;
  try
    LVersao := LResp.Json.AsInt('versao');

    FLock.Enter;
    try
      FMapa.Clear;

      LArr := LResp.Json.Get('disponiveis');
      if (LArr <> nil) and (LArr.Kind = jkArray) then
        for I := 0 to LArr.Count - 1 do
        begin
          LAb := LArr.Item(I);
          LE := Default(TAbastecidaEstado);
          JsonParaAbastecida(LAb, LE.Dados);
          LE.Estado := eaDisponivel;
          FMapa.AddOrSetValue(LE.Dados.Id, LE);
        end;

      LArr := LResp.Json.Get('bloqueadas');
      if (LArr <> nil) and (LArr.Kind = jkArray) then
        for I := 0 to LArr.Count - 1 do
        begin
          LItem := LArr.Item(I);
          LE := Default(TAbastecidaEstado);
          LE.Dados.Id := LItem.AsStr('id');
          LE.Estado := StrParaEstado(LItem.AsStr('estado'));
          LE.Pdv := LItem.AsStr('pdv');
          LE.Venda := LItem.AsStr('venda');
          LE.DesdeMs := LItem.AsInt('desde');
          FMapa.AddOrSetValue(LE.Dados.Id, LE);
        end;

      FUltimaVersao := LVersao;
      LPend.Assign(FBuffer);
      FBuffer.Clear;
      FBufferando := False;
      FSincronizado := True;
    finally
      FLock.Leave;
    end;
  finally
    LResp.Json.Free;
  end;

  for I := 0 to LPend.Count - 1 do
  begin
    LLinha := LPend[I];
    LSep := Pos(#9, LLinha);
    if LSep > 0 then
      AplicarEvento(Copy(LLinha, 1, LSep - 1), Copy(LLinha, LSep + 1, MaxInt));
  end;
  LPend.Free;

  Notificar;
end;

procedure TPostoSincronia.AplicarEvento(const ARota, ATexto: string);
var
  J: TJsonValue;
  LVersao: Int64;
  LId, LPdv: string;
  LE: TAbastecidaEstado;
  LTemId, LSalto: Boolean;
  LAb: TJsonValue;
begin
  J := nil;
  try
    J := TJsonValue.Parse(ATexto);
  except
    if J <> nil then J.Free;
    Exit;
  end;

  LSalto := False;
  try
    LVersao := J.AsInt('versao');
    LId := J.AsStr('id');

    FLock.Enter;
    try
      if FBufferando then
      begin
        FBuffer.Add(ARota + #9 + ATexto);
        Exit;
      end;
      if LVersao <= FUltimaVersao then
        Exit;   // ja' visto
      LSalto := LVersao > FUltimaVersao + 1;
      FUltimaVersao := LVersao;

      LTemId := FMapa.TryGetValue(LId, LE);

      if ARota = EVT_NOVA then
      begin
        LAb := J.Get('abastecida');
        if LAb <> nil then
        begin
          LE := Default(TAbastecidaEstado);
          JsonParaAbastecida(LAb, LE.Dados);
          LE.Estado := eaDisponivel;
          FMapa.AddOrSetValue(LE.Dados.Id, LE);
        end;
      end
      else if ARota = EVT_DISPONIVEL then
      begin
        if LTemId then
        begin
          LE.Estado := eaDisponivel;
          LE.Pdv := '';
          LE.Venda := '';
          FMapa.AddOrSetValue(LId, LE);
        end;
      end
      else if (ARota = EVT_LANCANDO) or (ARota = EVT_LANCADO) then
      begin
        if not LTemId then
        begin
          LE := Default(TAbastecidaEstado);
          LE.Dados.Id := LId;
        end;
        LPdv := J.AsStr('pdv');
        LE.Pdv := LPdv;
        LE.Venda := J.AsStr('venda');
        if ARota = EVT_LANCANDO then
          LE.Estado := eaLancando
        else
          LE.Estado := eaLancado;
        FMapa.AddOrSetValue(LId, LE);
      end
      else if ARota = EVT_DESCARTADA then
      begin
        if LTemId then
          FMapa.Remove(LId);
      end;
    finally
      FLock.Leave;
    end;
  finally
    J.Free;
  end;

  if LSalto then
    PedirResync;
  Notificar;
end;

procedure TPostoSincronia.PedirResync;
var
  LJaBufferando: Boolean;
begin
  FLock.Enter;
  try
    LJaBufferando := FBufferando;
  finally
    FLock.Leave;
  end;
  if not LJaBufferando then
    Ressincronizar;
end;

procedure TPostoSincronia.Notificar;
begin
  if Assigned(FOnMudou) then
    FOnMudou();
end;

function TPostoSincronia.Pendentes: TArray<TAbastecida>;
var
  LE: TAbastecidaEstado;
  L: TList<TAbastecida>;
begin
  L := TList<TAbastecida>.Create;
  FLock.Enter;
  try
    for LE in FMapa.Values do
      if LE.Estado = eaDisponivel then
        L.Add(LE.Dados);
    Result := L.ToArray;
  finally
    FLock.Leave;
    L.Free;
  end;
end;

function TPostoSincronia.TryEstado(const AId: string;
  out AEstado: TAbastecidaEstado): Boolean;
begin
  FLock.Enter;
  try
    Result := FMapa.TryGetValue(AId, AEstado);
  finally
    FLock.Leave;
  end;
end;

function TPostoSincronia.Sincronizado: Boolean;
begin
  FLock.Enter;
  try
    Result := FSincronizado;
  finally
    FLock.Leave;
  end;
end;

end.

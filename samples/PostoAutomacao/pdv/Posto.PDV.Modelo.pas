unit Posto.PDV.Modelo;

{$IFDEF FPC}{$MODE DELPHI}{$H+}{$ENDIF}

{ The sale in progress on the POS + the outbox + the reconciliation
  (ARCHITECTURE.md §8, §9).

  Each operation (add item, remove, finalize, cancel, reconcile)
  runs on a PcPool worker -- the UI never blocks on an RPC. The result
  comes back to the UI through OnMudou/OnLog (the UI marks itself dirty and
  reads the state).

  Online, adding an item is synchronous (immediate feedback). Offline / timeout,
  it is optimistic: the item goes in marked "pending" and the command goes to
  the outbox, replayed on reconnection.

  v1: the outbox is an in-memory list. In a real application it lives in the
  POS's local database (the automation runs in contingency). See ARCHITECTURE.md §9. }

interface

uses
  SysUtils, Classes, SyncObjs, Generics.Collections,
  AMQP.Wire, PascalCommon.ThreadPool,
  Posto.Json, Posto.Abastecida, Posto.Contratos,
  Posto.PDV.Cliente, Posto.PDV.Sincronia;

type
  TItemVenda = record
    Id: string;
    Abastecida: TAbastecida;
    Confirmado: Boolean;   // the server accepted the lancar
    Conflito: Boolean;     // the server refused (in use at another register)
    Obs: string;
  end;

  TOnModeloLog = procedure(const AMsg: string) of object;
  TOnModeloMudou = procedure of object;

  TPostoModelo = class
  private
    FCliente: TPostoPdvCliente;
    FSincronia: TPostoSincronia;
    FPdv: string;
    FTimeoutMs: Integer;
    FLock: TCriticalSection;
    FVenda: string;
    FItens: TList<TItemVenda>;
    FOutbox: TStringList;   // pending JSON requests (one per line)
    FSeq: Integer;
    FOnLog: TOnModeloLog;
    FOnMudou: TOnModeloMudou;
    procedure Log(const AMsg: string);
    procedure Notificar;
    function IndiceDe(const AId: string): Integer;
    procedure PoeItem(const AItem: TItemVenda);
    procedure MarcaItem(const AId: string; AConfirmado, AConflito: Boolean;
      const AObs: string);
    procedure EnfileiraOutbox(const AReqJson: TBytes);
    function GaranteVenda: string;
    function ChamarJson(const AReqJson: TBytes): TJsonValue; // nil = no reply
  public
    constructor Create(ACliente: TPostoPdvCliente; ASincronia: TPostoSincronia;
      const APdv: string);
    destructor Destroy; override;

    procedure NovaVenda;
    procedure AdicionarItem(const AId: string);
    procedure RemoverItem(const AId: string);
    procedure CancelarVenda;
    procedure FinalizarVenda;
    procedure ReconciliarAposReconexao;

    // --- blocking bodies (called by the workers) ---
    procedure ExecAdicionar(const AId: string);
    procedure ExecRemover(const AId: string);
    procedure ExecCancelar;
    procedure ExecFinalizar;
    procedure ExecReconciliar;

    function Itens: TArray<TItemVenda>;
    function VendaAtual: string;
    function ItensNoOutbox: Integer;

    property OnLog: TOnModeloLog read FOnLog write FOnLog;
    property OnMudou: TOnModeloMudou read FOnMudou write FOnMudou;
  end;

implementation

type
  TModeloOp = (moAdicionar, moRemover, moCancelar, moFinalizar, moReconciliar);

  TModeloWork = class(TPcWorkItem)
  private
    FModelo: TPostoModelo;
    FOp: TModeloOp;
    FArg: string;
  public
    constructor Create(AModelo: TPostoModelo; AOp: TModeloOp; const AArg: string);
    procedure Execute; override;
  end;

constructor TModeloWork.Create(AModelo: TPostoModelo; AOp: TModeloOp;
  const AArg: string);
begin
  inherited Create;
  FModelo := AModelo;
  FOp := AOp;
  FArg := AArg;
end;

procedure TModeloWork.Execute;
begin
  case FOp of
    moAdicionar:   FModelo.ExecAdicionar(FArg);
    moRemover:     FModelo.ExecRemover(FArg);
    moCancelar:    FModelo.ExecCancelar;
    moFinalizar:   FModelo.ExecFinalizar;
    moReconciliar: FModelo.ExecReconciliar;
  end;
end;

{ TPostoModelo }

constructor TPostoModelo.Create(ACliente: TPostoPdvCliente;
  ASincronia: TPostoSincronia; const APdv: string);
begin
  inherited Create;
  FCliente := ACliente;
  FSincronia := ASincronia;
  FPdv := APdv;
  FTimeoutMs := 3000;
  FLock := TCriticalSection.Create;
  FItens := TList<TItemVenda>.Create;
  FOutbox := TStringList.Create;
end;

destructor TPostoModelo.Destroy;
begin
  FOutbox.Free;
  FItens.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TPostoModelo.Log(const AMsg: string);
begin
  if Assigned(FOnLog) then
    FOnLog(AMsg);
end;

procedure TPostoModelo.Notificar;
begin
  if Assigned(FOnMudou) then
    FOnMudou();
end;

function TPostoModelo.IndiceDe(const AId: string): Integer;
var
  I: Integer;
begin
  for I := 0 to FItens.Count - 1 do
    if FItens[I].Id = AId then
      Exit(I);
  Result := -1;
end;

procedure TPostoModelo.PoeItem(const AItem: TItemVenda);
var
  LIdx: Integer;
begin
  FLock.Enter;
  try
    LIdx := IndiceDe(AItem.Id);
    if LIdx >= 0 then
      FItens[LIdx] := AItem
    else
      FItens.Add(AItem);
  finally
    FLock.Leave;
  end;
end;

procedure TPostoModelo.MarcaItem(const AId: string; AConfirmado, AConflito: Boolean;
  const AObs: string);
var
  LIdx: Integer;
  LItem: TItemVenda;
begin
  FLock.Enter;
  try
    LIdx := IndiceDe(AId);
    if LIdx < 0 then
      Exit;
    LItem := FItens[LIdx];
    LItem.Confirmado := AConfirmado;
    LItem.Conflito := AConflito;
    LItem.Obs := AObs;
    FItens[LIdx] := LItem;
  finally
    FLock.Leave;
  end;
end;

procedure TPostoModelo.EnfileiraOutbox(const AReqJson: TBytes);
begin
  FLock.Enter;
  try
    FOutbox.Add(BytesParaTexto(AReqJson));
  finally
    FLock.Leave;
  end;
end;

function TPostoModelo.GaranteVenda: string;
begin
  FLock.Enter;
  try
    if FVenda = '' then
    begin
      Inc(FSeq);
      FVenda := Format('V-%s-%.4d', [FormatDateTime('yyyymmdd-hhnnss', Now), FSeq]);
    end;
    Result := FVenda;
  finally
    FLock.Leave;
  end;
end;

function TPostoModelo.ChamarJson(const AReqJson: TBytes): TJsonValue;
var
  LResp: TRespostaRpc;
begin
  LResp := FCliente.Chamar(AReqJson, FTimeoutMs);
  if LResp.Chegou then
    Result := LResp.Json
  else
  begin
    if LResp.Json <> nil then
      LResp.Json.Free;
    Result := nil;
  end;
end;

{ --- triggers (UI thread) --- }

procedure TPostoModelo.NovaVenda;
begin
  FLock.Enter;
  try
    FVenda := '';
    FItens.Clear;
  finally
    FLock.Leave;
  end;
  Log('Nova venda.');
  Notificar;
end;

procedure TPostoModelo.AdicionarItem(const AId: string);
var
  LItem: TItemVenda;
  LE: TAbastecidaEstado;
begin
  if IndiceDe(AId) >= 0 then
    Exit;
  GaranteVenda;
  LItem := Default(TItemVenda);
  LItem.Id := AId;
  if FSincronia.TryEstado(AId, LE) then
    LItem.Abastecida := LE.Dados;
  LItem.Confirmado := False;
  LItem.Obs := 'enviando...';
  PoeItem(LItem);
  Notificar;
  PcPool.Queue(TModeloWork.Create(Self, moAdicionar, AId));
end;

procedure TPostoModelo.RemoverItem(const AId: string);
var
  LIdx: Integer;
  LEnviado: Boolean;
begin
  FLock.Enter;
  try
    LIdx := IndiceDe(AId);
    if LIdx < 0 then
      Exit;
    LEnviado := FItens[LIdx].Confirmado or (FItens[LIdx].Obs = 'enviando...');
    FItens.Delete(LIdx);
  finally
    FLock.Leave;
  end;
  Notificar;
  if LEnviado then
    PcPool.Queue(TModeloWork.Create(Self, moRemover, AId));
end;

procedure TPostoModelo.CancelarVenda;
begin
  PcPool.Queue(TModeloWork.Create(Self, moCancelar, ''));
end;

procedure TPostoModelo.FinalizarVenda;
begin
  PcPool.Queue(TModeloWork.Create(Self, moFinalizar, ''));
end;

procedure TPostoModelo.ReconciliarAposReconexao;
begin
  PcPool.Queue(TModeloWork.Create(Self, moReconciliar, ''));
end;

{ --- blocking bodies (pool worker) --- }

procedure TPostoModelo.ExecAdicionar(const AId: string);
var
  LVenda: string;
  LReq: TBytes;
  J: TJsonValue;
begin
  LVenda := VendaAtual;
  LReq := EncReqLancar(AId, FPdv, LVenda);
  J := ChamarJson(LReq);
  if J = nil then
  begin
    MarcaItem(AId, False, False, 'pendente (sem conexao)');
    EnfileiraOutbox(LReq);
    Log(Format('lancar %s: sem resposta -> outbox', [AId]));
  end
  else
    try
      if J.AsBool('ok') then
      begin
        MarcaItem(AId, True, False, '');
        Log(Format('lancar %s: OK', [AId]));
      end
      else
      begin
        MarcaItem(AId, False, True,
          Format('em uso no caixa %s', [J.AsStr('pdvAtual', '?')]));
        Log(Format('lancar %s RECUSADO: %s (caixa %s)',
          [AId, J.AsStr('motivo'), J.AsStr('pdvAtual')]));
      end;
    finally
      J.Free;
    end;
  Notificar;
end;

procedure TPostoModelo.ExecRemover(const AId: string);
var
  LReq: TBytes;
  J: TJsonValue;
begin
  LReq := EncReqEstornar(AId, FPdv, VendaAtual);
  J := ChamarJson(LReq);
  if J = nil then
  begin
    EnfileiraOutbox(LReq);
    Log(Format('estornar %s: sem resposta -> outbox', [AId]));
  end
  else
  begin
    J.Free;
    Log(Format('estornar %s: OK', [AId]));
  end;
  Notificar;
end;

procedure TPostoModelo.ExecCancelar;
var
  LIds: TArray<string>;
  LVenda: string;
  I: Integer;
  LReq: TBytes;
  J: TJsonValue;
begin
  FLock.Enter;
  try
    LVenda := FVenda;
    SetLength(LIds, FItens.Count);
    for I := 0 to FItens.Count - 1 do
      LIds[I] := FItens[I].Id;
    FItens.Clear;
    FVenda := '';
  finally
    FLock.Leave;
  end;
  Notificar;
  if LVenda = '' then
    Exit;

  for I := 0 to High(LIds) do
  begin
    LReq := EncReqEstornar(LIds[I], FPdv, LVenda);
    // CANCEL_VENDA reason in the body
    J := TJsonValue.Parse(BytesParaTexto(LReq));
    try
      J.Put('motivo', MOT_CANCEL_VENDA);
      LReq := JsonParaBytes(J);
    finally
      J.Free;
    end;
    J := ChamarJson(LReq);
    if J = nil then
      EnfileiraOutbox(LReq)
    else
      J.Free;
  end;
  Log(Format('Venda %s cancelada (%d itens).', [LVenda, Length(LIds)]));
  Notificar;
end;

procedure TPostoModelo.ExecFinalizar;
var
  LIds: TArray<string>;
  LVenda: string;
  I: Integer;
  LReq: TBytes;
  J, LItens, LIt: TJsonValue;
  LTudoOk: Boolean;
begin
  FLock.Enter;
  try
    LVenda := FVenda;
    SetLength(LIds, FItens.Count);
    for I := 0 to FItens.Count - 1 do
      LIds[I] := FItens[I].Id;
  finally
    FLock.Leave;
  end;
  if (LVenda = '') or (Length(LIds) = 0) then
  begin
    Log('Finalizar: venda vazia.');
    Exit;
  end;

  LReq := EncReqFinalizar(FPdv, LVenda, LIds);
  J := ChamarJson(LReq);
  if J = nil then
  begin
    EnfileiraOutbox(LReq);
    Log(Format('finalizar %s: sem resposta -> outbox ' +
      '(a venda fica aberta ate a reconciliacao)', [LVenda]));
    Notificar;
    Exit;
  end;

  try
    LTudoOk := True;
    LItens := J.Get('itens');
    if (LItens <> nil) and (LItens.Kind = jkArray) then
      for I := 0 to LItens.Count - 1 do
      begin
        LIt := LItens.Item(I);
        if not LIt.AsBool('ok') then
        begin
          LTudoOk := False;
          MarcaItem(LIt.AsStr('id'), False, True,
            'conflito ao finalizar: ' + LIt.AsStr('motivo'));
          Log(Format('finalizar item %s: CONFLITO (%s)',
            [LIt.AsStr('id'), LIt.AsStr('motivo')]));
        end;
      end;
  finally
    J.Free;
  end;

  if LTudoOk then
  begin
    FLock.Enter;
    try
      if FVenda = LVenda then
      begin
        FVenda := '';
        FItens.Clear;
      end;
    finally
      FLock.Leave;
    end;
    Log(Format('Venda %s finalizada.', [LVenda]));
  end
  else
    Log(Format('Venda %s finalizada com conflitos -- revise com o supervisor.',
      [LVenda]));
  Notificar;
end;

procedure TPostoModelo.ExecReconciliar;
var
  LFila: TStringList;
  I, LReenviados: Integer;
  J: TJsonValue;
  LItensAbertos: TArray<string>;
  LVenda: string;
  LE: TAbastecidaEstado;
begin
  // 1) drain the outbox, in order
  LFila := TStringList.Create;
  try
    FLock.Enter;
    try
      LFila.Assign(FOutbox);
      FOutbox.Clear;
    finally
      FLock.Leave;
    end;

    LReenviados := 0;
    I := 0;
    while I < LFila.Count do
    begin
      J := ChamarJson(AmqpUtf8Encode(LFila[I]));
      if J = nil then
      begin
        // still no server: put the rest back in the outbox and leave
        FLock.Enter;
        try
          while I < LFila.Count do
          begin
            FOutbox.Add(LFila[I]);
            Inc(I);
          end;
        finally
          FLock.Leave;
        end;
        Log('Reconciliacao: servidor indisponivel, outbox preservado.');
        Notificar;
        Exit;
      end;
      J.Free;
      Inc(LReenviados);
      Inc(I);
    end;
    if LReenviados > 0 then
      Log(Format('Reconciliacao: %d comandos do outbox reenviados.', [LReenviados]));
  finally
    LFila.Free;
  end;

  // 2) check the open sale's items against the already re-synced mirror
  FLock.Enter;
  try
    LVenda := FVenda;
    SetLength(LItensAbertos, FItens.Count);
    for I := 0 to FItens.Count - 1 do
      LItensAbertos[I] := FItens[I].Id;
  finally
    FLock.Leave;
  end;
  if LVenda = '' then
  begin
    Notificar;
    Exit;
  end;

  for I := 0 to High(LItensAbertos) do
  begin
    if not FSincronia.TryEstado(LItensAbertos[I], LE) then
      Continue;
    if (LE.Estado = eaLancando) and (LE.Pdv = FPdv) then
      MarcaItem(LItensAbertos[I], True, False, '')
    else if LE.Estado = eaDisponivel then
    begin
      // the reservation was lost during the outage -- try again
      J := ChamarJson(EncReqLancar(LItensAbertos[I], FPdv, LVenda));
      if J <> nil then
      try
        if J.AsBool('ok') then
          MarcaItem(LItensAbertos[I], True, False, 're-reservada')
        else
          MarcaItem(LItensAbertos[I], False, True,
            'perdida na queda (caixa ' + J.AsStr('pdvAtual', '?') + ')');
      finally
        J.Free;
      end;
    end
    else
      MarcaItem(LItensAbertos[I], False, True,
        Format('agora em %s no caixa %s',
        [EstadoParaStr(LE.Estado), LE.Pdv]));
  end;
  Log('Reconciliacao da venda concluida.');
  Notificar;
end;

{ --- reading (UI thread) --- }

function TPostoModelo.Itens: TArray<TItemVenda>;
begin
  FLock.Enter;
  try
    Result := FItens.ToArray;
  finally
    FLock.Leave;
  end;
end;

function TPostoModelo.VendaAtual: string;
begin
  FLock.Enter;
  try
    Result := FVenda;
  finally
    FLock.Leave;
  end;
end;

function TPostoModelo.ItensNoOutbox: Integer;
begin
  FLock.Enter;
  try
    Result := FOutbox.Count;
  finally
    FLock.Leave;
  end;
end;

end.

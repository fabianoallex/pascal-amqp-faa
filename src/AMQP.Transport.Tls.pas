unit AMQP.Transport.Tls;

{$I amqp.inc}

{ Transporte TLS (amqps://) via SChannel nativo do Windows (SSPI).

  Motivação de design (ver plano/CLAUDE): TLS SEM dependências externas — nada de
  DLLs de OpenSSL para distribuir/manter. Usa o SChannel do próprio Windows
  (secur32.dll), que a lib acessa via SSPI. A RTL do Delphi NÃO declara essa API,
  então as assinaturas/records/constantes necessárias são declaradas aqui mesmo
  (nada de terceiros — só a especificação da API Win32).

  TAMQPSchannelStream é um TStream que ENVOLVE outro TStream de bytes crus (o
  TAMQPSocketStream sobre o socket TCP) e faz a criptografia por cima. O resto da
  lib continua falando com um TStream — o framing (AMQP.Frame) não sabe que há TLS
  embaixo. Como o stream cru continua sendo o socket, fechar o socket
  (reconexão/heartbeat) desbloqueia a leitura exatamente como no caminho plain.

  Escopo: cliente, autenticação de servidor. Validação de certificado via cadeia
  do Windows (default) ou modo inseguro opt-in (self-signed em dev). mTLS e
  renegociação iniciada pelo servidor ficam fora desta primeira versão. }

interface

{ Unit exclusiva de Windows (SChannel). Em outras plataformas ela compila
  vazia; AMQP.Connection so a referencia sob AMQP_WINDOWS. TLS multiplataforma
  (OpenSSL) e' roadmap. }
{$IFDEF AMQP_WINDOWS}

uses
  SysUtils,
  Classes,
  Windows,
  AMQP.Transport; // EAMQPTls (comum aos transportes TLS)

type
  { Handles/structs SSPI usados como campos da classe (layout binário exato da
    API Win32). O restante da superfície SSPI (funções, SecBuffer, SCHANNEL_CRED,
    constantes) fica na implementation, pois só é usado dentro dos métodos. }
  PSecHandle = ^TSecHandle;
  TSecHandle = record
    dwLower: ULONG_PTR;
    dwUpper: ULONG_PTR;
  end;
  TCredHandle = TSecHandle;
  TCtxtHandle = TSecHandle;

  TSecPkgContext_StreamSizes = record
    cbHeader: ULONG;
    cbTrailer: ULONG;
    cbMaximumMessage: ULONG;
    cBuffers: ULONG;
    cbBlockSize: ULONG;
  end;

  { Stream TLS cliente. Faz o handshake no construtor (síncrono, sobre o stream
    cru), depois cifra/decifra em Read/Write. É dono do stream de baixo (Free
    libera os dois). }
  TAMQPSchannelStream = class(TStream)
  private
    FUnderlying: TStream;      // bytes crus (socket); TAMQPSchannelStream é dono
    FCred: TCredHandle;
    FCtxt: TCtxtHandle;
    FStreamSizes: TSecPkgContext_StreamSizes;
    // UnicodeString nos dois compiladores: a API SSPI e' wide (PWideChar) e no
    // FPC 'string' e' AnsiString — o cast direto nao existiria.
    FTargetName: UnicodeString; // SNI / nome para validação
    FVerifyPeer: Boolean;
    FCredValid: Boolean;
    FCtxtValid: Boolean;
    // buffer de plaintext já decifrado, aguardando consumo por Read
    FPlain: TBytes;
    FPlainPos: Integer;
    FPlainEnd: Integer;
    // ciphertext lido do socket ainda não processado (inclui SECBUFFER_EXTRA)
    FCipher: TBytes;
    FCipherLen: Integer;
    procedure RecvRaw;                 // lê um bloco do socket e acrescenta a FCipher
    procedure SendAll(APtr: Pointer; ALen: Integer);
    procedure AcquireCred;
    procedure DoHandshake;
    procedure FillPlain;               // decifra até haver plaintext (ou EOF)
    procedure ShutdownTls;
  public
    constructor Create(AUnderlying: TStream; const ATargetName: string;
      AVerifyPeer: Boolean);
    destructor Destroy; override;
    function Read(var Buffer; Count: Longint): Longint; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
  end;

{$ENDIF AMQP_WINDOWS}

implementation

{$IFDEF AMQP_WINDOWS}

{ ------------------------------------------------------------------------------
  Declarações SSPI / SChannel (Win32). Nomes seguem a API oficial; os records têm
  o layout binário exato esperado pelo secur32.dll.
  ------------------------------------------------------------------------------ }

type
  SECURITY_STATUS = LongInt;

  // Ponteiros para os handles (os records TSecHandle/TCredHandle/TCtxtHandle e
  // TSecPkgContext_StreamSizes estão na interface, pois são campos da classe).
  PCredHandle = PSecHandle;
  PCtxtHandle = PSecHandle;

  // A API usa TimeStamp (LARGE_INTEGER); só o recebemos como out e ignoramos.
  TSecTimeStamp = Int64;
  PSecTimeStamp = ^TSecTimeStamp;

  PSecBuffer = ^TSecBuffer;
  TSecBuffer = record
    cbBuffer: ULONG;
    BufferType: ULONG;
    pvBuffer: Pointer;
  end;

  PSecBufferDesc = ^TSecBufferDesc;
  TSecBufferDesc = record
    ulVersion: ULONG;
    cBuffers: ULONG;
    pBuffers: PSecBuffer;
  end;

  // SCHANNEL_CRED (versão 4). Só preenchemos dwVersion e dwFlags; o resto zerado.
  TSChannelCred = record
    dwVersion: DWORD;
    cCreds: DWORD;
    paCred: Pointer;
    hRootStore: THandle;
    cMappers: DWORD;
    aphMappers: Pointer;
    cSupportedAlgs: DWORD;
    palgSupportedAlgs: Pointer;
    grbitEnabledProtocols: DWORD;
    dwMinimumCipherStrength: DWORD;
    dwMaximumCipherStrength: DWORD;
    dwSessionLifespan: DWORD;
    dwFlags: DWORD;
    dwCredFormat: DWORD;
  end;

const
  UNISP_NAME = 'Microsoft Unified Security Protocol Provider';

  SECPKG_CRED_OUTBOUND     = 2;
  SECURITY_NATIVE_DREP     = $10;
  SECBUFFER_VERSION        = 0;
  SCHANNEL_CRED_VERSION    = 4;

  // Tipos de SecBuffer
  SECBUFFER_EMPTY          = 0;
  SECBUFFER_DATA           = 1;
  SECBUFFER_TOKEN          = 2;
  SECBUFFER_EXTRA          = 5;
  SECBUFFER_STREAM_TRAILER = 6;
  SECBUFFER_STREAM_HEADER  = 7;
  SECBUFFER_ALERT          = 17;

  // Flags de InitializeSecurityContext (cliente, modo stream)
  ISC_REQ_REPLAY_DETECT    = $00000004;
  ISC_REQ_SEQUENCE_DETECT  = $00000008;
  ISC_REQ_CONFIDENTIALITY  = $00000010;
  ISC_REQ_EXTENDED_ERROR   = $00004000;
  ISC_REQ_ALLOCATE_MEMORY  = $00000100;
  ISC_REQ_STREAM           = $00008000;

  // Flags de SCHANNEL_CRED.dwFlags
  SCH_CRED_NO_SYSTEM_MAPPER       = $00000002;
  SCH_CRED_NO_SERVERNAME_CHECK    = $00000004;
  SCH_CRED_MANUAL_CRED_VALIDATION = $00000008;
  SCH_CRED_NO_DEFAULT_CREDS       = $00000010;
  SCH_CRED_AUTO_CRED_VALIDATION   = $00000020;

  SECPKG_ATTR_STREAM_SIZES = 4;

  // Status (as constantes de erro têm o bit alto setado; comparar via Cardinal).
  SEC_E_OK                 = $00000000;
  SEC_I_CONTINUE_NEEDED    = $00090312;
  SEC_I_CONTEXT_EXPIRED    = $00090317;
  SEC_I_RENEGOTIATE        = $00090321;
  SEC_E_INCOMPLETE_MESSAGE = $80090318;

  SCHANNEL_SHUTDOWN = 1;

  RAW_CHUNK = 16384; // leitura de socket por rodada (record TLS máx ~16KB)

  // Flags pedidas no handshake (cliente, modo stream).
  HANDSHAKE_REQ = ISC_REQ_SEQUENCE_DETECT or ISC_REQ_REPLAY_DETECT or
    ISC_REQ_CONFIDENTIALITY or ISC_REQ_EXTENDED_ERROR or ISC_REQ_ALLOCATE_MEMORY or
    ISC_REQ_STREAM;

function AcquireCredentialsHandleW(pszPrincipal, pszPackage: PWideChar;
  fCredentialUse: ULONG; pvLogonID, pAuthData, pGetKeyFn, pvGetKeyArgument: Pointer;
  phCredential: PCredHandle; ptsExpiry: PSecTimeStamp): SECURITY_STATUS; stdcall;
  external 'secur32.dll';

function InitializeSecurityContextW(phCredential: PCredHandle;
  phContext: PCtxtHandle; pszTargetName: PWideChar; fContextReq, Reserved1,
  TargetDataRep: ULONG; pInput: PSecBufferDesc; Reserved2: ULONG;
  phNewContext: PCtxtHandle; pOutput: PSecBufferDesc; pfContextAttr: PULONG;
  ptsExpiry: PSecTimeStamp): SECURITY_STATUS; stdcall; external 'secur32.dll';

function QueryContextAttributesW(phContext: PCtxtHandle; ulAttribute: ULONG;
  pBuffer: Pointer): SECURITY_STATUS; stdcall; external 'secur32.dll';

function EncryptMessage(phContext: PCtxtHandle; fQOP: ULONG;
  pMessage: PSecBufferDesc; MessageSeqNo: ULONG): SECURITY_STATUS; stdcall;
  external 'secur32.dll';

function DecryptMessage(phContext: PCtxtHandle; pMessage: PSecBufferDesc;
  MessageSeqNo: ULONG; pfQOP: PULONG): SECURITY_STATUS; stdcall;
  external 'secur32.dll';

function ApplyControlToken(phContext: PCtxtHandle;
  pInput: PSecBufferDesc): SECURITY_STATUS; stdcall; external 'secur32.dll';

function DeleteSecurityContext(phContext: PCtxtHandle): SECURITY_STATUS; stdcall;
  external 'secur32.dll';

function FreeCredentialsHandle(phCredential: PCredHandle): SECURITY_STATUS; stdcall;
  external 'secur32.dll';

function FreeContextBuffer(pvContextBuffer: Pointer): SECURITY_STATUS; stdcall;
  external 'secur32.dll';

// Compara um SECURITY_STATUS com uma constante (as de erro têm bit alto setado).
function StatusIs(AStatus: SECURITY_STATUS; AConst: Cardinal): Boolean; inline;
begin
  Result := Cardinal(AStatus) = AConst;
end;

function StatusFailed(AStatus: SECURITY_STATUS): Boolean; inline;
begin
  Result := AStatus < 0; // HRESULT-like: negativo = falha
end;

{ TAMQPSchannelStream }

constructor TAMQPSchannelStream.Create(AUnderlying: TStream;
  const ATargetName: string; AVerifyPeer: Boolean);
begin
  inherited Create;
  FUnderlying := AUnderlying;
  FTargetName := UnicodeString(ATargetName);
  FVerifyPeer := AVerifyPeer;
  SetLength(FCipher, RAW_CHUNK);
  FCipherLen := 0;
  // Se AcquireCred/DoHandshake levantarem, o Delphi chama o destrutor
  // automaticamente — que libera credencial/contexto (guardados por
  // FCredValid/FCtxtValid) e o stream de baixo. Nada de cleanup manual aqui
  // (seria double-free).
  AcquireCred;
  DoHandshake;
end;

destructor TAMQPSchannelStream.Destroy;
begin
  try
    if FCtxtValid then
      ShutdownTls;
  except
  end;
  if FCtxtValid then
    DeleteSecurityContext(@FCtxt);
  if FCredValid then
    FreeCredentialsHandle(@FCred);
  FUnderlying.Free;
  inherited;
end;

procedure TAMQPSchannelStream.AcquireCred;
var
  LCred: TSChannelCred;
  LStatus: SECURITY_STATUS;
begin
  FillChar(LCred, SizeOf(LCred), 0);
  LCred.dwVersion := SCHANNEL_CRED_VERSION;
  if FVerifyPeer then
    LCred.dwFlags := SCH_CRED_AUTO_CRED_VALIDATION or SCH_CRED_NO_DEFAULT_CREDS
  else
    LCred.dwFlags := SCH_CRED_MANUAL_CRED_VALIDATION or
      SCH_CRED_NO_SERVERNAME_CHECK or SCH_CRED_NO_DEFAULT_CREDS;
  // grbitEnabledProtocols = 0: deixa o SChannel escolher (TLS 1.2/1.3).

  LStatus := AcquireCredentialsHandleW(nil, UNISP_NAME, SECPKG_CRED_OUTBOUND,
    nil, @LCred, nil, nil, @FCred, nil);
  if StatusFailed(LStatus) then
    raise EAMQPTls.CreateFmt('AcquireCredentialsHandle falhou (0x%.8x)', [Cardinal(LStatus)]);
  FCredValid := True;
end;

// Lê um bloco do socket e acrescenta ao fim de FCipher (crescendo o array se
// preciso). Levanta EAMQPTls em fim de stream.
procedure TAMQPSchannelStream.RecvRaw;
var
  LRead: Integer;
begin
  if Length(FCipher) - FCipherLen < RAW_CHUNK then
    SetLength(FCipher, FCipherLen + RAW_CHUNK);
  LRead := FUnderlying.Read(FCipher[FCipherLen], RAW_CHUNK);
  if LRead <= 0 then
    raise EAMQPTls.Create('conexão fechada durante TLS');
  Inc(FCipherLen, LRead);
end;

procedure TAMQPSchannelStream.SendAll(APtr: Pointer; ALen: Integer);
var
  LWritten, LNow: Integer;
  P: PByte;
begin
  P := APtr;
  LWritten := 0;
  while LWritten < ALen do
  begin
    LNow := FUnderlying.Write(P[LWritten], ALen - LWritten);
    if LNow <= 0 then
      raise EAMQPTls.Create('falha ao enviar dados TLS');
    Inc(LWritten, LNow);
  end;
end;

procedure TAMQPSchannelStream.DoHandshake;
var
  LInBuf: array[0..1] of TSecBuffer;
  LOutBuf: array[0..0] of TSecBuffer;
  LInDesc, LOutDesc: TSecBufferDesc;
  LAttr: ULONG;
  LStatus: SECURITY_STATUS;
  LTarget: PWideChar;
  LReadMore: Boolean;
  LExtra: Integer;
begin
  LTarget := nil;
  if FTargetName <> '' then
    LTarget := PWideChar(FTargetName);

  // 1ª chamada: sem token de entrada, gera o ClientHello.
  LOutBuf[0].cbBuffer := 0;
  LOutBuf[0].BufferType := SECBUFFER_TOKEN;
  LOutBuf[0].pvBuffer := nil;
  LOutDesc.ulVersion := SECBUFFER_VERSION;
  LOutDesc.cBuffers := 1;
  LOutDesc.pBuffers := @LOutBuf[0];

  LStatus := InitializeSecurityContextW(@FCred, nil, LTarget, HANDSHAKE_REQ, 0,
    SECURITY_NATIVE_DREP, nil, 0, @FCtxt, @LOutDesc, @LAttr, nil);
  if not StatusIs(LStatus, SEC_I_CONTINUE_NEEDED) then
  begin
    // Libera um eventual token de saída alocado (ex.: alerta TLS) antes de sair,
    // para não vazar o buffer do SSPI no caminho de falha.
    if LOutBuf[0].pvBuffer <> nil then
      FreeContextBuffer(LOutBuf[0].pvBuffer);
    raise EAMQPTls.CreateFmt('InitializeSecurityContext inicial falhou (0x%.8x)', [Cardinal(LStatus)]);
  end;
  FCtxtValid := True; // contexto criado
  if (LOutBuf[0].cbBuffer > 0) and (LOutBuf[0].pvBuffer <> nil) then
  begin
    SendAll(LOutBuf[0].pvBuffer, LOutBuf[0].cbBuffer);
    FreeContextBuffer(LOutBuf[0].pvBuffer);
  end;

  // Loop: lê resposta do servidor e alimenta o ISC até SEC_E_OK. Só lê mais bytes
  // quando o buffer está vazio ou o record chegou incompleto.
  LReadMore := True;
  while True do
  begin
    if LReadMore or (FCipherLen = 0) then
      RecvRaw;

    LInBuf[0].cbBuffer := FCipherLen;
    LInBuf[0].BufferType := SECBUFFER_TOKEN;
    LInBuf[0].pvBuffer := @FCipher[0];
    LInBuf[1].cbBuffer := 0;
    LInBuf[1].BufferType := SECBUFFER_EMPTY;
    LInBuf[1].pvBuffer := nil;
    LInDesc.ulVersion := SECBUFFER_VERSION;
    LInDesc.cBuffers := 2;
    LInDesc.pBuffers := @LInBuf[0];

    LOutBuf[0].cbBuffer := 0;
    LOutBuf[0].BufferType := SECBUFFER_TOKEN;
    LOutBuf[0].pvBuffer := nil;
    LOutDesc.ulVersion := SECBUFFER_VERSION;
    LOutDesc.cBuffers := 1;
    LOutDesc.pBuffers := @LOutBuf[0];

    LStatus := InitializeSecurityContextW(@FCred, @FCtxt, LTarget, HANDSHAKE_REQ,
      0, SECURITY_NATIVE_DREP, @LInDesc, 0, @FCtxt, @LOutDesc, @LAttr, nil);

    // Token de saída pendente (mesmo em SEC_E_OK pode haver): envia.
    if (LOutBuf[0].cbBuffer > 0) and (LOutBuf[0].pvBuffer <> nil) then
    begin
      SendAll(LOutBuf[0].pvBuffer, LOutBuf[0].cbBuffer);
      FreeContextBuffer(LOutBuf[0].pvBuffer);
      LOutBuf[0].pvBuffer := nil;
    end;

    if StatusIs(LStatus, SEC_E_INCOMPLETE_MESSAGE) then
    begin
      LReadMore := True; // record incompleto: mantém FCipher e lê mais
      Continue;
    end;

    if StatusIs(LStatus, SEC_I_CONTINUE_NEEDED) or StatusIs(LStatus, SEC_E_OK) then
    begin
      // Bytes não consumidos (início do próximo record) ficam em SECBUFFER_EXTRA,
      // no FIM do buffer de entrada. Preserva-os para a próxima etapa/leitura.
      if LInBuf[1].BufferType = SECBUFFER_EXTRA then
      begin
        LExtra := LInBuf[1].cbBuffer;
        if LExtra > 0 then
          Move(FCipher[FCipherLen - LExtra], FCipher[0], LExtra);
        FCipherLen := LExtra;
      end
      else
        FCipherLen := 0;

      if StatusIs(LStatus, SEC_E_OK) then
        Break; // handshake concluído
      // Só há mais o que processar sem ler se sobrou extra; senão, aguarda o
      // servidor (lê mais).
      LReadMore := (FCipherLen = 0);
      Continue;
    end;

    raise EAMQPTls.CreateFmt('handshake TLS falhou (0x%.8x)', [Cardinal(LStatus)]);
  end;

  // Tamanhos para cifrar/decifrar (header/trailer/máx por record).
  LStatus := QueryContextAttributesW(@FCtxt, SECPKG_ATTR_STREAM_SIZES, @FStreamSizes);
  if StatusFailed(LStatus) then
    raise EAMQPTls.CreateFmt('QueryContextAttributes(STREAM_SIZES) falhou (0x%.8x)', [Cardinal(LStatus)]);
end;

// Decifra do FCipher até haver plaintext em FPlain (ou sinaliza EOF via exceção
// tratada pelo chamador). Consome/guarda SECBUFFER_EXTRA entre records.
procedure TAMQPSchannelStream.FillPlain;
var
  LBuf: array[0..3] of TSecBuffer;
  LDesc: TSecBufferDesc;
  LStatus: SECURITY_STATUS;
  I, LExtra: Integer;
  LData, LExtraBuf: PSecBuffer;
begin
  while True do
  begin
    if FCipherLen = 0 then
      RecvRaw;

    LBuf[0].cbBuffer := FCipherLen;
    LBuf[0].BufferType := SECBUFFER_DATA;
    LBuf[0].pvBuffer := @FCipher[0];
    for I := 1 to 3 do
    begin
      LBuf[I].cbBuffer := 0;
      LBuf[I].BufferType := SECBUFFER_EMPTY;
      LBuf[I].pvBuffer := nil;
    end;
    LDesc.ulVersion := SECBUFFER_VERSION;
    LDesc.cBuffers := 4;
    LDesc.pBuffers := @LBuf[0];

    LStatus := DecryptMessage(@FCtxt, @LDesc, 0, nil);

    if StatusIs(LStatus, SEC_E_INCOMPLETE_MESSAGE) then
    begin
      RecvRaw; // record incompleto: lê mais e tenta de novo
      Continue;
    end;

    if StatusIs(LStatus, SEC_I_CONTEXT_EXPIRED) then
    begin
      FCipherLen := 0;
      FPlainPos := 0;
      FPlainEnd := 0;
      Exit; // servidor encerrou o TLS (close_notify) => tratamos como EOF
    end;

    if StatusIs(LStatus, SEC_I_RENEGOTIATE) then
      // RabbitMQ não renegocia; não suportado nesta versão.
      raise EAMQPTls.Create('renegociação TLS solicitada pelo servidor não suportada');

    if not StatusIs(LStatus, SEC_E_OK) then
      raise EAMQPTls.CreateFmt('DecryptMessage falhou (0x%.8x)', [Cardinal(LStatus)]);

    // Encontra o buffer de dados decifrados e eventual sobra (próximo record).
    LData := nil;
    LExtraBuf := nil;
    for I := 0 to 3 do
      if (LBuf[I].BufferType = SECBUFFER_DATA) and (LData = nil) then
        LData := @LBuf[I]
      else if (LBuf[I].BufferType = SECBUFFER_EXTRA) and (LExtraBuf = nil) then
        LExtraBuf := @LBuf[I];

    if (LData <> nil) and (LData.cbBuffer > 0) then
    begin
      SetLength(FPlain, LData.cbBuffer);
      Move(PByte(LData.pvBuffer)^, FPlain[0], LData.cbBuffer);
      FPlainPos := 0;
      FPlainEnd := LData.cbBuffer;
    end
    else
    begin
      FPlainPos := 0;
      FPlainEnd := 0;
    end;

    // Sobra = bytes do próximo record; move para o início de FCipher.
    if LExtraBuf <> nil then
    begin
      LExtra := LExtraBuf.cbBuffer;
      if LExtra > 0 then
        Move(PByte(LExtraBuf.pvBuffer)^, FCipher[0], LExtra);
      FCipherLen := LExtra;
    end
    else
      FCipherLen := 0;

    if FPlainEnd > 0 then
      Exit; // temos plaintext para servir
    // record sem dados de aplicação (ex.: handshake pós-troca): decifra o próximo
  end;
end;

function TAMQPSchannelStream.Read(var Buffer; Count: Longint): Longint;
var
  LAvail: Integer;
begin
  if Count <= 0 then
    Exit(0);
  if FPlainPos >= FPlainEnd then
  begin
    try
      FillPlain;
    except
      on EAMQPTls do
        Exit(0); // conexão caiu / EOF: sinaliza fim de stream ao framing
    end;
    if FPlainPos >= FPlainEnd then
      Exit(0); // EOF (close_notify)
  end;
  LAvail := FPlainEnd - FPlainPos;
  if LAvail > Count then
    LAvail := Count;
  Move(FPlain[FPlainPos], Buffer, LAvail);
  Inc(FPlainPos, LAvail);
  Result := LAvail;
end;

function TAMQPSchannelStream.Write(const Buffer; Count: Longint): Longint;
var
  LBuf: array[0..3] of TSecBuffer;
  LDesc: TSecBufferDesc;
  LStatus: SECURITY_STATUS;
  LChunk, LOffset, LTotal: Integer;
  LRec: TBytes;
  P: PByte;
begin
  P := @Buffer;
  LOffset := 0;
  while LOffset < Count do
  begin
    LChunk := Count - LOffset;
    if LChunk > Integer(FStreamSizes.cbMaximumMessage) then
      LChunk := Integer(FStreamSizes.cbMaximumMessage);

    // record = header + plaintext + trailer, contíguos.
    SetLength(LRec, FStreamSizes.cbHeader + Cardinal(LChunk) + FStreamSizes.cbTrailer);
    Move(P[LOffset], LRec[FStreamSizes.cbHeader], LChunk);

    LBuf[0].cbBuffer := FStreamSizes.cbHeader;
    LBuf[0].BufferType := SECBUFFER_STREAM_HEADER;
    LBuf[0].pvBuffer := @LRec[0];
    LBuf[1].cbBuffer := LChunk;
    LBuf[1].BufferType := SECBUFFER_DATA;
    LBuf[1].pvBuffer := @LRec[FStreamSizes.cbHeader];
    LBuf[2].cbBuffer := FStreamSizes.cbTrailer;
    LBuf[2].BufferType := SECBUFFER_STREAM_TRAILER;
    LBuf[2].pvBuffer := @LRec[FStreamSizes.cbHeader + Cardinal(LChunk)];
    LBuf[3].cbBuffer := 0;
    LBuf[3].BufferType := SECBUFFER_EMPTY;
    LBuf[3].pvBuffer := nil;
    LDesc.ulVersion := SECBUFFER_VERSION;
    LDesc.cBuffers := 4;
    LDesc.pBuffers := @LBuf[0];

    LStatus := EncryptMessage(@FCtxt, 0, @LDesc, 0);
    if StatusFailed(LStatus) then
      raise EAMQPTls.CreateFmt('EncryptMessage falhou (0x%.8x)', [Cardinal(LStatus)]);

    // EncryptMessage ajusta os cbBuffer; envia os três contíguos.
    LTotal := Integer(LBuf[0].cbBuffer) + Integer(LBuf[1].cbBuffer) +
      Integer(LBuf[2].cbBuffer);
    SendAll(@LRec[0], LTotal);

    Inc(LOffset, LChunk);
  end;
  Result := Count;
end;

function TAMQPSchannelStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  raise EAMQPTls.Create('TAMQPSchannelStream não suporta Seek');
end;

procedure TAMQPSchannelStream.ShutdownTls;
var
  LToken: DWORD;
  LCtrlBuf: TSecBuffer;
  LCtrlDesc: TSecBufferDesc;
  LOutBuf: TSecBuffer;
  LOutDesc: TSecBufferDesc;
  LAttr: ULONG;
  LStatus: SECURITY_STATUS;
  LTarget: PWideChar;
begin
  // Best-effort: avisa o servidor (close_notify). Falhas são ignoradas.
  LToken := SCHANNEL_SHUTDOWN;
  LCtrlBuf.cbBuffer := SizeOf(LToken);
  LCtrlBuf.BufferType := SECBUFFER_TOKEN;
  LCtrlBuf.pvBuffer := @LToken;
  LCtrlDesc.ulVersion := SECBUFFER_VERSION;
  LCtrlDesc.cBuffers := 1;
  LCtrlDesc.pBuffers := @LCtrlBuf;
  if StatusFailed(ApplyControlToken(@FCtxt, @LCtrlDesc)) then
    Exit;

  LTarget := nil;
  if FTargetName <> '' then
    LTarget := PWideChar(FTargetName);

  LOutBuf.cbBuffer := 0;
  LOutBuf.BufferType := SECBUFFER_TOKEN;
  LOutBuf.pvBuffer := nil;
  LOutDesc.ulVersion := SECBUFFER_VERSION;
  LOutDesc.cBuffers := 1;
  LOutDesc.pBuffers := @LOutBuf;

  LStatus := InitializeSecurityContextW(@FCred, @FCtxt, LTarget, HANDSHAKE_REQ, 0,
    SECURITY_NATIVE_DREP, nil, 0, @FCtxt, @LOutDesc, @LAttr, nil);
  if (not StatusFailed(LStatus)) and (LOutBuf.cbBuffer > 0) and
     (LOutBuf.pvBuffer <> nil) then
  begin
    try
      SendAll(LOutBuf.pvBuffer, LOutBuf.cbBuffer);
    except
    end;
    FreeContextBuffer(LOutBuf.pvBuffer);
  end;
end;

{$ENDIF AMQP_WINDOWS}

end.

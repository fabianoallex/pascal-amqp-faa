unit AMQP.Transport.OpenSSL;

{$I amqp.inc}

{ Transporte TLS (amqps://) via OpenSSL (libssl/libcrypto), multiplataforma.

  Compilado apenas sob a diretiva AMQP_OPENSSL (opt-in, definida pelo projeto
  consumidor — nunca automática): diferente do SChannel, que é garantido existir
  no Windows, o OpenSSL depende de libssl/libcrypto presentes na máquina. Sem a
  diretiva esta unit compila vazia e nada muda no build. Com ela, o OpenSSL é
  usado em QUALQUER plataforma (inclusive Windows, no lugar do SChannel).

  Mesma receita de AMQP.Transport.Tls (SChannel): bindings próprios contra a
  API pública (nada de código de terceiros — só as assinaturas da ABI), uma
  única unit compartilhada pelos dois compiladores. As bibliotecas são
  carregadas DINAMICAMENTE (dlopen/LoadLibrary) na primeira conexão TLS, com
  uma lista de sonames por versão (3.x preferida, 1.1.1 aceita): o binário não
  ganha dependência de link e, se a lib não estiver instalada, o erro só
  acontece — com mensagem clara — quando UseTls=True. Os bindings se limitam a
  símbolos com a MESMA assinatura em 1.1.1 e 3.x.

  TAMQPOpenSslStream é um TStream que envolve o stream de bytes crus (o
  TAMQPSocketStream sobre o socket) e cifra/decifra por cima — mesmo contrato
  do TAMQPSchannelStream, então AMQP.Connection só troca a classe no branch de
  compilação. O engine SSL trabalha sobre um PAR DE BIOs DE MEMÓRIA (nunca
  sobre o fd do socket), por dois motivos que são invariantes da lib:

  - Os bytes crus continuam passando por FUnderlying.Read/Write: fechar o
    socket (reconexão/heartbeat) desbloqueia a thread de leitura exatamente
    como no caminho plain, em qualquer SO.
  - O objeto SSL do OpenSSL NÃO aceita SSL_read/SSL_write concorrentes — e na
    lib a thread de leitura lê enquanto outras threads escrevem (serializadas
    por FWriteLock, mas concorrentes com a leitura). Com BIOs de memória as
    chamadas SSL_* nunca bloqueiam (o bloqueio fica no FUnderlying.Read/Write,
    FORA do lock), então um TCriticalSection interno (FLock) protege o objeto
    SSL sem NUNCA ser segurado durante I/O de socket: o ciphertext produzido é
    drenado do BIO sob FLock e enviado depois de soltá-lo (ordem no fio
    garantida por FSendLock), e a leitura segue fluindo mesmo com um envio
    bloqueado em backpressure.

  Contrato de threading (igual ao uso real em AMQP.Connection): UM leitor
  (a thread de leitura) e escritores já serializados entre si; Read nunca é
  chamado concorrente com outro Read.

  Escopo: cliente, autenticação de servidor. Validação de certificado via
  trust store do sistema (SSL_CTX_set_default_verify_paths) + hostname
  (SSL_set1_host), ou modo inseguro opt-in (self-signed em dev). mTLS e
  validação por IP-SAN ficam fora desta primeira versão, como no SChannel. }

interface

{$IFDEF AMQP_OPENSSL}

uses
  SysUtils,
  Classes,
  SyncObjs,
  AMQP.Transport; // EAMQPTls (comum aos transportes TLS)

type
  { Stream TLS cliente sobre OpenSSL. Faz o handshake no construtor (síncrono,
    sobre o stream cru), depois cifra/decifra em Read/Write. É dono do stream
    de baixo (Free libera os dois). }
  TAMQPOpenSslStream = class(TStream)
  private
    FUnderlying: TStream; // bytes crus (socket); TAMQPOpenSslStream é dono
    // Protege o objeto SSL (leitor x escritores). NUNCA segurado durante I/O
    // de socket: quem drena o FBioOut envia fora dele (ver DrainBioOut).
    FLock: TCriticalSection;
    // Ordena o envio do ciphertext drenado: registros TLS fora de ordem no
    // fio derrubam a conexão (MAC/sequência). Adquirir SEMPRE antes de FLock.
    FSendLock: TCriticalSection;
    FCtx: Pointer;    // SSL_CTX*
    FSsl: Pointer;    // SSL* (dono dos dois BIOs após SSL_set_bio)
    FBioIn: Pointer;  // BIO de memória: ciphertext rede -> SSL
    FBioOut: Pointer; // BIO de memória: ciphertext SSL -> rede
    // AnsiString nos dois compiladores: a API OpenSSL recebe char* (hostname
    // ASCII; IDN exigiria punycode do chamador).
    FTargetName: AnsiString; // SNI / nome para validação
    FVerifyPeer: Boolean;
    function DrainBioOut: TBytes;    // esvazia FBioOut (chamar com FLock)
    procedure SendRaw(const AData: TBytes); // envia tudo (SEM FLock)
    procedure FlushBioOut;           // DrainBioOut+SendRaw; só handshake/shutdown
    procedure FlushBioOutBestEffort; // idem, engolindo falhas (caminho de Read)
    function ReadRawIntoBio: Boolean; // lê do socket (SEM FLock) e alimenta FBioIn
    procedure SetupSsl;
    procedure DoHandshake;
    procedure ShutdownTls;
  public
    constructor Create(AUnderlying: TStream; const ATargetName: string;
      AVerifyPeer: Boolean);
    destructor Destroy; override;
    function Read(var Buffer; Count: Longint): Longint; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
  end;

{$ENDIF AMQP_OPENSSL}

implementation

{$IFDEF AMQP_OPENSSL}

// Delphi fora do Windows: LoadLibrary/GetProcAddress vêm do próprio SysUtils
// (já no uses da interface), então lá não há uses de implementation.
{$IFDEF FPC}
uses
  dynlibs;
{$ELSE}
  {$IFDEF MSWINDOWS}
uses
  Windows;
  {$ENDIF}
{$ENDIF}

{ ------------------------------------------------------------------------------
  Bindings OpenSSL (libssl/libcrypto). Assinaturas idênticas em 1.1.1 e 3.x;
  tudo cdecl. 'long' do C: 32 bits no Windows (LLP64), largura de ponteiro nos
  Unix (LP64) — por isso TSslLong/TSslULong.
  ------------------------------------------------------------------------------ }

type
  {$IFDEF AMQP_WINDOWS}
  TSslLong = LongInt;
  TSslULong = LongWord;
  {$ELSE}
  TSslLong = NativeInt;
  TSslULong = NativeUInt;
  {$ENDIF}

  TSslLibHandle = {$IFDEF FPC}TLibHandle{$ELSE}HMODULE{$ENDIF};

const
  // Pares (libcrypto, libssl) tentados em ordem; carregamos os DOIS handles
  // explicitamente porque no Windows GetProcAddress não enxerga dependências
  // (os símbolos ERR_*/BIO_* vivem no libcrypto). DLL de arquitetura errada
  // apenas falha o load e passa ao próximo par.
  {$IFDEF AMQP_WINDOWS}
  SSL_LIB_PAIRS: array[0..3, 0..1] of string = (
    ('libcrypto-3-x64.dll', 'libssl-3-x64.dll'),
    ('libcrypto-3.dll', 'libssl-3.dll'),
    ('libcrypto-1_1-x64.dll', 'libssl-1_1-x64.dll'),
    ('libcrypto-1_1.dll', 'libssl-1_1.dll'));
  {$ELSE}
  SSL_LIB_PAIRS: array[0..2, 0..1] of string = (
    ('libcrypto.so.3', 'libssl.so.3'),
    ('libcrypto.so.1.1', 'libssl.so.1.1'),
    ('libcrypto.so', 'libssl.so'));
  {$ENDIF}

  // SSL_get_error
  SSL_ERROR_WANT_READ   = 2;
  SSL_ERROR_WANT_WRITE  = 3;
  SSL_ERROR_ZERO_RETURN = 6;

  // SSL_CTX_set_verify
  SSL_VERIFY_NONE = 0;
  SSL_VERIFY_PEER = 1;

  // SSL_ctrl / SSL_CTX_ctrl (as "funções" SSL_set_min_proto_version e
  // SSL_set_tlsext_host_name são macros sobre estes cmds)
  SSL_CTRL_SET_TLSEXT_HOSTNAME    = 55;
  SSL_CTRL_SET_MIN_PROTO_VERSION  = 123;
  TLSEXT_NAMETYPE_HOST_NAME       = 0;
  TLS1_2_VERSION                  = $0303;

  X509_V_OK = 0;

  RAW_CHUNK = 16384; // leitura de socket por rodada (record TLS máx ~16KB)

var
  // libssl
  p_TLS_client_method: function: Pointer; cdecl;
  p_SSL_CTX_new: function(AMeth: Pointer): Pointer; cdecl;
  p_SSL_CTX_free: procedure(ACtx: Pointer); cdecl;
  p_SSL_CTX_ctrl: function(ACtx: Pointer; ACmd: Integer; ALarg: TSslLong;
    AParg: Pointer): TSslLong; cdecl;
  p_SSL_CTX_set_verify: procedure(ACtx: Pointer; AMode: Integer;
    ACallback: Pointer); cdecl;
  p_SSL_CTX_set_default_verify_paths: function(ACtx: Pointer): Integer; cdecl;
  p_SSL_new: function(ACtx: Pointer): Pointer; cdecl;
  p_SSL_free: procedure(ASsl: Pointer); cdecl;
  p_SSL_ctrl: function(ASsl: Pointer; ACmd: Integer; ALarg: TSslLong;
    AParg: Pointer): TSslLong; cdecl;
  p_SSL_set1_host: function(ASsl: Pointer; AHost: PAnsiChar): Integer; cdecl;
  p_SSL_set_bio: procedure(ASsl: Pointer; ARbio, AWbio: Pointer); cdecl;
  p_SSL_set_connect_state: procedure(ASsl: Pointer); cdecl;
  p_SSL_do_handshake: function(ASsl: Pointer): Integer; cdecl;
  p_SSL_read: function(ASsl: Pointer; ABuf: Pointer; ANum: Integer): Integer; cdecl;
  p_SSL_write: function(ASsl: Pointer; ABuf: Pointer; ANum: Integer): Integer; cdecl;
  p_SSL_get_error: function(ASsl: Pointer; ARet: Integer): Integer; cdecl;
  p_SSL_shutdown: function(ASsl: Pointer): Integer; cdecl;
  p_SSL_get_verify_result: function(ASsl: Pointer): TSslLong; cdecl;
  // libcrypto
  p_BIO_new: function(AMethod: Pointer): Pointer; cdecl;
  p_BIO_s_mem: function: Pointer; cdecl;
  p_BIO_free: function(ABio: Pointer): Integer; cdecl;
  p_BIO_read: function(ABio: Pointer; ABuf: Pointer; ALen: Integer): Integer; cdecl;
  p_BIO_write: function(ABio: Pointer; ABuf: Pointer; ALen: Integer): Integer; cdecl;
  p_BIO_ctrl_pending: function(ABio: Pointer): NativeUInt; cdecl;
  p_ERR_get_error: function: TSslULong; cdecl;
  p_ERR_error_string_n: procedure(AErr: TSslULong; ABuf: PAnsiChar;
    ALen: NativeUInt); cdecl;
  p_ERR_clear_error: procedure; cdecl;

  // Carregamento único por processo (as libs nunca são descarregadas).
  GLibLock: TCriticalSection;
  GLibLoaded: Boolean;
  GLibCrypto: TSslLibHandle;
  GLibSsl: TSslLibHandle;

function SslLoadLib(const AName: string): TSslLibHandle;
begin
  {$IFDEF FPC}
  Result := dynlibs.LoadLibrary(AName);
  {$ELSE}
  Result := LoadLibrary(PChar(AName));
  {$ENDIF}
end;

procedure SslFreeLib(AHandle: TSslLibHandle);
begin
  {$IFDEF FPC}
  dynlibs.FreeLibrary(AHandle);
  {$ELSE}
  FreeLibrary(AHandle);
  {$ENDIF}
end;

function SslGetProc(AHandle: TSslLibHandle; const AName: string): Pointer;
begin
  {$IFDEF FPC}
  Result := GetProcedureAddress(AHandle, AName);
  {$ELSE}
    {$IFDEF MSWINDOWS}
  // Winapi.Windows.GetProcAddress recebe LPCSTR (PAnsiChar); PChar aqui
  // seria PWideChar e nem compila. Nomes de símbolo são ASCII.
  Result := GetProcAddress(AHandle, PAnsiChar(AnsiString(AName)));
    {$ELSE}
  Result := GetProcAddress(AHandle, PChar(AName)); // SysUtils POSIX: PChar
    {$ENDIF}
  {$ENDIF}
end;

// Resolve um símbolo obrigatório; falha = instalação de OpenSSL quebrada.
function SslMustGet(AHandle: TSslLibHandle; const AName, ALib: string): Pointer;
begin
  Result := SslGetProc(AHandle, AName);
  if Result = nil then
    raise EAMQPTls.CreateFmt('símbolo %s não encontrado em %s (OpenSSL incompatível?)',
      [AName, ALib]);
end;

// Carrega libcrypto+libssl (uma vez por processo) e resolve os símbolos.
procedure EnsureOpenSsl;
var
  I: Integer;
  LCrypto, LSsl: TSslLibHandle;
  LCryptoName, LSslName, LTried: string;
begin
  // Sem fast-path fora do lock: é chamado uma vez por conexão TLS (fora de
  // caminho quente) e a checagem dupla sem barreira de memória não seria
  // segura em CPU de ordenação fraca (ARM está no roadmap).
  GLibLock.Enter;
  try
    if GLibLoaded then
      Exit;

    LCrypto := 0;
    LSsl := 0;
    LCryptoName := '';
    LSslName := '';
    LTried := '';
    for I := Low(SSL_LIB_PAIRS) to High(SSL_LIB_PAIRS) do
    begin
      if LTried <> '' then
        LTried := LTried + ', ';
      LTried := LTried + SSL_LIB_PAIRS[I][1];
      LCrypto := SslLoadLib(SSL_LIB_PAIRS[I][0]);
      if LCrypto = 0 then
        Continue;
      LSsl := SslLoadLib(SSL_LIB_PAIRS[I][1]);
      if LSsl = 0 then
      begin
        SslFreeLib(LCrypto);
        LCrypto := 0;
        Continue;
      end;
      LCryptoName := SSL_LIB_PAIRS[I][0];
      LSslName := SSL_LIB_PAIRS[I][1];
      Break;
    end;
    if LSslName = '' then
      raise EAMQPTls.CreateFmt(
        'OpenSSL não encontrado (tentados: %s). Instale libssl/libcrypto ou desabilite UseTls.',
        [LTried]);

    try
      p_TLS_client_method := SslMustGet(LSsl, 'TLS_client_method', LSslName);
      p_SSL_CTX_new := SslMustGet(LSsl, 'SSL_CTX_new', LSslName);
      p_SSL_CTX_free := SslMustGet(LSsl, 'SSL_CTX_free', LSslName);
      p_SSL_CTX_ctrl := SslMustGet(LSsl, 'SSL_CTX_ctrl', LSslName);
      p_SSL_CTX_set_verify := SslMustGet(LSsl, 'SSL_CTX_set_verify', LSslName);
      p_SSL_CTX_set_default_verify_paths :=
        SslMustGet(LSsl, 'SSL_CTX_set_default_verify_paths', LSslName);
      p_SSL_new := SslMustGet(LSsl, 'SSL_new', LSslName);
      p_SSL_free := SslMustGet(LSsl, 'SSL_free', LSslName);
      p_SSL_ctrl := SslMustGet(LSsl, 'SSL_ctrl', LSslName);
      p_SSL_set1_host := SslMustGet(LSsl, 'SSL_set1_host', LSslName);
      p_SSL_set_bio := SslMustGet(LSsl, 'SSL_set_bio', LSslName);
      p_SSL_set_connect_state := SslMustGet(LSsl, 'SSL_set_connect_state', LSslName);
      p_SSL_do_handshake := SslMustGet(LSsl, 'SSL_do_handshake', LSslName);
      p_SSL_read := SslMustGet(LSsl, 'SSL_read', LSslName);
      p_SSL_write := SslMustGet(LSsl, 'SSL_write', LSslName);
      p_SSL_get_error := SslMustGet(LSsl, 'SSL_get_error', LSslName);
      p_SSL_shutdown := SslMustGet(LSsl, 'SSL_shutdown', LSslName);
      p_SSL_get_verify_result := SslMustGet(LSsl, 'SSL_get_verify_result', LSslName);

      p_BIO_new := SslMustGet(LCrypto, 'BIO_new', LCryptoName);
      p_BIO_s_mem := SslMustGet(LCrypto, 'BIO_s_mem', LCryptoName);
      p_BIO_free := SslMustGet(LCrypto, 'BIO_free', LCryptoName);
      p_BIO_read := SslMustGet(LCrypto, 'BIO_read', LCryptoName);
      p_BIO_write := SslMustGet(LCrypto, 'BIO_write', LCryptoName);
      p_BIO_ctrl_pending := SslMustGet(LCrypto, 'BIO_ctrl_pending', LCryptoName);
      p_ERR_get_error := SslMustGet(LCrypto, 'ERR_get_error', LCryptoName);
      p_ERR_error_string_n := SslMustGet(LCrypto, 'ERR_error_string_n', LCryptoName);
      p_ERR_clear_error := SslMustGet(LCrypto, 'ERR_clear_error', LCryptoName);
    except
      // Instalação com símbolo faltando: descarrega pra não acumular uma
      // referência de load a cada nova tentativa de conexão.
      SslFreeLib(LSsl);
      SslFreeLib(LCrypto);
      raise;
    end;

    GLibCrypto := LCrypto;
    GLibSsl := LSsl;
    GLibLoaded := True;
  finally
    GLibLock.Leave;
  end;
end;

// Texto do erro mais recente da fila do OpenSSL ('' se vazia).
function LastSslErrorText: string;
var
  LCode: TSslULong;
  LBuf: array[0..255] of AnsiChar;
begin
  Result := '';
  LCode := p_ERR_get_error();
  if LCode = 0 then
    Exit;
  LBuf[0] := #0;
  p_ERR_error_string_n(LCode, @LBuf[0], SizeOf(LBuf));
  Result := string(AnsiString(PAnsiChar(@LBuf[0])));
end;

// Mensagem para uma falha de SSL_read/SSL_write/SSL_do_handshake.
function SslFailureText(AErr: Integer): string;
begin
  Result := LastSslErrorText;
  if Result = '' then
    Result := Format('SSL_get_error=%d', [AErr]);
end;

{ TAMQPOpenSslStream }

constructor TAMQPOpenSslStream.Create(AUnderlying: TStream;
  const ATargetName: string; AVerifyPeer: Boolean);
begin
  inherited Create;
  FUnderlying := AUnderlying;
  FTargetName := AnsiString(ATargetName);
  FVerifyPeer := AVerifyPeer;
  FLock := TCriticalSection.Create;
  FSendLock := TCriticalSection.Create;
  // Se algo abaixo levantar, o destrutor (auto-chamado) libera o que existir
  // (handles guardados em campos) e o stream de baixo — nada de cleanup manual
  // aqui (seria double-free). Mesmo contrato do TAMQPSchannelStream.
  EnsureOpenSsl;
  SetupSsl;
  DoHandshake;
end;

destructor TAMQPOpenSslStream.Destroy;
begin
  try
    if FSsl <> nil then
      ShutdownTls;
  except
  end;
  if FSsl <> nil then
    p_SSL_free(FSsl) // libera também os dois BIOs (adotados por SSL_set_bio)
  else
  begin
    // Falha entre criar os BIOs e criar o SSL: ainda são nossos.
    if FBioIn <> nil then
      p_BIO_free(FBioIn);
    if FBioOut <> nil then
      p_BIO_free(FBioOut);
  end;
  if FCtx <> nil then
    p_SSL_CTX_free(FCtx);
  FUnderlying.Free;
  FSendLock.Free;
  FLock.Free;
  inherited;
end;

procedure TAMQPOpenSslStream.SetupSsl;
begin
  FCtx := p_SSL_CTX_new(p_TLS_client_method());
  if FCtx = nil then
    raise EAMQPTls.CreateFmt('SSL_CTX_new falhou (%s)', [LastSslErrorText]);

  // Mínimo TLS 1.2, como no caminho SChannel (defaults modernos do sistema).
  p_SSL_CTX_ctrl(FCtx, SSL_CTRL_SET_MIN_PROTO_VERSION, TLS1_2_VERSION, nil);

  if FVerifyPeer then
  begin
    p_SSL_CTX_set_verify(FCtx, SSL_VERIFY_PEER, nil);
    // Trust store do sistema (ex.: /etc/ssl/certs no Linux).
    if p_SSL_CTX_set_default_verify_paths(FCtx) <> 1 then
      raise EAMQPTls.CreateFmt('SSL_CTX_set_default_verify_paths falhou (%s)',
        [LastSslErrorText]);
  end
  else
    p_SSL_CTX_set_verify(FCtx, SSL_VERIFY_NONE, nil);

  // Ordem importa pro cleanup do destrutor: BIOs antes do SSL (se SSL_new
  // falhar, FSsl=nil e o destrutor libera os BIOs); SSL_set_bio logo em
  // seguida (não falha) transfere a posse dos dois pro SSL.
  FBioIn := p_BIO_new(p_BIO_s_mem());
  FBioOut := p_BIO_new(p_BIO_s_mem());
  if (FBioIn = nil) or (FBioOut = nil) then
    raise EAMQPTls.Create('BIO_new(BIO_s_mem) falhou');
  FSsl := p_SSL_new(FCtx);
  if FSsl = nil then
    raise EAMQPTls.CreateFmt('SSL_new falhou (%s)', [LastSslErrorText]);
  p_SSL_set_bio(FSsl, FBioIn, FBioOut);

  if FTargetName <> '' then
  begin
    // SNI (SSL_set_tlsext_host_name é macro sobre SSL_ctrl).
    p_SSL_ctrl(FSsl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_HOST_NAME,
      PAnsiChar(FTargetName));
    // Validação de hostname contra o cert (só faz sentido com verify).
    if FVerifyPeer and (p_SSL_set1_host(FSsl, PAnsiChar(FTargetName)) <> 1) then
      raise EAMQPTls.Create('SSL_set1_host falhou');
  end;

  p_SSL_set_connect_state(FSsl);
end;

// Esvazia FBioOut (o ciphertext que o engine produziu) num buffer; devolve
// nil se não havia nada. Chamar segurando FLock — mas o ENVIO fica fora dele,
// pra nunca segurar o objeto SSL durante I/O de socket (um send bloqueado em
// backpressure não pode parar a thread de leitura, que precisa do FLock pra
// alimentar FBioIn). FSendLock, adquirido antes de FLock, garante que drenos
// concorrentes cheguem ao fio na ordem em que saíram do engine.
function TAMQPOpenSslStream.DrainBioOut: TBytes;
var
  LTotal, LN: Integer;
begin
  Result := nil;
  LTotal := 0;
  while p_BIO_ctrl_pending(FBioOut) > 0 do
  begin
    SetLength(Result, LTotal + RAW_CHUNK);
    LN := p_BIO_read(FBioOut, @Result[LTotal], RAW_CHUNK);
    if LN <= 0 then
      Break;
    Inc(LTotal, LN);
  end;
  SetLength(Result, LTotal);
end;

// Envia o buffer inteiro no stream de baixo. Chamar SEM FLock (ver DrainBioOut).
procedure TAMQPOpenSslStream.SendRaw(const AData: TBytes);
var
  LWritten, LNow: Integer;
begin
  LWritten := 0;
  while LWritten < Length(AData) do
  begin
    LNow := FUnderlying.Write(AData[LWritten], Length(AData) - LWritten);
    if LNow <= 0 then
      raise EAMQPTls.Create('falha ao enviar dados TLS');
    Inc(LWritten, LNow);
  end;
end;

// Drena e envia numa tacada. Só pro handshake (construtor: nenhuma outra
// thread conhece o objeto, dispensa locks); os caminhos concorrentes drenam
// sob FLock e enviam fora dele.
procedure TAMQPOpenSslStream.FlushBioOut;
begin
  SendRaw(DrainBioOut);
end;

// Flush do caminho de leitura (raro: resposta a mensagem pós-handshake que o
// SSL_read produziu). Best-effort dos dois lados:
// - TryEnter: se um escritor está com o FSendLock (possivelmente bloqueado num
//   send), não vale segurar a entrega dos bytes já decifrados — o dreno do
//   próprio escritor leva o pendente junto (e heartbeats garantem escritas
//   periódicas).
// - Falha de envio é engolida: perder este flush é melhor que perder os bytes
//   já decifrados (a falha do socket reaparece no próximo Read/Write).
procedure TAMQPOpenSslStream.FlushBioOutBestEffort;
var
  LOut: TBytes;
begin
  if not FSendLock.TryEnter then
    Exit;
  try
    FLock.Enter;
    try
      LOut := DrainBioOut;
    finally
      FLock.Leave;
    end;
    try
      SendRaw(LOut);
    except
    end;
  finally
    FSendLock.Leave;
  end;
end;

// Lê um bloco do socket (bloqueante, SEM segurar FLock — é aqui que a thread
// de leitura espera) e o entrega ao engine (FBioIn, sob FLock). False = EOF.
function TAMQPOpenSslStream.ReadRawIntoBio: Boolean;
var
  LBuf: array[0..RAW_CHUNK - 1] of Byte;
  LRead: Integer;
begin
  LRead := FUnderlying.Read(LBuf[0], RAW_CHUNK);
  if LRead <= 0 then
    Exit(False);
  FLock.Enter;
  try
    // BIO de memória cresce sob demanda: aceita o bloco inteiro.
    if p_BIO_write(FBioIn, @LBuf[0], LRead) <> LRead then
      raise EAMQPTls.Create('BIO_write falhou');
  finally
    FLock.Leave;
  end;
  Result := True;
end;

procedure TAMQPOpenSslStream.DoHandshake;
var
  LRet, LErr: Integer;
  LVerify: TSslLong;
begin
  // Síncrono, no construtor: nenhuma outra thread conhece este objeto ainda.
  p_ERR_clear_error();
  while True do
  begin
    LRet := p_SSL_do_handshake(FSsl);
    FlushBioOut; // envia o que foi produzido (ClientHello, Finished, ...)
    if LRet = 1 then
      Break; // handshake concluído
    LErr := p_SSL_get_error(FSsl, LRet);
    case LErr of
      SSL_ERROR_WANT_READ:
        if not ReadRawIntoBio then
          raise EAMQPTls.Create('conexão fechada durante o handshake TLS');
      SSL_ERROR_WANT_WRITE:
        ; // já drenado pelo FlushBioOut acima
    else
      begin
        // Diferencia falha de validação de cert (a mais comum) das demais.
        LVerify := p_SSL_get_verify_result(FSsl);
        if FVerifyPeer and (LVerify <> X509_V_OK) then
          raise EAMQPTls.CreateFmt(
            'validação do certificado do servidor falhou (X509 err %d; %s)',
            [Integer(LVerify), SslFailureText(LErr)]);
        raise EAMQPTls.CreateFmt('handshake TLS falhou (%s)', [SslFailureText(LErr)]);
      end;
    end;
  end;
end;

function TAMQPOpenSslStream.Read(var Buffer; Count: Longint): Longint;
var
  LRet, LErr: Integer;
  LHasOut: Boolean;
begin
  if Count <= 0 then
    Exit(0);
  try
    while True do
    begin
      FLock.Enter;
      try
        LRet := p_SSL_read(FSsl, @Buffer, Count);
        if LRet > 0 then
          LErr := 0
        else
          LErr := p_SSL_get_error(FSsl, LRet);
        LHasOut := p_BIO_ctrl_pending(FBioOut) > 0;
      finally
        FLock.Leave;
      end;
      if LHasOut then
        FlushBioOutBestEffort;
      if LRet > 0 then
        Exit(LRet);
      case LErr of
        SSL_ERROR_WANT_READ:
          if not ReadRawIntoBio then
            Exit(0); // conexão caiu: EOF pro framing
        SSL_ERROR_ZERO_RETURN:
          Exit(0);   // close_notify: servidor encerrou o TLS
      else
        Exit(0);     // erro TLS: framing trata como conexão encerrada
      end;
    end;
  except
    // Igual SChannel: Read sinaliza EOF, nunca propaga exceção — nem EAMQPTls
    // nem um erro de socket do transporte (no Delphi, Receive pode levantar
    // ESocketError em vez de devolver <= 0).
    Exit(0);
  end;
end;

function TAMQPOpenSslStream.Write(const Buffer; Count: Longint): Longint;
var
  LOffset, LChunk, LRet, LErr, LWaitedMs: Integer;
  P: PByte;
  LOut: TBytes;
begin
  P := @Buffer;
  LOffset := 0;
  while LOffset < Count do
  begin
    // Chunk + flush por rodada bounda a memória do FBioOut (uma mensagem
    // grande nunca acumula o ciphertext inteiro no BIO antes do 1º send).
    LChunk := Count - LOffset;
    if LChunk > RAW_CHUNK then
      LChunk := RAW_CHUNK;

    LWaitedMs := 0;
    repeat
      FSendLock.Enter;
      try
        FLock.Enter;
        try
          p_ERR_clear_error();
          LRet := p_SSL_write(FSsl, @P[LOffset], LChunk);
          if LRet <= 0 then
            LErr := p_SSL_get_error(FSsl, LRet)
          else
            LErr := 0;
          LOut := DrainBioOut;
        finally
          FLock.Leave;
        end;
        // Fora do FLock (a leitura segue fluindo); FSendLock garante a ordem.
        SendRaw(LOut);
      finally
        FSendLock.Leave;
      end;
      if LRet > 0 then
        Break;
      case LErr of
        SSL_ERROR_WANT_READ:
        begin
          // Engine precisa de bytes de entrada (renegociação/pós-handshake).
          // Quem alimenta FBioIn é a thread de leitura; aguarda e re-tenta.
          // Na prática não acontece com RabbitMQ (sem renegociação).
          Sleep(5);
          Inc(LWaitedMs, 5);
          if LWaitedMs > 10000 then
            raise EAMQPTls.Create('timeout aguardando dados do servidor durante escrita TLS');
        end;
        SSL_ERROR_WANT_WRITE:
          ; // saída já drenada; re-tenta
      else
        raise EAMQPTls.CreateFmt('falha ao enviar dados TLS (%s)', [SslFailureText(LErr)]);
      end;
    until False;

    // BIO de memória aceita tudo: SSL_write > 0 escreveu o chunk inteiro.
    Inc(LOffset, LRet);
  end;
  Result := Count;
end;

function TAMQPOpenSslStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  raise EAMQPTls.Create('TAMQPOpenSslStream não suporta Seek');
end;

procedure TAMQPOpenSslStream.ShutdownTls;
var
  LOut: TBytes;
begin
  // Best-effort: avisa o servidor (close_notify). Falhas são ignoradas.
  FSendLock.Enter;
  try
    FLock.Enter;
    try
      p_SSL_shutdown(FSsl);
      LOut := DrainBioOut;
    finally
      FLock.Leave;
    end;
    try
      SendRaw(LOut);
    except
    end;
  finally
    FSendLock.Leave;
  end;
end;

initialization
  GLibLock := TCriticalSection.Create;

finalization
  GLibLock.Free;

{$ENDIF AMQP_OPENSSL}

end.

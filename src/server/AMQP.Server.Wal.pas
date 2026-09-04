unit AMQP.Server.Wal;

{$I amqp.inc}

{ Camada de ARQUIVO do write-ahead log -- sub-modulo broker, WS1 da Fase 4
  (durabilidade, ver CLAUDE.md, decisoes D19-D28).

  ESCOPO: esta unit nao sabe o que e' uma fila, uma mensagem ou um confirm.
  Ela sabe gravar registros numerados num arquivo append-only, devolver o maior
  PREFIXO VALIDO deles e mandar ao disco o que foi escrito. O significado do
  campo Kind e' das camadas de cima (D22: CONTENT, ENQ, DEQ, TOPO). Quem
  orquestra lote e fsync e' a thread do journal (WS2); quem da' sentido aos
  registros e' a recuperacao (WS6).

  O NOME. O plano dizia AMQP.Server.Journal.File; nao da': `File` e' palavra
  reservada em Pascal e um segmento de nome de unit nao pode se chamar assim.
  Ficou AMQP.Server.Wal (o formato e os arquivos); AMQP.Server.Journal fica
  para a WS2 (a thread e a marca d'agua).

  DECISAO D23, QUE E' O CORACAO DESTA UNIT
  ----------------------------------------
  Nao ha marcador de transacao. Cada registro carrega o proprio comprimento e
  um CRC32, e a leitura devolve o maior prefixo integro -- a cauda torta e'
  descartada. Onde a atomicidade importa (o dead-letter, que aposenta um
  registro e cria outro), quem resolve e' a ORDEM em que as camadas de cima
  escrevem: o ENQ novo antes do DEQ antigo, de modo que falhar no meio produz
  DUPLICATA e nao PERDA. Duplicata o contrato at-least-once ja' admite; perda
  nao, e o cliente nao teria como descobrir.

  Consequencia para quem le: ReadPrefix NUNCA levanta por arquivo corrompido.
  Cauda torta e' o caso NORMAL depois de uma queda -- e' o que se espera
  encontrar. O motivo da parada sai em TAMQPWalStop para quem quiser registrar,
  e o unico erro de verdade e' o de I/O.

  O PONTO DE FSYNC E' TESTAVEL DE PROPOSITO
  -----------------------------------------
  A camada 2 da D28 exige provar que o fsync acontece na fronteira do lote e
  que uma falha de I/O nao corrompe estado em memoria. Por isso o segmento nao
  fala com o sistema de arquivos direto: fala com IAMQPWalFile. Em producao e'
  TAMQPWalOsFile; nos testes e' um duble que conta sync e falha na hora
  escolhida. A costura existe desde a WS1 porque enfia-la depois seria
  reescrever esta unit.

  PRIMITIVAS DE ARQUIVO -- O QUE A WS0 MEDIU
  ------------------------------------------
  - fsync: no FPC, SysUtils.FileFlush JA E' FlushFileBuffers no Windows e
    fpfsync no Unix, medido indistinguivel do fpfsync chamado na mao. So o
    Delphi precisa de codigo proprio, e por isso o unico condicional de
    plataforma desta unit esta' em AmqpFileSync (mais o de FileTruncate).
  - custo: FlushFileBuffers custa 329-674 us por chamada (Win64/NTFS), e a cada
    100 registros cai para 530-1278 us POR LOTE -- de ~2.000 para ~100.000
    registros/s. E' o numero que sustenta a D25 (group commit auto-ajustavel).
    Esta unit nao decide QUANDO sincronizar; so' oferece a operacao.
  - lock: UM lock para o DIRETORIO (TAMQPWalDirLock), nenhum para os segmentos.
    FileOpen com fmShareExclusive da' exclusao real nos dois SOs -- share mode
    no Windows, fpflock(LOCK_EX or LOCK_NB) no Unix. Travar tambem cada
    segmento nao acrescentaria seguranca e atrapalharia quem quisesse olhar o
    arquivo com o broker no ar.

  Nas chamadas de RTL, so' as formas de assinatura IDENTICA nos dois
  compiladores: FileCreate de um argumento e FileOpen de dois. As sobrecargas
  com share mode/rights divergem, e codigo Delphi nao compilado e' divida --
  foi ela que rendeu os quatro defeitos do System.Net.Socket na Fase 1.

  FORMATO
  -------
  Cabecalho do segmento, 16 bytes:

    magic    8   'AMQPWAL1'
    version  2   = 1
    reserved 2   = 0
    segno    4   numero do segmento

  Registro, 18 bytes de moldura mais o payload:

    lsn      8   numero de sequencia, ESTRITAMENTE CRESCENTE no segmento
    kind     1   significado do payload (das camadas de cima)
    flags    1   reservado, = 0
    len      4   tamanho do payload
    payload  len
    crc32    4   sobre TUDO acima (lsn..payload), menos o proprio crc

  Inteiros em LITTLE-ENDIAN, ao contrario do resto da lib. Isto nao e' dado de
  fio: e' formato privado deste broker, escrito e lido pela mesma maquina, e as
  duas plataformas-alvo sao little-endian -- a ordem de rede so' custaria bytes
  trocados em todo registro. O magic e a versao existem para um arquivo de
  outra origem (ou de um formato futuro) ser recusado na cara, em vez de virar
  registros tortos. }

interface

uses
  SysUtils,
  Classes,
  {$IFNDEF FPC}
  Windows,
  {$ENDIF}
  AMQP.Threading;

const
  /// Assinatura do cabecalho de segmento. Muda junto com o formato.
  AMQP_WAL_MAGIC = 'AMQPWAL1';
  AMQP_WAL_VERSION = 1;

  AMQP_WAL_SEG_HEADER_SIZE = 16;
  /// lsn(8) + kind(1) + flags(1) + len(4)
  AMQP_WAL_REC_HEADER_SIZE = 14;
  /// crc32(4)
  AMQP_WAL_REC_TRAILER_SIZE = 4;
  AMQP_WAL_REC_OVERHEAD = AMQP_WAL_REC_HEADER_SIZE + AMQP_WAL_REC_TRAILER_SIZE;

  /// Teto de payload de um registro. Existe para um `len` corrompido nao virar
  /// uma alocacao gigante ANTES de o CRC poder desmenti-lo -- a checagem de
  /// "cabe no que resta do arquivo" cobre quase tudo, este teto cobre o resto.
  AMQP_WAL_MAX_PAYLOAD = 64 * 1024 * 1024;

  /// Tamanho alvo de um segmento fechado (D26). Nao e' imposto aqui -- quem
  /// rola o segmento e' a WS7; a constante vive junto do formato.
  AMQP_WAL_SEGMENT_BYTES = 16 * 1024 * 1024;

  AMQP_WAL_SEGMENT_EXT = '.wal';
  AMQP_WAL_SEGMENT_PREFIX = 'seg-';
  /// Quantos digitos o numero do segmento ocupa no nome do arquivo. DEZ, e
  /// nao oito: com oito, um ASegNo acima de 99.999.999 produziria um nome com
  /// digitos demais -- que o proprio AmqpWalParseSegmentName nao reconheceria
  /// de volta, e o segmento sumiria da listagem em silencio. Dez cobrem TODO
  /// o Cardinal, entao nao ha valor do tipo que gere nome irrecuperavel.
  AMQP_WAL_SEGMENT_DIGITS = 10;
  /// Nome do arquivo de lock do diretorio de dados.
  AMQP_WAL_LOCK_NAME = 'amqp.lock';

type
  EAMQPWal = class(Exception);

  /// Um registro do log. Kind e' opaco aqui de proposito (ver o cabecalho).
  TAMQPWalRecord = record
    Lsn: UInt64;
    Kind: Byte;
    Payload: TBytes;
  end;

  TAMQPWalRecords = array of TAMQPWalRecord;

  { Por que a leitura parou. Nenhum destes valores e' erro: num WAL, encontrar
    a cauda torta e' o caso normal depois de uma queda. Existe para o log do
    broker poder dizer O QUE achou, e para os testes distinguirem "acabou
    inteiro" de "descartei 37 bytes". }
  TAMQPWalStop = (
    /// Chegou ao fim do arquivo com todos os registros integros.
    awsFim,
    /// Faltavam bytes para completar a moldura ou o payload do ultimo.
    awsCauda,
    /// O CRC do ultimo registro nao bateu -- escrita interrompida no meio.
    awsCrc,
    /// O `len` era maior que o teto (o caso de "maior que o que resta" e'
    /// indistinguivel de cauda, e sai como awsCauda).
    awsTamanho,
    /// O LSN nao cresceu. So' acontece com lixo que passou pelo CRC (ou com
    /// arquivo remontado a mao); o prefixo valido termina aqui.
    awsLsn
  );

  { Costura de arquivo -- ver "O ponto de fsync e' testavel de proposito".

    Toda implementacao e' de UM arquivo ja' aberto, e a posse do descritor e'
    dela: soltar a ultima referencia da interface fecha. Nunca segure a
    implementacao por referencia de CLASSE -- e' refcontada. }
  IAMQPWalFile = interface
    ['{6D2A17C4-9B38-4E05-A17F-3C84E9D5B260}']
    /// Acrescenta ao FIM e devolve o offset em que os bytes comecaram.
    /// Escrita parcial e' tratada dentro (laco ate' o fim ou erro).
    function Append(const ABytes: TBytes): Int64;
    /// Le o arquivo inteiro. Segmentos sao pequenos por construcao (D26), e
    /// ler de uma vez torna a varredura de prefixo -- e a varredura exaustiva
    /// de truncamento da D28 -- simples e rapida.
    function ReadAll: TBytes;
    /// Manda ao dispositivo o que foi escrito. False = a chamada falhou, e
    /// nesse caso NENHUM confirm que dependia deste lote pode ser liberado.
    function Sync: Boolean;
    function Size: Int64;
    /// Corta o arquivo em ASize bytes (descarte da cauda torta).
    procedure TruncateTo(ASize: Int64);
  end;

  { A implementacao de verdade, sobre um descritor do sistema operacional. }
  TAMQPWalOsFile = class(TInterfacedObject, IAMQPWalFile)
  private
    FHandle: THandle;
    FPath: string;
  public
    /// ACreate = True cria (falhando se ja' existir), False abre um existente.
    /// Sem share mode exclusivo: quem exclui outro dono e' o lock do
    /// DIRETORIO (TAMQPWalDirLock).
    constructor Create(const APath: string; ACreate: Boolean);
    destructor Destroy; override;

    function Append(const ABytes: TBytes): Int64;
    function ReadAll: TBytes;
    function Sync: Boolean;
    function Size: Int64;
    procedure TruncateTo(ASize: Int64);

    property Path: string read FPath;
  end;

  { Um segmento do log: cabecalho + N registros, so' cresce no fim.

    Quem abre um segmento existente recebe o ponto de append ja' posicionado no
    fim do PREFIXO VALIDO -- e, por default, com a cauda torta truncada. Nao e'
    zelo: escrever depois de bytes que a leitura descarta produziria um
    registro integro que a recuperacao nunca alcanca, o que e' pior que a queda
    original. }
  TAMQPWalSegment = class
  private
    FFile: IAMQPWalFile;
    FSegNo: Cardinal;
    FEndOffset: Int64;
    FLastLsn: UInt64;
    FStop: TAMQPWalStop;
    FDescartados: Int64;
    /// Le o arquivo inteiro e valida o cabecalho. Levanta se nao for um
    /// segmento deste formato -- e' a UNICA familia de erro que a leitura
    /// levanta, porque um arquivo estranho no DataDir e' erro de operacao, nao
    /// consequencia de queda.
    function LeEValidaCabecalho: TBytes;
    function Varre(const ABuf: TBytes; AColeta: Boolean;
      out ARecords: TAMQPWalRecords; out AStop: TAMQPWalStop;
      out AEndOffset: Int64; out ALastLsn: UInt64): Integer;
  public
    /// Segmento novo: escreve o cabecalho e sincroniza (o cabecalho e' a unica
    /// coisa do arquivo que nao pode ser reconstruida por replay).
    constructor CreateNew(const AFile: IAMQPWalFile; ASegNo: Cardinal);
    /// Segmento existente: valida o cabecalho, varre o prefixo valido e
    /// posiciona o append. ATrim = True (default) trunca a cauda torta.
    constructor OpenExisting(const AFile: IAMQPWalFile; ATrim: Boolean = True);
    destructor Destroy; override;

    /// Acrescenta um registro. ALsn tem de ser MAIOR que o ultimo gravado --
    /// e' a invariante que a leitura confere, e violar seria escrever um
    /// arquivo que a propria recuperacao truncaria ali. Devolve o offset.
    function Append(ALsn: UInt64; AKind: Byte; const APayload: TBytes): Int64;
    /// fsync. False = a chamada falhou.
    function Sync: Boolean;

    /// Le o maior prefixo valido. Nao levanta por cauda torta.
    function ReadPrefix(out ARecords: TAMQPWalRecords;
      out AStop: TAMQPWalStop): Integer;

    /// Numero do segmento, lido do cabecalho.
    property SegNo: Cardinal read FSegNo;
    /// Primeiro byte depois do ultimo registro integro -- onde o proximo
    /// Append escreve.
    property EndOffset: Int64 read FEndOffset;
    /// LSN do ultimo registro integro (0 = segmento sem registros).
    property LastLsn: UInt64 read FLastLsn;
    /// Por que a varredura de abertura parou.
    property Stop: TAMQPWalStop read FStop;
    /// Quantos bytes de cauda torta a abertura encontrou (e truncou, se
    /// ATrim). Zero e' o caso de um segmento fechado com saude.
    property BytesDescartados: Int64 read FDescartados;
  end;

  { Um dono por diretorio de dados.

    Dois brokers sobre o mesmo DataDir corromperiam o log em silencio: os dois
    escreveriam LSNs proprios nos mesmos arquivos. O lock e' o amqp.lock aberto
    com fmShareExclusive -- share mode no Windows, fpflock advisory no Unix, os
    dois medidos na WS0. Liberado no destrutor, e pelo sistema operacional se o
    processo morrer, que e' o caso que de fato importa. }
  TAMQPWalDirLock = class
  private
    FHandle: THandle;
    FPath: string;
  public
    /// Levanta EAMQPWal se ja' houver dono. Cria o diretorio se nao existir.
    constructor Create(const ADir: string);
    destructor Destroy; override;
    property Path: string read FPath;
  end;

/// CRC-32 (IEEE 802.3, o do zip/png/gzip): polinomio refletido $EDB88320,
/// init $FFFFFFFF, xor final $FFFFFFFF. Escolhido por ser conferivel contra
/// vetores publicos -- um checksum caseiro seria impossivel de validar.
function AmqpCrc32(const ABytes: TBytes): Cardinal; overload;
/// Forma incremental/parcial: sem o init nem o xor final, para quem calcula
/// sobre um pedaco de buffer maior.
function AmqpCrc32(ACrc: Cardinal; const ABytes: TBytes;
  AOffset, ALen: Integer): Cardinal; overload;

/// fsync portavel.
function AmqpFileSync(AHandle: THandle): Boolean;

/// Nome canonico de um segmento: seg-00000001.wal. Zeros a' esquerda para a
/// ordem alfabetica do diretorio coincidir com a numerica.
function AmqpWalSegmentName(ASegNo: Cardinal): string;
/// Le o numero de volta. False se o nome nao for de um segmento.
function AmqpWalParseSegmentName(const AName: string;
  out ASegNo: Cardinal): Boolean;
/// Segmentos existentes em ADir, ORDENADOS por numero.
function AmqpWalListSegments(const ADir: string): TArray<Cardinal>;

/// Quantos bytes um registro com este payload ocupa no arquivo.
function AmqpWalRecordSize(APayloadLen: Integer): Integer;

implementation

uses
  Generics.Collections;

var
  GCrcTable: array[0..255] of Cardinal;

procedure MontaTabelaCrc;
var
  I, J: Integer;
  C: Cardinal;
begin
  for I := 0 to 255 do
  begin
    C := Cardinal(I);
    for J := 0 to 7 do
      if (C and 1) <> 0 then
        C := $EDB88320 xor (C shr 1)
      else
        C := C shr 1;
    GCrcTable[I] := C;
  end;
end;

function AmqpCrc32(ACrc: Cardinal; const ABytes: TBytes;
  AOffset, ALen: Integer): Cardinal;
var
  I: Integer;
begin
  Result := ACrc;
  for I := AOffset to AOffset + ALen - 1 do
    Result := GCrcTable[(Result xor ABytes[I]) and $FF] xor (Result shr 8);
end;

function AmqpCrc32(const ABytes: TBytes): Cardinal;
begin
  Result := AmqpCrc32($FFFFFFFF, ABytes, 0, Length(ABytes)) xor $FFFFFFFF;
end;

function AmqpFileSync(AHandle: THandle): Boolean;
begin
  {$IFDEF FPC}
  // Medido na WS0: no FPC isto e' FlushFileBuffers no Windows e fpfsync no
  // Unix, com tempos indistinguiveis das chamadas diretas.
  Result := FileFlush(AHandle);
  {$ELSE}
  Result := FlushFileBuffers(AHandle);
  {$ENDIF}
end;

function AmqpWalRecordSize(APayloadLen: Integer): Integer;
begin
  Result := AMQP_WAL_REC_OVERHEAD + APayloadLen;
end;

function AmqpWalSegmentName(ASegNo: Cardinal): string;
begin
  Result := AMQP_WAL_SEGMENT_PREFIX
    + Format('%.*d', [AMQP_WAL_SEGMENT_DIGITS, Int64(ASegNo)])
    + AMQP_WAL_SEGMENT_EXT;
end;

function AmqpWalParseSegmentName(const AName: string;
  out ASegNo: Cardinal): Boolean;
var
  LNum: string;
  LVal: Int64;
  I: Integer;
begin
  ASegNo := 0;
  Result := False;
  if Length(AName) <> Length(AMQP_WAL_SEGMENT_PREFIX) + AMQP_WAL_SEGMENT_DIGITS
    + Length(AMQP_WAL_SEGMENT_EXT) then
    Exit;
  if not SameText(Copy(AName, 1, Length(AMQP_WAL_SEGMENT_PREFIX)),
    AMQP_WAL_SEGMENT_PREFIX) then
    Exit;
  if not SameText(Copy(AName, Length(AName) - Length(AMQP_WAL_SEGMENT_EXT) + 1,
    Length(AMQP_WAL_SEGMENT_EXT)), AMQP_WAL_SEGMENT_EXT) then
    Exit;
  LNum := Copy(AName, Length(AMQP_WAL_SEGMENT_PREFIX) + 1,
    AMQP_WAL_SEGMENT_DIGITS);
  // Digito a digito: StrToInt64Def sozinho aceitaria ' 12' e '+12', e um nome
  // desses viraria um segmento fantasma com o numero de outro.
  for I := 1 to Length(LNum) do
    if (LNum[I] < '0') or (LNum[I] > '9') then
      Exit;
  LVal := StrToInt64Def(LNum, -1);
  if (LVal < 0) or (LVal > Int64(High(Cardinal))) then
    Exit;
  ASegNo := Cardinal(LVal);
  Result := True;
end;

function AmqpWalListSegments(const ADir: string): TArray<Cardinal>;
var
  LRec: TSearchRec;
  LLista: TList<Cardinal>;
  LNo, LTmp: Cardinal;
  I, J: Integer;
begin
  Result := nil;
  LLista := TList<Cardinal>.Create;
  try
    if FindFirst(IncludeTrailingPathDelimiter(ADir) + '*'
      + AMQP_WAL_SEGMENT_EXT, faAnyFile, LRec) = 0 then
    begin
      try
        repeat
          if (LRec.Attr and faDirectory) = 0 then
            if AmqpWalParseSegmentName(LRec.Name, LNo) then
              LLista.Add(LNo);
        until FindNext(LRec) <> 0;
      finally
        FindClose(LRec);
      end;
    end;
    SetLength(Result, LLista.Count);
    for I := 0 to LLista.Count - 1 do
      Result[I] := LLista[I];
  finally
    LLista.Free;
  end;
  // Ordenacao por insercao, na mao: TArray.Sort<T> nao existe no
  // Generics.Collections do FPC 3.2 (tabela de proibidos do CLAUDE.md). Sao
  // dezenas de segmentos, nao milhares.
  for I := 1 to High(Result) do
  begin
    LTmp := Result[I];
    J := I - 1;
    while (J >= 0) and (Result[J] > LTmp) do
    begin
      Result[J + 1] := Result[J];
      Dec(J);
    end;
    Result[J + 1] := LTmp;
  end;
end;

// --- empacotamento little-endian (ver "Formato" no cabecalho) --------------

procedure PoeU8(var ABuf: TBytes; AOffset: Integer; AValue: Byte);
begin
  ABuf[AOffset] := AValue;
end;

procedure PoeU16(var ABuf: TBytes; AOffset: Integer; AValue: Word);
begin
  ABuf[AOffset] := Byte(AValue);
  ABuf[AOffset + 1] := Byte(AValue shr 8);
end;

procedure PoeU32(var ABuf: TBytes; AOffset: Integer; AValue: Cardinal);
begin
  ABuf[AOffset] := Byte(AValue);
  ABuf[AOffset + 1] := Byte(AValue shr 8);
  ABuf[AOffset + 2] := Byte(AValue shr 16);
  ABuf[AOffset + 3] := Byte(AValue shr 24);
end;

procedure PoeU64(var ABuf: TBytes; AOffset: Integer; AValue: UInt64);
begin
  PoeU32(ABuf, AOffset, Cardinal(AValue and $FFFFFFFF));
  PoeU32(ABuf, AOffset + 4, Cardinal(AValue shr 32));
end;

function PegaU16(const ABuf: TBytes; AOffset: Integer): Word;
begin
  Result := Word(ABuf[AOffset]) or (Word(ABuf[AOffset + 1]) shl 8);
end;

function PegaU32(const ABuf: TBytes; AOffset: Integer): Cardinal;
begin
  Result := Cardinal(ABuf[AOffset])
    or (Cardinal(ABuf[AOffset + 1]) shl 8)
    or (Cardinal(ABuf[AOffset + 2]) shl 16)
    or (Cardinal(ABuf[AOffset + 3]) shl 24);
end;

function PegaU64(const ABuf: TBytes; AOffset: Integer): UInt64;
begin
  Result := UInt64(PegaU32(ABuf, AOffset))
    or (UInt64(PegaU32(ABuf, AOffset + 4)) shl 32);
end;

{ TAMQPWalOsFile }

constructor TAMQPWalOsFile.Create(const APath: string; ACreate: Boolean);
var
  H: THandle;
begin
  inherited Create;
  // ANTES de qualquer coisa que possa levantar: um construtor que falha tem o
  // destrutor chamado, e o campo zerado por construcao valeria 0 -- que no
  // Unix e' um descritor legitimo (a entrada padrao). Fechar o fd 0 por
  // engano e' o tipo de estrago que so' aparece muito depois.
  FHandle := THandle(-1);
  FPath := APath;
  if ACreate then
  begin
    if FileExists(APath) then
      raise EAMQPWal.CreateFmt('segmento ja existe: %s', [APath]);
    H := FileCreate(APath);
    if H = THandle(-1) then
      raise EAMQPWal.CreateFmt('nao consegui criar %s: %s',
        [APath, SysErrorMessage(GetLastOSError)]);
    FileClose(H);
  end
  else
    if not FileExists(APath) then
      raise EAMQPWal.CreateFmt('segmento nao existe: %s', [APath]);

  FHandle := FileOpen(APath, fmOpenReadWrite or fmShareDenyNone);
  if FHandle = THandle(-1) then
    raise EAMQPWal.CreateFmt('nao consegui abrir %s: %s',
      [APath, SysErrorMessage(GetLastOSError)]);
end;

destructor TAMQPWalOsFile.Destroy;
begin
  if FHandle <> THandle(-1) then
  begin
    FileClose(FHandle);
    FHandle := THandle(-1);
  end;
  inherited Destroy;
end;

function TAMQPWalOsFile.Append(const ABytes: TBytes): Int64;
var
  LEscrito, LTotal, LResto: Integer;
begin
  Result := FileSeek(FHandle, Int64(0), 2 { fim });
  LTotal := 0;
  LResto := Length(ABytes);
  while LResto > 0 do
  begin
    LEscrito := FileWrite(FHandle, ABytes[LTotal], LResto);
    if LEscrito <= 0 then
      raise EAMQPWal.CreateFmt('escrita falhou em %s apos %d de %d bytes: %s',
        [FPath, LTotal, Length(ABytes), SysErrorMessage(GetLastOSError)]);
    Inc(LTotal, LEscrito);
    Dec(LResto, LEscrito);
  end;
end;

function TAMQPWalOsFile.ReadAll: TBytes;
var
  LSize: Int64;
  LLido, LTotal, LResto: Integer;
begin
  Result := nil;
  LSize := Size;
  if LSize <= 0 then
    Exit;
  if LSize > MaxInt then
    raise EAMQPWal.CreateFmt('segmento grande demais para ler de uma vez: '
      + '%s (%d bytes)', [FPath, LSize]);
  SetLength(Result, Integer(LSize));
  FileSeek(FHandle, Int64(0), 0 { inicio });
  LTotal := 0;
  LResto := Integer(LSize);
  while LResto > 0 do
  begin
    LLido := FileRead(FHandle, Result[LTotal], LResto);
    if LLido <= 0 then
    begin
      // Arquivo encolheu debaixo de nos -- nao deveria, ha um dono so'.
      // Devolve o que deu; a varredura trata o resto como cauda.
      SetLength(Result, LTotal);
      Exit;
    end;
    Inc(LTotal, LLido);
    Dec(LResto, LLido);
  end;
end;

function TAMQPWalOsFile.Sync: Boolean;
begin
  Result := AmqpFileSync(FHandle);
end;

function TAMQPWalOsFile.Size: Int64;
var
  LPos: Int64;
begin
  LPos := FileSeek(FHandle, Int64(0), 1 { atual });
  Result := FileSeek(FHandle, Int64(0), 2 { fim });
  FileSeek(FHandle, LPos, 0);
end;

procedure TAMQPWalOsFile.TruncateTo(ASize: Int64);
begin
  FileSeek(FHandle, ASize, 0);
  {$IFDEF FPC}
  if not FileTruncate(FHandle, ASize) then
  {$ELSE}
  if not SetEndOfFile(FHandle) then
  {$ENDIF}
    raise EAMQPWal.CreateFmt('nao consegui truncar %s em %d: %s',
      [FPath, ASize, SysErrorMessage(GetLastOSError)]);
end;

{ TAMQPWalSegment }

constructor TAMQPWalSegment.CreateNew(const AFile: IAMQPWalFile;
  ASegNo: Cardinal);
var
  LBuf: TBytes;
  I: Integer;
begin
  inherited Create;
  if AFile = nil then
    raise EAMQPWal.Create('segmento sem arquivo');
  FFile := AFile;
  FSegNo := ASegNo;
  if FFile.Size <> 0 then
    raise EAMQPWal.CreateFmt('CreateNew sobre arquivo nao vazio (%d bytes)',
      [FFile.Size]);

  SetLength(LBuf, AMQP_WAL_SEG_HEADER_SIZE);
  for I := 1 to Length(AMQP_WAL_MAGIC) do
    LBuf[I - 1] := Byte(AMQP_WAL_MAGIC[I]);
  PoeU16(LBuf, 8, AMQP_WAL_VERSION);
  PoeU16(LBuf, 10, 0);
  PoeU32(LBuf, 12, FSegNo);
  FFile.Append(LBuf);
  // O cabecalho e' a unica coisa do arquivo que nao pode ser reconstruida por
  // replay: sem ele o segmento inteiro fica irreconhecivel. Vai ao disco ja'.
  FFile.Sync;

  FEndOffset := AMQP_WAL_SEG_HEADER_SIZE;
  FLastLsn := 0;
  FStop := awsFim;
  FDescartados := 0;
end;

constructor TAMQPWalSegment.OpenExisting(const AFile: IAMQPWalFile;
  ATrim: Boolean);
var
  LBuf: TBytes;
  LRecs: TAMQPWalRecords;
begin
  inherited Create;
  if AFile = nil then
    raise EAMQPWal.Create('segmento sem arquivo');
  FFile := AFile;
  LBuf := LeEValidaCabecalho;
  Varre(LBuf, False, LRecs, FStop, FEndOffset, FLastLsn);
  FDescartados := Int64(Length(LBuf)) - FEndOffset;
  if ATrim and (FDescartados > 0) then
  begin
    FFile.TruncateTo(FEndOffset);
    FFile.Sync;
  end;
end;

destructor TAMQPWalSegment.Destroy;
begin
  FFile := nil;
  inherited Destroy;
end;

function TAMQPWalSegment.LeEValidaCabecalho: TBytes;
var
  I: Integer;
  LVer: Word;
begin
  Result := FFile.ReadAll;
  if Length(Result) < AMQP_WAL_SEG_HEADER_SIZE then
    raise EAMQPWal.CreateFmt('segmento truncado no cabecalho (%d bytes de %d)',
      [Length(Result), AMQP_WAL_SEG_HEADER_SIZE]);
  for I := 1 to Length(AMQP_WAL_MAGIC) do
    if Result[I - 1] <> Byte(AMQP_WAL_MAGIC[I]) then
      raise EAMQPWal.Create('nao e um segmento WAL deste broker '
        + '(assinatura nao confere)');
  LVer := PegaU16(Result, 8);
  if LVer <> AMQP_WAL_VERSION then
    raise EAMQPWal.CreateFmt('versao de segmento %d, esperada %d',
      [LVer, AMQP_WAL_VERSION]);
  FSegNo := PegaU32(Result, 12);
end;

function TAMQPWalSegment.Varre(const ABuf: TBytes; AColeta: Boolean;
  out ARecords: TAMQPWalRecords; out AStop: TAMQPWalStop;
  out AEndOffset: Int64; out ALastLsn: UInt64): Integer;
var
  LPos, LTam, LCap: Integer;
  LLsn, LUltimoLsn: UInt64;
  LKind: Byte;
  LLen: Cardinal;
  LCrcLido, LCrcCalc: Cardinal;
begin
  ARecords := nil;
  LTam := Length(ABuf);
  LPos := AMQP_WAL_SEG_HEADER_SIZE;
  LUltimoLsn := 0;
  LCap := 0;
  Result := 0;
  AStop := awsFim;

  while True do
  begin
    if LPos >= LTam then
    begin
      AStop := awsFim;
      Break;
    end;
    if LTam - LPos < AMQP_WAL_REC_HEADER_SIZE then
    begin
      AStop := awsCauda;
      Break;
    end;

    LLsn := PegaU64(ABuf, LPos);
    LKind := ABuf[LPos + 8];
    LLen := PegaU32(ABuf, LPos + 10);

    // Ordem das checagens: o tamanho e' validado ANTES de qualquer leitura ou
    // alocacao guiada por ele, porque ele mesmo pode estar corrompido.
    if LLen > AMQP_WAL_MAX_PAYLOAD then
    begin
      AStop := awsTamanho;
      Break;
    end;
    if Int64(LTam - LPos) < Int64(AMQP_WAL_REC_OVERHEAD) + Int64(LLen) then
    begin
      AStop := awsCauda;
      Break;
    end;

    LCrcLido := PegaU32(ABuf, LPos + AMQP_WAL_REC_HEADER_SIZE + Integer(LLen));
    LCrcCalc := AmqpCrc32($FFFFFFFF, ABuf, LPos,
      AMQP_WAL_REC_HEADER_SIZE + Integer(LLen)) xor $FFFFFFFF;
    if LCrcLido <> LCrcCalc then
    begin
      AStop := awsCrc;
      Break;
    end;

    // So' depois do CRC: LSN fora de ordem num registro INTEGRO e' outra coisa
    // (arquivo remontado, bug de quem escreveu), e vale distinguir de lixo.
    if LLsn <= LUltimoLsn then
    begin
      AStop := awsLsn;
      Break;
    end;

    if AColeta then
    begin
      if Result = LCap then
      begin
        if LCap = 0 then
          LCap := 16
        else
          LCap := LCap * 2;
        SetLength(ARecords, LCap);
      end;
      ARecords[Result].Lsn := LLsn;
      ARecords[Result].Kind := LKind;
      ARecords[Result].Payload := Copy(ABuf,
        LPos + AMQP_WAL_REC_HEADER_SIZE, Integer(LLen));
    end;

    Inc(Result);
    LUltimoLsn := LLsn;
    Inc(LPos, AMQP_WAL_REC_OVERHEAD + Integer(LLen));
  end;

  AEndOffset := LPos;
  ALastLsn := LUltimoLsn;
  if AColeta then
    SetLength(ARecords, Result);
end;

function TAMQPWalSegment.Append(ALsn: UInt64; AKind: Byte;
  const APayload: TBytes): Int64;
var
  LBuf: TBytes;
  LLen: Integer;
  LCrc: Cardinal;
begin
  if ALsn <= FLastLsn then
    raise EAMQPWal.CreateFmt('LSN %d nao e maior que o ultimo gravado (%d) -- '
      + 'a leitura truncaria o segmento aqui', [ALsn, FLastLsn]);
  LLen := Length(APayload);
  if LLen > AMQP_WAL_MAX_PAYLOAD then
    raise EAMQPWal.CreateFmt('payload de %d bytes acima do teto de %d',
      [LLen, AMQP_WAL_MAX_PAYLOAD]);

  SetLength(LBuf, AmqpWalRecordSize(LLen));
  PoeU64(LBuf, 0, ALsn);
  PoeU8(LBuf, 8, AKind);
  PoeU8(LBuf, 9, 0);
  PoeU32(LBuf, 10, Cardinal(LLen));
  if LLen > 0 then
    Move(APayload[0], LBuf[AMQP_WAL_REC_HEADER_SIZE], LLen);
  LCrc := AmqpCrc32($FFFFFFFF, LBuf, 0, AMQP_WAL_REC_HEADER_SIZE + LLen)
    xor $FFFFFFFF;
  PoeU32(LBuf, AMQP_WAL_REC_HEADER_SIZE + LLen, LCrc);

  Result := FFile.Append(LBuf);
  // O fim vem do offset que o PROPRIO arquivo devolveu, nao de um acumulador:
  // assim o segmento nao tem como divergir do arquivo (o caso de abrir com
  // ATrim=False e escrever em seguida deixaria de fechar a conta).
  FEndOffset := Result + Length(LBuf);
  FLastLsn := ALsn;
end;

function TAMQPWalSegment.Sync: Boolean;
begin
  Result := FFile.Sync;
end;

function TAMQPWalSegment.ReadPrefix(out ARecords: TAMQPWalRecords;
  out AStop: TAMQPWalStop): Integer;
var
  LBuf: TBytes;
  LFim: Int64;
  LLsn: UInt64;
begin
  LBuf := LeEValidaCabecalho;
  Result := Varre(LBuf, True, ARecords, AStop, LFim, LLsn);
end;

{ TAMQPWalDirLock }

constructor TAMQPWalDirLock.Create(const ADir: string);
var
  H: THandle;
begin
  inherited Create;
  FHandle := THandle(-1); // ver a nota do TAMQPWalOsFile.Create
  if not ForceDirectories(ADir) then
    raise EAMQPWal.CreateFmt('nao consegui criar o diretorio de dados %s: %s',
      [ADir, SysErrorMessage(GetLastOSError)]);
  FPath := IncludeTrailingPathDelimiter(ADir) + AMQP_WAL_LOCK_NAME;
  if not FileExists(FPath) then
  begin
    H := FileCreate(FPath);
    if H = THandle(-1) then
      raise EAMQPWal.CreateFmt('nao consegui criar %s: %s',
        [FPath, SysErrorMessage(GetLastOSError)]);
    FileClose(H);
  end;
  // fmShareExclusive: share mode no Windows, fpflock advisory no Unix -- a WS0
  // mediu os dois recusando o segundo dono e liberando no close.
  FHandle := FileOpen(FPath, fmOpenReadWrite or fmShareExclusive);
  if FHandle = THandle(-1) then
    raise EAMQPWal.CreateFmt('o diretorio de dados %s ja tem dono '
      + '(outro broker esta usando %s)', [ADir, AMQP_WAL_LOCK_NAME]);
end;

destructor TAMQPWalDirLock.Destroy;
begin
  if FHandle <> THandle(-1) then
  begin
    FileClose(FHandle);
    FHandle := THandle(-1);
  end;
  inherited Destroy;
end;

initialization
  // Na inicializacao da unit, e nao sob demanda com uma flag: o CRC e'
  // calculado pela thread do publicador E pela do journal (D24), e montar a
  // tabela em duas threads ao mesmo tempo faria uma ler entradas que a outra
  // ainda nao escreveu -- um CRC errado gravado no disco, sem sintoma nenhum
  // ate' a recuperacao descartar o registro.
  MontaTabelaCrc;

end.

unit AMQP.Server.Journal;

{$I amqp.inc}

{ A thread do journal e o group commit -- sub-modulo broker, WS2 da Fase 4
  (durabilidade, ver CLAUDE.md, decisoes D19-D28).

  Esta unit e' a que transforma "escrever no arquivo" (WS1, AMQP.Server.Wal) em
  "prometer durabilidade": ela numera os registros, junta o que chegou num
  lote, escreve, da' UM fsync e so' entao avanca a MARCA D'AGUA -- o numero a
  partir do qual um confirm pode ser liberado (D24).

  O LACO, QUE E' A D25 INTEIRA
  ----------------------------
      espera ter algo na fila
      tira TUDO que esta' la'
      escreve os registros
      UM fsync
      avanca a marca d'agua e avisa
      repete

  Nao ha janela de espera nem tamanho de lote configuravel, e isso e'
  deliberado: sob carga baixa o lote e' 1 e a latencia e' a de um fsync
  isolado; sob carga alta o lote cresce sozinho, porque o que chega DURANTE um
  fsync e' exatamente o proximo lote. O ganho medido na WS0 (35x-50x no
  Windows, ~250x no Linux da VM) vem so' disso -- e cresce quanto pior for o
  disco, que e' a propriedade que se quer de um lote auto-ajustavel.

  QUEM PODE BLOQUEAR, E QUEM NAO PODE (D24, corolario c)
  ------------------------------------------------------
  A fila de submissao tem teto, e a contrapressao vive no PUBLICADOR:

    Submit        BLOQUEIA enquanto a fila estiver acima do teto. Quem chama
                  e' a thread de leitura da conexao, e para ela de ler aquela
                  conexao e' exatamente a contrapressao desejada -- a mesma
                  forma da fila de saida do writer.
    SubmitNoWait  NUNCA bloqueia e NUNCA recusa: e' por onde o ATOR da fila
                  escreve as retiradas que so' ele decide (ack, drop-head,
                  expiracao, dead-letter). A D2 proibe o ator de esperar I/O, e
                  o volume desses registros e' limitado pelo numero de entradas
                  vivas -- nao ha como o ator, sozinho, encher a fila para
                  sempre.

  O SINK NAO PODE BLOQUEAR, pela mesma razao que o ator nao pode: ele roda NA
  thread do journal, e segura-lo trava o group commit inteiro.

  FALHA FECHA A PORTA, NAO ABAIXA A GUARDA
  ----------------------------------------
  fsync que falha NAO avanca a marca d'agua -- os registros continuam
  pendentes e o lote seguinte tenta de novo (os bytes ja' estao no arquivo; um
  fsync posterior cobre os anteriores). Ja' ESCRITA que falha e' outra coisa: o
  registro nao esta' em lugar nenhum, e nao ha como repor a promessa. Nesse
  caso o journal entra em estado FALHO, definitivo: Submit passa a levantar,
  WaitDurable devolve False, e o sink e' avisado uma vez. Um broker que nao
  consegue mais escrever tem de dizer isso, nao continuar aceitando publish
  persistente como se nada fosse.

  O QUE NAO ESTA' AQUI
  --------------------
  Compactacao de segmento e' a WS7b: esta versao ROTACIONA (fecha o segmento
  cheio e abre o seguinte) mas ainda nao apaga nem reescreve nada. Antes disso
  havia UM segmento
  ativo e nao o rola quando ele passa do tamanho alvo. O registro de confirms
  pendentes por canal e' a WS5 -- aqui existe so' a costura
  (IAMQPDurabilitySink) e a marca d'agua que ela consulta. }

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Generics.Collections,
  AMQP.Threading,
  AMQP.Server.Wal;

const
  /// Teto da fila de submissao, em bytes de payload. Acima disso o Submit
  /// BLOQUEIA (o SubmitNoWait do ator ignora). Quatro megabytes seguram varios
  /// milhares de registros tipicos -- fundo suficiente para o group commit
  /// crescer, raso o bastante para a contrapressao chegar ao publicador antes
  /// de a memoria virar problema.
  AMQP_JOURNAL_MAX_PENDING_BYTES = 4 * 1024 * 1024;

  /// Quanto um Submit bloqueado espera antes de desistir. Generoso: o journal
  /// nao espera peer nenhum, so' o disco, entao estourar isto e' disco parado
  /// ou bug nosso.
  AMQP_JOURNAL_SUBMIT_TIMEOUT_MS = 30000;

  /// Quanto o Stop espera a thread drenar o que ja' foi aceito.
  AMQP_JOURNAL_STOP_TIMEOUT_MS = 30000;

type
  EAMQPJournal = class(Exception);

  /// Um registro a gravar. Kind e' o mesmo campo opaco da WS1 -- quem lhe da'
  /// significado (D22: CONTENT, ENQ, DEQ, TOPO) e' a WS3/WS4.
  TAMQPJournalRecord = record
    Kind: Byte;
    Payload: TBytes;
  end;

  TAMQPJournalRecords = array of TAMQPJournalRecord;

  { Quem quer saber que um LSN ficou duravel. Implementado na WS5 pelo registro
    de confirms pendentes.

    NAO PODE BLOQUEAR NEM FAZER I/O: roda na thread do journal, entre o fsync e
    o proximo lote. E' a mesma regra do ator da fila (D2) e pela mesma razao. }
  IAMQPDurabilitySink = interface
    ['{4A1D7E90-2C63-4B58-8F07-D9E3A5C61B24}']
    /// Tudo com LSN <= ALsn esta' no disco.
    procedure Durable(ALsn: UInt64);
    /// O journal nao vai mais durar nada. Chamado UMA vez.
    procedure JournalFailed(const AMessage: string);
  end;

  { Contagens do journal. Vao juntas num registro lido SOB O LOCK, e nao como
    propriedades soltas, por uma razao medida e nao estetica: o build Delphi
    deste projeto e' Win32, onde Inc() num Int64 e' leitura-modificacao-escrita
    NAO atomica -- uma thread de teste lendo um contador enquanto a do journal
    o incrementa poderia ver metade de um valor. E' a mesma razao de existirem
    os AmqpAtomic*64 na AMQP.Threading. }
  TAMQPJournalStats = record
    /// Rodadas do laco que escreveram alguma coisa.
    Lotes: Int64;
    /// fsyncs tentados. Registros/Syncs e' o tamanho medio do lote -- e' este
    /// numero que prova que o group commit esta' agrupando.
    Syncs: Int64;
    /// fsyncs que devolveram False (nao avancaram a marca d'agua).
    SyncsFalhos: Int64;
    Registros: Int64;
    /// Maior lote ja' escrito numa rodada.
    MaiorLote: Integer;
    /// Quantas vezes o segmento cheio foi fechado e outro aberto (D26).
    Rotacoes: Int64;
    /// Bytes de payload esperando na fila de submissao AGORA.
    PendingBytes: Int64;
    DurableLsn: UInt64;
    /// LSN que o proximo registro submetido vai receber.
    ProximoLsn: UInt64;
  end;

  TAMQPJournal = class;

  { A thread que escreve. Uma por journal, persistente, sem FreeOnTerminate --
    a mesma forma da TAMQPMonitorThread do broker. }
  TAMQPJournalThread = class(TThread)
  private
    FJournal: TAMQPJournal;
  protected
    procedure Execute; override;
  public
    constructor Create(AJournal: TAMQPJournal);
  end;

  { Um item na fila de submissao. O LSN ja' vem atribuido: e' o Submit que o
    da', sob o lock, porque o publicador precisa dele ANTES de voltar (D24 --
    e' o numero que ele guarda no confirm pendente). }
  TAMQPJournalItem = record
    Lsn: UInt64;
    Kind: Byte;
    Payload: TBytes;
  end;

  TAMQPJournal = class
  private
    FDir: string;
    FLock: TAMQPWalDirLock;
    FSegmento: TAMQPWalSegment;
    FArquivo: IAMQPWalFile;
    FThread: TAMQPJournalThread;
    FMon: TAMQPMonitor;
    FPendentes: TQueue<TAMQPJournalItem>;
    FPendingBytes: Int64;
    FMaxPendingBytes: Int64;
    FMaxSegmentBytes: Int64;
    FSegNo: Cardinal;
    FProximoLsn: UInt64;
    FDurableLsn: UInt64;   // atomico -- leitura fora do lock
    FRodando: Boolean;
    FParando: Boolean;
    FFalho: Boolean;
    FErro: string;
    FSink: IAMQPDurabilitySink;
    FLotes: Int64;
    FRotacoes: Int64;
    FSyncs: Int64;
    FRegistros: Int64;
    FMaiorLote: Integer;
    FSyncsFalhos: Int64;
    procedure AbreOuRecupera;
    procedure MarcaFalho(const AMensagem: string);
    function SubmitInterno(const ARecs: array of TAMQPJournalRecord;
      AEsperarVaga: Boolean): UInt64;
  protected
    /// Uma rodada do laco: drena, escreve, sincroniza, avanca a marca.
    /// Devolve False quando nao ha mais nada a fazer e a parada foi pedida.
    function RodaUmLote: Boolean;
    /// Ponto exato entre "tirei o lote da fila" e "escrevi" -- a janela em que
    /// um Submit concorrente decide se entra neste lote ou no proximo. No-op
    /// em producao; o teste sobrescreve para postar de outra thread dentro
    /// dela. NAO PODE LEVANTAR. (Mesma ideia do ActorRoundEnding da fila.)
    procedure LoteTomado; virtual;
    /// A costura de arquivo do segmento ativo. Virtual para o teste injetar um
    /// duble que conta fsyncs e falha na hora escolhida (camada 2 da D28).
    function CriaArquivo(const APath: string; ACriar: Boolean): IAMQPWalFile; virtual;
    /// Fecha o segmento cheio e abre o proximo. Chamado SO' na fronteira de
    /// lote -- ver o comentario na chamada.
    procedure Rotaciona;
  public
    /// ADir e' o DataDir. O lock exclusivo do diretorio e' tomado no Start,
    /// nao aqui -- construir um journal nao pode roubar o diretorio de um
    /// broker que ainda esta' de pe'.
    constructor Create(const ADir: string);
    destructor Destroy; override;

    /// Toma o lock do diretorio, abre (ou cria) o segmento ativo, recupera o
    /// proximo LSN do que ja' estava gravado e sobe a thread. Idempotente.
    procedure Start;
    /// Drena o que ja' foi aceito, sincroniza uma ultima vez e para a thread.
    /// Idempotente; chamado pelo destrutor.
    procedure Stop;

    /// Enfileira ARecs como um LOTE INDIVISIVEL (LSNs contiguos, na ordem
    /// dada) e devolve o LSN do ULTIMO -- o numero que o chamador guarda para
    /// saber quando o lote ficou duravel. BLOQUEIA enquanto a fila estiver
    /// acima do teto. Levanta se o journal estiver parado ou falho.
    function Submit(const ARecs: array of TAMQPJournalRecord): UInt64;
    /// Igual, mas NUNCA bloqueia e NUNCA recusa -- e' por onde o ator escreve
    /// (ver o cabecalho da unit).
    function SubmitNoWait(const ARecs: array of TAMQPJournalRecord): UInt64;

    /// Tudo com LSN <= isto esta' no disco. Leitura atomica, sem lock.
    function DurableLsn: UInt64;
    /// Espera ALsn ficar duravel. False no timeout ou se o journal falhar.
    function WaitDurable(ALsn: UInt64; ATimeoutMs: Cardinal): Boolean;

    property Dir: string read FDir;
    property Running: Boolean read FRodando;
    /// True depois de uma falha de ESCRITA -- definitivo (ver o cabecalho).
    property Failed: Boolean read FFalho;
    property LastError: string read FErro;
    /// Quem e' avisado a cada fsync bem-sucedido. Injetado antes do Start.
    property DurabilitySink: IAMQPDurabilitySink read FSink write FSink;
    property MaxPendingBytes: Int64 read FMaxPendingBytes
      write FMaxPendingBytes;
    /// Quanto um segmento cresce antes de o journal fechar e abrir o proximo
    /// (D26). Mexer nisto so' faz sentido em teste: o default e' o do WAL.
    property MaxSegmentBytes: Int64 read FMaxSegmentBytes
      write FMaxSegmentBytes;
    /// Numero do segmento ATIVO. Cresce a cada rotacao.
    function SegmentoAtivo: Cardinal;

    /// Contagens, lidas sob o lock (ver TAMQPJournalStats).
    function Stats: TAMQPJournalStats;
  end;

implementation

{ TAMQPJournalThread }

constructor TAMQPJournalThread.Create(AJournal: TAMQPJournal);
begin
  FJournal := AJournal;
  inherited Create(False);
  {$IFDEF FPC}
  NameThreadForDebugging('amqp-journal');
  {$ELSE}
  NameThreadForDebugging('amqp-journal', ThreadID);
  {$ENDIF}
end;

procedure TAMQPJournalThread.Execute;
begin
  while FJournal.RodaUmLote do
    ;
end;

{ TAMQPJournal }

constructor TAMQPJournal.Create(const ADir: string);
begin
  inherited Create;
  FDir := ADir;
  FMon := TAMQPMonitor.Create;
  FPendentes := TQueue<TAMQPJournalItem>.Create;
  FMaxPendingBytes := AMQP_JOURNAL_MAX_PENDING_BYTES;
  FMaxSegmentBytes := AMQP_WAL_SEGMENT_BYTES;
  FProximoLsn := 1;
end;

destructor TAMQPJournal.Destroy;
begin
  Stop;
  FPendentes.Free;
  FSegmento.Free;
  FArquivo := nil;
  FLock.Free;
  FMon.Free;
  inherited Destroy;
end;

function TAMQPJournal.CriaArquivo(const APath: string;
  ACriar: Boolean): IAMQPWalFile;
begin
  Result := TAMQPWalOsFile.Create(APath, ACriar);
end;

procedure TAMQPJournal.AbreOuRecupera;
var
  LSegs: TArray<Cardinal>;
  LNo: Cardinal;
  LPath: string;
begin
  LSegs := AmqpWalListSegments(FDir);
  if Length(LSegs) = 0 then
  begin
    LNo := 1;
    LPath := IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LNo);
    FArquivo := CriaArquivo(LPath, True);
    FSegmento := TAMQPWalSegment.CreateNew(FArquivo, LNo);
    FSegNo := LNo;
  end
  else
  begin
    // O ULTIMO segmento e' o ativo. Abrir com trim descarta a cauda torta que
    // uma queda deixou -- e' o que torna o proximo append alcancavel (WS1).
    LNo := LSegs[High(LSegs)];
    LPath := IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(LNo);
    FArquivo := CriaArquivo(LPath, False);
    FSegmento := TAMQPWalSegment.OpenExisting(FArquivo, True);
    FSegNo := LNo;
  end;
  // O LSN CONTINUA DE ONDE PAROU. Recomecar do 1 depois de um restart faria o
  // proprio Append da WS1 levantar (LSN nao crescente) -- e, pior, se nao
  // levantasse, produziria um arquivo que a recuperacao truncaria no ponto da
  // volta.
  FProximoLsn := FSegmento.LastLsn + 1;
  // O que ja' estava no arquivo esta' no disco por definicao: ninguem promete
  // nada sobre ele agora, mas a marca d'agua nao pode nascer ATRAS dele.
  AmqpAtomicWrite64(FDurableLsn, FSegmento.LastLsn);
end;

procedure TAMQPJournal.Start;
begin
  FMon.Enter;
  try
    if FRodando then
      Exit;
    FParando := False;
    FFalho := False;
    FErro := '';
  finally
    FMon.Leave;
  end;

  // Fora do lock: tomar o lock do diretorio e abrir arquivo sao I/O, e podem
  // levantar. Se levantarem, o journal continua parado e nada foi prometido.
  FLock := TAMQPWalDirLock.Create(FDir);
  try
    AbreOuRecupera;
  except
    on E: Exception do
    begin
      FreeAndNil(FLock);
      raise;
    end;
  end;

  FMon.Enter;
  try
    FRodando := True;
  finally
    FMon.Leave;
  end;
  FThread := TAMQPJournalThread.Create(Self);
end;

procedure TAMQPJournal.Stop;
begin
  FMon.Enter;
  try
    if not FRodando then
    begin
      FParando := True;
      FMon.PulseAll;
      Exit;
    end;
    FParando := True;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;

  if FThread <> nil then
  begin
    // A thread sai do laco sozinha depois de drenar: RodaUmLote so' devolve
    // False quando a fila esta' vazia E a parada foi pedida. Por isso NAO ha
    // "comando de parada" na fila -- o que ja' foi aceito ainda vai ao disco.
    // O WaitFor nao pode pendurar: a espera do laco tem timeout de 100 ms e
    // re-checa FParando a cada volta.
    FThread.WaitFor;
    FreeAndNil(FThread);
  end;

  FMon.Enter;
  try
    FRodando := False;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;

  FreeAndNil(FSegmento);
  FArquivo := nil;
  FreeAndNil(FLock);
end;

procedure TAMQPJournal.MarcaFalho(const AMensagem: string);
var
  LSink: IAMQPDurabilitySink;
  LAvisar: Boolean;
begin
  FMon.Enter;
  try
    LAvisar := not FFalho;
    FFalho := True;
    if FErro = '' then
      FErro := AMensagem;
    FMon.PulseAll; // solta quem espera vaga e quem espera durabilidade
  finally
    FMon.Leave;
  end;
  LSink := FSink;
  if LAvisar and (LSink <> nil) then
    try
      LSink.JournalFailed(AMensagem);
    except
    end;
end;

function TAMQPJournal.SubmitInterno(const ARecs: array of TAMQPJournalRecord;
  AEsperarVaga: Boolean): UInt64;
var
  I: Integer;
  LItem: TAMQPJournalItem;
  LBytes: Int64;
  LPrazo: UInt64;
begin
  if Length(ARecs) = 0 then
    raise EAMQPJournal.Create('Submit sem registro nenhum');

  LBytes := 0;
  for I := 0 to High(ARecs) do
    Inc(LBytes, Length(ARecs[I].Payload));

  FMon.Enter;
  try
    if AEsperarVaga then
    begin
      // Contrapressao: espera a fila baixar do teto. Re-checa em laco com
      // deadline, como manda o contrato do TAMQPMonitor (wakeup espurio).
      LPrazo := AmqpTickMs + AMQP_JOURNAL_SUBMIT_TIMEOUT_MS;
      while FRodando and (not FParando) and (not FFalho)
        and (FPendingBytes >= FMaxPendingBytes) and (AmqpTickMs < LPrazo) do
        FMon.Wait(50);
    end;

    if FFalho then
      raise EAMQPJournal.CreateFmt('journal em falha: %s', [FErro]);
    if (not FRodando) or FParando then
      raise EAMQPJournal.Create('journal parado');
    if AEsperarVaga and (FPendingBytes >= FMaxPendingBytes) then
      raise EAMQPJournal.CreateFmt('fila do journal cheia ha mais de %d ms '
        + '(%d bytes pendentes)',
        [AMQP_JOURNAL_SUBMIT_TIMEOUT_MS, FPendingBytes]);

    // LSNs contiguos, atribuidos AQUI: o publicador precisa do numero antes de
    // voltar (D24). E a fila fica ordenada por LSN de graca, porque a
    // atribuicao e a insercao acontecem sob o mesmo lock -- e' o que torna o
    // lote indivisivel sem nenhum marcador.
    Result := 0;
    for I := 0 to High(ARecs) do
    begin
      LItem.Lsn := FProximoLsn;
      LItem.Kind := ARecs[I].Kind;
      LItem.Payload := ARecs[I].Payload;
      FPendentes.Enqueue(LItem);
      Result := FProximoLsn;
      Inc(FProximoLsn);
    end;
    Inc(FPendingBytes, LBytes);
    FMon.PulseAll; // acorda a thread do journal
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.Submit(const ARecs: array of TAMQPJournalRecord): UInt64;
begin
  Result := SubmitInterno(ARecs, True);
end;

function TAMQPJournal.SubmitNoWait(
  const ARecs: array of TAMQPJournalRecord): UInt64;
begin
  Result := SubmitInterno(ARecs, False);
end;

function TAMQPJournal.DurableLsn: UInt64;
begin
  Result := AmqpAtomicRead64(FDurableLsn);
end;

function TAMQPJournal.WaitDurable(ALsn: UInt64; ATimeoutMs: Cardinal): Boolean;
var
  LPrazo: UInt64;
begin
  LPrazo := AmqpTickMs + ATimeoutMs;
  FMon.Enter;
  try
    while (AmqpAtomicRead64(FDurableLsn) < ALsn) and (not FFalho)
      and (AmqpTickMs < LPrazo) do
      FMon.Wait(20);
    Result := (not FFalho) and (AmqpAtomicRead64(FDurableLsn) >= ALsn);
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.Stats: TAMQPJournalStats;
begin
  FMon.Enter;
  try
    Result.Lotes := FLotes;
    Result.Rotacoes := FRotacoes;
    Result.Syncs := FSyncs;
    Result.SyncsFalhos := FSyncsFalhos;
    Result.Registros := FRegistros;
    Result.MaiorLote := FMaiorLote;
    Result.PendingBytes := FPendingBytes;
    Result.DurableLsn := AmqpAtomicRead64(FDurableLsn);
    Result.ProximoLsn := FProximoLsn;
  finally
    FMon.Leave;
  end;
end;

procedure TAMQPJournal.LoteTomado;
begin
  // no-op em producao -- ver a declaracao
end;

// Fecha o segmento cheio e abre o proximo. O LSN CONTINUA: a numeracao e' do
// journal inteiro, nao do arquivo, e e' o que permite a recuperacao ler os
// segmentos em sequencia como se fossem um log so'.
procedure TAMQPJournal.Rotaciona;
var
  LPath: string;
begin
  FreeAndNil(FSegmento);
  FArquivo := nil; // fecha o arquivo cheio
  Inc(FSegNo);
  LPath := IncludeTrailingPathDelimiter(FDir) + AmqpWalSegmentName(FSegNo);
  FArquivo := CriaArquivo(LPath, True);
  FSegmento := TAMQPWalSegment.CreateNew(FArquivo, FSegNo);
  FMon.Enter;
  try
    Inc(FRotacoes);
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.SegmentoAtivo: Cardinal;
begin
  FMon.Enter;
  try
    Result := FSegNo;
  finally
    FMon.Leave;
  end;
end;

function TAMQPJournal.RodaUmLote: Boolean;
var
  LLote: array of TAMQPJournalItem;
  LN, I: Integer;
  LMaiorLsn: UInt64;
  LSink: IAMQPDurabilitySink;
  LSyncOk: Boolean;
begin
  Result := True;
  LLote := nil;
  LN := 0;

  FMon.Enter;
  try
    while (FPendentes.Count = 0) and (not FParando) and (not FFalho) do
      FMon.Wait(100);

    if FFalho then
      Exit(False);

    LN := FPendentes.Count;
    if LN = 0 then
    begin
      // Fila vazia E parada pedida: a thread sai. Este e' o UNICO caminho de
      // saida normal, e ele so' acontece depois de tudo que foi aceito ter
      // sido escrito -- e' o que faz o Stop nao perder registro.
      if FParando then
        Exit(False);
      Exit(True);
    end;

    // TIRA TUDO -- e' o group commit. O que chegar durante a escrita ja' e' o
    // proximo lote, e e' assim que o tamanho do lote se ajusta sozinho a'
    // carga sem nenhum parametro (D25).
    SetLength(LLote, LN);
    for I := 0 to LN - 1 do
      LLote[I] := FPendentes.Dequeue;
    FPendingBytes := 0;
    FMon.PulseAll; // libera quem estava bloqueado por contrapressao
  finally
    FMon.Leave;
  end;

  LoteTomado;

  // --- fora do lock: I/O ---
  LMaiorLsn := LLote[LN - 1].Lsn;
  try
    for I := 0 to LN - 1 do
      FSegmento.Append(LLote[I].Lsn, LLote[I].Kind, LLote[I].Payload);
  except
    on E: Exception do
    begin
      // Escrita que falha nao tem como ser reposta: os registros nao estao em
      // lugar nenhum. Estado falho, definitivo.
      MarcaFalho(Format('falha ao escrever no journal: %s', [E.Message]));
      Exit(False);
    end;
  end;

  // fsync que falha NAO e' fatal: os bytes estao no arquivo, e um fsync
  // posterior cobre este lote. O que NAO pode acontecer e' a marca d'agua
  // andar -- seria prometer durabilidade que ninguem confirmou.
  LSyncOk := FSegmento.Sync;
  if LSyncOk then
    AmqpAtomicWrite64(FDurableLsn, LMaiorLsn);

  // ROTACAO, e SO' AQUI: na fronteira de lote, depois do fsync. Nunca no meio
  // de um lote -- o lote e' indivisivel (D24) e parti-lo entre dois arquivos
  // daria uma cauda torta artificial no primeiro, que a recuperacao
  // descartaria junto com metade de um publish ja' aceito.
  //
  // Depois de um fsync que FALHOU nao se rotaciona: os bytes deste lote ainda
  // nao estao garantidos no disco, e fechar o arquivo agora tiraria a chance
  // de um fsync posterior cobri-los.
  if LSyncOk and (FSegmento.EndOffset >= FMaxSegmentBytes) then
  begin
    try
      Rotaciona;
    except
      on E: Exception do
      begin
        // Nao conseguir abrir o proximo segmento e' falha de escrita: os
        // proximos registros nao teriam onde ir.
        MarcaFalho(Format('falha ao rotacionar o segmento: %s', [E.Message]));
        Exit(False);
      end;
    end;
  end;

  FMon.Enter;
  try
    Inc(FLotes);
    Inc(FRegistros, LN);
    Inc(FSyncs);
    if not LSyncOk then
      Inc(FSyncsFalhos);
    if LN > FMaiorLote then
      FMaiorLote := LN;
    FMon.PulseAll; // acorda WaitDurable
  finally
    FMon.Leave;
  end;

  if not LSyncOk then
    Exit(True);

  LSink := FSink;
  if LSink <> nil then
    try
      LSink.Durable(LMaiorLsn);
    except
      // Sink que levanta nao pode derrubar a thread do journal: o registro JA'
      // esta' duravel e a marca d'agua ja' andou. Engolir e seguir e' menos
      // ruim que parar de durar por causa de quem so' queria ser avisado.
    end;
end;

end.

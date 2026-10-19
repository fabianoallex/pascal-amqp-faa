unit AMQP.TracingTests;

{ Testes da parte sem broker do tracing (AMQP.Tracing e o BuildContentHeader
  com contexto de trace): a injecao do traceparent nos bytes do header, que
  nunca altera a tabela do chamador nem duplica nome, a leitura do contexto
  de uma mensagem recebida e os nomes de destino dos spans. Os spans de
  verdade (publish -> process numa trace so') sao testados contra o broker
  embutido, em tests\Server\AMQP.ServerTracingTests.pas.

  Mestre DUnitX no dialeto do FPCUnit (TAssert.*); o espelho FPCUnit em
  fpc\ e' gerado por tools/gen_fpc_mirror.py. }


interface

uses
  DUnitX.TestFramework,
  AMQP.DUnitXCompat,
  SysUtils, Rtti,
  AMQP.Wire,
  AMQP.Basic.Methods,
  PascalCommon.Tracing,
  AMQP.Tracing;

type
  [TestFixture]
  TTracingCodecTests = class
  public
    [Test] procedure ContentHeader_SemTraceParent_IgualAoDeSempre;
    [Test] procedure ContentHeader_SemHeaders_InjetaTraceParent;
    [Test] procedure ContentHeader_ComHeaders_AcrescentaSemMexerNaTabela;
    [Test] procedure ContentHeader_TraceParentDoChamador_Vence;
    [Test] procedure ContentHeader_TraceStateDoChamador_NaoDuplica;
    [Test] procedure TraceParentOf_SemHeaders_False;
    [Test] procedure TraceParentOf_LeTraceParentETraceState;
    [Test] procedure TraceParentOf_ValorQueNaoEString_False;
    [Test] procedure SpanDestination_NomesDoSpan;
    [Test] procedure TracerDesligado_NaoAbreSpan;
  end;

implementation

const
  TP = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
  TP_DO_CHAMADOR = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

function Decodifica(const ABytes: TBytes): TAMQPContentHeader;
var
  LReader: TAMQPReader;
begin
  LReader := TAMQPReader.Create(ABytes);
  try
    Result := DecodeContentHeader(LReader);
  finally
    LReader.Free;
  end;
end;

function TextoDe(ATable: TAMQPFieldTable; const AName: string): string;
var
  LValue: TValue;
begin
  Result := '';
  if ATable.TryGetValue(AName, LValue) then
    Result := LValue.AsString;
end;

procedure LiberaHeaders(const AHeader: TAMQPContentHeader);
begin
  if bpHeaders in AHeader.Properties.Flags then
    AHeader.Properties.Headers.Free;
end;

function MesmosBytes(const A, B: TBytes): Boolean;
begin
  Result := (Length(A) = Length(B)) and
    ((Length(A) = 0) or CompareMem(@A[0], @B[0], Length(A)));
end;

procedure TTracingCodecTests.ContentHeader_SemTraceParent_IgualAoDeSempre;
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  TAssert.AssertTrue('sem traceparent, os mesmos bytes',
    MesmosBytes(BuildContentHeader(3, LProps), BuildContentHeader(3, LProps, '', 'x=1')));
end;

procedure TTracingCodecTests.ContentHeader_SemHeaders_InjetaTraceParent;
var
  LProps: TAMQPBasicProperties;
  LHeader: TAMQPContentHeader;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  LHeader := Decodifica(BuildContentHeader(3, LProps, TP, ''));
  try
    TAssert.AssertTrue('ganhou headers', bpHeaders in LHeader.Properties.Flags);
    TAssert.AssertEquals('so o traceparent', 1, Integer(LHeader.Properties.Headers.Count));
    TAssert.AssertEquals('traceparent', TP, TextoDe(LHeader.Properties.Headers, 'traceparent'));
    TAssert.AssertEquals('o resto intacto', 'text/plain', LHeader.Properties.ContentType);
    TAssert.AssertEquals('body size', 3, Integer(LHeader.BodySize));
  finally
    LiberaHeaders(LHeader);
  end;
end;

procedure TTracingCodecTests.ContentHeader_ComHeaders_AcrescentaSemMexerNaTabela;
var
  LProps: TAMQPBasicProperties;
  LTabela: TAMQPFieldTable;
  LValue: TValue;
  LHeader: TAMQPContentHeader;
begin
  LTabela := TAMQPFieldTable.Create;
  try
    LValue := TValue.From<string>('pedido');
    LTabela.Put('tipo', LValue);
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetHeaders(LTabela);
    LHeader := Decodifica(BuildContentHeader(0, LProps, TP, 'vendor=abc'));
    try
      TAssert.AssertEquals('tres entradas', 3, Integer(LHeader.Properties.Headers.Count));
      TAssert.AssertEquals('a do chamador', 'pedido', TextoDe(LHeader.Properties.Headers, 'tipo'));
      TAssert.AssertEquals('traceparent', TP, TextoDe(LHeader.Properties.Headers, 'traceparent'));
      TAssert.AssertEquals('tracestate', 'vendor=abc',
        TextoDe(LHeader.Properties.Headers, 'tracestate'));
    finally
      LiberaHeaders(LHeader);
    end;
    TAssert.AssertEquals('a tabela do chamador nao mudou', 1, Integer(LTabela.Count));
    TAssert.AssertFalse('sem traceparent nela', LTabela.ContainsKey('traceparent'));
  finally
    LTabela.Free;
  end;
end;

procedure TTracingCodecTests.ContentHeader_TraceParentDoChamador_Vence;
var
  LProps: TAMQPBasicProperties;
  LTabela: TAMQPFieldTable;
  LValue: TValue;
  LHeader: TAMQPContentHeader;
begin
  LTabela := TAMQPFieldTable.Create;
  try
    LValue := TValue.From<string>(TP_DO_CHAMADOR);
    LTabela.Put('traceparent', LValue);
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetHeaders(LTabela);
    TAssert.AssertTrue('nada acrescentado: os mesmos bytes',
      MesmosBytes(BuildContentHeader(0, LProps), BuildContentHeader(0, LProps, TP, 'x=1')));
    LHeader := Decodifica(BuildContentHeader(0, LProps, TP, 'x=1'));
    try
      TAssert.AssertEquals('uma entrada', 1, Integer(LHeader.Properties.Headers.Count));
      TAssert.AssertEquals('o do chamador', TP_DO_CHAMADOR,
        TextoDe(LHeader.Properties.Headers, 'traceparent'));
    finally
      LiberaHeaders(LHeader);
    end;
  finally
    LTabela.Free;
  end;
end;

procedure TTracingCodecTests.ContentHeader_TraceStateDoChamador_NaoDuplica;
var
  LProps: TAMQPBasicProperties;
  LTabela: TAMQPFieldTable;
  LValue: TValue;
  LHeader: TAMQPContentHeader;
begin
  LTabela := TAMQPFieldTable.Create;
  try
    LValue := TValue.From<string>('meu=1');
    LTabela.Put('tracestate', LValue);
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetHeaders(LTabela);
    LHeader := Decodifica(BuildContentHeader(0, LProps, TP, 'vendor=abc'));
    try
      TAssert.AssertEquals('duas entradas, sem nome repetido', 2,
        Integer(LHeader.Properties.Headers.Count));
      TAssert.AssertEquals('traceparent injetado', TP,
        TextoDe(LHeader.Properties.Headers, 'traceparent'));
      TAssert.AssertEquals('tracestate do chamador', 'meu=1',
        TextoDe(LHeader.Properties.Headers, 'tracestate'));
    finally
      LiberaHeaders(LHeader);
    end;
  finally
    LTabela.Free;
  end;
end;

procedure TTracingCodecTests.TraceParentOf_SemHeaders_False;
var
  LProps: TAMQPBasicProperties;
  LParent, LState: string;
begin
  LProps := TAMQPBasicProperties.Empty;
  TAssert.AssertFalse('sem headers', AmqpTraceParentOf(LProps, LParent, LState));
  TAssert.AssertEquals('traceparent vazio', '', LParent);
  TAssert.AssertEquals('tracestate vazio', '', LState);
end;

procedure TTracingCodecTests.TraceParentOf_LeTraceParentETraceState;
var
  LProps: TAMQPBasicProperties;
  LHeader: TAMQPContentHeader;
  LParent, LState: string;
begin
  LProps := TAMQPBasicProperties.Empty;
  // O caminho de verdade: a tabela decodificada do wire.
  LHeader := Decodifica(BuildContentHeader(0, LProps, TP, 'vendor=abc'));
  try
    TAssert.AssertTrue('achou', AmqpTraceParentOf(LHeader.Properties, LParent, LState));
    TAssert.AssertEquals('traceparent', TP, LParent);
    TAssert.AssertEquals('tracestate', 'vendor=abc', LState);
  finally
    LiberaHeaders(LHeader);
  end;
end;

procedure TTracingCodecTests.TraceParentOf_ValorQueNaoEString_False;
var
  LProps: TAMQPBasicProperties;
  LTabela: TAMQPFieldTable;
  LValue: TValue;
  LParent, LState: string;
begin
  LTabela := TAMQPFieldTable.Create;
  try
    LValue := TValue.From<Int64>(42);
    LTabela.Put('traceparent', LValue);
    LProps := TAMQPBasicProperties.Empty;
    LProps.SetHeaders(LTabela);
    TAssert.AssertFalse('numero nao e traceparent', AmqpTraceParentOf(LProps, LParent, LState));
    TAssert.AssertEquals('traceparent vazio', '', LParent);
  finally
    LTabela.Free;
  end;
end;

procedure TTracingCodecTests.SpanDestination_NomesDoSpan;
begin
  TAssert.AssertEquals('exchange padrao', 'amq.default', AmqpSpanDestination('', True));
  TAssert.AssertEquals('exchange com nome', 'pedidos', AmqpSpanDestination('pedidos', True));
  TAssert.AssertEquals('fila gerada', '(temporary)', AmqpSpanDestination('amq.gen-Xy12', False));
  TAssert.AssertEquals('fila com nome', 'pedidos', AmqpSpanDestination('pedidos', False));
  TAssert.AssertEquals('fila vazia nao e exchange', '', AmqpSpanDestination('', False));
end;

procedure TTracingCodecTests.TracerDesligado_NaoAbreSpan;
var
  LProps: TAMQPBasicProperties;
begin
  TAssert.AssertFalse('nenhum teste deixou o tracer ligado', TPcTracing.Enabled);
  LProps := TAMQPBasicProperties.Empty;
  TAssert.AssertTrue('publish', AmqpStartPublishSpan('ex', 'rk', 1, LProps) = nil);
  TAssert.AssertTrue('process', AmqpStartProcessSpan('q', LProps) = nil);
  TAssert.AssertTrue('receive', AmqpStartReceiveSpan('q') = nil);
  AmqpFinishSpan(nil, nil); // nil e no-op
end;

initialization
  TDUnitX.RegisterTestFixture(TTracingCodecTests);

end.

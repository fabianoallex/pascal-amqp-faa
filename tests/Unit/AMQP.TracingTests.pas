unit AMQP.TracingTests;

{ Testes da parte sem broker do tracing (AMQP.Tracing e o BuildContentHeader
  com contexto de trace): a injecao do traceparent nos bytes do header, que
  nunca altera a tabela do chamador nem duplica nome, a leitura do contexto
  de uma mensagem recebida e os nomes de destino dos spans. Os spans de
  verdade (publish -> process numa trace so') sao testados contra o broker
  embutido, em tests\Server\AMQP.ServerTracingTests.pas.

  Espelho FPCUnit de tests\Unit\fpc\AMQP.TracingTests.pas -- mantenha os dois em
  sincronia (mesmos nomes de teste, mesmas assercoes). }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils, System.Rtti,
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
  Assert.IsTrue(MesmosBytes(BuildContentHeader(3, LProps), BuildContentHeader(3, LProps, '', 'x=1')), 'sem traceparent, os mesmos bytes');
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
    Assert.IsTrue(bpHeaders in LHeader.Properties.Flags, 'ganhou headers');
    Assert.AreEqual(1, Integer(LHeader.Properties.Headers.Count), 'so o traceparent');
    Assert.AreEqual(TP, TextoDe(LHeader.Properties.Headers, 'traceparent'), 'traceparent');
    Assert.AreEqual('text/plain', LHeader.Properties.ContentType, 'o resto intacto');
    Assert.AreEqual(3, Integer(LHeader.BodySize), 'body size');
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
      Assert.AreEqual(3, Integer(LHeader.Properties.Headers.Count), 'tres entradas');
      Assert.AreEqual('pedido', TextoDe(LHeader.Properties.Headers, 'tipo'), 'a do chamador');
      Assert.AreEqual(TP, TextoDe(LHeader.Properties.Headers, 'traceparent'), 'traceparent');
      Assert.AreEqual('vendor=abc', TextoDe(LHeader.Properties.Headers, 'tracestate'), 'tracestate');
    finally
      LiberaHeaders(LHeader);
    end;
    Assert.AreEqual(1, Integer(LTabela.Count), 'a tabela do chamador nao mudou');
    Assert.IsFalse(LTabela.ContainsKey('traceparent'), 'sem traceparent nela');
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
    Assert.IsTrue(MesmosBytes(BuildContentHeader(0, LProps), BuildContentHeader(0, LProps, TP, 'x=1')), 'nada acrescentado: os mesmos bytes');
    LHeader := Decodifica(BuildContentHeader(0, LProps, TP, 'x=1'));
    try
      Assert.AreEqual(1, Integer(LHeader.Properties.Headers.Count), 'uma entrada');
      Assert.AreEqual(TP_DO_CHAMADOR, TextoDe(LHeader.Properties.Headers, 'traceparent'), 'o do chamador');
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
      Assert.AreEqual(2, Integer(LHeader.Properties.Headers.Count), 'duas entradas, sem nome repetido');
      Assert.AreEqual(TP, TextoDe(LHeader.Properties.Headers, 'traceparent'), 'traceparent injetado');
      Assert.AreEqual('meu=1', TextoDe(LHeader.Properties.Headers, 'tracestate'), 'tracestate do chamador');
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
  Assert.IsFalse(AmqpTraceParentOf(LProps, LParent, LState), 'sem headers');
  Assert.AreEqual('', LParent, 'traceparent vazio');
  Assert.AreEqual('', LState, 'tracestate vazio');
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
    Assert.IsTrue(AmqpTraceParentOf(LHeader.Properties, LParent, LState), 'achou');
    Assert.AreEqual(TP, LParent, 'traceparent');
    Assert.AreEqual('vendor=abc', LState, 'tracestate');
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
    Assert.IsFalse(AmqpTraceParentOf(LProps, LParent, LState), 'numero nao e traceparent');
    Assert.AreEqual('', LParent, 'traceparent vazio');
  finally
    LTabela.Free;
  end;
end;

procedure TTracingCodecTests.SpanDestination_NomesDoSpan;
begin
  Assert.AreEqual('amq.default', AmqpSpanDestination('', True), 'exchange padrao');
  Assert.AreEqual('pedidos', AmqpSpanDestination('pedidos', True), 'exchange com nome');
  Assert.AreEqual('(temporary)', AmqpSpanDestination('amq.gen-Xy12', False), 'fila gerada');
  Assert.AreEqual('pedidos', AmqpSpanDestination('pedidos', False), 'fila com nome');
  Assert.AreEqual('', AmqpSpanDestination('', False), 'fila vazia nao e exchange');
end;

procedure TTracingCodecTests.TracerDesligado_NaoAbreSpan;
var
  LProps: TAMQPBasicProperties;
begin
  Assert.IsFalse(TPcTracing.Enabled, 'nenhum teste deixou o tracer ligado');
  LProps := TAMQPBasicProperties.Empty;
  Assert.IsTrue(AmqpStartPublishSpan('ex', 'rk', 1, LProps) = nil, 'publish');
  Assert.IsTrue(AmqpStartProcessSpan('q', LProps) = nil, 'process');
  Assert.IsTrue(AmqpStartReceiveSpan('q') = nil, 'receive');
  AmqpFinishSpan(nil, nil); // nil e no-op
end;

initialization
  TDUnitX.RegisterTestFixture(TTracingCodecTests);

end.

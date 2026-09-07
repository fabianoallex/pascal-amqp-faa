program AMQPServer;

{ Broker AMQP embutido com observabilidade ligada (Fase 4.1).

  Mesmo fonte para FPC e Delphi, como o SmokeTest. Sobe um broker em
  127.0.0.1:5672, assina os eventos e imprime cada um. Encerra sozinho depois
  de --segundos (default 20), para servir de passo de verificacao e nao so' de
  demonstracao.

    AMQPServer.exe [--porta N] [--segundos N] [--tipos lifecycle|todos]

  FPC:     lazbuild samples\Server\AMQPServer.lpi
  Delphi:  abrir samples\Server\AMQPServer.dproj no IDE (a CE nao tem CLI).

  O QUE ESTE SAMPLE MOSTRA, e que e' o unico jeito de ver a D30 funcionando:
  o handler roda na thread NOTIFICADORA do broker, nunca na thread de leitura
  de uma conexao e nunca num worker do pool. Por isso ele pode escrever na
  console -- que e' lenta e serializada -- sem atrasar o broker. O preco esta'
  no relatorio final: sob rajada, o contador de DESCARTADOS sobe, e e'
  exatamente o contrato best-effort da D31. Eventos nao sao log de auditoria.

  Os tipos de origem-ator (enqueue/deliver/expire/DLX/drop) e o de journal
  ainda nao tem emissor -- sao o Inc. 2 desta fase. Assinar 'todos' ja'
  funciona; eles simplesmente nao chegam ainda. }

{$IFDEF FPC}
{$MODE DELPHI}{$H+}
{$ENDIF}
{$APPTYPE CONSOLE}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  SysUtils,
  AMQP.Threading,
  AMQP.Server.Events,
  AMQP.Server.Broker;

type
  { Handler de exemplo. Nao precisa de lock nenhum: a notificadora e' UMA
    thread, entao o handler nunca roda concorrente consigo mesmo (D30). Um
    handler que fale com estado compartilhado com OUTRAS threads, ai' sim,
    precisa se proteger. }
  TImpressor = class
  private
    FTotal: Integer;
  public
    procedure AoEvento(const AEvent: TAMQPServerEvent);
    property Total: Integer read FTotal;
  end;

var
  GBroker: TAMQPServer;
  GImpressor: TImpressor;
  GPorta: Integer = 5672;
  GSegundos: Integer = 20;
  GTodos: Boolean = False;

procedure TImpressor.AoEvento(const AEvent: TAMQPServerEvent);
var
  LOnde: string;
begin
  Inc(FTotal);
  LOnde := AmqpEventTypeName(AEvent.EventType);
  if AEvent.ChannelNumber > 0 then
    LOnde := LOnde + Format(' [canal %d]', [AEvent.ChannelNumber]);

  Write(Format('%4d  %-34s conn=%d', [FTotal, LOnde, AEvent.ConnectionId]));
  if AEvent.RemoteAddr <> '' then
    Write(' de ', AEvent.RemoteAddr);
  if AEvent.Username <> '' then
    Write(Format(' user=%s vhost=%s', [AEvent.Username, AEvent.VHost]));
  if AEvent.QueueName <> '' then
    Write(' fila=', AEvent.QueueName);
  if AEvent.ConsumerTag <> '' then
    Write(' tag=', AEvent.ConsumerTag);
  if AEvent.RoutingKey <> '' then
    Write(Format(' rota=%s/%s', [AEvent.ExchangeName, AEvent.RoutingKey]));
  if AEvent.MessageSize > 0 then
    Write(Format(' %d bytes', [AEvent.MessageSize]));
  if AEvent.DeliveryTag > 0 then
    Write(Format(' tag-entrega=%d', [AEvent.DeliveryTag]));
  if AEvent.Reason <> '' then
    Write(' (', AEvent.Reason, ')');
  Writeln;
end;

procedure LeArgumentos;
var
  I: Integer;
  LArg: string;
begin
  I := 1;
  while I <= ParamCount do
  begin
    LArg := ParamStr(I);
    if (LArg = '--porta') and (I < ParamCount) then
    begin
      Inc(I);
      GPorta := StrToIntDef(ParamStr(I), GPorta);
    end
    else if (LArg = '--segundos') and (I < ParamCount) then
    begin
      Inc(I);
      GSegundos := StrToIntDef(ParamStr(I), GSegundos);
    end
    else if (LArg = '--tipos') and (I < ParamCount) then
    begin
      Inc(I);
      GTodos := ParamStr(I) = 'todos';
    end;
    Inc(I);
  end;
end;

var
  LFim: UInt64;
begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  {$IFDEF FPC}
  SetMultiByteConversionCodePage(CP_UTF8);
  {$ENDIF}
  LeArgumentos;

  GImpressor := TImpressor.Create;
  GBroker := TAMQPServer.Create;
  try
    GBroker.BindAddress := '127.0.0.1';
    GBroker.Port := GPorta;

    // Assinar ANTES do Start e' o caso normal: quem observa quer o primeiro
    // evento, nao o segundo.
    if GTodos then
      GBroker.Subscribe(GImpressor.AoEvento)
    else
      // Filtrar por tipo custa uma mascara e evita montar record que ninguem
      // vai olhar -- e' o que a D32 existe para permitir.
      GBroker.Subscribe(GImpressor.AoEvento,
        [seConnectionEstablished, seConnectionAuthenticated,
         seConnectionClosed, seChannelOpened, seChannelClosed,
         seMessagePublished, seMessagePublishRejected,
         seConsumerRegistered, seConsumerCancelled,
         seMessageAcked, seMessageNacked, seMessageRejected]);

    GBroker.Start;
    Writeln(Format('broker em 127.0.0.1:%d por %d s -- aponte um cliente nele',
      [GBroker.Port, GSegundos]));
    Writeln('');

    LFim := AmqpTickMs + UInt64(GSegundos) * 1000;
    while AmqpTickMs < LFim do
      Sleep(200);

    // Drena antes de relatar: a entrega e' assincrona (D30), entao sem a
    // barreira o relatorio sairia na frente dos ultimos eventos.
    GBroker.DrainEvents(2000);
    Writeln('');
    Writeln(Format('eventos entregues:  %d', [GImpressor.Total]));
    Writeln(Format('eventos emitidos:   %d', [GBroker.EventsEmitted]));
    // Se este numero nao for zero, o handler nao acompanhou a carga -- e' o
    // best-effort da D31 cobrando o preco, e o unico jeito de saber que ele
    // foi cobrado.
    Writeln(Format('DESCARTADOS:        %d', [GBroker.EventsDropped]));
    Writeln(Format('handlers que falharam: %d', [GBroker.EventsFailed]));
    if GBroker.LastEventFailure <> '' then
      Writeln('ultima falha: ', GBroker.LastEventFailure);

    GBroker.Stop;
    // Desassinar ANTES de liberar o dono do metodo: e' o contrato da D33, e o
    // sample segue o que a documentacao manda o usuario fazer.
    GBroker.Unsubscribe(GImpressor.AoEvento);
  finally
    GBroker.Free;
    GImpressor.Free;
  end;
end.

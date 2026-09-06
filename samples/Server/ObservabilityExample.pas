{$I amqp.inc}

{ Exemplo: Auditoria e rastreamento de abastecidas com observabilidade.

  Neste exemplo simulamos um sistema de controle de combustível em um posto:
  - Abastecidas são publicadas em fila "abastecidas"
  - PDVs consomem (reservam) abastecidas
  - Confirmamos auditoria de cada movimento

  O sistema de eventos permite rastrear TUDO que acontece:
  - Quem publicou (PDV de origem)
  - Quando foi publicado (WallTime)
  - Qual PDV consumiu (ConsumerTag)
  - Se foi confirmado ou nacked
  - Qualquer erro (TTL, descarte, etc.)

  Ideal para compliance e debugging em uma aplicação business-critical.
}

unit ObservabilityExample;

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  AMQP.Server.Broker,
  AMQP.Server.Events;

type
  { Rastreador de auditoria para abastecidas. }
  TAuditLog = class
  private
    FLock: TObject;
    FLog: TStrings;
    FAbastecidas: TDictionary<string, string>; // nome -> PDV consumidor
  public
    constructor Create;
    destructor Destroy; override;

    { Registra um evento da observabilidade. }
    procedure OnServerEvent(const Event: TAMQPServerEvent);

    { Log de auditoria. }
    property Log: TStrings read FLog;

    { Dicionário: abastecida -> PDV consumidor. }
    property Abastecidas: TDictionary<string, string> read FAbastecidas;
  end;

  { Exemplo de uso. }
  TFuelPumpDemo = class
  private
    FServer: TAMQPServer;
    FAudit: TAuditLog;
  public
    constructor Create;
    destructor Destroy; override;

    { Sobe o broker e conecta auditoria. }
    procedure Start;
    procedure Stop;

    property Server: TAMQPServer read FServer;
    property Audit: TAuditLog read FAudit;
  end;

implementation

uses
  SyncObjs;

constructor TAuditLog.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FLog := TStringList.Create;
  FAbastecidas := TDictionary<string, string>.Create;
end;

destructor TAuditLog.Destroy;
begin
  FAbastecidas.Free;
  FLog.Free;
  TCriticalSection(FLock).Free;
  inherited;
end;

procedure TAuditLog.OnServerEvent(const Event: TAMQPServerEvent);
var
  LLine: string;
begin
  TCriticalSection(FLock).Enter;
  try
    case Event.EventType of
      seMessagePublished:
        begin
          LLine := Format('[PUBLISH] Tempo: %s | Usuario: %s | Fila: %s | '
            + 'Rota: %s -> %s | Tamanho: %d bytes',
            [DateTimeToStr(Event.WallTime), Event.Username,
             Event.QueueName, Event.ExchangeName, Event.RoutingKey,
             Event.MessageSize]);
          FLog.Add(LLine);
        end;

      seMessageDelivered:
        begin
          LLine := Format('[DELIVER] Tempo: %s | Consumidor: %s | Fila: %s | '
            + 'DeliveryTag: %d | Redelivered: %s',
            [DateTimeToStr(Event.WallTime), Event.ConsumerTag,
             Event.QueueName, Event.DeliveryTag,
             BoolToStr(Event.Redelivered, True)]);
          FLog.Add(LLine);

          // Marcar que esta abastecida foi consumida por este PDV.
          // Implementação simplificada; em produção seria mais estruturada.
          if Event.QueueName = 'abastecidas' then
            FAbastecidas.AddOrSetValue(Event.QueueName + '/' +
              IntToStr(Event.DeliveryTag), Event.ConsumerTag);
        end;

      seMessageAcked:
        begin
          LLine := Format('[ACK] Tempo: %s | Fila: %s | DeliveryTag: %d | '
            + 'Consumidor confirmou | Status: OK',
            [DateTimeToStr(Event.WallTime), Event.QueueName, Event.DeliveryTag]);
          FLog.Add(LLine);
        end;

      seMessageNacked, seMessageRejected:
        begin
          LLine := Format('[NACK] Tempo: %s | Fila: %s | DeliveryTag: %d | '
            + 'Consumidor rejeitou | Razao: %s',
            [DateTimeToStr(Event.WallTime), Event.QueueName, Event.DeliveryTag,
             Event.Reason]);
          FLog.Add(LLine);

          { Liberar a abastecida — outro PDV pode consumir. }
          FAbastecidas.Remove(Event.QueueName + '/' +
            IntToStr(Event.DeliveryTag));
        end;

      seMessageExpired:
        begin
          LLine := Format('[EXPIRE] Tempo: %s | Fila: %s | Mensagem venceu | '
            + 'TTL: %d ms',
            [DateTimeToStr(Event.WallTime), Event.QueueName,
             Event.MessageExpirationMs]);
          FLog.Add(LLine);
        end;

      seMessageDropped:
        begin
          LLine := Format('[DROP] Tempo: %s | Fila: %s | Mensagem descartada | '
            + 'Razao: %s',
            [DateTimeToStr(Event.WallTime), Event.QueueName, Event.Reason]);
          FLog.Add(LLine);
        end;

      seConnectionEstablished:
        begin
          LLine := Format('[CONN] Tempo: %s | Nova conexao | RemoteAddr: %s | '
            + 'ConnId: %d',
            [DateTimeToStr(Event.WallTime), Event.RemoteAddr,
             Event.ConnectionId]);
          FLog.Add(LLine);
        end;

      seConnectionAuthenticated:
        begin
          LLine := Format('[AUTH] Tempo: %s | Usuario autenticado: %s | '
            + 'VHost: %s',
            [DateTimeToStr(Event.WallTime), Event.Username, Event.VHost]);
          FLog.Add(LLine);
        end;

      seConnectionClosed:
        begin
          LLine := Format('[DISC] Tempo: %s | Conexao fechada | Usuario: %s | '
            + 'Razao: %s',
            [DateTimeToStr(Event.WallTime), Event.Username, Event.Reason]);
          FLog.Add(LLine);
        end;

      seJournalFlushed:
        begin
          if Event.JournalLsn > 0 then
            LLine := Format('[FSYNC] Tempo: %s | Journal flushed | LSN: %d',
              [DateTimeToStr(Event.WallTime), Event.JournalLsn])
          else
            LLine := Format('[FSYNC] Tempo: %s | Journal flushed',
              [DateTimeToStr(Event.WallTime)]);
          FLog.Add(LLine);
        end;
    end;
  finally
    TCriticalSection(FLock).Leave;
  end;
end;

constructor TFuelPumpDemo.Create;
begin
  inherited Create;
  FServer := TAMQPServer.Create;
  FAudit := TAuditLog.Create;
end;

destructor TFuelPumpDemo.Destroy;
begin
  FAudit.Free;
  FServer.Free;
  inherited;
end;

procedure TFuelPumpDemo.Start;
begin
  { Registra o handler de auditoria ANTES de iniciar o broker. }
  FServer.Subscribe(FAudit.OnServerEvent);

  { Configuração básica. }
  FServer.BindAddress := '127.0.0.1';
  FServer.Port := 5672;

  { Opcionalmente: ligar durabilidade. }
  // FServer.DataDir := 'C:\broker-data';

  FServer.Start;
end;

procedure TFuelPumpDemo.Stop;
begin
  if FServer.Running then
    FServer.Stop;
end;

end.

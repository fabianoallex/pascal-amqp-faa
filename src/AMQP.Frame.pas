unit AMQP.Frame;

{$I amqp.inc}

{ Camada de framing do AMQP 0-9-1 (spec 2.3.5).

  Layout de um frame no wire:

    +--------+----------+---------+ +--------------+ +-----------+
    | type   | channel  | size    | | payload      | | frame-end |
    | octet  | u16 (BE) | u32(BE) | | size octetos | | octet 0xCE|
    +--------+----------+---------+ +--------------+ +-----------+

  'size' e' o tamanho apenas do payload; o octeto frame-end (0xCE) que o segue
  nao entra na contagem. Se o octeto lido nessa posicao for diferente de 0xCE, o
  stream esta dessincronizado e levantamos EAMQPFrame. }

interface

uses
  SysUtils,
  Classes,
  AMQP.Protocol;

type
  EAMQPFrame = class(Exception);

  TAMQPFrame = record
    FrameType: Byte;
    Channel: Word;
    Payload: TBytes;

    class function Create(AFrameType: Byte; AChannel: Word;
      const APayload: TBytes): TAMQPFrame; static;

    /// Frame de heartbeat (tipo 8, canal 0, payload vazio).
    class function Heartbeat: TAMQPFrame; static;

    /// Serializa o frame completo (cabecalho + payload + frame-end) em AStream.
    procedure WriteTo(AStream: TStream);

    /// Le um frame completo de AStream. Se AMaxPayload > 0, recusa payloads
    /// maiores (protecao contra frame-max estourado / stream corrompido).
    class function ReadFrom(AStream: TStream;
      AMaxPayload: Cardinal = 0): TAMQPFrame; static;

    function IsHeartbeat: Boolean;
    function IsMethod: Boolean;
  end;

/// Envia o cabecalho de protocolo ("AMQP" 0 0 9 1) — primeira coisa que o
/// cliente escreve no socket, antes de qualquer frame.
procedure WriteProtocolHeader(AStream: TStream);

implementation

// Le exatamente ACount octetos de AStream, em loop (um Read de socket pode
// retornar menos que o pedido). Levanta EAMQPFrame em fim de stream prematuro.
procedure ReadFull(AStream: TStream; var ABuffer: TBytes; ACount: Integer);
var
  LTotal, LRead: Integer;
begin
  SetLength(ABuffer, ACount);
  LTotal := 0;
  while LTotal < ACount do
  begin
    LRead := AStream.Read(ABuffer[LTotal], ACount - LTotal);
    if LRead <= 0 then
      raise EAMQPFrame.Create('fim de stream ao ler frame (conexao fechada?)');
    Inc(LTotal, LRead);
  end;
end;

{ TAMQPFrame }

class function TAMQPFrame.Create(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes): TAMQPFrame;
begin
  Result.FrameType := AFrameType;
  Result.Channel := AChannel;
  Result.Payload := APayload;
end;

class function TAMQPFrame.Heartbeat: TAMQPFrame;
begin
  Result := TAMQPFrame.Create(AMQP_FRAME_HEARTBEAT, AMQP_CHANNEL_CONNECTION, nil);
end;

function TAMQPFrame.IsHeartbeat: Boolean;
begin
  Result := FrameType = AMQP_FRAME_HEARTBEAT;
end;

function TAMQPFrame.IsMethod: Boolean;
begin
  Result := FrameType = AMQP_FRAME_METHOD;
end;

procedure TAMQPFrame.WriteTo(AStream: TStream);
var
  LHeader: array[0..6] of Byte;
  LSize: Cardinal;
  LEnd: Byte;
begin
  LSize := Cardinal(Length(Payload));
  LHeader[0] := FrameType;
  LHeader[1] := Byte(Channel shr 8);
  LHeader[2] := Byte(Channel);
  LHeader[3] := Byte(LSize shr 24);
  LHeader[4] := Byte(LSize shr 16);
  LHeader[5] := Byte(LSize shr 8);
  LHeader[6] := Byte(LSize);
  // Buffer untyped/por-referencia: sem @ antes da variavel (ver CLAUDE.md).
  AStream.WriteBuffer(LHeader[0], SizeOf(LHeader));
  if LSize > 0 then
    AStream.WriteBuffer(Payload[0], Integer(LSize));
  LEnd := AMQP_FRAME_END;
  AStream.WriteBuffer(LEnd, 1);
end;

class function TAMQPFrame.ReadFrom(AStream: TStream;
  AMaxPayload: Cardinal): TAMQPFrame;
var
  LHeader: TBytes;
  LSize: Cardinal;
  LEnd: TBytes;
begin
  ReadFull(AStream, LHeader, 7);
  Result.FrameType := LHeader[0];
  Result.Channel := (Word(LHeader[1]) shl 8) or Word(LHeader[2]);
  LSize := (Cardinal(LHeader[3]) shl 24) or (Cardinal(LHeader[4]) shl 16) or
           (Cardinal(LHeader[5]) shl 8) or Cardinal(LHeader[6]);

  if (AMaxPayload > 0) and (LSize > AMaxPayload) then
    raise EAMQPFrame.CreateFmt(
      'payload de frame (%u) excede o maximo negociado (%u)', [LSize, AMaxPayload]);

  if LSize > 0 then
    ReadFull(AStream, Result.Payload, Integer(LSize))
  else
    Result.Payload := nil;

  ReadFull(AStream, LEnd, 1);
  if LEnd[0] <> AMQP_FRAME_END then
    raise EAMQPFrame.CreateFmt(
      'octeto frame-end invalido: esperado 0x%.2x, veio 0x%.2x',
      [AMQP_FRAME_END, LEnd[0]]);
end;

{ Cabecalho de protocolo }

procedure WriteProtocolHeader(AStream: TStream);
begin
  AStream.WriteBuffer(AMQP_PROTOCOL_HEADER[0], Length(AMQP_PROTOCOL_HEADER));
end;

end.

unit AMQP.Protocol;

{$I amqp.inc}

{ Constantes do protocolo AMQP 0-9-1.

  Fonte: especificacao publica AMQP 0-9-1 (documento de protocolo). Nada aqui
  deriva de codigo de terceiros — sao apenas os valores numericos definidos pela
  especificacao (tipos de frame, octeto de fim de frame, IDs de classe/metodo).

  Todos os inteiros multi-byte no protocolo trafegam em big-endian (network
  byte order). Ver AMQP.Wire para o encode/decode. }

interface

const
  /// Cabecalho de protocolo enviado pelo cliente logo apos abrir o socket:
  /// os 4 octetos ASCII "AMQP" seguidos de major=0, minor=9, revision=1.
  /// (o 5o octeto e' o protocol-id, 0 para AMQP; depois vem 0,9,1).
  AMQP_PROTOCOL_HEADER: array[0..7] of Byte =
    (Ord('A'), Ord('M'), Ord('Q'), Ord('P'), 0, 0, 9, 1);

  // --- Tipos de frame (octeto 0 do frame) ---------------------------------
  AMQP_FRAME_METHOD    = 1;
  AMQP_FRAME_HEADER    = 2;
  AMQP_FRAME_BODY      = 3;
  AMQP_FRAME_HEARTBEAT = 8;

  /// Octeto que encerra todo frame (0xCE). Se o byte lido nesta posicao for
  /// diferente disto, o stream esta dessincronizado / corrompido.
  AMQP_FRAME_END = $CE;

  /// Tamanho minimo de frame que todo peer deve aceitar (spec 4.2.5).
  /// Usado como piso ao negociar frame-max no handshake.
  AMQP_FRAME_MIN_SIZE = 4096;

  /// Canal 0 e' reservado para metodos de nivel de conexao (Connection.*).
  AMQP_CHANNEL_CONNECTION = 0;

  // --- IDs de classe ------------------------------------------------------
  AMQP_CLASS_CONNECTION = 10;
  AMQP_CLASS_CHANNEL    = 20;
  AMQP_CLASS_EXCHANGE   = 40;
  AMQP_CLASS_QUEUE      = 50;
  AMQP_CLASS_BASIC      = 60;
  /// Extensao 'confirm' da RabbitMQ (publisher confirms). Nao faz parte do
  /// AMQP 0-9-1 base, mas e' amplamente suportada; ver AMQP.Basic.Methods.
  AMQP_CLASS_CONFIRM    = 85;
  AMQP_CLASS_TX         = 90;

  // --- Metodos de Connection (classe 10) ----------------------------------
  AMQP_CONNECTION_START    = 10;
  AMQP_CONNECTION_START_OK  = 11;
  AMQP_CONNECTION_SECURE   = 20;
  AMQP_CONNECTION_SECURE_OK = 21;
  AMQP_CONNECTION_TUNE     = 30;
  AMQP_CONNECTION_TUNE_OK   = 31;
  AMQP_CONNECTION_OPEN     = 40;
  AMQP_CONNECTION_OPEN_OK   = 41;
  AMQP_CONNECTION_CLOSE    = 50;
  AMQP_CONNECTION_CLOSE_OK  = 51;
  /// Extensao RabbitMQ: o broker avisa (canal 0) quando entra/sai de um
  /// resource alarm (memoria/disco) e para/volta a aceitar publishes. So sao
  /// enviados se o cliente anunciou a capability 'connection.blocked'.
  AMQP_CONNECTION_BLOCKED   = 60;
  AMQP_CONNECTION_UNBLOCKED = 61;

  // --- Metodos de Channel (classe 20) -------------------------------------
  AMQP_CHANNEL_OPEN     = 10;
  AMQP_CHANNEL_OPEN_OK   = 11;
  AMQP_CHANNEL_FLOW     = 20;
  AMQP_CHANNEL_FLOW_OK   = 21;
  AMQP_CHANNEL_CLOSE    = 40;
  AMQP_CHANNEL_CLOSE_OK  = 41;

  // --- Metodos de Exchange (classe 40) ------------------------------------
  AMQP_EXCHANGE_DECLARE    = 10;
  AMQP_EXCHANGE_DECLARE_OK  = 11;
  AMQP_EXCHANGE_DELETE     = 20;
  AMQP_EXCHANGE_DELETE_OK   = 21;
  /// Binding exchange->exchange (extensao RabbitMQ 'exchange_exchange_bindings',
  /// nao faz parte do 0-9-1 core). Nota: unbind-ok e' 51 (nao 41), por
  /// peculiaridade da spec estendida da RabbitMQ.
  AMQP_EXCHANGE_BIND       = 30;
  AMQP_EXCHANGE_BIND_OK     = 31;
  AMQP_EXCHANGE_UNBIND     = 40;
  AMQP_EXCHANGE_UNBIND_OK   = 51;

  // --- Metodos de Queue (classe 50) ---------------------------------------
  AMQP_QUEUE_DECLARE    = 10;
  AMQP_QUEUE_DECLARE_OK  = 11;
  AMQP_QUEUE_BIND       = 20;
  AMQP_QUEUE_BIND_OK     = 21;
  AMQP_QUEUE_UNBIND     = 50;
  AMQP_QUEUE_UNBIND_OK   = 51;
  AMQP_QUEUE_PURGE      = 30;
  AMQP_QUEUE_PURGE_OK    = 31;
  AMQP_QUEUE_DELETE     = 40;
  AMQP_QUEUE_DELETE_OK   = 41;

  // --- Metodos de Basic (classe 60) ---------------------------------------
  AMQP_BASIC_QOS        = 10;
  AMQP_BASIC_QOS_OK      = 11;
  AMQP_BASIC_CONSUME    = 20;
  AMQP_BASIC_CONSUME_OK  = 21;
  AMQP_BASIC_CANCEL     = 30;
  AMQP_BASIC_CANCEL_OK   = 31;
  AMQP_BASIC_PUBLISH    = 40;
  AMQP_BASIC_RETURN     = 50;
  AMQP_BASIC_DELIVER    = 60;
  AMQP_BASIC_GET        = 70;
  AMQP_BASIC_GET_OK      = 71;
  AMQP_BASIC_GET_EMPTY   = 72;
  AMQP_BASIC_ACK        = 80;
  AMQP_BASIC_REJECT     = 90;
  AMQP_BASIC_NACK       = 120;

  // --- Metodos de Confirm (classe 85, extensao RabbitMQ) ------------------
  AMQP_CONFIRM_SELECT    = 10;
  AMQP_CONFIRM_SELECT_OK = 11;

implementation

end.

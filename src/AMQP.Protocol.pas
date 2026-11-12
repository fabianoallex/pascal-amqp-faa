unit AMQP.Protocol;

{$I amqp.inc}

{ AMQP 0-9-1 protocol constants.

  Source: the public AMQP 0-9-1 specification (protocol document). Nothing here
  derives from third-party code — these are only the numeric values defined by the
  specification (frame types, frame-end octet, class/method IDs).

  Every multi-byte integer in the protocol travels big-endian (network
  byte order). See AMQP.Wire for encode/decode. }

interface

const
  /// Protocol header sent by the client right after opening the socket:
  /// the 4 ASCII octets "AMQP" followed by major=0, minor=9, revision=1.
  /// (the 5th octet is the protocol-id, 0 for AMQP; then comes 0,9,1).
  AMQP_PROTOCOL_HEADER: array[0..7] of Byte =
    (Ord('A'), Ord('M'), Ord('Q'), Ord('P'), 0, 0, 9, 1);

  // --- Frame types (octet 0 of the frame) ---------------------------------
  AMQP_FRAME_METHOD    = 1;
  AMQP_FRAME_HEADER    = 2;
  AMQP_FRAME_BODY      = 3;
  AMQP_FRAME_HEARTBEAT = 8;

  /// Octet that ends every frame (0xCE). If the byte read at this position is
  /// anything else, the stream is out of sync / corrupted.
  AMQP_FRAME_END = $CE;

  /// Minimum frame size every peer must accept (spec 4.2.5).
  /// Used as the floor when negotiating frame-max in the handshake.
  AMQP_FRAME_MIN_SIZE = 4096;

  /// Channel 0 is reserved for connection-level methods (Connection.*).
  AMQP_CHANNEL_CONNECTION = 0;

  // --- Class IDs ----------------------------------------------------------
  AMQP_CLASS_CONNECTION = 10;
  AMQP_CLASS_CHANNEL    = 20;
  AMQP_CLASS_EXCHANGE   = 40;
  AMQP_CLASS_QUEUE      = 50;
  AMQP_CLASS_BASIC      = 60;
  /// RabbitMQ 'confirm' extension (publisher confirms). Not part of base
  /// AMQP 0-9-1, but widely supported; see AMQP.Basic.Methods.
  AMQP_CLASS_CONFIRM    = 85;
  AMQP_CLASS_TX         = 90;

  // --- Connection methods (class 10) --------------------------------------
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
  /// RabbitMQ extension: the broker notifies (channel 0) when it enters/leaves a
  /// resource alarm (memory/disk) and stops/resumes accepting publishes. Only
  /// sent if the client announced the 'connection.blocked' capability.
  AMQP_CONNECTION_BLOCKED   = 60;
  AMQP_CONNECTION_UNBLOCKED = 61;

  // --- Channel methods (class 20) -----------------------------------------
  AMQP_CHANNEL_OPEN     = 10;
  AMQP_CHANNEL_OPEN_OK   = 11;
  AMQP_CHANNEL_FLOW     = 20;
  AMQP_CHANNEL_FLOW_OK   = 21;
  AMQP_CHANNEL_CLOSE    = 40;
  AMQP_CHANNEL_CLOSE_OK  = 41;

  // --- Exchange methods (class 40) ----------------------------------------
  AMQP_EXCHANGE_DECLARE    = 10;
  AMQP_EXCHANGE_DECLARE_OK  = 11;
  AMQP_EXCHANGE_DELETE     = 20;
  AMQP_EXCHANGE_DELETE_OK   = 21;
  /// Exchange->exchange binding (RabbitMQ extension 'exchange_exchange_bindings',
  /// not part of core 0-9-1). Note: unbind-ok is 51 (not 41), a
  /// quirk of RabbitMQ's extended spec.
  AMQP_EXCHANGE_BIND       = 30;
  AMQP_EXCHANGE_BIND_OK     = 31;
  AMQP_EXCHANGE_UNBIND     = 40;
  AMQP_EXCHANGE_UNBIND_OK   = 51;

  // --- Queue methods (class 50) -------------------------------------------
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

  // --- Basic methods (class 60) -------------------------------------------
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

  // --- Confirm methods (class 85, RabbitMQ extension) ---------------------
  AMQP_CONFIRM_SELECT    = 10;
  AMQP_CONFIRM_SELECT_OK = 11;

  // --- Reply codes (spec 0-9-1, "constants" section) ----------------------
  // These are the codes that go in the reply-code of Connection.Close / Channel.Close.
  // The spec classifies each one as a "channel error" (closes only the channel) or
  // a "connection error" (closes the whole connection) — noted alongside.
  AMQP_REPLY_SUCCESS       = 200; // normal reply (Close requested by the peer)
  AMQP_CONTENT_TOO_LARGE   = 311; // channel
  AMQP_NO_ROUTE            = 312; // channel
  AMQP_NO_CONSUMERS        = 313; // channel
  AMQP_CONNECTION_FORCED   = 320; // connection (operator dropped it)
  AMQP_INVALID_PATH        = 402; // connection (invalid vhost/path)
  AMQP_ACCESS_REFUSED      = 403; // channel (also used for the auth 403)
  AMQP_NOT_FOUND           = 404; // channel
  AMQP_RESOURCE_LOCKED     = 405; // channel
  AMQP_PRECONDITION_FAILED = 406; // channel
  AMQP_FRAME_ERROR         = 501; // connection (malformed/out-of-sync frame)
  AMQP_SYNTAX_ERROR        = 502; // connection
  AMQP_COMMAND_INVALID     = 503; // connection (method invalid in this state)
  AMQP_CHANNEL_ERROR       = 504; // connection (channel nonexistent/already open)
  AMQP_UNEXPECTED_FRAME    = 505; // connection (content out of sequence)
  AMQP_RESOURCE_ERROR      = 506; // connection
  AMQP_NOT_ALLOWED         = 530; // connection (vhost denied, tune out of range)
  AMQP_NOT_IMPLEMENTED     = 540; // connection/channel (method not implemented)
  AMQP_INTERNAL_ERROR      = 541; // connection

implementation

end.

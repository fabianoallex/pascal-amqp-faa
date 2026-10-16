unit AMQP.TimeoutIntegrationTests;

{ Prazos da abertura (ConnectTimeoutMs / HandshakeTimeoutMs). Três testes não
  precisam de broker (peer mudo local, porta fechada, endereço que não
  responde); o último abre uma conexão real e confirma que o prazo do
  handshake não sobra para a conexão aberta.

  - Um peer que aceita o TCP e não fala AMQP prendia o Open para sempre: agora
    levanta EAMQPConnection ('handshake timeout') no prazo.
  - Com ConnectTimeoutMs, uma porta fechada levanta EAMQPTransport (a RTL do
    Delphi, com prazo, deixava a conexão recusada "conectada").
  - Depois do handshake a leitura volta a ser sem prazo: uma conexão ociosa
    por mais tempo que HandshakeTimeoutMs continua aberta.

  Espelho DUnitX em tests\Integration\AMQP.TimeoutIntegrationTests.pas:
  os corpos dos testes são idênticos; só as pontes Checa*, o uses e o
  registro mudam de lado. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  PascalCommon.Threading,
  AMQP.Transport,
  AMQP.Connection,
  AMQP.IntegrationConfig;

type
  TAMQPTimeoutIntegrationTests = class(TTestCase)
  private
    function AbreEsperandoErro(const AParams: TAMQPConnectionParams;
      out AClasse, AMensagem: string; out AMs: UInt64): Boolean;
  published
    procedure PeerMudo_HandshakeTimeout_OpenLevantaNoPrazo;
    procedure PortaFechada_ComConnectTimeout_LevantaEAMQPTransport;
    procedure EnderecoSemResposta_ConnectTimeout_LevantaNoPrazo;
    procedure HandshakeTimeout_NaoValeDepoisDoOpen;
  end;

implementation

// As pontes para as asserções do dialeto: só elas mudam entre os espelhos.
procedure ChecaOk(const AMsg: string; ACond: Boolean);
begin
  TAssert.AssertTrue(AMsg, ACond);
end;

procedure ChecaStr(const AMsg: string; const AEsperado, AObtido: string);
begin
  TAssert.AssertEquals(AMsg, AEsperado, AObtido);
end;

// Open que deve falhar: devolve a classe e a mensagem da exceção e quanto
// tempo o Open levou; False se o Open não levantou.
function TAMQPTimeoutIntegrationTests.AbreEsperandoErro(const AParams: TAMQPConnectionParams;
  out AClasse, AMensagem: string; out AMs: UInt64): Boolean;
var
  LConn: TAMQPConnection;
  LInicio: UInt64;
begin
  Result := False;
  AClasse := '';
  AMensagem := '';
  LConn := TAMQPConnection.Create(AParams);
  try
    LInicio := PcTickMs;
    try
      LConn.Open;
    except
      on E: Exception do
      begin
        Result := True;
        AClasse := E.ClassName;
        AMensagem := E.Message;
      end;
    end;
    AMs := PcTickMs - LInicio;
    ChecaOk('a conexão não pode ficar aberta', not LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TAMQPTimeoutIntegrationTests.PeerMudo_HandshakeTimeout_OpenLevantaNoPrazo;
var
  LListener: TAMQPTcpListener;
  LParams: TAMQPConnectionParams;
  LClasse, LMensagem: string;
  LMs: UInt64;
begin
  // Escuta sem nunca aceitar: o backlog do SO completa o TCP e ninguém fala.
  LListener := TAMQPTcpListener.Create;
  try
    LListener.Listen('127.0.0.1', 0);
    LParams := TAMQPConnectionParams.Localhost;
    LParams.Host := '127.0.0.1';
    LParams.Port := LListener.Port;
    LParams.ConnectTimeoutMs := 2000;
    LParams.HandshakeTimeoutMs := 500;
    ChecaOk('Open num peer mudo deveria levantar', AbreEsperandoErro(LParams, LClasse, LMensagem, LMs));
    ChecaStr('classe', 'EAMQPConnection', LClasse);
    ChecaOk('mensagem cita o prazo: ' + LMensagem, Pos('handshake timeout', LMensagem) > 0);
    ChecaOk('levanta no prazo (ms: ' + IntToStr(LMs) + ')', LMs < 5000);
  finally
    LListener.Close;
    LListener.Free;
  end;
end;

procedure TAMQPTimeoutIntegrationTests.PortaFechada_ComConnectTimeout_LevantaEAMQPTransport;
var
  LListener: TAMQPTcpListener;
  LParams: TAMQPConnectionParams;
  LPorta: Word;
  LClasse, LMensagem: string;
  LMs: UInt64;
begin
  // Uma porta que acabou de ser liberada: ninguém escuta nela.
  LListener := TAMQPTcpListener.Create;
  try
    LListener.Listen('127.0.0.1', 0);
    LPorta := LListener.Port;
    LListener.Close;
  finally
    LListener.Free;
  end;
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '127.0.0.1';
  LParams.Port := LPorta;
  LParams.ConnectTimeoutMs := 5000;
  ChecaOk('Open numa porta fechada deveria levantar', AbreEsperandoErro(LParams, LClasse, LMensagem, LMs));
  ChecaStr('classe (' + LMensagem + ')', 'EAMQPTransport', LClasse);
  ChecaOk('levanta no prazo (ms: ' + IntToStr(LMs) + ')', LMs < 8000);
end;

procedure TAMQPTimeoutIntegrationTests.EnderecoSemResposta_ConnectTimeout_LevantaNoPrazo;
var
  LParams: TAMQPConnectionParams;
  LClasse, LMensagem: string;
  LMs: UInt64;
begin
  // Endereço privado que não responde: o connect fica pendente até o prazo
  // (numa máquina sem rota para ele, o erro vem antes, o que também serve).
  LParams := TAMQPConnectionParams.Localhost;
  LParams.Host := '10.255.255.1';
  LParams.ConnectTimeoutMs := 500;
  ChecaOk('Open sem resposta deveria levantar', AbreEsperandoErro(LParams, LClasse, LMensagem, LMs));
  ChecaOk('levanta no prazo (ms: ' + IntToStr(LMs) + ', ' + LClasse + ': ' + LMensagem + ')', LMs < 5000);
end;

procedure TAMQPTimeoutIntegrationTests.HandshakeTimeout_NaoValeDepoisDoOpen;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LCh: TAMQPChannel;
begin
  LParams := IntegrationParams;
  LParams.HandshakeTimeoutMs := 300;
  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.Open;
    TThread.Sleep(1500); // ociosa por mais que o prazo do handshake
    ChecaOk('a conexão ociosa continua aberta', LConn.IsOpen);
    LCh := LConn.CreateChannel;
    try
      ChecaOk('o canal abre', LCh <> nil);
    finally
      LCh.Free;
    end;
  finally
    LConn.Free;
  end;
end;

initialization
  RegisterTest(TAMQPTimeoutIntegrationTests);

end.

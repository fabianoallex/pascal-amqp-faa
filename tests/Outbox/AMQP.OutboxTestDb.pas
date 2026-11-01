unit AMQP.OutboxTestDb;

{$I amqp.inc}

{ The database of the outbox bridge's tests (tests/Outbox): a SQLite file on
  pascal-db-faa, with the PROCESSED_EVENTS table and the reference statements
  of docs/outbox.md. Not generated: both runners compile this unit, so the
  adapter's switch (SQLdb on FPC, FireDAC on Delphi) lives here and never in
  a test body.

  FPC loads the SQLite client library: libsqlite3.so.0 on Unix; on Windows
  sqlite.org's sqlite3.dll found by the default search (next to the runner),
  or AMQP_SQLITE_CLIENT. FireDAC links SQLite into the program. }

interface

uses
  PascalDb.Interfaces;

type
  TOutboxTestDb = class
  private
    FFile: string;
    FFactory: IDBFactory;
  public
    /// A new database in a file of its own in the temporary directory, with
    /// the table created.
    constructor Create;
    /// Releases the factory (its pool and connections) and deletes the file.
    destructor Destroy; override;
    procedure Exec(const ASql: string);
    /// SELECT COUNT(*) FROM AFrom.
    function Count(const AFrom: string): Integer;
    /// SELECT AColumn FROM AFrom, the first row, as Int64.
    function Int64Of(const AColumn, AFrom: string): Int64;
    property Factory: IDBFactory read FFactory;
  end;

implementation

uses
  SysUtils,
  Classes,
  PascalCommon.Crypto,
  PascalCommon.Random,
  PascalDb.Adapter.Base,
  PascalDb.SqlSources,
  {$IFDEF FPC}
  PascalDb.Adapter.SQLdb,
  {$ELSE}
  PascalDb.Adapter.FireDAC,
  {$ENDIF}
  AMQP.TestSupport,
  AMQP.Outbox;

const
  SQL_DIR = 'COMMON';

// docs/outbox.md's table and statements (the SQLite and PostgreSQL forms).
function OutboxSqlSource: ISqlSource;
var
  LSource: TMemorySqlSource;
begin
  LSource := TMemorySqlSource.Create;
  Result := LSource;
  LSource
    .Add(SQL_DIR, PROCESSED_SQL_EXISTS,
      'SELECT 1 AS FOUND FROM PROCESSED_EVENTS WHERE CONSUMER = :CONSUMER ' +
      'AND TENANT_ID = :TENANT_ID AND EVENT_ID = :EVENT_ID')
    .Add(SQL_DIR, PROCESSED_SQL_INSERT,
      'INSERT INTO PROCESSED_EVENTS (CONSUMER, TENANT_ID, EVENT_ID, PROCESSED_AT) ' +
      'VALUES (:CONSUMER, :TENANT_ID, :EVENT_ID, :NOW) ' +
      'ON CONFLICT (CONSUMER, TENANT_ID, EVENT_ID) DO NOTHING')
    .Add(SQL_DIR, PROCESSED_SQL_PURGE,
      'DELETE FROM PROCESSED_EVENTS WHERE PROCESSED_AT < :BEFORE');
end;

procedure SetSQLiteParams(AParams: TStrings; const AFile: string);
begin
  {$IFDEF FPC}
  AParams.Values['ConnectorType'] := 'SQLite3';
  AParams.Values['DatabaseName'] := AFile;
  {$IFDEF UNIX}
  AParams.Values['ClientLibrary'] := GetEnvironmentVariable('AMQP_SQLITE_CLIENT');
  if AParams.Values['ClientLibrary'] = '' then
    AParams.Values['ClientLibrary'] := 'libsqlite3.so.0';
  {$ELSE}
  AParams.Values['ClientLibrary'] := GetEnvironmentVariable('AMQP_SQLITE_CLIENT');
  {$ENDIF}
  {$ELSE}
  AParams.Values['DriverID'] := 'SQLite';
  AParams.Values['Database'] := AFile;
  {$ENDIF}
end;

{ TOutboxTestDb }

constructor TOutboxTestDb.Create;
var
  LConfig: IDatabaseConfig; // an interface variable (pascal-db-faa's rule)
begin
  inherited Create;
  FFile := IncludeTrailingPathDelimiter(DirTemporario) + 'amqp-outbox-' +
    LowerCase(PcBytesToHex(PcRandomBytes(6))) + '.sqlite';
  LConfig := TDatabaseConfig.Create;
  SetSQLiteParams(LConfig.ConnectionParams, FFile);
  LConfig.SQLDialect := 'SQLite';
  LConfig.SQLDirectory := SQL_DIR;
  LConfig.SqlSource := OutboxSqlSource;
  LConfig.PoolName := 'amqp-outbox-tests';
  LConfig.PoolIniConnections := 1;
  LConfig.PoolMaxConnections := 10;
  LConfig.PoolWaitMaxAttemps := 50;
  LConfig.PoolWaitMilliseconds := 100;
  {$IFDEF FPC}
  FFactory := TSQLdbFactory.Create(LConfig);
  {$ELSE}
  FFactory := TFDFactory.Create(LConfig);
  {$ENDIF}
  Exec('CREATE TABLE PROCESSED_EVENTS (' +
    'CONSUMER VARCHAR(60) NOT NULL, ' +
    'TENANT_ID VARCHAR(40) NOT NULL, ' +
    'EVENT_ID VARCHAR(32) NOT NULL, ' +
    'PROCESSED_AT BIGINT NOT NULL, ' +
    'CONSTRAINT PK_PROCESSED_EVENTS PRIMARY KEY (CONSUMER, TENANT_ID, EVENT_ID))');
  Exec('CREATE INDEX IX_PROCESSED_EVENTS_AT ON PROCESSED_EVENTS (PROCESSED_AT)');
end;

destructor TOutboxTestDb.Destroy;
begin
  FFactory := nil;
  if FFile <> '' then
    SysUtils.DeleteFile(FFile);
  inherited;
end;

procedure TOutboxTestDb.Exec(const ASql: string);
var
  LQuery: IQuery;
  LTx: IScopeTransaction;
begin
  LTx := FFactory.GetPool.AcquireQuery(LQuery);
  LTx.StartTransaction;
  try
    LQuery.Sql := ASql;
    LQuery.ExecSql;
    LTx.Commit;
  except
    LTx.Rollback;
    raise;
  end;
end;

function TOutboxTestDb.Count(const AFrom: string): Integer;
var
  LQuery: IQuery;
  LTx: IScopeTransaction;
begin
  LTx := FFactory.GetPool.AcquireQuery(LQuery);
  LQuery.Sql := 'SELECT COUNT(*) AS N FROM ' + AFrom;
  Result := LQuery.Open.Integers['N'];
end;

function TOutboxTestDb.Int64Of(const AColumn, AFrom: string): Int64;
var
  LQuery: IQuery;
  LTx: IScopeTransaction;
begin
  LTx := FFactory.GetPool.AcquireQuery(LQuery);
  LQuery.Sql := 'SELECT ' + AColumn + ' AS V FROM ' + AFrom;
  Result := LQuery.Open.Int64s['V'];
end;

end.

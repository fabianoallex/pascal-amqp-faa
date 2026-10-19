unit AMQP.DUnitXCompat;

{ Thin adapter: exposes FPCUnit's assertion API (TAssert.AssertEquals,
  AssertTrue, AssertFalse, Fail) on top of DUnitX's Assert. From the
  dual-compiler-delphi-lazarus skill's assets/DUnitXCompat.pas.template:
  only the unit name and this comment are the project's; everything from
  "interface" down is the template's (scripts/check_test_kit.py compares it).

  Tests are written ONCE, in FPCUnit's dialect, in the DUnitX masters
  (tests/Unit/*Tests.pas); the FPCUnit mirror (tests/Unit/fpc) is generated
  by tools/gen_fpc_mirror.py, swapping only the fixture declarations. This
  adapter is what lets the master compile on Delphi.

  The overload set mirrors FPCUnit 3.2.2's TAssert, including what it does
  NOT have: there is no delta-less AssertEquals(Double, Double). Such an
  overload here would let the master compile on Delphi comparing floating
  point one way, while on FPC the same line falls into the Currency overload
  (4 decimal places) and compares another way: that is how a TDateTime test
  passed on FPC at Currency precision (pascal-db-faa). Floating point always
  takes an explicit delta (0 = exact).

  Text comparison is always case-sensitive, like FPCUnit's: DUnitX's
  Assert.AreEqual(string, string) ignores case BY DEFAULT. }

interface

uses
  DUnitX.TestFramework;

type
  TAssert = class
  public
    class procedure AssertEquals(const AExpected, AActual: string); overload;
    class procedure AssertEquals(const AMessage, AExpected, AActual: string); overload;
    class procedure AssertEquals(AExpected, AActual: Integer); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual: Integer); overload;
    class procedure AssertEquals(AExpected, AActual: Int64); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual: Int64); overload;
    class procedure AssertEquals(AExpected, AActual: Currency); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual: Currency); overload;
    class procedure AssertEquals(AExpected, AActual, ADelta: Double); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual, ADelta: Double); overload;
    class procedure AssertEquals(AExpected, AActual: Boolean); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual: Boolean); overload;

    class procedure AssertTrue(ACondition: Boolean); overload;
    class procedure AssertTrue(const AMessage: string; ACondition: Boolean); overload;
    class procedure AssertFalse(ACondition: Boolean); overload;
    class procedure AssertFalse(const AMessage: string; ACondition: Boolean); overload;

    class procedure Fail(const AMessage: string);
  end;

implementation

class procedure TAssert.AssertEquals(const AExpected, AActual: string);
begin
  // False = case-sensitive. DUnitX's default would be True.
  Assert.AreEqual(AExpected, AActual, False);
end;

class procedure TAssert.AssertEquals(const AMessage, AExpected, AActual: string);
begin
  Assert.AreEqual(AExpected, AActual, False, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual: Integer);
begin
  Assert.AreEqual(AExpected, AActual);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual: Integer);
begin
  Assert.AreEqual(AExpected, AActual, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual: Int64);
begin
  Assert.AreEqual(AExpected, AActual);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual: Int64);
begin
  Assert.AreEqual(AExpected, AActual, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual: Currency);
begin
  Assert.AreEqual(AExpected, AActual);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual: Currency);
begin
  Assert.AreEqual(AExpected, AActual, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual, ADelta: Double);
begin
  Assert.AreEqual(AExpected, AActual, ADelta);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual, ADelta: Double);
begin
  Assert.AreEqual(AExpected, AActual, ADelta, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual: Boolean);
begin
  Assert.AreEqual(AExpected, AActual);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual: Boolean);
begin
  Assert.AreEqual(AExpected, AActual, AMessage);
end;

class procedure TAssert.AssertTrue(ACondition: Boolean);
begin
  Assert.IsTrue(ACondition);
end;

class procedure TAssert.AssertTrue(const AMessage: string; ACondition: Boolean);
begin
  Assert.IsTrue(ACondition, AMessage);
end;

class procedure TAssert.AssertFalse(ACondition: Boolean);
begin
  Assert.IsFalse(ACondition);
end;

class procedure TAssert.AssertFalse(const AMessage: string; ACondition: Boolean);
begin
  Assert.IsFalse(ACondition, AMessage);
end;

class procedure TAssert.Fail(const AMessage: string);
begin
  Assert.Fail(AMessage);
end;

end.

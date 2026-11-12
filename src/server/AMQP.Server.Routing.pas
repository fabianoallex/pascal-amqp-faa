unit AMQP.Server.Routing;

{$I amqp.inc}

{ Pure routing matchers -- broker sub-module, WS2 of Phase 2 (in-memory
  routing engine, see CLAUDE.md).

  No function here touches topology (that is AMQP.Server.VHost, which uses
  these): they only decide, given a binding and a message, whether they match. The
  acceptance vectors that guide this unit were written BEFORE the code and are
  normative -- see the vectors file cited in the WS2 plan. Where this
  code diverges from the vectors, the vectors win.

  Performance requirement: matching runs on the hot publish path, on the
  PUBLISHER thread (see the "new invariant" in CLAUDE.md) -- that is why
  AmqpTopicMatches walks the strings by INDICES (no Copy, no building a
  word list, no regex): each "word" is just a (start,
  length) pair inside the original string. }

interface

uses
  SysUtils,
  Rtti,
  AMQP.Wire,
  AMQP.Server.Resources; // AmqpValuesEqual (scalar comparer exported by WS1)

type
  { Match mode of a headers-exchange binding (spec: the 'x-match'
    property of the bind arguments). Default (absent) is amqhmAll. }
  TAMQPHeadersMatch = (amqhmAll, amqhmAny);

/// True if ARoutingKey matches ABindingKey by the topic exchange rules
/// (spec 3.1.3.3): '*' matches EXACTLY one word (including the empty
/// word); '#' matches ZERO or more words, in any position, as many times as it
/// appears; any other word must match literally, byte by byte
/// (case-sensitive). Words are separated by '.'; an empty key ("") has
/// ZERO words, not one empty word -- that is the definition locked in the acceptance
/// vector (a rule specific to this project where the spec is silent).
///
/// Implementation: recursive backtracking over INDICES in both strings (see
/// TryGetWord in the implementation) -- no allocation per call (no Copy,
/// no TStringList, no regex). The 10k-match test in the suite covers this.
function AmqpTopicMatches(const ABindingKey, ARoutingKey: string): Boolean;

/// True if ARoutingKey is EXACTLY equal to ABindingKey (direct exchange,
/// spec 3.1.3.1) -- byte by byte, case-sensitive. It exists as its own function
/// (instead of just using "=" directly in the caller) so the dispatch by exchange
/// type in AMQP.Server.VHost.Route stays uniform across the four types.
function AmqpDirectMatches(const ABindingKey, ARoutingKey: string): Boolean;

/// Reads the 'x-match' property of AArgs (arguments of a Queue.Bind or
/// Exchange.Bind on a headers exchange) and returns the mode in AMode.
/// - AArgs = nil OR without the 'x-match' key: AMode = amqhmAll (spec default).
/// - 'x-match' = 'all' or 'any': AMode filled in, returns True.
/// - any other value: returns False -- the BIND is invalid (WS5 maps
///   this to 406 PRECONDITION_FAILED on the Exchange.Bind/Queue.Bind itself,
///   it does NOT silently fall back to 'all').
function AmqpParseHeadersMatch(AArgs: TAMQPFieldTable;
  out AMode: TAMQPHeadersMatch): Boolean;

/// True if AMessageHeaders matches the criteria of ABindingArgs (headers
/// exchange, spec 3.1.3.4), in mode AMode:
/// - amqhmAll: EVERY criterion (key that does NOT start with 'x-') must exist
///   in AMessageHeaders with an EQUAL value (AmqpValuesEqual: type and value).
///   Zero criteria => True (vacuous truth -- "all of an empty set").
/// - amqhmAny: ONE matching criterion is ENOUGH. Zero criteria => False (vacuous
///   falsehood -- "some element of an empty set exists" is always false).
/// Binding keys that start with 'x-' (including 'x-match', which is
/// precisely the mode, not a criterion) are ignored in the comparison.
/// ABindingArgs = nil is equivalent to "no criteria". AMessageHeaders = nil
/// is equivalent to "message without headers" (empty table).
function AmqpHeadersMatch(ABindingArgs, AMessageHeaders: TAMQPFieldTable;
  AMode: TAMQPHeadersMatch): Boolean;

implementation

{ ---- Topic matcher: walking by indices, without allocating ---- }

// Extracts the NEXT word of string S starting at cursor APos (1-based),
// advancing APos to the start of the following word (or to an "exhausted"
// state after the last one). Cursor convention, see the comment on
// AmqpTopicMatches: a string of length ALen=0 has ZERO words (rule
// locked in the acceptance vector) -- TryGetWord returns False right away in that
// case, even on the first call. For ALen>0, every word -- including the
// empty one after a trailing '.' or between two consecutive '.' -- is emitted.
//
// Allocates nothing: it only moves the AStart/AWordLen index over the original string.
function TryGetWord(const S: string; var APos: Integer; ALen: Integer;
  out AStart, AWordLen: Integer): Boolean;
begin
  if (ALen = 0) or (APos > ALen + 1) then
    Exit(False); // empty original string (zero words) OR exhausted cursor

  AStart := APos;
  AWordLen := 0;
  while (APos <= ALen) and (S[APos] <> '.') do
  begin
    Inc(APos);
    Inc(AWordLen);
  end;

  if APos <= ALen then
    Inc(APos) // skips the '.' -- the next call starts at the following word
  else
    Inc(APos); // ALen+1 -> ALen+2: "done" sentinel for the next call

  Result := True;
end;

// Compares two character ranges (one "word" from each string) byte by
// byte, without materializing any substring.
function WordsEqual(const S1: string; AStart1, ALen1: Integer;
  const S2: string; AStart2, ALen2: Integer): Boolean;
var
  I: Integer;
begin
  if ALen1 <> ALen2 then
    Exit(False);
  for I := 0 to ALen1 - 1 do
    if S1[AStart1 + I] <> S2[AStart2 + I] then
      Exit(False);
  Result := True;
end;

// Core of topic matching. Iterative on purpose, with backtracking
// ONLY to the last '#' seen -- the classic wildcard matching algorithm
// (the same as glob, where '#' plays the role of '*' and '*' that of '?').
//
// WHY NOT RECURSION: the recursive version "try 0, 1, 2... words for
// each '#'" is exponential in the number of '#'. Measured on this machine, with a
// 24-word routing key: 5 '#' = 0 ms, 6 = 15 ms, 7 = 110 ms, 8 = 453 ms.
// A binding key holds 255 octets, that is, more than 100 '#' -- a client
// could declare 'hash.hash....zz' and make EVERY publish to that exchange
// cost seconds, on the publisher thread and holding the read RWLock.
// Denial of service with a one-line declare. Here the cost is O(n*m) in the worst
// case and O(n) in the common case, with no recursion and no allocation.
//
// The key to the algorithm: remembering the LAST '#' is enough. If the rest of the pattern
// fails further on, the only useful thing to try is letting that '#' swallow
// one more word -- resuming from an earlier '#' never opens a possibility that
// this resumption does not already cover.
function AmqpTopicMatches(const ABindingKey, ARoutingKey: string): Boolean;
var
  LBLen, LRLen: Integer;
  LBPos, LRPos: Integer;           // current cursors (binding and routing)
  LStarBPos, LStarRPos: Integer;   // resume point of the last '#'; -1 = none yet
  LNextB, LNextR: Integer;         // trial cursors (TryGetWord advances)
  LBStart, LBWordLen: Integer;
  LRStart, LRWordLen: Integer;
  LTemB: Boolean;
begin
  LBLen := Length(ABindingKey);
  LRLen := Length(ARoutingKey);
  LBPos := 1;
  LRPos := 1;
  LStarBPos := -1;
  LStarRPos := -1;

  // While there is a routing word to match.
  while True do
  begin
    LNextR := LRPos;
    if not TryGetWord(ARoutingKey, LNextR, LRLen, LRStart, LRWordLen) then
      Break; // routing exhausted -- decide on the tail, after the loop

    LNextB := LBPos;
    LTemB := TryGetWord(ABindingKey, LNextB, LBLen, LBStart, LBWordLen);

    // '#': for now it consumes ZERO words; remember where to resume if it fails.
    if LTemB and (LBWordLen = 1) and (ABindingKey[LBStart] = '#') then
    begin
      LStarBPos := LNextB;  // binding right after the '#'
      LStarRPos := LRPos;   // routing where the '#' started
      LBPos := LNextB;
      Continue;
    end;

    // '*' matches exactly one word (including empty); a literal matches byte by
    // byte. In both cases, it consumes one word from each side.
    if LTemB and (((LBWordLen = 1) and (ABindingKey[LBStart] = '*')) or
       WordsEqual(ABindingKey, LBStart, LBWordLen,
                  ARoutingKey, LRStart, LRWordLen)) then
    begin
      LBPos := LNextB;
      LRPos := LNextR;
      Continue;
    end;

    // No match here. If there is a pending '#', let it swallow one more
    // word and resume from there; otherwise, it is over.
    if LStarBPos < 0 then
      Exit(False);

    LNextR := LStarRPos;
    if not TryGetWord(ARoutingKey, LNextR, LRLen, LRStart, LRWordLen) then
      Exit(False); // no more words for the '#' to swallow
    LStarRPos := LNextR;
    LRPos := LNextR;
    LBPos := LStarBPos;
  end;

  // Routing exhausted: what is left of the binding can only be '#' (which matches zero
  // words). Any pending literal or '*' means a missing word.
  while True do
  begin
    LNextB := LBPos;
    if not TryGetWord(ABindingKey, LNextB, LBLen, LBStart, LBWordLen) then
      Exit(True);
    if (LBWordLen <> 1) or (ABindingKey[LBStart] <> '#') then
      Exit(False);
    LBPos := LNextB;
  end;
end;

{ ---- Matcher direct ---- }

function AmqpDirectMatches(const ABindingKey, ARoutingKey: string): Boolean;
begin
  Result := ABindingKey = ARoutingKey;
end;

{ ---- Matcher headers ---- }

function AmqpIsXKey(const AKey: string): Boolean;
begin
  Result := (Length(AKey) >= 2) and (AKey[1] = 'x') and (AKey[2] = '-');
end;

function AmqpParseHeadersMatch(AArgs: TAMQPFieldTable;
  out AMode: TAMQPHeadersMatch): Boolean;
var
  LValue: TValue;
  LMatch: string;
begin
  AMode := amqhmAll; // spec default when 'x-match' is absent
  if (AArgs = nil) or (not AArgs.TryGetValue('x-match', LValue)) then
    Exit(True);

  LMatch := LValue.AsString;
  if LMatch = 'all' then
  begin
    AMode := amqhmAll;
    Exit(True);
  end;
  if LMatch = 'any' then
  begin
    AMode := amqhmAny;
    Exit(True);
  end;

  Result := False; // unknown 'x-match' value -- invalid bind (406)
end;

function AmqpHeadersMatch(ABindingArgs, AMessageHeaders: TAMQPFieldTable;
  AMode: TAMQPHeadersMatch): Boolean;
var
  LKey: string;
  LBindVal, LMsgVal: TValue;
  LMatched: Boolean;
begin
  if ABindingArgs <> nil then
    for LKey in ABindingArgs.Keys do
    begin
      if AmqpIsXKey(LKey) then
        Continue; // 'x-match' and any other 'x-' key are not criteria

      LBindVal := ABindingArgs[LKey]; // indexer -> local before use (FPC gotcha)
      LMatched := (AMessageHeaders <> nil) and
        AMessageHeaders.TryGetValue(LKey, LMsgVal) and
        AmqpValuesEqual(LBindVal, LMsgVal);

      case AMode of
        amqhmAll:
          if not LMatched then
            Exit(False); // one criterion failed -> "all" can no longer match
        amqhmAny:
          if LMatched then
            Exit(True); // one criterion matched -> "any" is already enough
      end;
    end;

  // The loop ended without deciding by short-circuit:
  // - amqhmAll: no criterion failed (including the zero-criteria case --
  //   vacuous truth, vector #8);
  // - amqhmAny: no criterion matched (including zero criteria -- vacuous
  //   falsehood, vector #9).
  case AMode of
    amqhmAll: Result := True;
    amqhmAny: Result := False;
  else
    Result := False;
  end;
end;

end.

grammar LogicalTypes;

// ─── parser rules ─────────────────────────────────────────────────────────────

script
    : ( declareNamespaceStmt ';' )?
      ( referenceTypeStmt ';' )*
      ( createTypeStmt ( ';' createTypeStmt )* ';'? )?
      ( registerTypeStmt ';'? )?
      EOF
    ;

// ─── namespace declaration ────────────────────────────────────────────────────

declareNamespaceStmt
    : NAMESPACE qualifiedName
    ;

// ─── external type declarations ──────────────────────────────────────────────
//
// Optional `AS SYNONYM FOR <stringLiteral>` carries a synthetic-wrapper URI
// binding for the named external — used when the source isn't addressable by
// FQN alone (whole-doc JSON refs, arbitrary JSON-Pointer refs, etc.). Visitor
// records (qualifiedName -> URI) in the LT's externalImports; writers emit the
// URI as the reference target. Without `AS SYNONYM FOR`, the external is
// canonical and the writer discovers its source by walking resolvedReferences.
//
// `AS SYNONYM FOR` controls only the WIRE-FORMAT shape of the reference (the
// $ref URI in JSON, the import string in Proto). It deliberately does NOT
// carry SR coordinates (subject + version) — those are deployment metadata
// that vary across environments and aren't expressible in DDL. To produce an
// SR-publishable LT from DDL, the caller attaches a SchemaReference list
// (with matching resolvedReferences content) to the LogicalType separately
// after the visitor runs.

referenceTypeStmt
    : REFERENCE TYPE qualifiedName ( AS SYNONYM FOR stringLiteral )?
    ;

// ─── named type declarations ──────────────────────────────────────────────────
//
// Two kind-specific shapes — `ROW <name> (...)` for record-like types and
// `ENUM <name> (...)` for enums. The leading kind keyword tells the reader
// (and the parser) what's coming without burying the kind in the body. Inline
// uses of ROW (as a type expression at field/branch positions) are unchanged;
// the parser disambiguates declaration vs. inline by structural position in
// `script`.
//
// Naming convention for `qualifiedName` keys (see
// LogicalTypesSchemaVisitor#qualifyWithNamespace):
//   - No dots → unqualified, prefixed with the active NAMESPACE.
//   - Dots, and a dotted prefix matches a previously-declared local type →
//     NESTED under that local type. Example: with `ROW Outer (...)`
//     in scope, `ROW Outer.Inner (...)` declares Inner nested in Outer.
//   - Dots, no matching local-type prefix → namespace-qualified top-level
//     type (e.g., `ROW other.Bar (...)` declares Bar in namespace `other`).
//
// Parents must be declared before children (one-pass parse). Gaps in a
// nesting chain (`Outer` exists, `Outer.Mid` doesn't, `Outer.Mid.Inner`
// can't be declared) are rejected.
//
// Only the Protobuf writer acts on the nesting structurally — Avro and JSON
// emit dotted names as flat top-level types.

createTypeStmt
    : ROW  qualifiedName structBody commentClause? tagsClause? withClause?
    | ENUM qualifiedName enumBody   commentClause? withClause?
    ;

// ─── root registration ────────────────────────────────────────────────────────
//
// Trailing `TYPE <typeExpr>` (no body) names the root for SR publication when
// the single-root sugar can't infer it, or when the root needs a typeExpr
// shape that isn't a named row/enum (a primitive, ARRAY, anonymous ROW, etc.).

registerTypeStmt
    : TYPE typeExpr
    ;

// ─── struct body ──────────────────────────────────────────────────────────────

structBody
    : '(' structBodyItem ( ',' structBodyItem )* ')'
    ;

structBodyItem
    : fieldDef
    | tableConstraint
    ;

fieldDef
    : fieldName typeExpr defaultClause? columnConstraint* commentClause? tagsClause? withClause?
    ;

fieldName
    : identifier
    ;

nullability
    : NULL
    | NOT NULL
    ;

defaultClause
    : DEFAULT literal
    ;

commentClause
    : COMMENT stringLiteral
    | stringLiteral
    ;

tagsClause
    : TAGS '(' stringLiteral ( ',' stringLiteral )* ')'
    ;

withClause
    : WITH '(' withProperty ( ',' withProperty )* ')'
    ;

withProperty
    : stringLiteral '=' stringLiteral
    ;

// ─── CHECK constraints ────────────────────────────────────────────────────────
//
// CHECK constraints come in two placements:
//   - column-level (inside `fieldDef`, via `columnConstraint`): expression
//     refers only to the field it's attached to.
//   - table-level (inside `structBody` alongside `fieldDef`, via
//     `tableConstraint`): expression may refer to any field in the
//     surrounding struct.
//
// `columnConstraint` and `tableConstraint` are wrapper rules that both
// delegate to `checkClause` so the constraint body is defined exactly once
// (no drift between placements). The wrapper names self-document placement
// in the rule index, and either side can later add placement-only kinds
// (e.g., column-only NOT NULL hooks, table-only PRIMARY KEY) without
// touching the other.
//
// Optional `CONSTRAINT <name>` provides a machine identifier for diagnostics;
// optional `MESSAGE <stringLiteral>` provides a human-readable error string.
// Distinct keywords (`MESSAGE` vs `COMMENT`) avoid binding ambiguity with the
// surrounding `fieldDef`'s `commentClause`.
//
// The `check_expr` cascade mirrors postgres's `a_expr_*` precedence ladder
// (see antlr/grammars-v4/sql/postgresql) — only the subset that translates
// cleanly to CEL is admitted. See the design doc for the full mapping.

columnConstraint
    : checkClause
    ;

tableConstraint
    : checkClause
    ;

checkClause
    : ( CONSTRAINT identifier )? CHECK '(' check_expr ')' messageClause?
    ;

messageClause
    : MESSAGE stringLiteral
    ;

// Mirrors antlr/grammars-v4 PostgreSQL's `a_expr` cascade naming, with one
// deliberate deviation: `check_expr_unary_not` is placed ABOVE the qualifier
// family (between/in/isnull/compare/like) so prefix `NOT` scopes over them
// (matching real PG's bison `%right NOT` precedence). antlr/grammars-v4 PG
// inherits a precedence bug where `NOT x BETWEEN 1 AND 10` parses as
// `(NOT x) BETWEEN 1 AND 10`; we don't reproduce it.
check_expr
    : check_expr_or
    ;

check_expr_or
    : check_expr_and ( OR check_expr_and )*
    ;

check_expr_and
    : check_expr_unary_not ( AND check_expr_unary_not )*
    ;

// Recursive form admits chained `NOT NOT x` (real PG accepts this via its
// `NOT a_expr` bison rule). antlr/grammars-v4 PG uses `NOT? a_expr_isnull`
// (non-recursive) which can't express it; we deviate to preserve PG behavior.
check_expr_unary_not
    : NOT check_expr_unary_not                                     # CheckExprNot
    | check_expr_between                                           # CheckExprNotPass
    ;

check_expr_between
    : check_expr_in ( NOT? BETWEEN SYMMETRIC? check_expr_in AND check_expr_in )?
    ;

check_expr_in
    : check_expr_isnull ( NOT? IN in_target )?
    ;

in_target
    : '(' check_expr_list ')'                                      # InTargetParenList
    | check_expr_isnull                                            # InTargetExpr
    ;

check_expr_isnull
    : check_expr_compare ( IS NOT? NULL )?
    ;

check_expr_compare
    : check_expr_like ( ( '=' | '<>' | '!=' | '<' | '<=' | '>' | '>=' ) check_expr_like )?
    ;

check_expr_like
    : check_expr_concat ( NOT? LIKE stringLiteral escape_clause? )?
    ;

escape_clause
    : ESCAPE stringLiteral
    ;

// `||` is its own precedence level between LIKE and additive — looser than
// `+`/`-` (matches both PG Table 4.2 where `||` is "any other operator" #10
// and `+/-` is #9, and matches the antlr/grammars-v4 PostgreSQL grammar
// where `||` lives in `a_expr_qual_op` above `a_expr_add`). Without this
// separation, `'x' || 'y' + 'z'` would left-associate as `('x' || 'y') + 'z'`
// instead of PG's `'x' || ('y' + 'z')`.
check_expr_concat
    : check_expr_add ( '||' check_expr_add )*
    ;

check_expr_add
    : check_expr_mul ( ( '+' | '-' ) check_expr_mul )*
    ;

check_expr_mul
    : check_expr_unary_sign ( ( '*' | '/' | '%' ) check_expr_unary_sign )*
    ;

check_expr_unary_sign
    : ( '-' | '+' )? c_expr
    ;

c_expr
    : func_expr                              # CheckFunc
    | columnref                              # CheckColumnRef
    | literal                                # CheckLiteral
    | '(' check_expr ')' indirection?        # CheckParen
    | case_expr                              # CheckCase
    ;

// Function calls split into:
//   - func_application: regular f(args) form, name is any identifier;
//     translator validates name against the closed whitelist (LENGTH, UPPER,
//     LOWER, STARTS_WITH, ENDS_WITH, CONTAINS, REPLACE, MATCHES, COALESCE,
//     NULLIF, GREATEST, LEAST, IS_EMAIL, IS_HOSTNAME, IS_IPV4, IS_IPV6,
//     IS_URI, IS_URI_REF, IS_UUID).
//   - func_expr_common_subexpr: SQL-spec keyword forms whose syntax differs
//     from f(args) — CAST, EXTRACT, SUBSTRING, POSITION, TRIM, plus the
//     no-parens CURRENT_TIMESTAMP runtime variable.

func_expr
    : func_expr_common_subexpr
    | func_application
    ;

func_application
    : identifier '(' check_expr_list? ')'
    ;

func_expr_common_subexpr
    : CAST '(' check_expr AS castType ')'                                  # FuncCast
    | EXTRACT '(' identifier FROM check_expr ')'                           # FuncExtract
    | SUBSTRING '(' check_expr FROM check_expr ( FOR check_expr )? ')'     # FuncSubstringFromFor
    | SUBSTRING '(' check_expr ',' check_expr ( ',' check_expr )? ')'      # FuncSubstringCommas
    | POSITION '(' check_expr IN check_expr ')'                            # FuncPosition
    | TRIM '(' ( BOTH | LEADING | TRAILING )? check_expr ( FROM check_expr )? ')'  # FuncTrim
    | CURRENT_TIMESTAMP                                                    # FuncCurrentTimestamp
    ;

// Type name accepted by CAST. Reuses the LT primitive type rule with its
// optional length/precision/scale parameters; CEL has no fixed-point or
// length-typed primitives so the parameters are accepted but ignored at
// translation time.
castType
    : primitiveType
    ;

case_expr
    : CASE check_expr? when_clause+ ( ELSE check_expr )? END
    ;

when_clause
    : WHEN check_expr THEN check_expr
    ;

columnref
    : colid indirection?
    ;

colid
    : identifier
    ;

indirection
    : indirection_el+
    ;

indirection_el
    : '.' colid
    | '[' check_expr ']'
    ;

check_expr_list
    : check_expr ( ',' check_expr )*
    ;

// ─── enum body ────────────────────────────────────────────────────────────────
//
// The leading `ENUM` keyword has moved to the declaration head; the body is
// just the parenthesized value list.

enumBody
    : '(' enumValue ( ',' enumValue )* ')'
    ;

enumValue
    : stringLiteral commentClause? withClause?
    ;

// ─── type expressions ─────────────────────────────────────────────────────────

typeExpr
    : primitiveType nullability?              # PrimitiveTypeExpr
    | variantType nullability?                # VariantTypeExpr
    | rowType nullability?                    # RowTypeExpr
    | unionType nullability?                  # UnionTypeExpr
    | mapType nullability?                    # MapTypeExpr
    | qualifiedName nullability?              # QualifiedNameExpr
    | ARRAY '<' typeExpr '>' nullability?     # PrefixArray
    | typeExpr ARRAY nullability?             # PostfixArray
    | MULTISET '<' typeExpr '>' nullability?  # PrefixMultiset
    | typeExpr MULTISET nullability?          # PostfixMultiset
    ;

primitiveType
    : BOOLEAN
    | TINYINT
    | SMALLINT
    | INTEGER
    | INT
    | BIGINT
    | FLOAT
    | REAL
    | DOUBLE PRECISION?
    | DECIMAL ( '(' intLiteral ( ',' intLiteral )? ')' )?
    | DEC     ( '(' intLiteral ( ',' intLiteral )? ')' )?
    | NUMERIC ( '(' intLiteral ( ',' intLiteral )? ')' )?
    | CHARACTER VARYING ( '(' intLiteral ')' )?
    | VARCHAR ( '(' intLiteral ')' )?
    | STRING
    | CHARACTER ( '(' intLiteral ')' )?
    | CHAR    ( '(' intLiteral ')' )?
    | BINARY  VARYING ( '(' intLiteral ')' )?
    | BINARY  ( '(' intLiteral ')' )?
    | VARBINARY ( '(' intLiteral ')' )?
    | BYTES
    | DATE
    | TIME ( '(' intLiteral ')' )?
    | TIMESTAMP ( '(' intLiteral ')' )? ( WITHOUT TIME ZONE )?
    | TIMESTAMP ( '(' intLiteral ')' )? WITH LOCAL TIME ZONE
    | TIMESTAMP_LTZ ( '(' intLiteral ')' )?
    ;

variantType
    : VARIANT
    ;

rowType
    : ROW '<' fieldDef ( ',' fieldDef )* '>'
    | ROW '(' fieldDef ( ',' fieldDef )* ')'
    ;

unionType
    : UNION '<' unionBranch ( ',' unionBranch )* '>'
    | UNION '(' unionBranch ( ',' unionBranch )* ')'
    ;

unionBranch
    : identifier typeExpr commentClause? withClause?
    ;

mapType
    : MAP '<' typeExpr ',' typeExpr '>'
    ;

// ─── names ────────────────────────────────────────────────────────────────────

qualifiedName
    : identifier ( '.' identifier )*
    ;

// ─── literals ─────────────────────────────────────────────────────────────────

literal
    : intLiteral
    | floatLiteral
    | stringLiteral
    | bytesLiteral
    | boolLiteral
    | NULL
    ;

intLiteral
    : '-'? INT_LITERAL
    ;

floatLiteral
    : '-'? FLOAT_LITERAL
    ;

stringLiteral
    : STRING_LITERAL
    ;

bytesLiteral
    : BYTES_LITERAL
    ;

boolLiteral
    : TRUE
    | FALSE
    ;

// ─── identifiers ──────────────────────────────────────────────────────────────

identifier
    : ID
    | QUOTED_ID
    | nonReservedKeyword
    ;

nonReservedKeyword
    : ENUM
    | MAP
    | NAMESPACE
    | REFERENCE
    | TAGS
    | TYPE
    | VARIANT
    | ZONE
    ;

// ─── lexer rules ──────────────────────────────────────────────────────────────

AND             : A N D ;
ARRAY           : A R R A Y ;
AS              : A S ;
BETWEEN         : B E T W E E N ;
BIGINT          : B I G I N T ;
BINARY          : B I N A R Y ;
BOOLEAN         : B O O L E A N ;
BOTH            : B O T H ;
BYTES           : B Y T E S ;
CASE            : C A S E ;
CAST            : C A S T ;
CHARACTER       : C H A R A C T E R ;
CHAR            : C H A R ;
CHECK           : C H E C K ;
COMMENT         : C O M M E N T ;
CONSTRAINT      : C O N S T R A I N T ;
CURRENT_TIMESTAMP : C U R R E N T '_' T I M E S T A M P ;
DATE            : D A T E ;
DEC             : D E C ;
DECIMAL         : D E C I M A L ;
DEFAULT         : D E F A U L T ;
DOUBLE          : D O U B L E ;
ELSE            : E L S E ;
END             : E N D ;
ENUM            : E N U M ;
ESCAPE          : E S C A P E ;
EXTRACT         : E X T R A C T ;
FALSE           : F A L S E ;
FLOAT           : F L O A T ;
FOR             : F O R ;
FROM            : F R O M ;
IN              : I N ;
INT             : I N T ;
INTEGER         : I N T E G E R ;
IS              : I S ;
LEADING         : L E A D I N G ;
LIKE            : L I K E ;
LOCAL           : L O C A L ;
MAP             : M A P ;
MESSAGE         : M E S S A G E ;
MULTISET        : M U L T I S E T ;
NAMESPACE       : N A M E S P A C E ;
NOT             : N O T ;
NULL            : N U L L ;
NUMERIC         : N U M E R I C ;
OR              : O R ;
POSITION        : P O S I T I O N ;
PRECISION       : P R E C I S I O N ;
REAL            : R E A L ;
ROW             : R O W ;
SMALLINT        : S M A L L I N T ;
STRING          : S T R I N G ;
SUBSTRING       : S U B S T R I N G ;
SYMMETRIC       : S Y M M E T R I C ;
SYNONYM         : S Y N O N Y M ;
TAGS            : T A G S ;
THEN            : T H E N ;
TIME            : T I M E ;
TIMESTAMP_LTZ   : T I M E S T A M P '_' L T Z ;
TIMESTAMP       : T I M E S T A M P ;
TINYINT         : T I N Y I N T ;
TRAILING        : T R A I L I N G ;
TRIM            : T R I M ;
TRUE            : T R U E ;
TYPE            : T Y P E ;
UNION           : U N I O N ;
REFERENCE       : R E F E R E N C E ;
VARBINARY       : V A R B I N A R Y ;
VARCHAR         : V A R C H A R ;
VARYING         : V A R Y I N G ;
VARIANT         : V A R I A N T ;
WHEN            : W H E N ;
WITH            : W I T H ;
WITHOUT         : W I T H O U T ;
ZONE            : Z O N E ;

// literals

INT_LITERAL
    : [0-9]+
    ;

FLOAT_LITERAL
    : [0-9]+ '.' [0-9]*
    | '.' [0-9]+
    | [0-9]+ '.' [0-9]* E [+-]? [0-9]+
    | [0-9]+             E [+-]? [0-9]+
    ;

STRING_LITERAL
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

// ANSI SQL bytes literal: X'<hex digits>' or x'<hex digits>'.
// Lexer permits any STRING_LITERAL body; visitor validates hex content
// and even length, producing better error messages than ANTLR could.
BYTES_LITERAL
    : [xX] STRING_LITERAL
    ;

QUOTED_ID
    : '`' ( ~'`' | '``' )* '`'
    ;

ID
    : [a-zA-Z_] [a-zA-Z_0-9]*
    ;

// whitespace and comments

WS
    : [ \t\r\n]+ -> skip
    ;

LINE_COMMENT
    : '//' ~[\r\n]* -> skip
    ;

BLOCK_COMMENT
    : '/*' .*? '*/' -> skip
    ;

// ─── case-insensitive letter fragments ────────────────────────────────────────

fragment A : [aA] ;
fragment B : [bB] ;
fragment C : [cC] ;
fragment D : [dD] ;
fragment E : [eE] ;
fragment F : [fF] ;
fragment G : [gG] ;
fragment H : [hH] ;
fragment I : [iI] ;
fragment J : [jJ] ;
fragment K : [kK] ;
fragment L : [lL] ;
fragment M : [mM] ;
fragment N : [nN] ;
fragment O : [oO] ;
fragment P : [pP] ;
fragment Q : [qQ] ;
fragment R : [rR] ;
fragment S : [sS] ;
fragment T : [tT] ;
fragment U : [uU] ;
fragment V : [vV] ;
fragment W : [wW] ;
fragment X : [xX] ;
fragment Y : [yY] ;
fragment Z : [zZ] ;

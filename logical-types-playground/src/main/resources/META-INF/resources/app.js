require.config({
  paths: { vs: 'https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/min/vs' }
});

require(['vs/editor/editor.main'], function () {
  const KEYWORDS = [
    'ARRAY','AS','BIGINT','BINARY','BOOLEAN','BYTES','CHAR','CHARACTER','COMMENT',
    'DATE','DEC','DECIMAL','DEFAULT','DOUBLE','ENUM','FALSE',
    'FLOAT','INT','INTEGER','LOCAL','MAP','MULTISET','NAMESPACE','NOT','NULL',
    'NUMERIC','PRECISION','REAL','REFERENCE','ROW','SCHEMA','SMALLINT','STRING',
    'TAGS','TIME','TIMESTAMP','TIMESTAMP_LTZ','TINYINT','TRUE','TYPE','UNION',
    'VARBINARY','VARCHAR','VARIANT','VARYING','WITH','WITHOUT','ZONE'
  ];

  monaco.languages.register({ id: 'logicaltypes' });

  monaco.languages.setMonarchTokensProvider('logicaltypes', {
    ignoreCase: true,
    keywords: KEYWORDS,
    tokenizer: {
      root: [
        [/\/\/.*$/, 'comment'],
        [/\/\*/, { token: 'comment', next: '@blockComment' }],
        [/'([^']|'')*'/, 'string'],
        [/[xX]'[0-9a-fA-F]*'/, 'string'],
        [/`([^`]|``)*`/, 'identifier'],
        [/[0-9]+(\.[0-9]*)?([eE][+-]?[0-9]+)?/, 'number'],
        [/[a-zA-Z_][a-zA-Z_0-9]*/, {
          cases: { '@keywords': 'keyword', '@default': 'identifier' }
        }],
        [/[(){}<>;,.=]/, 'delimiter']
      ],
      blockComment: [
        [/\*\//, { token: 'comment', next: '@pop' }],
        [/./, 'comment']
      ]
    }
  });

  monaco.languages.setLanguageConfiguration('logicaltypes', {
    comments: { lineComment: '//', blockComment: ['/*', '*/'] },
    brackets: [['(', ')'], ['<', '>']],
    autoClosingPairs: [
      { open: '(', close: ')' }, { open: '<', close: '>' },
      { open: "'", close: "'" }, { open: '`', close: '`' }
    ]
  });

  monaco.languages.registerCompletionItemProvider('logicaltypes', {
    triggerCharacters: [' ', '(', ',', '<', '.'],
    provideCompletionItems: async function (model, position) {
      const sql = model.getValue();
      const caretOffset = model.getOffsetAt(position);
      const word = model.getWordUntilPosition(position);
      const wordRange = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn
      };
      // Range that covers the full dotted prefix left of the caret — so
      // qualified type names replace `io.t` cleanly instead of being grafted
      // onto whatever's left of the trailing word.
      const dottedRange = dottedPrefixRange(model, position, caretOffset);

      // After `REFERENCE TYPE`, only suggest qualified names of types defined
      // in *other* tabs (no keywords). Handled client-side because the server
      // only sees the active doc, while these suggestions span the workspace.
      // Replaces the full dotted prefix (e.g. `io.t`), not just the trailing
      // word, so the qualified name doesn't get jammed onto an existing
      // namespace.
      const before = sql.substring(0, caretOffset);
      if (/\bREFERENCE\s+TYPE\s+[\w.]*$/i.test(before)) {
        return crossTabTypeSuggestions(model, position, caretOffset, false);
      }

      const r = await fetch('/api/complete', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ sql, caretOffset })
      });
      const data = r.ok ? await r.json() : { items: [] };
      const kindMap = {
        Keyword: monaco.languages.CompletionItemKind.Keyword,
        Type: monaco.languages.CompletionItemKind.Class
      };
      const suggestions = (data.items || []).map(it => ({
        label: it.label,
        kind: kindMap[it.kind] || monaco.languages.CompletionItemKind.Text,
        insertText: it.insertText,
        // Type suggestions may carry dots (e.g. io.type.Foo); replacing only
        // the trailing word would mangle a partially-typed namespace.
        range: it.kind === 'Type' ? dottedRange : wordRange
      }));

      // At a typeExpr position (after `(`, `,`, `<`, or `AS`), also offer
      // named types from *other* tabs alongside the server's local types.
      // The user still needs to add `REFERENCE TYPE foo.X;` for these to
      // resolve at convert time, but completing them here makes the workflow
      // less mode-switchy.
      if (atTypeExprPosition(before)) {
        const extra = crossTabTypeSuggestions(model, position, caretOffset, true).suggestions;
        // Skip names that already appear in the server's suggestion set so
        // we don't duplicate types the user already imported via REFERENCE.
        const seen = new Set(suggestions.map(s => s.label));
        for (const item of extra) {
          if (!seen.has(item.label)) suggestions.push(item);
        }
      }

      return { suggestions };
    }
  });

  /**
   * Build a Monaco completion list of named types declared in tabs other than
   * the active one, filtered by the dotted prefix immediately left of caret.
   * The completion range covers the full dotted prefix so qualified names
   * replace cleanly. When {@code withImport} is true, accepting a suggestion
   * also inserts a `REFERENCE TYPE name;` line at the top of the active doc
   * (unless one already exists for that name).
   */
  function crossTabTypeSuggestions(model, position, caretOffset, withImport) {
    const text = model.getValue();
    const range = dottedPrefixRange(model, position, caretOffset);
    let start = caretOffset;
    while (start > 0 && /[\w.]/.test(text[start - 1])) {
      start--;
    }
    const prefix = text.substring(start, caretOffset).toLowerCase();

    const existingRefs = withImport ? referencedTypesIn(text) : new Set();
    const importPos = withImport ? referenceInsertionPosition(text) : null;

    const seen = new Set();
    const items = [];
    tabs.forEach((tab, idx) => {
      if (idx === activeIndex) return;
      const ddl = tab.model ? tab.model.getValue() : (tab.ddl || '');
      for (const name of namedTypesIn(ddl)) {
        if (seen.has(name)) continue;
        seen.add(name);
        if (prefix && !name.toLowerCase().startsWith(prefix)) continue;
        const item = {
          label: name,
          kind: monaco.languages.CompletionItemKind.Class,
          insertText: name,
          range: range,
          detail: tab.name
        };
        if (withImport && !existingRefs.has(name)) {
          item.additionalTextEdits = [{
            range: {
              startLineNumber: importPos.lineNumber,
              startColumn: importPos.column,
              endLineNumber: importPos.lineNumber,
              endColumn: importPos.column
            },
            text: `REFERENCE TYPE ${name};\n`
          }];
        }
        items.push(item);
      }
    });
    return { suggestions: items };
  }

  /** Names already imported in the active doc via `REFERENCE TYPE ...;`. */
  function referencedTypesIn(ddl) {
    const out = new Set();
    if (!ddl) return out;
    const stripped = ddl
        .replace(/\/\/.*$/gm, '')
        .replace(/\/\*[\s\S]*?\*\//g, '');
    const re = /\bREFERENCE\s+TYPE\s+([a-zA-Z_][\w.]*)\s*;/gi;
    let m;
    while ((m = re.exec(stripped)) !== null) out.add(m[1]);
    return out;
  }

  /**
   * Position to insert a new `REFERENCE TYPE` line: just below the last existing
   * `REFERENCE TYPE` (preferred), otherwise just below `NAMESPACE`, otherwise
   * the very top of the doc. Returned as a 1-based line/column.
   */
  function referenceInsertionPosition(ddl) {
    const lines = ddl.split('\n');
    let lastRef = -1;
    let lastNs = -1;
    for (let i = 0; i < lines.length; i++) {
      if (/^\s*REFERENCE\s+TYPE\b/i.test(lines[i])) lastRef = i;
      else if (/^\s*NAMESPACE\b/i.test(lines[i])) lastNs = i;
    }
    const anchor = lastRef >= 0 ? lastRef : lastNs;
    if (anchor < 0) return { lineNumber: 1, column: 1 };
    return { lineNumber: anchor + 2, column: 1 };
  }

  /**
   * Mirror of CompletionService#atTypePosition: true when the previous
   * non-whitespace token (skipping the dotted prefix being typed) is `(`,
   * `,`, `<`, the keyword `AS`, a fieldName identifier (an identifier
   * preceded by `(` or `,`, which opens a fieldDef in a struct body), or a
   * statement-leading bare `TYPE` keyword (the trailing root-registration
   * `TYPE <typeExpr>`). `REFERENCE TYPE` is handled separately by the
   * dedicated cross-tab-only short-circuit above.
   */
  function atTypeExprPosition(textBefore) {
    const stripped = textBefore.replace(/[\w.]*$/, '').replace(/\s+$/, '');
    if (!stripped) return false;
    const lastChar = stripped[stripped.length - 1];
    if (lastChar === '(' || lastChar === ',' || lastChar === '<') return true;
    const lastWord = stripped.match(/\b([a-zA-Z_]\w*)$/);
    if (!lastWord) return false;
    if (lastWord[1].toUpperCase() === 'AS') return true;
    const beforeWord = stripped
        .substring(0, stripped.length - lastWord[1].length)
        .replace(/\s+$/, '');
    // Statement-leading bare `TYPE`: the trailing root-registration form
    // `TYPE <typeExpr>`. typeExpr completions are appropriate.
    if (lastWord[1].toUpperCase() === 'TYPE') {
      if (!beforeWord) return true;
      return beforeWord[beforeWord.length - 1] === ';';
    }
    if (!beforeWord) return false;
    // Identifier preceded by `(` or `,` → fieldName, typeExpr is next.
    const c = beforeWord[beforeWord.length - 1];
    return c === '(' || c === ',';
  }

  /** Monaco range covering `[\w.]*` immediately to the left of the caret. */
  function dottedPrefixRange(model, position, caretOffset) {
    const text = model.getValue();
    let start = caretOffset;
    while (start > 0 && /[\w.]/.test(text[start - 1])) {
      start--;
    }
    const startPos = model.getPositionAt(start);
    return {
      startLineNumber: startPos.lineNumber,
      startColumn: startPos.column,
      endLineNumber: position.lineNumber,
      endColumn: position.column
    };
  }

  /** Extract qualified named-type names declared in a DDL string. */
  function namedTypesIn(ddl) {
    if (!ddl) return [];
    const stripped = ddl
        .replace(/\/\/.*$/gm, '')
        .replace(/\/\*[\s\S]*?\*\//g, '');
    const nsMatch = /(?:^|;)\s*NAMESPACE\s+([a-zA-Z_][\w.]*)/i.exec(stripped);
    const namespace = nsMatch ? nsMatch[1] : '';
    const qualify = n => (!namespace || n.includes('.')) ? n : namespace + '.' + n;
    const out = [];
    let m;
    // `ROW <name> (` and `ENUM <name> (` are the two declaration shapes. The
    // lead anchor (start of doc or `;`) keeps us from matching ROW/ENUM that
    // appear elsewhere (e.g., a `ROW(...)` type expression at field position).
    const re = /(?:^|;)\s*(?:ROW|ENUM)\s+([a-zA-Z_][\w.]*)\s*\(/gi;
    while ((m = re.exec(stripped)) !== null) out.push(qualify(m[1]));
    return out;
  }

  // ---------------------------------------------------------------------------
  // Multi-tab document workspace
  // ---------------------------------------------------------------------------
  // Each tab is a (name, ddl) document with its own Monaco model (so undo/redo
  // history is per-tab). Workspace state is persisted in localStorage so a
  // browser refresh keeps everything. The active tab is what gets converted;
  // other tabs are available as REFERENCE TYPE targets in the active tab.

  const STORAGE_KEY = 'lt-playground/workspace/v1';

  function loadWorkspace() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && Array.isArray(parsed.tabs) && parsed.tabs.length > 0) {
          return parsed;
        }
      }
    } catch (_) { /* fall through */ }
    return { tabs: [{ name: 'main', ddl: '' }], activeIndex: 0 };
  }

  function saveWorkspace() {
    try {
      // Sync the in-memory tab DDL from its Monaco model before persisting.
      tabs.forEach(t => { t.ddl = t.model ? t.model.getValue() : (t.ddl || ''); });
      const payload = {
        tabs: tabs.map(t => ({ name: t.name, ddl: t.ddl })),
        activeIndex: activeIndex
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch (_) { /* best-effort */ }
  }

  const ws = loadWorkspace();
  // Each tab = { name, ddl, model: Monaco TextModel }
  const tabs = ws.tabs.map(t => ({
    name: t.name,
    ddl: t.ddl || '',
    model: monaco.editor.createModel(t.ddl || '', 'logicaltypes')
  }));
  let activeIndex = Math.min(Math.max(0, ws.activeIndex || 0), tabs.length - 1);

  const editor = monaco.editor.create(document.getElementById('editor'), {
    model: tabs[activeIndex].model,
    theme: 'vs-dark',
    fontSize: 13,
    minimap: { enabled: false },
    automaticLayout: true,
    scrollBeyondLastLine: false,
    quickSuggestions: { other: true, comments: false, strings: false },
    acceptSuggestionOnEnter: 'off'
  });

  // Empty-state placeholder: visible whenever the active tab's model has no
  // content, so a fresh tab doesn't look broken/blank. Hidden as soon as any
  // character is typed (and re-shown if all content is deleted).
  const placeholderEl = document.getElementById('editor-placeholder');
  function updatePlaceholder() {
    placeholderEl.hidden = editor.getModel().getValueLength() > 0;
  }
  editor.onDidChangeModelContent(updatePlaceholder);
  editor.onDidChangeModel(updatePlaceholder);
  updatePlaceholder();

  const output = monaco.editor.create(document.getElementById('output'), {
    value: '',
    language: 'json',
    theme: 'vs-dark',
    readOnly: true,
    fontSize: 13,
    minimap: { enabled: false },
    automaticLayout: true,
    scrollBeyondLastLine: false
  });

  let target = 'avro';
  const targetLanguage = { avro: 'json', protobuf: 'plaintext', json: 'json' };
  const targetLabel = { avro: 'Avro', protobuf: 'Protobuf', json: 'JSON Schema' };
  const outputLabel = document.getElementById('output-label');

  document.querySelectorAll('.toggle button').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.toggle button').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      target = btn.dataset.target;
      updateOutputLabel();
      monaco.editor.setModelLanguage(output.getModel(), targetLanguage[target]);
      convert();
    });
  });

  // ---------------------------------------------------------------------------
  // Tab strip rendering
  // ---------------------------------------------------------------------------

  const tabStrip = document.getElementById('tab-strip');

  function renderTabs() {
    tabStrip.innerHTML = '';
    tabs.forEach((tab, idx) => {
      const el = document.createElement('button');
      el.className = 'tab' + (idx === activeIndex ? ' active' : '');
      el.setAttribute('role', 'tab');
      el.setAttribute('aria-selected', idx === activeIndex ? 'true' : 'false');
      el.textContent = tab.name;
      el.addEventListener('click', e => {
        if (e.target.classList.contains('close')) return;
        switchTo(idx);
      });

      if (tabs.length > 1) {
        const close = document.createElement('span');
        close.className = 'close';
        close.textContent = '×';
        close.title = 'Close';
        close.addEventListener('click', e => {
          e.stopPropagation();
          closeTab(idx);
        });
        el.appendChild(close);
      }
      tabStrip.appendChild(el);
    });

    const add = document.createElement('button');
    add.className = 'add';
    add.title = 'New document';
    add.textContent = '+';
    add.addEventListener('click', addTab);
    tabStrip.appendChild(add);
  }

  function switchTo(idx) {
    if (idx < 0 || idx >= tabs.length || idx === activeIndex) return;
    // Persist the current tab's editor content before switching.
    tabs[activeIndex].ddl = tabs[activeIndex].model.getValue();
    activeIndex = idx;
    editor.setModel(tabs[activeIndex].model);
    renderTabs();
    updateOutputLabel();
    saveWorkspace();
    convert();
  }

  function addTab() {
    const name = uniqueDefaultName();
    const t = { name, ddl: '', model: monaco.editor.createModel('', 'logicaltypes') };
    tabs.push(t);
    activeIndex = tabs.length - 1;
    editor.setModel(t.model);
    renderTabs();
    updateOutputLabel();
    saveWorkspace();
    convert();
  }

  /** Generate a placeholder tab name that doesn't collide with existing tabs. */
  function uniqueDefaultName() {
    let n = tabs.length + 1;
    while (tabs.some(t => t.name === `untitled-${n}`)) n++;
    return `untitled-${n}`;
  }

  /**
   * Derive a tab name from the DDL: prefer the qualified name of the type
   * named by the trailing root-registration {@code TYPE <name>;} statement,
   * then fall back to the name defined by the last {@code ROW <name>} or
   * {@code ENUM <name>} declaration. Returns null if nothing's derivable.
   */
  function deriveTabName(ddl) {
    if (!ddl) return null;
    // Strip line and block comments so they don't trip the regex.
    const stripped = ddl
        .replace(/\/\/.*$/gm, '')
        .replace(/\/\*[\s\S]*?\*\//g, '');
    const nsMatch = /(?:^|;)\s*NAMESPACE\s+([a-zA-Z_][\w.]*)/i.exec(stripped);
    const namespace = nsMatch ? nsMatch[1] : '';
    function qualify(name) {
      if (!namespace || name.includes('.')) return name;
      return namespace + '.' + name;
    }
    // Trailing root-registration form: `TYPE <name> [NOT NULL|NULL]? (;|$)`.
    let m = /(?:^|;)\s*TYPE\s+([a-zA-Z_][\w.]*)\s*(?:NOT\s+NULL|NULL)?\s*(?:;|$)/i.exec(stripped);
    if (m) return qualify(m[1]);
    m = /(?:^|;)\s*(?:ROW|ENUM)\s+([a-zA-Z_][\w.]*)\s*\(/i.exec(stripped);
    if (m) return qualify(m[1]);
    return null;
  }

  /**
   * Try to rename the active tab based on its current DDL. No-op if the derived
   * name matches the current name, is empty, or would collide with another tab.
   */
  function maybeRenameActiveTab() {
    const tab = tabs[activeIndex];
    const derived = deriveTabName(tab.model.getValue());
    if (!derived || derived === tab.name) return;
    if (tabs.some((t, i) => i !== activeIndex && t.name === derived)) return;
    tab.name = derived;
    renderTabs();
    updateOutputLabel();
  }

  function closeTab(idx) {
    if (tabs.length <= 1) return;
    if (!confirm(`Close tab '${tabs[idx].name}'? Its content will be lost.`)) return;
    tabs[idx].model.dispose();
    tabs.splice(idx, 1);
    if (activeIndex >= tabs.length) activeIndex = tabs.length - 1;
    if (activeIndex < 0) activeIndex = 0;
    editor.setModel(tabs[activeIndex].model);
    renderTabs();
    updateOutputLabel();
    saveWorkspace();
    convert();
  }

  function updateOutputLabel() {
    outputLabel.textContent = `${targetLabel[target]} — ${tabs[activeIndex].name}`;
  }

  // ---------------------------------------------------------------------------
  // Conversion (debounced on edits, triggered on tab/target switches)
  // ---------------------------------------------------------------------------

  let timer = null;
  editor.onDidChangeModelContent(() => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      maybeRenameActiveTab();
      saveWorkspace();
      convert();
    }, 250);
  });

  async function convert() {
    // Sync in-memory ddl from current model.
    tabs[activeIndex].ddl = tabs[activeIndex].model.getValue();
    const documents = tabs.map(t => ({ name: t.name, ddl: t.ddl || '' }));
    const activeName = tabs[activeIndex].name;
    if (!tabs[activeIndex].ddl.trim()) {
      output.setValue('');
      monaco.editor.setModelMarkers(tabs[activeIndex].model, 'convert', []);
      return;
    }
    try {
      const r = await fetch('/api/convert', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          documents,
          activeDocument: activeName,
          target,
          rowName: 'Row'
        })
      });
      const data = await r.json();
      const errors = data.errors || [];
      monaco.editor.setModelMarkers(tabs[activeIndex].model, 'convert', errors.map(e => ({
        startLineNumber: e.line,
        startColumn: e.column + 1,
        endLineNumber: e.endLine,
        endColumn: e.endColumn + 1,
        message: e.message,
        severity: monaco.MarkerSeverity.Error
      })));
      if (data.schema) {
        if (target === 'avro' || target === 'json') {
          try {
            output.setValue(JSON.stringify(JSON.parse(data.schema), null, 2));
          } catch (_) {
            output.setValue(data.schema);
          }
        } else {
          output.setValue(data.schema);
        }
      } else if (errors.length === 0) {
        output.setValue('');
      }
    } catch (e) {
      output.setValue('// request failed: ' + e.message);
    }
  }

  // ---------------------------------------------------------------------------
  // Examples dropdown — preset DDL snippets that load into fresh tabs.
  // ---------------------------------------------------------------------------
  // Each example is either a single doc (`ddl: '...'`) or a multi-doc workspace
  // (`docs: [{name, ddl}, ...], active: '<name>'`). Loading replaces the current
  // workspace; this is intentional for demo flow — picking an example resets the
  // editor to a known state without leaving stale tabs around.

  const EXAMPLES = [
    {
      label: 'Blank workspace',
      desc: 'Start fresh — discards all current tabs',
      ddl: ''
    },
    {
      label: 'Basic struct',
      desc: 'A simple ROW with primitive fields',
      ddl:
`ROW Person (
  id STRING NOT NULL,
  name STRING NOT NULL,
  age INT,
  active BOOLEAN NOT NULL
);
`
    },
    {
      label: 'Logical & temporal types',
      desc: 'DATE, TIMESTAMP, DECIMAL, VARCHAR, BINARY',
      ddl:
`ROW Person (
  id STRING NOT NULL,
  amount DECIMAL(12, 2) NOT NULL,
  occurred_at TIMESTAMP_LTZ(6) NOT NULL,
  birthdate DATE,
  short_code VARCHAR(8) NOT NULL,
  signature BINARY(32) NOT NULL
);
`
    },
    {
      label: 'Nested + collections',
      desc: 'Nested ROW, ARRAY, MAP, MULTISET',
      ddl:
`ROW Collections (
  user ROW(
    id STRING NOT NULL,
    email STRING NOT NULL
  ) NOT NULL,
  tags ARRAY<STRING NOT NULL> NOT NULL,
  attributes MAP<STRING NOT NULL, STRING NOT NULL>,
  events MULTISET<ROW(
    kind STRING NOT NULL,
    at TIMESTAMP_LTZ(3) NOT NULL
  ) NOT NULL>
);
`
    },
    {
      label: 'Named types, enums, defaults',
      desc: 'ROW, ENUM, DEFAULT values',
      ddl:
`NAMESPACE com.example.demo;

ENUM Status ('ACTIVE', 'PAUSED', 'CANCELLED');

ROW Address (
  street STRING NOT NULL,
  city STRING NOT NULL,
  postal_code VARCHAR(16)
);

ROW Order (
  id STRING NOT NULL,
  retries INT NOT NULL DEFAULT 0,
  status Status NOT NULL,
  shipping Address,
  billing Address
);
`
    },
    {
      label: 'Union & variant',
      desc: 'UNION branches and the VARIANT type',
      ddl:
`ROW UnionAndVariant (
  id STRING NOT NULL,
  payload UNION<
    text STRING NOT NULL,
    number BIGINT NOT NULL,
    blob BINARY(16) NOT NULL
  > NOT NULL,
  metadata VARIANT
);
`
    },
    {
      label: 'Nested types (proto)',
      desc: 'Dotted names denote nesting — visible in the Protobuf output',
      ddl:
`NAMESPACE com.example.demo;

ROW Outer (
  name STRING NOT NULL,
  inner Outer.Inner NOT NULL,
  tag Outer.Status NOT NULL
);

ROW Outer.Inner (
  x INT NOT NULL,
  y INT NOT NULL
);

ENUM Outer.Status ('ACTIVE', 'INACTIVE');
`
    },
    {
      label: 'CHECK constraints',
      desc: 'Column-level and table-level CHECKs translated to CEL rules',
      ddl:
`ROW Order (
  id STRING NOT NULL CHECK (LENGTH(id) > 0),
  email STRING NOT NULL CHECK (IS_EMAIL(email)),
  quantity INT NOT NULL CHECK (quantity BETWEEN 1 AND 100),
  unit_price DECIMAL(12, 2) NOT NULL CHECK (unit_price > 0.0),
  status STRING NOT NULL
    CHECK (status IN ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED')),
  promo_code VARCHAR(16)
    CHECK (promo_code LIKE 'PROMO-%'),
  tags ARRAY<STRING NOT NULL>
    CHECK (EVERY(tags, t, LENGTH(t) > 0)),
  shipping_address ROW(
    street STRING NOT NULL,
    city STRING NOT NULL,
    postal_code VARCHAR(10) NOT NULL
      CHECK (MATCHES(postal_code, '^[0-9]{5}$'))
  ) NOT NULL,
  created_at TIMESTAMP_LTZ(6) NOT NULL
    CHECK (created_at < CURRENT_TIMESTAMP),
  CONSTRAINT total_within_limit
    CHECK (CAST(quantity AS DECIMAL(12, 2)) * unit_price < 100000.0)
    MESSAGE 'order total must stay below the 100k limit',
  CONSTRAINT shipping_postal_consistency
    CHECK (
      CASE WHEN status = 'SHIPPED'
        THEN LENGTH(shipping_address.postal_code) = 5
        ELSE TRUE
      END
    )
);
`
    },
    {
      label: 'Cross-tab references',
      desc: 'Two tabs: a shared Address type referenced from an Order',
      docs: [
        {
          name: 'com.example.demo.Address',
          ddl:
`NAMESPACE com.example.demo;

ROW Address (
  street STRING NOT NULL,
  city STRING NOT NULL,
  postal_code VARCHAR(16)
);
`
        },
        {
          name: 'com.example.demo.Order',
          ddl:
`NAMESPACE com.example.demo;

REFERENCE TYPE com.example.demo.Address;

ROW Order (
  id STRING NOT NULL,
  amount DECIMAL(12, 2) NOT NULL,
  shipping com.example.demo.Address NOT NULL,
  billing com.example.demo.Address
);
`
        }
      ],
      active: 'com.example.demo.Order'
    }
  ];

  function buildExamplesMenu() {
    const menu = document.getElementById('examples-menu');
    menu.innerHTML = '';
    EXAMPLES.forEach((ex, idx) => {
      const li = document.createElement('li');
      li.setAttribute('role', 'menuitem');
      li.innerHTML = `${escapeHtml(ex.label)}<span class="desc">${escapeHtml(ex.desc)}</span>`;
      li.addEventListener('click', () => {
        loadExample(idx);
        closeExamplesMenu();
      });
      menu.appendChild(li);
    });
  }

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, c => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
    ));
  }

  /** Replace the workspace with the example's tab(s) and switch to its active. */
  function loadExample(idx) {
    const ex = EXAMPLES[idx];
    // Dispose existing models so we don't leak Monaco state.
    tabs.forEach(t => t.model && t.model.dispose());
    tabs.length = 0;
    if (ex.docs) {
      for (const d of ex.docs) {
        tabs.push({
          name: d.name,
          ddl: d.ddl,
          model: monaco.editor.createModel(d.ddl, 'logicaltypes')
        });
      }
      activeIndex = Math.max(0, tabs.findIndex(t => t.name === ex.active));
    } else {
      const name = deriveTabName(ex.ddl) || 'main';
      tabs.push({
        name,
        ddl: ex.ddl,
        model: monaco.editor.createModel(ex.ddl, 'logicaltypes')
      });
      activeIndex = 0;
    }
    editor.setModel(tabs[activeIndex].model);
    renderTabs();
    updateOutputLabel();
    saveWorkspace();
    convert();
  }

  const examplesButton = document.getElementById('examples-button');
  const examplesMenu = document.getElementById('examples-menu');

  function openExamplesMenu() {
    examplesMenu.hidden = false;
    examplesButton.setAttribute('aria-expanded', 'true');
  }

  function closeExamplesMenu() {
    examplesMenu.hidden = true;
    examplesButton.setAttribute('aria-expanded', 'false');
  }

  examplesButton.addEventListener('click', e => {
    e.stopPropagation();
    if (examplesMenu.hidden) openExamplesMenu(); else closeExamplesMenu();
  });
  document.addEventListener('click', e => {
    if (!examplesMenu.hidden
        && !examplesMenu.contains(e.target)
        && e.target !== examplesButton) {
      closeExamplesMenu();
    }
  });
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeExamplesMenu();
  });

  buildExamplesMenu();

  // Initial render
  renderTabs();
  updateOutputLabel();
  convert();
});

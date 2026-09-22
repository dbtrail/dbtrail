// In-page measurement for the first-run walk (#1800). Loaded into every page
// the walk opens (and into the fixtures of first_run_walk.test.mjs); defines
// window.__firstRunMeasure(opts), which reads the rendered page, never the
// source files, and returns:
//
//   layer           which surface the step is: "login", "notice", "modal" or
//                   "view". An open dialog or notice sits over the page and
//                   makes it inert, so the step is the topmost open surface.
//   wordsAboveFold  words visible inside the viewport on that surface (the
//                   ratcheted count). Copyable SQL in <pre> is left out, and so
//                   is a folded Details placed after the step's primary
//                   button: the agreed design lets that fold name the
//                   mechanism, and it is written here so the rule is visible.
//   wordsWhole      the count the 2026-09-22 inventory used: every word of the
//                   main area plus every open dialog, below the fold included
//                   (innerText). Evidence only, to line up with that record.
//   chunks          the visible sentences on screen, for the banned-word
//                   check: the top surface, or the page plus the sidebar when
//                   nothing is open. Text inside code, pre, kbd and samp is
//                   something copied, not a sentence, and is left out.
//   alarms          the warning and error elements on that surface (below).
//   primary         how far the step's primary button sits below the visible
//                   area, in px (0 when fully visible), or missing.
(function () {
  // "Yellow or red" means the interface's own warning and error classes, not
  // a pixel colour. These are the classes style.css paints in the warning
  // register (--ochre / --orange) or the error register (--delete), plus the
  // persistent failure toast.
  const ALARMS = [
    ".error-box",            // --delete box
    ".notice.err",           // notice with a --delete top edge
    ".doctor-card.fail",     // failed check card, --delete border
    ".form-msg.err",         // --delete message line
    ".srv-status.err",       // server row status in --delete
    ".chip-nosrc",           // NO SOURCE chip, --delete
    ".chip-fail",            // verification MISMATCH chip, --delete
    ".cov-chip.bad",         // coverage chip, --delete
    ".cov-line.bad",         // coverage line, --delete
    ".cov-asof.bad",         // coverage stamp, --delete
    ".hstat-err",            // replication health, --delete
    ".fr-step.failed",       // first-run step failed, --delete mark
    ".vfy-row.fail",         // verification row, --delete
    ".bk-card-state.alarm",  // backup card state, --delete
    ".bks-value.bks-refused",// backup setting refused, --delete
    ".toast-error",          // the failure toast: the interface's error node
    ".warn-box",             // --orange / --ochre box
    ".warn-item",            // --orange / --ochre line
    ".notice.warn",          // notice with an --ochre top edge
    ".doctor-card.warn",     // "!" check card, --ochre border
    ".vfy-row.warn",         // verification row, --ochre
    ".hstat-warn",           // replication health, --orange
    ".hstale-warn",          // stale health note, --orange
    ".hlist",                // replication-health list, --orange
    ".tt-warn",              // Time-travel warning, --orange
    ".chip-mon",             // monitor state chip, --orange on --ochre
    ".chip-live",            // RUNNING job chip, --orange on --ochre
  ];
  // Named like warnings but painted neutral on purpose, because the fact is
  // benign (each exists so a benign fact does not read as an incident). Not
  // counted: .cov-line.warn (--ink-2), .cov-chip.warn (--update blue),
  // .doctor-card.note, .note-item, .muted-box, .hstale, .chip-age.

  const LAYERS = [["login", "#login-mount"], ["notice", "#notice-mount"], ["modal", "#modal"]];
  const COPY_TAGS = new Set(["CODE", "PRE", "KBD", "SAMP"]);

  const openLayer = () => {
    for (const [name, sel] of LAYERS) {
      const el = document.querySelector(sel);
      if (el && el.firstElementChild && visible(el.firstElementChild)) return [name, el];
    }
    return ["view", document.getElementById("view") || document.body];
  };

  function visible(el) {
    if (!el || !el.isConnected) return false;
    if (typeof el.checkVisibility === "function" &&
        !el.checkVisibility({ opacityProperty: true, visibilityProperty: true, checkOpacity: true, checkVisibilityCSS: true })) return false;
    for (let p = el; p; p = p.parentElement) {
      const cs = getComputedStyle(p);
      if (cs.display === "none" || cs.visibility === "hidden" || cs.visibility === "collapse" || parseFloat(cs.opacity) === 0) return false;
      if (p.hidden) return false;
    }
    const r = el.getBoundingClientRect();
    return r.width > 0 || r.height > 0 || el.getClientRects().length > 0;
  }

  // A node inside a folded <details>, outside its <summary>, is not shown.
  // `child` is the node on the path that sits directly inside the <details>,
  // so text written straight into the details (no wrapping element) counts
  // as folded too.
  function inClosedDetails(node) {
    for (let child = node, p = node.parentNode; p; child = p, p = p.parentNode) {
      if (p.tagName === "DETAILS" && !p.open && !(child.nodeType === 1 && child.tagName === "SUMMARY")) return true;
    }
    return false;
  }

  // The exemption: a folded <details> that comes after the primary button in
  // document order is skipped whole, summary included.
  function exemptDetails(node, primary) {
    if (!primary) return false;
    for (let p = node.nodeType === 1 ? node : node.parentElement; p; p = p.parentElement) {
      if (p.tagName === "DETAILS" && !p.open &&
          (primary.compareDocumentPosition(p) & Node.DOCUMENT_POSITION_FOLLOWING) && !p.contains(primary)) return true;
    }
    return false;
  }

  function insideTag(node, tags) {
    for (let p = node.nodeType === 1 ? node : node.parentElement; p; p = p.parentElement) {
      if (tags.has(p.tagName)) return true;
    }
    return false;
  }

  // The part of the viewport an element can show: the viewport cut down by
  // every ancestor that clips its overflow (a scrolled dialog body, say).
  function clipRect(el) {
    let top = 0, left = 0, bottom = window.innerHeight, right = window.innerWidth;
    for (let p = el.parentElement; p && p !== document.documentElement; p = p.parentElement) {
      const cs = getComputedStyle(p);
      if (cs.overflowX !== "visible" || cs.overflowY !== "visible") {
        const r = p.getBoundingClientRect();
        top = Math.max(top, r.top); left = Math.max(left, r.left);
        bottom = Math.min(bottom, r.bottom); right = Math.min(right, r.right);
      }
      if (cs.position === "fixed") break;
    }
    return { top, left, bottom, right };
  }
  const intersects = (r, c) => r.bottom > c.top && r.top < c.bottom && r.right > c.left && r.left < c.right && (r.width > 0 || r.height > 0);

  function textNodes(root) {
    const out = [];
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    for (let n = walker.nextNode(); n; n = walker.nextNode()) {
      if (n.nodeValue && n.nodeValue.trim()) out.push(n);
    }
    return out;
  }
  const hasWordChar = (t) => /[\p{L}\p{N}]/u.test(t);

  // Words of one text node that sit inside the visible area.
  function wordsInView(node) {
    const el = node.parentElement;
    const clip = clipRect(el);
    let n = 0;
    const range = document.createRange();
    for (const m of node.nodeValue.matchAll(/\S+/g)) {
      if (!hasWordChar(m[0])) continue;
      range.setStart(node, m.index);
      range.setEnd(node, m.index + m[0].length);
      if (Array.from(range.getClientRects()).some((r) => intersects(r, clip))) n++;
    }
    return n;
  }
  const countWords = (t) => String(t || "").split(/\s+/).filter(hasWordChar).length;

  // Placeholder text of an empty field, and the chosen option of a select,
  // are words the interface shows even though they are not text nodes.
  function fieldTexts(root) {
    const out = [];
    for (const f of root.querySelectorAll("input, textarea, select")) {
      if (f.type === "hidden") continue;
      let text = "";
      if (f.tagName === "SELECT") text = f.selectedOptions[0] ? f.selectedOptions[0].textContent : "";
      else if (!f.value && f.placeholder) text = f.placeholder;
      if (text.trim()) out.push([f, text]);
    }
    return out;
  }

  function primaryInfo(sel, label) {
    const el = typeof sel === "string" ? document.querySelector(sel) : sel;
    if (!el || !visible(el)) return { missing: true, belowFoldPx: null, label: label || String(sel) };
    const r = el.getBoundingClientRect();
    const clip = clipRect(el);
    return { missing: false, belowFoldPx: Math.max(0, Math.round(r.bottom - clip.bottom)), label: label || (el.textContent || "").trim() };
  }

  window.__firstRunMeasure = function (opts) {
    opts = opts || {};
    const primaryEl = opts.primary ? (typeof opts.primary === "string" ? document.querySelector(opts.primary) : opts.primary) : null;
    const [layer, surface] = openLayer();
    const skip = (node) => inClosedDetails(node) || exemptDetails(node, primaryEl);

    let wordsAboveFold = 0;
    for (const node of textNodes(surface)) {
      if (!visible(node.parentElement) || skip(node) || insideTag(node, new Set(["PRE"]))) continue;
      wordsAboveFold += wordsInView(node);
    }
    for (const [f, text] of fieldTexts(surface)) {
      if (!visible(f) || skip(f)) continue;
      if (intersects(f.getBoundingClientRect(), clipRect(f))) wordsAboveFold += countWords(text);
    }

    // The inventory's count: the main area and every open dialog, whole.
    let wordsWhole = countWords((document.getElementById("view") || document.body).innerText);
    for (const [, sel] of LAYERS) {
      const el = document.querySelector(sel);
      if (el && el.firstElementChild) wordsWhole += countWords(el.innerText);
    }

    // Sentences for the banned-word check: the whole top surface (not cut by
    // the fold), or the page plus the sidebar when no dialog is open.
    const scopes = layer === "view" ? [surface, document.querySelector(".side")].filter(Boolean) : [surface];
    const chunks = [];
    for (const scope of scopes) {
      for (const node of textNodes(scope)) {
        if (!visible(node.parentElement) || skip(node) || insideTag(node, COPY_TAGS)) continue;
        chunks.push(node.nodeValue.replace(/\s+/g, " ").trim());
      }
      for (const [f, text] of fieldTexts(scope)) {
        if (visible(f) && !skip(f)) chunks.push(text.trim());
      }
    }

    const alarms = [];
    const seen = new Set();
    const toast = document.getElementById("toast-error");
    for (const sel of ALARMS) {
      // The failure toast floats over every layer, so it is on the step
      // wherever it is; everything else must sit on the step's surface.
      const found = sel === ".toast-error"
        ? (toast && !toast.hidden ? [toast] : [])
        : scopes.flatMap((scope) => Array.from(scope.querySelectorAll(sel)));
      for (const el of found) {
        if (seen.has(el)) continue;
        const text = (el.innerText || el.textContent || "").replace(/\s+/g, " ").trim();
        if (!text || !visible(el) || skip(el)) continue;
        seen.add(el);
        alarms.push({ selector: sel, text: text.length > 80 ? text.slice(0, 77) + "..." : text });
      }
    }

    return {
      layer, wordsAboveFold, wordsWhole, chunks, alarms,
      primary: opts.primary ? primaryInfo(opts.primary, opts.primaryLabel) : null,
    };
  };
})();

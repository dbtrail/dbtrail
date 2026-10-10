// The theme loader (#1969): light, dark, or follow the system.
//
// Its own file, loaded in <head> ahead of style.css, for one reason: the
// attribute has to be on <html> before the first paint, or a dark-mode
// viewer gets a white flash on every load. The console's CSP is
// script-src 'self', so an inline <script> would be blocked silently and
// the flash would come back with no test failing.
//
// The choice is per viewer, in localStorage. Reading it THROWS where site
// data is blocked, so every access is wrapped: a browser that remembers
// nothing still follows the system and still honours a choice for the visit.
// What lands on <html data-theme> is always the RESOLVED theme (light or
// dark), so style.css carries one dark block and no media-query copy of it.
(function () {
  var KEY = "dbtrail.theme";
  var CHOICES = ["light", "dark", "system"];
  var mq = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;

  function stored() {
    try {
      var v = window.localStorage.getItem(KEY);
      return CHOICES.indexOf(v) >= 0 ? v : "system";
    } catch (e) { return "system"; }
  }

  var choice = stored();

  function paint() {
    var dark = choice === "system" ? !!(mq && mq.matches) : choice === "dark";
    window.document.documentElement.setAttribute("data-theme", dark ? "dark" : "light");
  }

  // The system flipping only moves a page that is following it. Safari
  // before 14 has addListener only.
  function systemFlipped() { if (choice === "system") paint(); }
  if (mq && mq.addEventListener) mq.addEventListener("change", systemFlipped);
  else if (mq && mq.addListener) mq.addListener(systemFlipped);

  // The choice changed somewhere this page did not see: another tab wrote
  // it (a storage event), or this page sat in the back/forward cache while
  // one did (pageshow). Read it again, repaint, and tell the control, or
  // its lit pill would name a theme that is not the one painted.
  // Not while this page holds a choice storage refused to keep: what is
  // stored is then older than what the viewer picked here.
  var onchange = null;
  var unsaved = false;
  function reread() {
    if (unsaved) return;
    var v = stored();
    if (v === choice) return;
    choice = v;
    paint();
    if (onchange) onchange(choice);
  }
  if (window.addEventListener) {
    window.addEventListener("storage", function (e) { if (e.key === KEY || e.key === null) reread(); });
    window.addEventListener("pageshow", function (e) { if (e.persisted) reread(); });
  }

  paint();

  window.dbtrailTheme = {
    get choice() { return choice; },
    // one listener: called with the new choice when it changed from outside
    set onchange(fn) { onchange = fn; },
    set: function (v) {
      if (CHOICES.indexOf(v) < 0) return;
      choice = v;
      try { window.localStorage.setItem(KEY, v); unsaved = false; } catch (e) { unsaved = true; /* this visit only */ }
      paint();
    },
  };
})();

# Connect an AI assistant in 5 minutes

You run DBTrail. You'd like to ask your database's change history questions in
plain English — from **Claude Desktop**, claude.ai, or any MCP-capable client:

> "What got deleted from `orders` in the last hour?"
>
> "Generate the SQL to bring customer 42's row back."
>
> "Did anyone ALTER a table this week?"

This page is the shortest path there. No JSON files, no DSNs, no SSH gymnastics.
(If you want every option and knob instead, that's [mcp-server.md](mcp-server.md).)

**What the AI can and cannot do, up front:** it gets six **read-only** tools —
search changes, draft reversal SQL (including for foreign-key cascade side
effects), reconstruct a row's state at a point in time, show index status, list
schema changes. It sees exactly what the web console
shows you, with the same result caps and the same redactions. It **never executes
SQL** and never connects to your source database — recovery SQL is text you review
and run yourself. (Time travel needs a baseline snapshot configured for that
server, exactly like the console's Time-travel tab; without one the tool says so.)

---

## Step 1 — you need the console with a token

> The numbers here match the numbered cards on the console's **Connect AI**
> page (1 Create a token, 2 Copy the address, 3 Add it to Claude), so you can
> follow either surface.

The AI connects to your **web console**, which serves an MCP endpoint at
`/mcp`. If you already run `bintrail-console` (standalone `serve`, `watch`, or
the Docker stack), you're nearly done — the AI client just needs an **access
token**, and you mint one without leaving the browser:

Open **Settings → Connect AI** in the sidebar. If no token is configured yet,
the **Create a token** card has a **Generate token** button — click it, copy the
value it shows (it appears exactly once and is never stored), and you're done.
No flags, no environment variables, no restart. The same card replaces the
token (**New token**) or deletes it (**Delete token**) later. The generated token is scoped to the MCP tools only —
it cannot administer the console — and it carries the permission grants of the
session that minted it, so each MCP tool works only if your session could use
the matching console surface directly (details in
[console.md → MCP endpoint](console.md#mcp-endpoint)).

Password login (the browser kind) does **not** work for MCP clients — the
token is their credential.

> **Prefer configuration-managed credentials?** A static token via `--token`
> or the `BINTRAIL_CONSOLE_TOKEN` environment variable (set where the console
> process starts, then restart it) also works, and is reported on the Connect
> AI page as environment-owned.


> **No console yet?** The [quickstart](quickstart.md) gets a full stack up in a
> few minutes. Come back to this page after.

---

## Step 2 — copy your MCP URL

Open the console in your browser → **Settings → Connect AI**. Copy the URL it
shows, e.g.:

```
http://your-host:8090/mcp
```

Two things worth knowing (the page handles both for you):

- **Multiple servers?** `/mcp` targets the console's default server;
  `/mcp/{name}` targets a specific one. The card shows the URL for whichever
  server you have selected in the sidebar.
- The URL only needs to be reachable **from the machine where the AI client
  runs** — LAN, VPN, or an SSH/SSM port-forward are all fine. Nothing needs to
  be on the public internet.

## Step 3 — install the bundle in Claude Desktop

1. Grab the `.mcpb` file from the **Connect AI** page's download button (or the
   [releases page](https://github.com/dbtrail/dbtrail/releases)) — pick the one
   matching the machine where Claude Desktop runs.
2. **Double-click it.** Claude Desktop opens an install dialog.
3. Fill in the two fields:
   - **Web address / MCP endpoint** — the URL from step 2
   - **Access token** — your console token (Claude Desktop stores it as a
     sensitive value)

That's it. No config files were harmed.

> **Platform note:** the Linux bundles (amd64/arm64) are built by CI on every
> release. The macOS bundles (darwin-arm64 and darwin-amd64) are attached by
> hand and MOST releases carry them, but a few skipped the step - if your
> release lacks them, take the newest release's darwin file or build your own
> with `make mcpb` (output in `dist/mcpb/`). There is no Windows bundle yet -
> on Windows, add the console as a claude.ai custom connector (public HTTPS
> deployments); it needs no download at all.

## Step 4 — ask something

Open a new Claude Desktop conversation. The `query`, `recover`, `recover_cascade`,
`status`, `list_schema_changes`, and `reconstruct` tools are now available. Try:

> "Show me the last 20 changes in the `wordpress` schema."
>
> "Someone fat-fingered an UPDATE on `users` around 3pm — what did it change,
> and write me the SQL to undo it."
>
> "Is the capture stream healthy? Any gaps I should worry about?"

Claude searches the **index** — the source database is never touched.

---

## Variants

**claude.ai (web) as a custom connector.** If your console is reachable over
public **HTTPS**, the same `/mcp` URL works directly: claude.ai → Settings →
Connectors → add custom connector. No bundle, no local install. (Not public?
Keep using Desktop + the bundle — it works over private networks.)

**Any other MCP client** (Claude Code, Cursor, …). Anything that launches stdio
MCP servers can use the bridge; the Connect AI page has a copy-paste snippet:

```json
{
  "mcpServers": {
    "dbtrail": {
      "command": "bintrail-mcp",
      "args": ["--connect", "http://your-host:8090/mcp", "--token", "YOUR_CONSOLE_TOKEN"]
    }
  }
}
```

**No console at all** (CLI-only setups): `bintrail-mcp` can talk to the index
directly with a DSN — see [mcp-server.md](mcp-server.md).

---

## When something doesn't work

| Symptom | Likely cause, in order of likelihood |
|---|---|
| **401 unauthorized** | Wrong token; or the token was wiped by a container recreation, which happens when the managed-token file is not on a mounted volume (`--mcp-token-file` / `BINTRAIL_CONSOLE_MCP_TOKEN_FILE`; the shipped compose sets it, see [Docker](docker.md)); or you're not talking to the console you think you are, such as a port-forward pointing at a stale process or another console on the same port. Check with `curl -H "Authorization: Bearer $TOKEN" http://host:8090/api/capabilities`: it should return JSON with `"mcp": true`. |
| **403 "no token configured"** | No token exists yet. Open **Settings → Connect AI** and click **Generate token** (step 1) — no restart needed. (Or set `--token` / `BINTRAIL_CONSOLE_TOKEN` and restart.) |
| **Connection refused / timeout** | The URL isn't reachable from the AI client's machine. `curl http://host:8090/api/healthz` from that machine; if you tunnel, remember tunnels can idle out — re-establish and retry. |
| **Bundle download 404s** | That release predates the bundles. Take the latest release, or build with `make mcpb`. |
| **Tools don't appear in Claude Desktop** | Check Desktop's MCP logs (Settings → Extensions): the bridge exits with a one-line reason — bad URL and rejected token are spelled out, it never hangs silently. |
| **Results look incomplete** | The MCP surface has the console's result caps (1,000 events / 10,000 recover statements per call). Ask Claude to narrow the time range or filter — that's the intended workflow, not a bug. |

---

## Security model, honestly

- The endpoint requires the console token on **every** request; unknown hosts
  are rejected by the same host-header allowlist as the rest of the console.
- The six tools are read-only and annotated as such (`ReadOnlyHint`); the AI
  cannot write to the index, the source, or anywhere else through them.
- Responses carry the console's field redactions — e.g. captured SQL statement
  text is withheld, same as the console UI.
- The token is worth protecting: anyone holding it can *read* your change
  history (row images included). Treat it like a database credential — prefer
  HTTPS or private networks, rotate it if leaked.
- Recovery SQL is **always** a draft for a human. Read it before you run it.

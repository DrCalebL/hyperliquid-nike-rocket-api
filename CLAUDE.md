# CLAUDE.md — hyperliquid-nike-rocket-api

> Keep this file lean (it loads into every session). Detailed change history lives in
> `docs/progress/` — read those on demand, don't paste them here.

<!-- FULL-WORKFLOW:START -->
## Full workflow (Opus 5.5, multi-agent)
Before each wave, reread root `CLAUDE.md`. The coordinator and every worker run Claude Opus 5.5 (`claude-opus-5-5`).
Routine work: the coordinator builds, then one fresh, independent, read-only reviewer checks it.
"Full workflow" (high-risk or explicitly requested) runs five stages in order, scaled to the task:
1. **Plan** — several independent planners converge on an exact consensus spec BEFORE any code.
2. **Build** — builders implement strictly to the spec (disjoint files, or worktrees where they would overlap).
3. **Review** — fresh adversarial reviewers on independent lenses (correctness, coverage, regression, security/money-safety, voice).
4. **QC** — the coordinator re-verifies every finding, fixes each worth fixing (nits too when cheap and correct), reruns affected gates, and sends material fixes to fresh review.
5. **Deliver** — CLAUDE.md for durable facts only + a dated `docs/progress/` entry; commit, fetch, rebase on current `origin/main`, rerun gates, push only the feature branch (`--force-with-lease` only after rewriting a published branch).
Keep this block identical in the root `CLAUDE.md` of all four rocket repos (massive-rocket-algos, kraken-nike-rocket-api, hyperliquid-nike-rocket-api, hyperliquid-vault-rocket-api) and log each touched repo in its `docs/progress/`.
<!-- FULL-WORKFLOW:END -->

## What this is
The **follower / customer service** for $NIKEPIG's Massive Rocket. The master
(`massive-rocket-algos`, a separate repo) trades its own account and broadcasts
signals; this service mirrors those signals onto **paying customers' own Hyperliquid
accounts**. FastAPI, deployed on **Railway** (`railway.json`). **Postgres-backed**
(SQLAlchemy). **Live money, customer-facing** — verify before deploy.

- Customers sign up, then enter their **Hyperliquid API-wallet** credentials at `/setup`.
  Creds are stored **encrypted** (Fernet, `CREDENTIALS_ENCRYPTION_KEY`) in
  `follower_users` — see `User.set_hl_credentials` / `get_hl_credentials`
  (`follower_models.py`). API wallets **cannot withdraw**, only trade.
- Billing is 30-day rolling (`billing_service_30day.py`, `config.py` fee tiers).

## Layout (key files)
- `main.py` — the FastAPI app: routes, admin endpoints, and the customer **dashboard
  HTML** (`/dashboard`, ~L1804) plus `/setup` (`/setup` served from `setup.html`).
  **Huge file.** The dashboard is one big Python **f-string**.
- `follower_endpoints.py` — customer API: `POST /api/setup-agent` (validates + encrypts
  + stores creds, sets `api_wallet_expires_at = now + 180d`, ~L1048), `GET
  /api/agent-status` (~L1079), `/api/start-agent`, `/api/stop-agent`.
- `follower_models.py` — SQLAlchemy models. `User` (`follower_users`) holds
  `hl_private_key_encrypted`, `hl_wallet_address`, `api_wallet_expires_at` (L89), billing.
- `api_expiry_service.py` — **authoritative** API-wallet expiry service: 6-hourly cron
  that emails reminders (30/14/7/3/1 days, Resend) and **deactivates** the agent on
  expiry (`days_left < 0` → `agent_active=false`). Also the admin `/admin/api-expiry/*`.
- `hosted_trading_loop.py` — executes signals per customer: IOC entry, fill confirmed from the position,
  then **SL first**, then TP, both sized to the confirmed fill. Every failure after the entry may have filled
  writes a `needs_recovery` row (`_record_unprotected_position`). A raised IOC is never re-sent (only a
  definitive 4xx rejection is retried). Dedup fails closed. `position_monitor.py`, `order_utils.py`,
  `price_cache.py`, `trade_reconciliation.py` support it.
- `position_guardian.py` (Wave 1, started in `main.py` after the expiry checker) — two asyncio tasks:
  the **recovery lane** (every 30 s, in **every** `GUARDIAN_MODE`, charge-only I/O) owns `needs_recovery` rows;
  the **full pass** (every 240 s, never within 180 s of a signal) runs the two-leg invariant.
  `GUARDIAN_MODE=off|shadow|enforce`, **default shadow** (shadow = log-only `GUARDIAN_WOULD_*`). Rowless
  positions only alarm. Its Info/Exchange carry a 10 s timeout; a timed-out SL placement is unknown, never
  a reason to close.
- `hl_rate.py` — process-wide HL weight meter (one 60 s window; assumes 1 Railway replica). Full-pass reads
  wait under `HL_WEIGHT_CEILING` (700) and `HL_GUARDIAN_WEIGHT_PER_MIN` (300); trading, monitor, mutations
  and the lane only charge. HL's own limit is 1200/min per IP.
- Shared Wave 1 modules: `bracket_guard.py` (byte-identical to `massive-rocket-algos`), `follower_guard.py`
  and `signal_guard.py` (byte-identical to `kraken-nike-rocket-api`). Never edit one copy alone.
- `config.py` — fee tiers + `utc_now()` / `ensure_utc_aware()` tz helpers.
- HTML: `setup.html`, `signup.html`, `login.html`; the dashboard is inline in `main.py`.

## ⚠️ Gotchas — verified, high-impact
1. **The `/dashboard` HTML is a Python f-string** (`html = f"""…"""`). Every literal
   CSS/JS brace must be **doubled** (`{{` / `}}`); `{api_key}` is the only interpolation.
   An undoubled brace either fails `py_compile` (if it forms an invalid expression) or —
   worse — silently interpolates a Python name at render and 500s `/dashboard` for
   **every** customer. Always render-test after editing it (see Gates).
2. **Two status renderers both write `#agent-status-display`.** `displayAgentStatus`
   (lean, banner-only) runs on a **30s interval**; `checkAgentStatus` (full — also badge +
   Start/Stop buttons + details) runs on load / refresh / after start-stop. Keep their
   per-state banner copy **byte-identical** or the banner flips every 30s, and remember
   the interval renderer does **not** manage badge/buttons except where explicitly mirrored.
3. **`api_wallet_expires_at` may be NAIVE UTC** (setup writes `datetime.utcnow()`).
   Always stamp `tzinfo=utc` before comparing. **Legacy NULL = unknown, never "expired."**
   The UI `expired` flag is `days_remaining < 0`, deliberately identical to the boundary
   `api_expiry_service.py` deactivates on, so UI and backend never disagree.
4. **Customer creds are encrypted at rest** and API wallets expire (max 180d). Renewal is
   customer self-serve via `/setup?key=<nk_ key>` — the service never sees raw keys long-term.
5. **Roll back the guardian with `GUARDIAN_MODE=off|shadow`, not a code redeploy.** Only the Wave 1
   recovery lane (`position_guardian.py`, runs in every mode) acts on `open_positions` rows with
   `status='needs_recovery'` (ENTRY_UNCONFIRMED, SL_/TP_FAILED_CLOSE_FAILED: possibly naked positions);
   pre-Wave-1 builds ignore them. If a code rollback is unavoidable, first run
   `SELECT id, user_id, hl_coin, reason FROM open_positions WHERE status='needs_recovery'` and resolve
   each position by hand (close it or attach an SL).
6. **error_logs names:** guardian rows are stored `GUARDIAN_<CODE>` (e.g. `GUARDIAN_ROW_AMBIGUOUS`),
   entry-path rows bare (`NEEDS_RECOVERY_RECORDED`, `POSITION_RECORD_FAILED`, `EMERGENCY_CLOSE_FAILED`,
   `HL_UNEXPECTED_STATUS`, ...). Query with the stored name.
7. **`ADMIN_PASSWORD` has no default** (SEC-3): every admin route answers 503 until it is set on Railway.
8. **`MASTER_API_KEY` has no default** (SEC-2): `/api/broadcast-signal` answers 503 while it is unset. The
   payload is checked by `signal_guard` (422 before any DB write; `risk_pct` required, `0 < risk_pct <= 0.03`).
9. **HL SDK shapes:** `open_orders()` has no `reduceOnly` and returns int oids; read legs with
   `frontend_open_orders` and compare oids as str. An envelope `status='ok'` does not mean filled: check
   `statuses[0]`, and prove a close with a flat re-read.

## Gates (this repo)
- `pip install -r requirements-gate.txt` (CPython 3.13; not the production set), then `python tools/run_gates.py`
  must print `GATES PASS` (0 failures, errors, skips; shared-file sha256 equal). Tests run offline with the
  network blocked; fakes live in `tests/fakes/`. `tools/chaos_follower.py` is owner-run only, on HL testnet.
- Dashboard edits: `python -m py_compile`; render the dashboard f-string in isolation (extract via `ast`, `eval`
  with only `api_key` in scope — proves no stray interpolation without the full app installed);
  unit-check server logic on edge inputs (NULL / naive / aware / past / today / malformed).
- Review findings are verified by a skeptic that tries to refute them; only CONFIRMED ones survive.

## Conventions
- Develop on the session's feature branch; commit + push there; **no PRs unless asked**.
- `python -m py_compile` after edits. **Live money** — verify before deploy.
- Timezone-aware datetimes everywhere expiry/billing is compared (naive stored = UTC).

## History
See `docs/progress/` (e.g. `2026-08-10-api-wallet-expiry-surfacing.md`, `2026-09-28-wave1-phase0-safety.md`).

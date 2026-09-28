# 2026-09-28 — Wave 1 / Phase 0 safety fixes (hyperliquid-nike-rocket-api)

Status: **built, reviewed, QC'd and gated on branch `claude/wave1-phase0-safety`. Not pushed. Not deployed.**
Last code commit before this docs commit: `fb55183` (43 commits ahead of `origin/main`).
Spec: `massive-rocket-algos/docs/superpowers/specs/2026-09-28-wave1-phase0-safety-design.md` (rev 3 + A1-A10).
Amendments: `massive-rocket-algos/docs/superpowers/plans/2026-09-28-wave1-coordinator-amendments.md`.
Review and QC data: `massive-rocket-algos/analysis/report/wave1_review.json`, `wave1_qc.json`, `wave1_rereview.json`
(untracked, local only).

Wave 1 touched three repos: `massive-rocket-algos` (masters), `kraken-nike-rocket-api` and
`hyperliquid-nike-rocket-api` (followers). `hyperliquid-vault-rocket-api` got only the CLAUDE.md workflow block.
Each touched repo has its own copy of this entry. The deploy runbook below is the same in all three.

## Why: the two P0 defects

1. **HL masters closed every position about 5 minutes after entry** (`hyperliquid-agent.py`,
   `hyperliquid-vault.py`, since a1fa2e6, 2026-03-19). The guardian compared int order ids with a set of str
   ids, so no tracked position ever counted as tracked. Its fallback read `reduceOnly` from `info.open_orders()`,
   which has no such field. Every bracketed position was judged naked and market-closed at the first guardian
   pass (about HH:05:10) and again at every restart. HL followers kept their own brackets, so they diverged from
   the master.
2. **The Kraken master could not open a position** (`kraken.py`, since b0b05c1, 2026-04-20).
   `place_3_order_bracket` called `self.force_cancel_all_orders`, which lives on the TPSL manager, not the bot.
   The AttributeError was caught, the entry was aborted with zero exchange calls, and the method returned None.
   The broadcast had already gone out, so Kraken followers traded signals the master never took, and the flat
   master re-broadcast every hour the signal stayed valid.

Around these, the planners found and verified many more gaps: guardians that never ran after the first hour
(Binance, Phemex), a 10-minute guardian gap around each decision (Kraken, HL), one-leg "protected" tests,
TP-first brackets with no stop during TP retries, a stop-limit SL fallback that can rest unfilled, fail-open
follower dedup, non-atomic state files, follower risk above the master's real risk, and a committed HL
broadcast key. The owner chose KEEP CURRENT RISK: no strategy, parameter or signal changes.

## What changed in this repo (HL follower)

- **Shared modules** (B1): `bracket_guard.py` (byte-identical to the master), `follower_guard.py` and
  `signal_guard.py` (byte-identical to the Kraken follower).
- **`hosted_trading_loop.py`**: IOC double-entry guard; fill confirmed from the position; SL first, sized to the
  confirmed fill, floored to the lot grid; `_record_unprotected_position` added: every post-entry failure
  writes a `needs_recovery` row; billing UPDATE separate from the 'open' INSERT; dedup fails closed.
- **`order_utils.py`**: a raised IOC is never re-sent (a definitive 4xx is retried); verified cancels.
- **Emergency close**: a close counts only on a real fill plus a flat re-read; the ladder continues on partial
  fills.
- **New `position_guardian.py`** (started in `main.py`): recovery lane every 30 s in every mode, charge-only
  I/O, rotated so no row starves; full pass every 240 s, `GUARDIAN_MODE=off|shadow|enforce`, default shadow;
  a critical GUARDIAN_SHADOW_SL_MISSING for a naked rowed position in shadow; GUARDIAN_PASS heartbeat.
- **New `hl_rate.py`**: one process-wide weight meter (`HL_WEIGHT_CEILING` 700, `HL_GUARDIAN_WEIGHT_PER_MIN`
  300); `position_monitor.py` only gained charge lines; Exchange objects built with cached meta.
- **`follower_endpoints.py`**: broadcast validated by `signal_guard` (422); SEC-2 `MASTER_API_KEY` no default.
  **SEC-3**: `ADMIN_PASSWORD` no default, 503 while unset, constant-time compare, no password in logs.
- **Gates**: `requirements-gate.txt`, `tools/run_gates.py`, offline SDK stubs and FakePool; `.env` ignored;
  `tools/chaos_follower.py` for the owner on HL testnet. 355 tests at HEAD.

## Review, QC and re-review

- **Review:** 12 fresh adversarial lenses (correctness x4, money-safety x2, regression x2, coverage x2, security,
  voice). A skeptic tried to refute every finding. **97 confirmed**: 7 high, 38 medium, 45 low, 7 nit.
  By lens: voice 21, coverage-masters 10, money-masters 9, money-followers 8, regression-masters 8, security 8,
  coverage-followers 7, correctness-kraken-hl 6, correctness-followers 6, correctness-core 5,
  correctness-binance-phemex 5, regression-followers 4.
- **QC:** the coordinator re-verified each finding and fixed it in 7 parts: core (bracket_guard, rocket_safety),
  binance, kraken-hl, phemex, tooling, kraken-follower, hl-follower. About 70 fix entries landed, each test-first.
  Items not fixed were skipped with a written reason (owner decisions, locked strategy code, design changes);
  they are listed under residual risks below.
- **Re-review of the QC fixes:** 13 new findings, 8 confirmed, 1 uncertain, 4 refuted. QC round 2 fixed 6 of the
  8: A10 keep-alarm dedup on HL/Kraken/Phemex (`massive-rocket-algos` 1197095); Kraken SL-failure close needs a
  sent close (2aa775a) and `_env_int` survives `inf` (5148387); HL lane rotation so a slow user never starves
  the newest rows (bbd1e6d), a timed-out SL placement is unknown and never a close reason (fb55183), and the
  entry is retried after a definitive 4xx (0f68135). The 2 left open are the master stop-limit items below.
- **Gates at HEAD (Deliver, 2026-09-29, CPython 3.13.7):** master `GATES PASS 939 0 0 0` (STRATEGY LOCK PASS);
  Kraken follower `GATES PASS n_tests=320`; HL follower `GATES PASS n_tests=355`; 0 failures, errors and skips;
  every shared-file sha256 equal across the repos.

## Residual risks (known, not fixed)

- **Owner stop-limits and trailing stops do not count as the SL on masters.** If the owner swaps the bot SL for
  a stop-limit or trailing stop, the guardian adds a stop-market at the recorded level, or closes at market if
  price is already past that level. An untracked position whose only stop is a stop-limit gets a stop-market at
  the same trigger and the owner's stop-limit is cancelled. Both fail toward the strategy's own SL. Needs the
  owner's A10 policy call (re-review masters 1 and 3).
- A well-formed false-flat read at the hourly decision still lets the locked `_hourly_decision` call
  `record_trade_outcome` (money-masters-4). The entry itself is blocked by a fresh read.
- No out-of-loop watchdog for GUARDIAN_STALL (coverage-masters-9). A hung guardian pass is seen only when the
  loop returns.
- The strategy lock was written at 59f4641. Hashing the scheduler and run loops into it needs a re-lock with a
  new protocol entry (regression-masters-1 remainder).
- A persistently unreadable state file now stops a master from starting (STATE_FILE_UNREADABLE, critical).
  Before, it silently fell back to `.bak` or defaults.
- `parity_report` exits 1 (`PARITY NOT EVALUATED`) until 30 exit-matched live trades exist. `guardian_log_audit`
  fails the week on any single GUARDIAN_EXCEPTION, GUARDIAN_BLIND or GUARDIAN_STALL.
- Phemex: `close_naked` closes a position with no exit-side stop in either the orders or the position fields.
  That includes TP-only positions, which base code left open. Its entry grace means a truly naked new entry is
  acted on at the first grid pass after the grace, up to about 5 minutes later. Enforce waits for the testnet
  shapes capture.
- Kraken follower: an entry rejected with an unmapped ExchangeError is now a missed trade, not a retry. The
  240 s flat wait before `closed_manual` can keep leftover legs resting, so dedup may skip a new signal for up
  to about 6 minutes (re-review nit, uncertain).
- HL follower: a raised IOC entry (5xx, dropped connection) is never re-sent. If no position appears within 8 s,
  the user gets an ENTRY_UNCONFIRMED `needs_recovery` row and misses that signal. The trading path still makes
  synchronous SDK calls (spec C42 pins them).
- Both followers: the admin password still travels as `?password=`. Moving it to a header breaks the admin
  dashboard and needs an owner/UI decision.
- The old HL key literal remains in git history and in the spec and plan docs. It is harmless once rotated.
- Per-step tags (`wave1-safety-*`, `wave1-parity-*`, `wave1-hotfix-a`, `wave1-safety-*-follower`) point at
  pre-QC commits. **Do not deploy them.** Deploy branch HEAD.

## Owner-open items (block the matching deploy step)

1. **HOTFIX-A / HL master consent** (required before any HL master deploy; record it in
   `massive-rocket-algos/docs/progress/`). Effects: (a) HL master positions are held to SL/TP for hours instead
   of being market-closed about 5 min after entry: more exposure, fees and funding; (b) no signal while a
   position is open, so HL followers get far fewer signals; (c) streak reversal becomes reachable on HL.
2. **Rotate the HL broadcast key** before SEC-1 (HL masters) and SEC-2 (followers) deploy. Set the new
   `MASTER_API_KEY` on every HL master host and on the HL follower service in one window. Set
   `FOLLOWER_API_URL` on each HL master host. Confirm which master broadcasts to which follower API. After
   SEC-1 an HL master without `MASTER_API_KEY` does not broadcast and raises a critical alarm.
3. **Tell followers about risk parity** before the kraken.py or HL master deploy. Follower `risk_pct` becomes
   the master's real fraction: 1.25% / 2.5% in drawdown mode (where the 1.94 seed keeps HL and the flat Kraken
   master), 1% on reversal trades. Today it is 2% / 3%.
4. **Stop-limit / trailing-stop policy under A10** (see residual risks): count them as the SL on masters, alarm
   instead of closing, or write the exclusion into A10.
5. **Phemex `close_naked` canary**: the dryrun hour must see at least one live Phemex bracket (a GUARDIAN_PASS
   with a position open) and zero PHEMEX_POSITION_NO_SL_SEEN. A flat book must not pass it by default.
6. **SEC-3 admin password**: set `ADMIN_PASSWORD` (non-default) on both follower services before they deploy.
   Every admin route answers 503 while it is unset. Check without printing it.
7. Still open from A10: follower counts and AUM; whether master hosts keep `*_tpsl_orders.json` and
   `*_streak_state.json` across redeploys; Railway replica count (must be 1 for both followers).

## DEPLOY RUNBOOK

Deploy **branch HEAD** of `claude/wave1-phase0-safety` in each repo, after a rebase on current `origin/main`
and a green gate run. Never deploy a `wave1-*` tag: they predate the QC fixes. HEAD also contains the
risk-parity and security commits, so their preconditions (steps 0.3-0.5) apply to the first deploy.

### Step 0 — Owner pre-flight (read-only)
1. Gates at the deploy commit. Master: `python tools/run_gates.py` -> `GATES PASS`. Each follower:
   `pip install -r requirements-gate.txt` (CPython 3.13), then `python tools/run_gates.py` -> `GATES PASS`.
2. Owner chaos runs, one PASS report per venue archived in `docs/progress/` before its enforce step:
   masters `python tools/chaos_testnet.py --venue <v> --cases a,b,c,d,e --runs 20 --i-accept-testnet-orders`
   (Binance on Binance Demo Trading); followers `python tools/chaos_follower.py --cases a,b,c,d,e --runs 20`.
   PASS = 100% restored within 330 s, SL absent <= 330 s, zero false closes.
3. Record HOTFIX-A consent (owner item 1). Without it, skip every HL master step.
4. Tell followers about risk parity (owner item 3).
5. Follower services, checked without printing values: `MASTER_API_KEY` and `ADMIN_PASSWORD` set and not the old
   defaults; replicas = 1; `signals.risk_pct` is double precision. Rotate the HL key (owner item 2).
6. HL weight baseline: `SELECT COUNT(*) FILTER (WHERE hl_private_key_encrypted IS NOT NULL) AS cred_users,
   COUNT(*) FILTER (WHERE agent_active) AS active_users FROM follower_users`. Steady load is about
   20 x cred_users + 2 x open rows per minute. If that is >= 700, the HL follower ships with
   `GUARDIAN_MODE=off` (its recovery lane still runs).
7. Master hosts: timezone offset is a whole hour; state JSON files survive restarts.

### Step 1 — Followers first, shadow mode, 48 h
1. Deploy between HH:15 and HH:45 so startup bursts drain before the next HH:00 signal.
   Kraken follower: `GUARDIAN_MODE=shadow`. HL follower: `GUARDIAN_MODE=shadow` (or `off` per 0.6).
2. Live at once in any mode: SL-first brackets sized to the fill, close-first TP fallback, `needs_recovery`
   rows, fail-closed dedup, 422 payload validation, no entry re-send, in-flight tokens, HL close verification,
   the HL recovery lane, SEC-2 and SEC-3. The Kraken legacy guardian still acts, with its safety edits.
3. After 48 h, review `error_logs` by stored name: zero `GUARDIAN_WOULD_CLOSE` on healthy positions; every
   `GUARDIAN_ROW_AMBIGUOUS` explained; no `GUARDIAN_BUDGET`; HL `HL_WEIGHT` below the ceiling; every
   `NEEDS_RECOVERY_RECORDED` row handled within about 2 min; no `NEEDS_RECOVERY_RECORD_FAILED`.
4. Then set `GUARDIAN_MODE=enforce` on each follower (this also enables TP-failure-keep).
5. Rollback: Kraken `GUARDIAN_MODE=legacy`; HL `GUARDIAN_MODE=off` or `shadow` (the lane keeps running). Prefer
   the env switch to a code redeploy. Before any code rollback, drain `needs_recovery` rows by hand
   (`SELECT id, user_id, reason FROM open_positions WHERE status='needs_recovery'`): pre-Wave-1 code ignores
   them, and it also restores the old key and password defaults.

### Step 2 — Masters, one port per day, flat book where possible
Order: HL agent and vault (only with consent, rotated key and followers told) -> Binance -> Kraken -> Phemex.
Per port:
1. On the host: `git tag pre-wave1-<port>-<date>`; copy the state JSON files to `backups/<UTC>/`.
2. Deploy the HEAD tree: the port file plus `bracket_guard.py` and `rocket_safety.py` (every port imports them).
3. Start with `ROCKET_GUARDIAN_MODE_<PORT>=dryrun` for 1 h: 12 GUARDIAN_PASS lines, zero GUARDIAN_WOULD on a
   healthy bracket, no critical rows in `logs/alarms.jsonl`.
4. Restart in enforce (unset the variable or set `enforce`). Watch the first live trade end to end: SL placed
   first, a guardian pass about 5 s after entry, the exit classified in `logs/trade_journal.jsonl`.
- HL: set `MASTER_API_KEY` (new key) and `FOLLOWER_API_URL` first (SEC-1 removed the fallback).
- Kraken: `KRAKEN_MASTER_EXECUTE` stays unset (0). It still broadcasts, so risk parity goes live here.
- Phemex: dryrun hour (see owner item 5), then `ROCKET_GUARDIAN_MODE_PHEMEX=close_naked`. Enforce only after
  the Phemex testnet `shapes.json` exists. The global `ROCKET_GUARDIAN_MODE` never selects Phemex enforce.
- Rollback: `ROCKET_GUARDIAN_MODE_<PORT>=dryrun` + restart, or `git checkout pre-wave1-<port>-<date> --
  <port>.py` (old code ignores the new modules; v2 state files keep v1 keys). A Binance or Phemex code rollback
  brings back the dead guardian: watch by hand. An **HL code rollback brings back the 5-minute false close**:
  use dryrun instead. `HALT_NEW_ENTRIES` in the working dir stops entries and broadcasts; guardians keep running.

### Step 3 — Operate
- Weekly: review `logs/alarms.jsonl` and run `python tools/guardian_log_audit.py logs/*.log` on each master;
  review follower `GUARDIAN_ROW_AMBIGUOUS`, `GUARDIAN_FLAT_ROW_UNRECORDED`, `NEEDS_RECOVERY_*` and `HL_WEIGHT`.
- `python tools/parity_report.py` after each deploy and monthly. Live acceptance over the next 150 trades:
  zero SL_GAP / UNPROTECTED_INTERVAL over 330 s, every exit SL, TP or closed_by guardian/entry_path, zero
  unexplained EXIT_OTHER, follower `signals.risk_pct` equal to BROADCAST_RISK.
- Rollback triggers: a guardian close of a position with both legs live; unexplained SL_GAP or
  UNPROTECTED_INTERVAL; GUARDIAN_STALL twice in 24 h; CLASSIFIER_SUSPECT, GUARDIAN_CHURN or SL_UNMAINTAINABLE;
  unexplained ROW_AMBIGUOUS; repeated GUARDIAN_BUDGET; DECISION_MISSED more than daily; unexpected follower
  skips; any NEEDS_RECOVERY_RECORD_FAILED. Record every rollback in `docs/progress/`.

## Not done in this Deliver pass
- Not pushed. Before pushing: fetch, rebase on current `origin/main`, rerun the gates, push only
  `claude/wave1-phase0-safety`.
- Nothing deployed and nothing sent to any venue.

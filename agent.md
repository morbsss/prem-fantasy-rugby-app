# Build Spec — Meatyboys Rugby Fantasy

> **You are extending an existing repository** that already contains a skeleton for a fantasy league app. Build the features below on top of it. Do **not** scaffold a new project or replace the existing structure.

---

## 0. Your task as the agent (read first)

Before writing any code:

1. **Explore the repo.** Read the README, the package manifest (`package.json` / `requirements.txt` / etc.), the existing data models, routes/controllers, components, and the current styling/theme (CSS variables, Tailwind config, or theme file). Match the existing stack, conventions, naming, and folder layout — do not introduce a new framework or styling system.
2. **Report what you found** in one short paragraph (stack, where models/routes/components/styles live, what the skeleton already implements) before you start, so we agree on the foundation.
3. **Build in milestones** (see §9). After each milestone, summarise what changed and what's testable. Don't attempt the whole app in one pass.
4. **Keep it runnable at every step.** External data (SuperBru, ESPN) is unreliable to depend on during development — implement the **adapter + mock pattern in §3** so the app runs end-to-end with seeded mock data and no live network access.
5. **When something is genuinely ambiguous**, consult §8. If a default is stated there, proceed with it and note the assumption. Only stop to ask if a decision would be expensive to reverse.

---

## 1. Product summary

A rugby fantasy app running **two independent leagues concurrently**:

| League | Real competition | Default theme |
|---|---|---|
| `meatyboys` | Super Rugby Pacific | Existing repo colours |
| `Owen Farrell Disappreciation Society` | English Premiership Rugby | Red, blue, white |

Users sign up, join one league, name a team, take part in a snake draft, then manage a squad through a round-robin regular season followed by playoffs.

---

## 2. Stack & constraints

- Inherit the stack, language, package manager, DB, auth approach, and styling system **already present in the repo**. Fill these in from your exploration in §0 rather than assuming.
- All persistent timestamps stored in **UTC**; convert to local competition timezones only at display/scheduling boundaries (see §4.5).
- Secrets (DB creds, any scraping endpoints) via environment variables, never committed.

---

## 3. Architecture guidance

**Data-source abstraction (required).** Do not call SuperBru/ESPN directly from business logic. Define a small interface per data type, with two implementations:

- a **live adapter** (SuperBru scraper / ESPN client), and
- a **mock adapter** that returns realistic seeded fixtures, lineups, players, and scores from local files.

Select the adapter via config/env (e.g. `DATA_SOURCE=mock|live`), defaulting to `mock`. This keeps the whole app buildable and testable offline and isolates the brittle scraping code.

**Scheduler.** A single timezone-aware scheduler/cron service drives the ingestion jobs in §4. Each job is idempotent and logs its run. It must support per-league timezones and DST.

**Layering.** Keep ingestion → domain (scoring, standings, draft, playoffs) → API → UI cleanly separated so scoring logic is unit-testable without a network or browser.

---

## 4. Data sources & ingestion

### 4.1 Players — source: SuperBru
Scrape the full player pool for each competition. Persist player identity + position so they can be drafted and scored. Expose via the player adapter (§3).

### 4.2 Season fixtures — source: ESPN
Ingest the full season fixture list per competition: every round's fixtures with **date and kickoff time**. Derive from this (a) the current **round number** and (b) whether any match is **live** (now is within a fixture's game window). These two derived values gate the live-scraping jobs below.

> **Note:** ESPN does not publish an official public rugby API. Build the ESPN client against whatever endpoint is available behind the adapter, and treat the mock adapter as the contract of record. If the live endpoint can't be reached, the app must still function on mock data.

### 4.3 Round lineups — source: ESPN
Scrape team lineups weekly and **join them to players** with a status flag: `S` = starting, `B` = bench, `O` = out. Run on this schedule:

| Competition | Window | Frequency | Timezone |
|---|---|---|---|
| Super Rugby Pacific | Wed 15:00 before the round → then Fri 15:00 to Sun 17:00 | every 2 hours within the Fri–Sun window | AEST/AEDT |
| English Premiership | Thu 14:00 → Sun 18:00 | every 2 hours | GMT/BST |

### 4.4 Player round scoring data — source: SuperBru
- **During live matches:** scrape player scoring data every **3 minutes**.
- **Finalisation:** a definitive scrape at **Monday 12:00** following the gameweek (AEST/AEDT for Super Rugby; GMT/BST for the Premiership). The finalised values are authoritative for standings.

### 4.5 Timezone & DST handling
Use IANA zones — `Australia/Sydney` (Super Rugby) and `Europe/London` (Premiership) — so AEST↔AEDT and GMT↔BST switch automatically. Schedule jobs in the competition's local zone; store results in UTC.

---

## 5. Domain rules

### 5.1 Leagues
Two leagues run independently with separate players, fixtures, drafts, standings, and themes (§1). A user belongs to one league.

### 5.2 League size & byes
- A league has **8–10 fantasy teams**.
- **Any league with an odd team count gives one team a bye each round, rotating** so every team gets an equal number of byes over the season. (The example given — 9 teams gets a bye — is the odd-count case.)
- A team on bye scores the **average of all other teams' scores in that league for that round**, and that result counts toward standings like a normal fixture.

### 5.3 Regular season
- Each round is a **round-robin** of fantasy-vs-fantasy matchups.
- The full season fixture list is **randomly generated one week before the real season starts** and then fixed.
- The **last 3 real rounds are reserved for playoffs** (§5.5), not regular-season fixtures.

### 5.4 Scoring & standings
A fantasy team's match score is the sum of its scoring players' SuperBru round data (§4.4). League points per matchup:

| Outcome | League points |
|---|---|
| Win | 4 |
| Tie | 2 (each team) |
| Loss | 0 |
| **Bonus:** win by ≥ 27 | +1 |
| **Bonus:** lose by ≤ 11 | +1 |

**Standings order:** by league points, then **higher `Points For`** breaks ties. Track `Points For` and `Points Against` per team.

### 5.5 Playoffs (final 3 rounds)
- **Top 4** of the regular-season table → **Championship bracket**. **Bottom 4** → **Sacko bracket**.
- Each bracket runs **two-legged aggregate semi-finals** (rounds 1–2 of the playoff window) and a **single final** (round 3):
  - **Championship:** semi-final *winners* advance; the final winner is the **Champion**.
  - **Sacko:** inverted — semi-final *losers* advance; the final **loser is the Sacko**.
- Seeding within each bracket mirrors regular-season rank (1v4, 2v3 for Championship; the inverse logic for Sacko — confirm exact Sacko seeding, see §8).
- See §8 for how middle teams are handled when a league has 9 or 10 teams.

---

## 6. User flows

### 6.1 Auth & onboarding
1. Sign up / sign in with **email + password**.
2. New user chooses to **join a fantasy league**: Super Rugby Pacific or English Premiership.
3. They pick the specific league: `meatyboys` or `Owen Farrell Disappreciation Society`.
4. They set a **team name**.

### 6.2 Fantasy draft
- The **commissioner** sets the draft date/time, which **must complete the week before** that league's real season starts, and inputs the **draft order**.
- Draft runs in **snake order** until every team has filled this roster:

| Slot | Count |
|---|---|
| Front Row | 1 |
| Lock | 1 |
| Loose Forwards | 2 |
| Half Back | 1 |
| Fly Half | 1 |
| Midfielders | 2 |
| Outside Backs | 3 |
| Bench (any position) | 6 |
| **Total** | **17** |

- All users must log in and join the live draft. **If a user is not present, the system auto-drafts** valid players that satisfy the roster rules above on their behalf.

---

## 7. UI / UX

**Keep the current design.** Only add/adjust the following.

**Theming states:**
- After login, while the user is **choosing** a league → render the site in **grayscale**.
- Once a league is chosen → apply its theme: **existing repo colours** for Super Rugby Pacific; **red / blue / white** for the English Premiership. Implement via the repo's existing theme mechanism (CSS variables / theme tokens), not hardcoded one-offs.

**Primary tabs:** `Squad` · `Fixtures` · `League Table` · `Player Hub` · `Transfers`.

**League table extras:**
- Per-team **up/down/no-change arrow** vs the team's position last week.
- A **graph of each team's historical league position** over the season (line chart, lower = better rank).

---

## 8. Open questions / assumptions (confirm or adjust)

1. **Playoff middle teams (9–10-team leagues).** Top 4 → Championship, bottom 4 → Sacko leaves 1 leftover (9 teams) or 2 (10 teams). **Default:** those middle teams finish the season at their regular-season rank and don't play in the playoff window. Confirm if you'd rather expand the brackets.
2. **Sacko seeding/aggregation.** Mirror Championship (1v4, 2v3 by inverse rank) with losers advancing. Confirm the exact pairing and how aggregate is computed (combined two-leg score).
3. **Transfers tab scope.** Not specified in the source. **Default:** waiver/free-agent pickups against the undrafted player pool, swapping in/out of the 17-man roster, locked during live game windows. Confirm the rules you want.
4. **Squad lock timing.** **Default:** starting XV locks at each player's real kickoff. Confirm.
5. **Commissioner assignment.** **Default:** the league's creator is commissioner. Confirm.
6. **Live external endpoints.** SuperBru/ESPN have no official public APIs; the live adapters are best-effort and the app is built/tested against the mock adapter. Confirm this is acceptable.

---

## 9. Build milestones

1. **Repo recon + plan** — report stack/structure/theme; confirm assumptions in §8.
2. **Domain model + migrations** — leagues, teams, users, players, fixtures, rosters, lineups, round scores, standings.
3. **Data-source adapters + mock seed data** (§3, §4) so the app runs offline.
4. **Auth & onboarding flow** (§6.1) + theming states (§7).
5. **Draft engine** — snake order, roster rules, live join, auto-draft (§6.2).
6. **Scheduler + ingestion jobs** (§4) with timezone/DST handling.
7. **Scoring & standings** — matchup scoring, bonus points, tiebreaks, byes (§5.2–5.4).
8. **Fixture generation + regular season + playoffs** (§5.3, §5.5).
9. **UI** — five tabs, league-table arrows, historical-position graph (§7).
10. **Tests** — unit tests for scoring, standings, byes, draft validity, and playoff progression.

---

## 10. Acceptance criteria

- App runs end-to-end on mock data with no live network access.
- Scoring (win/tie/loss + both bonus conditions) and tiebreak by `Points For` are unit-tested.
- Bye scoring equals the average of other teams' scores that round.
- Snake draft yields a valid 17-player roster per team; absent users are auto-drafted to a valid roster.
- Round-robin fixtures are generated once, a week before the season, and fixed thereafter.
- Playoffs use two-legged aggregate semis + single final, with the correct Championship (winners advance → Champion) and Sacko (losers advance → Sacko) logic.
- League choice toggles theming: grayscale during selection, then the correct league theme.
- League table shows per-team movement arrows and a historical-position graph.

## New Updates
- Loosen the rules set in 6.2, user teams have have any players they want and trade between any players they want. However, when picking their starting team they most comply with the rule in 6.2.
- When making a trade with a free agent or another user, give a 'Are you sure?' option 
- In Player Hub, allow for filters:
1. Round
2. User Teams (including All and Free Players)
3. Remove Price, that becaomes reduntant
4. Points Type (Form (last 3 games ave), total points (tie to round if a round is picked))
- For Front Row, make it the real team front row, so instead of picking a single player, you pick the 2xProps and 1xHkers and any benched players for that game.
- Remove the 2 semi finals fixtures in the fixtures page
- In the fixtures page, make the bye ave v user the last game in the table 
- Add a Match Up tab, where you can filter out the fixture and have the user teams lined up next to each other with totaly scores at the bottom
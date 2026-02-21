# Project Backlog

Use this file to track bugs and features for the assignment bot and teacher dashboard.

## Item Template

- `ID`:
- `Type`: Bug | Feature | Improvement
- `Title`:
- `Priority`: P0 | P1 | P2
- `Status`: Todo | In Progress | Blocked | Done
- `Owner`:
- `Area`: bot | dashboard | database | sync | ai
- `Problem`:
- `Expected`:
- `Steps to Reproduce` (bugs only):
  1.
  2.
- `Acceptance Criteria`:
  - [ ]
  - [ ]
- `Notes`:

---

## BL-001

- `ID`: BL-001
- `Type`: Bug
- `Title`: Campaign "Send now" should deliver immediately
- `Priority`: P0
- `Status`: Todo
- `Owner`: unassigned
- `Area`: dashboard
- `Problem`: A campaign created with "Send now" can remain pending and not send to learners right away.
- `Expected`: "Send now" creates and executes the campaign in the same action.
- `Steps to Reproduce` (bugs only):
  1. Open Campaigns tab.
  2. Select a template and choose "Send now".
  3. Create campaign and check Recent Jobs.
- `Acceptance Criteria`:
  - [ ] "Send now" jobs execute without waiting for scheduler time checks.
  - [ ] Recent Jobs shows accurate target and sent counts after creation.
  - [ ] Failure states are visible with clear error text.
- `Notes`: Related to missing-work campaign operations.

## BL-002

- `ID`: BL-002
- `Type`: Bug
- `Title`: Learner summary can become stale after sync/import
- `Priority`: P1
- `Status`: Todo
- `Owner`: unassigned
- `Area`: database
- `Problem`: Some learner totals and averages are outdated after data changes.
- `Expected`: Summary values are rebuilt whenever assignment/submission data changes.
- `Steps to Reproduce` (bugs only):
  1. Import/update classroom data.
  2. Compare assignment rows vs learner summary in dashboard.
  3. Observe mismatch in totals or average.
- `Acceptance Criteria`:
  - [ ] Summary rows are refreshed after sync/import/update operations.
  - [ ] Dashboard and bot show matching totals for the same learner.
  - [ ] A manual rebuild option still works and is idempotent.
- `Notes`: Include validation query in final fix notes.

## BL-003

- `ID`: BL-003
- `Type`: Feature
- `Title`: Natural-language filters for learner assignment views
- `Priority`: P2
- `Status`: Todo
- `Owner`: unassigned
- `Area`: bot
- `Problem`: Learners must use fixed buttons/flows and cannot quickly ask for filtered views (for example quiz-only or due-this-week).
- `Expected`: Bot supports simple natural-language filters and returns formatted filtered results.
- `Steps to Reproduce` (bugs only):
  1. N/A
  2. N/A
- `Acceptance Criteria`:
  - [ ] Queries like "show only quiz work" and "show this week due" are recognized.
  - [ ] Filters apply to learner-specific assignments only.
  - [ ] Response format is concise and easy to scan on mobile.
- `Notes`: Start with keyword/rule based parser before adding LLM intent parsing.

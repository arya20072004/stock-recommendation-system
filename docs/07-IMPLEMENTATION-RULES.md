# StockIntel UI Redesign --- Implementation Rules

**Document:** `07-IMPLEMENTATION-RULES.md`\
**Product:** StockIntel\
**Status:** Implementation specification\
**Audience:** Coding agents, software engineers, reviewers\
**Purpose:** Define how the approved StockIntel redesign must be
implemented in the existing codebase.

------------------------------------------------------------------------

# 1. Purpose & Authority

This document defines the operational rules for implementing the
StockIntel UI redesign.

It translates the approved design documentation into implementation
constraints.

The implementation agent must use these eight redesign documents together:

1. `docs/ui-redesign/00-DESIGN-REDESIGN-MASTER.md`
2. `docs/ui-redesign/01-UX-PRINCIPLES.md`
3. `docs/ui-redesign/02-INFORMATION-ARCHITECTURE.md`
4. `docs/ui-redesign/03-DESIGN-SYSTEM.md`
5. `docs/ui-redesign/04-THEME-SPECIFICATION.md`
6. `docs/ui-redesign/05-PAGE-SPECIFICATIONS.md`
7. `docs/ui-redesign/06-COMPONENT-SPECIFICATIONS.md`
8. `docs/ui-redesign/07-IMPLEMENTATION-RULES.md`

When documents conflict, use the authority hierarchy defined below and the
most recent explicit user instruction.

The implementation agent must never use implementation convenience as
justification for silently changing approved product semantics.

------------------------------------------------------------------------

# 1A. Authority and Precedence

Use the following precedence when interpreting the redesign documents:

```text
Backend / API / ML / evaluation semantics
                ↓
00-DESIGN-REDESIGN-MASTER.md
                ↓
01-UX-PRINCIPLES.md
                ↓
02-INFORMATION-ARCHITECTURE.md
                ↓
03-DESIGN-SYSTEM.md
                ↓
04-THEME-SPECIFICATION.md
                ↓
05-PAGE-SPECIFICATIONS.md
                ↓
06-COMPONENT-SPECIFICATIONS.md
                ↓
07-IMPLEMENTATION-RULES.md
```

Backend/API/ML semantics are authoritative for:

- prediction meaning;
- confidence meaning;
- canonical ranking;
- maturity;
- valid trading-session calculations;
- settlement;
- evaluation;
- outcome;
- return;
- correctness;
- model version;
- sentiment/relevance fields;
- any other domain metric.

UI documentation must never override actual canonical backend semantics.

If the backend contradicts a UI assumption, report the discrepancy rather than
inventing a frontend interpretation.

# 2. Implementation Philosophy

The redesign is an evolution of an existing production system, not
permission to rebuild everything from scratch.

The implementation should:

-   preserve working backend behavior;
-   preserve production data semantics;
-   reuse sound existing infrastructure;
-   replace weak UI structures deliberately;
-   introduce reusable components;
-   preserve analytical density;
-   improve hierarchy and comprehension;
-   support light and dark themes equally;
-   handle imperfect data honestly.

The goal is:

> **A coherent intelligence product built on the existing StockIntel
> system, not a visually impressive rewrite that quietly changes how
> StockIntel works.**

------------------------------------------------------------------------

# 3. Mandatory Agent Workflow

Every implementation task must follow:

``` text
UNDERSTAND
    ↓
INSPECT
    ↓
PLAN
    ↓
IMPLEMENT
    ↓
VALIDATE
    ↓
REVIEW AGAINST SPECIFICATION
    ↓
REPORT
```

Skipping inspection or validation is not acceptable.

------------------------------------------------------------------------

## 3.1 Understand

Before editing:

-   identify the requested scope;
-   identify the relevant page specification;
-   identify relevant component specifications;
-   identify relevant design-system/theme requirements;
-   identify dependencies between frontend and backend;
-   identify constraints and acceptance criteria.

Do not begin coding based only on a screenshot or vague visual
interpretation.

------------------------------------------------------------------------

## 3.2 Inspect

Inspect the existing codebase before changing it.

At minimum, determine:

-   frontend framework;
-   application entry points;
-   routing;
-   page structure;
-   shared components;
-   styling architecture;
-   theme implementation;
-   data-fetching layer;
-   API contracts;
-   state management;
-   existing tests;
-   build/lint/type-check commands;
-   relevant environment/configuration.

Search before creating.

If an existing component already solves most of the problem, evaluate
whether it should be extended rather than duplicated.

------------------------------------------------------------------------

## 3.3 Plan

Before substantial implementation, identify:

-   files to change;
-   components to reuse;
-   components to create;
-   API/data dependencies;
-   theme implications;
-   responsive implications;
-   tests required;
-   visual QA required.

For larger tasks, state the intended implementation sequence.

Do not modify unrelated areas merely because they are nearby.

------------------------------------------------------------------------

## 3.4 Implement

Implement the smallest coherent change that satisfies the specification.

Prefer:

-   existing project patterns;
-   reusable components;
-   semantic tokens;
-   existing API clients;
-   existing utilities;
-   focused changes.

Avoid:

-   unnecessary rewrites;
-   speculative abstractions;
-   unrelated refactors;
-   duplicate logic;
-   temporary hacks that become permanent architecture.

------------------------------------------------------------------------

## 3.5 Validate

Run the applicable:

-   type checks;
-   lint;
-   unit tests;
-   integration tests;
-   build;
-   relevant API checks;
-   responsive checks;
-   light/dark checks;
-   visual inspection.

Do not report a check as passed unless it was actually performed.

------------------------------------------------------------------------

## 3.6 Review Against Specification

After implementation, compare the result against:

-   page specification;
-   component specification;
-   theme specification;
-   UX principles;
-   acceptance criteria.

Explicitly check for:

-   hierarchy drift;
-   invented UI semantics;
-   missing states;
-   theme inconsistencies;
-   responsive failures;
-   accessibility regressions;
-   unnecessary scope.

------------------------------------------------------------------------

## 3.7 Report

Every implementation task must end with a concise implementation report
containing:

### Changes

What was changed.

### Files

Files created, modified, or deleted.

### Requirements

Which specification requirements were satisfied.

### Validation

Commands/checks actually run and their results.

### Visual QA

Themes/viewports inspected.

### Known Issues

Remaining issues.

### Deviations

Any deviation from specification, including why it was necessary.

If no deviations exist, explicitly state:

> No specification deviations.

------------------------------------------------------------------------

# 4. Existing Codebase First

The existing codebase must be treated as the starting point.

Before replacing an implementation, determine:

-   why it exists;
-   what depends on it;
-   whether it contains hidden domain behavior;
-   whether it is used elsewhere;
-   whether it contains accessibility behavior;
-   whether it is coupled to API/state logic.

Do not replace a component simply because its markup is visually
outdated.

------------------------------------------------------------------------

# 5. Architecture Preservation

## 5.1 Preserve working architecture

Do not change:

-   framework;
-   routing architecture;
-   state architecture;
-   API architecture;
-   build system;

unless the requested task requires it or the user explicitly authorizes
it.

## 5.2 Separate concerns

Maintain separation between:

``` text
UI
↓
presentation state
↓
API/data layer
↓
backend/domain logic
```

Do not bury business logic inside visual components.

## 5.3 Reuse existing utilities

Before creating:

-   date utilities;
-   number formatting;
-   API helpers;
-   query builders;
-   responsive utilities;
-   theme helpers;

search the codebase for existing equivalents.

------------------------------------------------------------------------

# 6. Backend & API Preservation

UI redesign work must not silently alter backend behavior.

The frontend must consume canonical API contracts.

Do not change API semantics merely to make UI implementation easier.

If an API response lacks information required by the approved UX:

1.  identify the missing data;
2.  determine whether an existing endpoint already exposes it;
3.  document the gap;
4.  propose a backend change if necessary;
5.  do not fabricate the missing value.

------------------------------------------------------------------------

# 6A. Missing Canonical Backend Fields

When a UI requirement depends on a canonical backend field:

```text
UI requirement
      ↓
Inspect existing API/backend
      ↓
Field exists?
 ├── YES → render canonical value
 └── NO  → inspect whether equivalent canonical data exists elsewhere
             ↓
          if genuinely absent:
             render an explicit unavailable state where appropriate
             and report the missing backend contract
```

Do not:

- recreate backend domain logic in the frontend;
- estimate missing values;
- calculate maturity independently;
- calculate settlement independently;
- invent confidence tiers;
- invent ranking logic;
- fabricate metrics.

A missing backend field is an implementation dependency or blocker, not
permission to create an alternative frontend definition.

# 7. ML, Prediction & Evaluation Semantics

This section is mandatory for StockIntel.

The frontend must not redefine model behavior.

## 7.1 Prediction signal

BUY/HOLD/SELL must come from the canonical prediction output.

Do not derive a signal from:

-   price movement;
-   confidence;
-   sentiment;
-   frontend thresholds;

unless the backend explicitly defines that relationship.

## 7.2 Confidence

Display confidence according to its actual model/API meaning.

Do not assume it represents:

-   probability of profit;
-   probability of price increase;
-   expected return.

Do not invent confidence tiers without documented semantics.

## 7.3 Recommendation ranking

If the backend provides a canonical recommendation ranking, use it.

Do not replace it with:

``` text
sort by confidence
```

or another frontend-created ranking unless explicitly specified.

## 7.4 Historical predictions

Historical prediction records must be treated as immutable snapshots.

Current model state must not overwrite historical:

-   signal;
-   confidence;
-   model/version;
-   prediction context;
-   prediction price/data basis.

## 7.5 Prediction maturity

The production evaluation horizon is based on valid trading sessions.

The frontend must not calculate maturity using calendar-day arithmetic.

Do not independently implement a trading calendar unless explicitly
required and formally specified.

## 7.6 Evaluation

Evaluation status and outcome must come from canonical evaluation logic.

The frontend must not independently decide whether a prediction was:

-   correct;
-   incorrect;
-   profitable;
-   unsuccessful.

## 7.7 Settlement

Evaluation must use the canonical settlement-price basis defined by the
production system.

The frontend must not substitute another:

-   closing price;
-   market-data source;
-   timestamp;
-   settlement rule.

## 7.8 Unavailable evaluation

An unavailable/failed evaluation is not automatically an incorrect
prediction.

Represent:

> Unable to evaluate

unless the canonical evaluator explicitly provides an incorrect result.

------------------------------------------------------------------------

# 8. Data & State Handling

## 8.1 Explicit state model

Components and pages must distinguish:

-   loading;
-   empty;
-   no results;
-   unavailable;
-   error;
-   stale;
-   pending;
-   evaluated;
-   demo.

## 8.2 Missing values

Never replace missing financial/model values with:

-   zero;
-   fabricated placeholders;
-   stale values presented as current.

Use an explicit unavailable representation.

## 8.3 Zero values

Zero is valid data.

Do not use the same visual treatment for:

``` text
0
```

and:

``` text
unavailable
```

## 8.4 Partial failure

If one data source fails:

-   preserve usable content;
-   isolate the error;
-   explain which section is affected.

Do not turn an auxiliary failure into a complete-page failure.

## 8.5 Stale data

Stale data must not be presented as current.

Where relevant, expose:

-   data date;
-   timestamp;
-   stale indicator.

## 8.6 Demo data

Demo data must remain unmistakable.

Never use demo data to fill production empty states without an explicit
demo mode.

------------------------------------------------------------------------

# 9. Design-System Implementation

## 9.1 Use approved tokens

Use the semantic tokens defined in `03-DESIGN-SYSTEM.md` and
`04-THEME-SPECIFICATION.md`.

Do not create arbitrary values for:

-   colors;
-   typography;
-   spacing;
-   radii;
-   shadows;
-   borders;

when an approved token exists.

## 9.2 No hard-coded semantic colors

Do not embed page-specific BUY/SELL colors.

Use semantic signal/status tokens.

## 9.3 Consistent spacing

Use the approved spacing scale.

Do not create one-off spacing values to repair local alignment unless
there is a documented reason.

## 9.4 Typography

Use the approved type hierarchy.

Do not compensate for weak hierarchy by making everything larger or
bolder.

------------------------------------------------------------------------

# 10. Theme Implementation

Light and dark themes are first-class implementations.

## 10.1 No theme inversion shortcut

Do not design dark mode and mechanically invert it into light mode.

Both themes must preserve:

-   hierarchy;
-   contrast;
-   semantic meaning;
-   interaction states.

## 10.2 Token-driven theme

Theme differences belong in theme tokens, not component conditionals
scattered throughout the application.

Avoid:

``` text
if dark:
    use color X
else:
    use color Y
```

when a semantic token can provide the same result.

## 10.3 Theme QA

Every major page and component must be checked in:

-   light;
-   dark.

Check:

-   text contrast;
-   borders;
-   surfaces;
-   charts;
-   status indicators;
-   hover/focus;
-   disabled states;
-   empty/error states.

------------------------------------------------------------------------

# 11. Component Implementation

## 11.1 Reuse before duplication

Before creating a component:

1.  Search for an existing equivalent.
2.  Compare responsibilities.
3.  Extend it if appropriate.
4.  Create a new component only when the concept is genuinely different.

## 11.2 Components must have clear responsibility

A component should not simultaneously:

-   fetch unrelated data;
-   calculate domain logic;
-   manage page navigation;
-   render multiple unrelated concepts.

## 11.3 Domain logic stays outside presentation components

Examples:

`SignalBadge` must not determine signals.

`ConfidenceDisplay` must not interpret confidence.

`PredictionCard` must not calculate maturity.

`SettlementResult` must not calculate settlement.

## 11.4 Variants

Create variants only when they represent legitimate UX differences.

Do not create dozens of boolean props for tiny visual differences.

## 11.5 Component consistency

Shared concepts must use shared components.

Examples:

-   SignalBadge everywhere;
-   StatusBadge everywhere;
-   StockIdentity everywhere;
-   DataFreshness everywhere.

------------------------------------------------------------------------

# 12. Page Implementation

Each page must follow `05-PAGE-SPECIFICATIONS.md`.

The agent must preserve:

-   approved hierarchy;
-   intended user question;
-   page-specific mental model;
-   required states;
-   navigation relationships.

Do not copy a page structure from another workspace merely because it
looks convenient.

------------------------------------------------------------------------

# 13. Responsive Implementation

## 13.1 Desktop

Preserve analytical density.

Do not create unnecessary whitespace.

## 13.2 Tablet

Adjust:

-   navigation;
-   grid structure;
-   table density;
-   secondary metadata.

## 13.3 Mobile

Transform the interface where necessary.

Examples:

``` text
Desktop table
→
Mobile compact cards
```

``` text
Desktop filter sidebar
→
Mobile filter drawer
```

Do not simply:

-   shrink fonts;
-   reduce spacing;
-   hide random columns;

without deciding what information remains essential.

## 13.4 Touch

Interactive controls must remain usable on touch devices.

------------------------------------------------------------------------

# 14. Accessibility

Every implementation must preserve:

-   semantic HTML;
-   keyboard navigation;
-   visible focus;
-   screen-reader labels;
-   sufficient contrast;
-   accessible tables;
-   accessible form controls;
-   accessible chart summaries.

Do not rely on:

-   color;
-   hover;
-   animation;

as the only way to communicate important information.

------------------------------------------------------------------------

# 15. Loading, Empty, Error & Stale States

Every data-dependent page must explicitly consider:

``` text
Loading
↓
Success
├── Data
├── Empty
└── No Results
↓
Failure
├── Error
├── Partial Failure
└── Unavailable
```

Where relevant, add:

``` text
Stale
Pending
Evaluated
Demo
```

A page specification determines which states are required.

Do not create generic copy such as:

> Something went wrong.

when a more useful scoped message is possible.

------------------------------------------------------------------------

# 16. Financial Data Integrity

This is a hard requirement.

The UI must never misrepresent financial/model data.

## Forbidden

-   fake stock prices;
-   fake recommendations;
-   fake confidence;
-   fake historical predictions;
-   fake evaluation outcomes;
-   fake model metrics;
-   fake news;
-   fabricated target prices;
-   invented fundamentals;
-   invented sentiment;
-   invented model explanations.

## Required

-   canonical backend values;
-   explicit unavailable states;
-   correct timestamps;
-   correct status;
-   correct historical context;
-   consistent formatting.

------------------------------------------------------------------------

# 17. Routing & Navigation

Navigation must reflect the information architecture.

Primary destinations:

``` text
Dashboard
Recommendations
Screener
News Intelligence
Prediction History
Model Intelligence
```

Stock Detail is the common deep-dive destination.

## 17.1 Preserve context

Where technically practical, preserve:

-   filters;
-   sorting;
-   pagination;
-   originating page.

Example:

``` text
Recommendations
→ Stock Detail
→ Back
→ previous Recommendations context
```

## 17.2 Avoid dead ends

Every major analytical item should have an obvious next investigation
step.

------------------------------------------------------------------------

# 18. Performance

The redesign should not introduce unnecessary performance regressions.

## 18.1 Avoid unnecessary rendering

Use existing project patterns for:

-   memoization;
-   query caching;
-   pagination;
-   virtualization;

only where justified.

## 18.2 Tables

For moderate datasets, normal pagination is preferred.

Use virtualization only when actual scale requires it.

## 18.3 Charts

Do not render expensive charts when:

-   the dataset is empty;
-   the chart is not visible;
-   a simpler representation is sufficient.

## 18.4 Images

Use appropriate image sizes and loading strategies.

Do not load large assets unnecessarily.

------------------------------------------------------------------------

# 19. Dependency Rules

Before adding a dependency:

1.  Search the existing project.
2.  Check whether the functionality already exists.
3.  Check whether an existing dependency can provide it.
4.  Evaluate bundle/build/runtime impact.
5.  Confirm the dependency is necessary.
6.  Document the reason.

Prefer the smallest justified dependency set.

Do not introduce a package for a trivial helper that can be implemented
safely with existing tooling.

------------------------------------------------------------------------

# 20. Animation Rules

Animation is subordinate to information.

Use animation only when it improves:

-   state transition;
-   navigation;
-   comprehension;
-   perceived responsiveness.

Avoid:

-   decorative motion;
-   continuous background animation;
-   excessive chart animation;
-   attention-grabbing effects;
-   unnecessary page transitions.

Respect reduced-motion accessibility preferences.

------------------------------------------------------------------------

# 21. Testing & Validation

## 21.1 Required checks

Use the project's actual commands for:

-   lint;
-   type checking;
-   unit tests;
-   integration tests;
-   build.

Do not invent commands if the repository already defines them.

## 21.2 Data contract testing

Where applicable, verify:

-   API response assumptions;
-   null/missing fields;
-   prediction states;
-   evaluation states;
-   historical data;
-   timestamps.

## 21.3 State testing

At minimum, test relevant:

-   loading;
-   success;
-   empty;
-   no-results;
-   error;
-   partial failure;
-   stale;
-   pending;
-   evaluated;
-   unavailable;
-   demo.

## 21.4 Regression testing

Existing functionality must remain intact unless explicitly changed.

Pay particular attention to:

-   routing;
-   filters;
-   sorting;
-   pagination;
-   stock navigation;
-   theme switching;
-   API integration.

------------------------------------------------------------------------

# 22. Visual QA

Visual QA is mandatory for significant UI work.

## 22.1 Required viewports

At minimum:

-   desktop;
-   mobile.

Test representative intermediate widths when responsive behavior is
complex.

## 22.2 Required themes

-   light;
-   dark.

## 22.3 Compare against specifications

Inspect:

-   hierarchy;
-   spacing;
-   typography;
-   component consistency;
-   table density;
-   status semantics;
-   chart behavior;
-   empty/error states;
-   responsive transformations.

## 22.4 Visual QA is not subjective approval

The agent should verify the implementation against documented
requirements.

Do not report:

> Looks good.

Instead report concrete checks.

------------------------------------------------------------------------

# 23. Git & Change Management

## 23.1 Inspect before modifying

Check:

-   current branch;
-   working tree;
-   recent relevant commits.

Do not overwrite unrelated user work.

## 23.2 Focused changes

Keep changes scoped to the requested work.

Avoid drive-by:

-   formatting;
-   renaming;
-   refactoring;
-   dependency upgrades;
-   backend changes.

## 23.3 Diff review

Before reporting completion:

-   inspect changed files;
-   inspect the diff;
-   identify accidental modifications;
-   verify no debug code remains.

## 23.4 No destructive commands without authorization

Do not:

-   reset user work;
-   delete unrelated files;
-   force-rewrite history;
-   overwrite unrelated branches;

without explicit authorization.

------------------------------------------------------------------------

# 24. Scope Control

## 24.1 No silent scope creep

Do not implement unrelated improvements merely because they are visible
during the task.

Examples:

-   rewriting authentication;
-   redesigning backend APIs;
-   changing model training;
-   replacing the charting system;
-   redesigning unrelated pages;
-   upgrading the entire dependency tree.

## 24.2 Blocking issues

If an unrelated issue genuinely blocks the requested work:

1.  identify it;
2.  explain why it blocks implementation;
3.  make the smallest safe change necessary;
4.  report it explicitly.

## 24.3 Improvements discovered during implementation

Record useful out-of-scope improvements rather than silently
implementing them.

------------------------------------------------------------------------

# 25. Forbidden Implementation Patterns

The following are explicitly prohibited unless the user authorizes them:

-   fake data;
-   invented metrics;
-   invented financial semantics;
-   invented model explanations;
-   frontend duplication of backend evaluation logic;
-   calendar-day prediction maturity;
-   alternate settlement-price calculations;
-   overwriting historical predictions;
-   arbitrary ranking algorithms;
-   hard-coded theme colors;
-   page-specific copies of shared components;
-   excessive dependencies;
-   unrelated refactors;
-   destructive Git operations;
-   silent API contract changes;
-   accessibility regressions;
-   desktop-only implementations;
-   dark-only implementations;
-   decorative charts;
-   unnecessary animation;
-   giant card-based replacements for every data table;
-   unsupported AI-generated financial claims.

------------------------------------------------------------------------

# 26. Agent Decision Rules

When uncertain, use this decision sequence:

### Question 1

Is the behavior specified?

If yes, follow the specification.

### Question 2

Is it a backend/domain semantic?

If yes, use the backend contract. Do not invent it.

### Question 3

Is it a reusable UI behavior?

If yes, check `06-COMPONENT-SPECIFICATIONS.md`.

### Question 4

Is it a theme/token decision?

If yes, check `03-DESIGN-SYSTEM.md` and `04-THEME-SPECIFICATION.md`.

### Question 5

Is it an implementation detail?

Choose the smallest solution consistent with the existing architecture.

### Question 6

Would the choice materially change UX, domain semantics, architecture,
or scope?

If yes, do not silently decide. Document the ambiguity and escalate
according to the project's agent workflow.

------------------------------------------------------------------------

# 27. Definition of Done

A UI implementation is complete only when all applicable conditions are
satisfied.

## Product

-   [ ] Page purpose is clear.
-   [ ] Approved information hierarchy is implemented.
-   [ ] User workflows are preserved.
-   [ ] Cross-page navigation works.

## Components

-   [ ] Shared components are reused.
-   [ ] No unnecessary duplicates exist.
-   [ ] Component states are implemented.
-   [ ] Domain logic is outside presentation components.

## Data

-   [ ] API semantics are preserved.
-   [ ] No fake data exists.
-   [ ] Missing data is represented honestly.
-   [ ] Freshness/staleness is correct.
-   [ ] Historical data is not overwritten.

## Prediction/Evaluation

Where applicable:

-   [ ] Canonical prediction signal is used.
-   [ ] Confidence semantics are preserved.
-   [ ] Ten valid trading-session maturity is respected.
-   [ ] Canonical settlement price is used.
-   [ ] Pending is distinct from incorrect.
-   [ ] Unable-to-evaluate is distinct from incorrect.
-   [ ] Historical prediction snapshots remain immutable.

## Theme

-   [ ] Light theme works.
-   [ ] Dark theme works.
-   [ ] Semantic tokens are used.
-   [ ] No hard-coded semantic colors exist.

## Responsive

-   [ ] Desktop works.
-   [ ] Mobile works.
-   [ ] Intermediate responsive behavior is acceptable.
-   [ ] Tables/filtering transform appropriately.

## Accessibility

-   [ ] Keyboard navigation works.
-   [ ] Focus is visible.
-   [ ] Important information does not rely on color alone.
-   [ ] Interactive controls have accessible names.
-   [ ] Tables/charts have appropriate accessibility support.

## Quality

-   [ ] Lint passes or known failures are documented.
-   [ ] Type checking passes or known failures are documented.
-   [ ] Tests pass or known failures are documented.
-   [ ] Production build passes or known failures are documented.
-   [ ] Visual QA is complete.
-   [ ] Git diff has been reviewed.
-   [ ] No unrelated modifications remain.

## Documentation

-   [ ] Implementation report is provided.
-   [ ] Known issues are documented.
-   [ ] Deviations are documented.
-   [ ] New architectural decisions are documented where necessary.

------------------------------------------------------------------------

# 28. Required Implementation Report Template

Every substantial implementation task should finish with:

``` text
## Implementation Report

### Summary
<what was implemented>

### Files Changed
- <file>
- <file>

### Specification Coverage
- <requirement>
- <requirement>

### Validation
- Type check: PASS/FAIL/NOT RUN
- Lint: PASS/FAIL/NOT RUN
- Tests: PASS/FAIL/NOT RUN
- Build: PASS/FAIL/NOT RUN

### Visual QA
- Light desktop: PASS/FAIL/NOT RUN
- Dark desktop: PASS/FAIL/NOT RUN
- Light mobile: PASS/FAIL/NOT RUN
- Dark mobile: PASS/FAIL/NOT RUN

### Known Issues
- <issue or "None">

### Specification Deviations
- <deviation and reason or "None">
```

Never claim a validation step was performed when it was not.

------------------------------------------------------------------------

# 29. Final Implementation Principle

> **Implement the approved product, preserve the production truth, reuse
> the system intelligently, validate the result rigorously, and make
> every deviation visible.**

The implementation agent is not the product designer, ML engineer, data
scientist, or backend architect unless explicitly assigned those
responsibilities.

Its job is to turn approved StockIntel requirements into a robust,
maintainable, accessible interface without silently changing what
StockIntel means.

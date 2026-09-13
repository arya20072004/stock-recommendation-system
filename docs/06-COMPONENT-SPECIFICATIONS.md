# StockIntel UI Redesign --- Component Specifications

**Document:** `06-COMPONENT-SPECIFICATIONS.md`\
**Product:** StockIntel\
**Status:** Design specification\
**Scope:** Reusable UI components and their behavioral contracts\
**Audience:** Product/design/implementation agents

------------------------------------------------------------------------

## 1. Purpose

This document defines the reusable component vocabulary for the
StockIntel redesign.

It exists to prevent seven pages from becoming seven unrelated
implementations of the same ideas.

This document specifies:

-   component purpose;
-   anatomy;
-   variants;
-   states;
-   content rules;
-   interaction rules;
-   responsive behavior;
-   theme requirements;
-   accessibility;
-   anti-patterns.

It does **not** define exact color values, typography tokens, spacing
tokens, or implementation-specific CSS. Those belong to the
design-system and theme documents.

It does **not** define page composition. That belongs to
`05-PAGE-SPECIFICATIONS.md`.

------------------------------------------------------------------------

# 2. Component Design Principles

## 2.1 Reuse without forced abstraction

A component should be reusable when it represents a genuine shared
concept.

Do not create abstractions merely to reduce line count.

A component is justified when:

-   it appears on multiple pages;
-   it has consistent semantics;
-   it has meaningful variants/states;
-   consistency provides product value.

## 2.2 Semantic components

Components represent domain concepts without owning domain logic.

For example:

`SignalBadge`

renders a signal.

It does not calculate a signal.

`PredictionStatus`

renders prediction status.

It does not decide whether a prediction has matured.

## 2.3 Backend semantics remain authoritative

Components must not invent:

-   prediction meaning;
-   confidence meaning;
-   ranking;
-   evaluation;
-   settlement;
-   maturity;
-   financial metrics;
-   sentiment methodology.

The component receives canonical data and presents it.

## 2.4 Semantic theme tokens

Components must use semantic design tokens.

Do not hard-code visual values inside individual components.

Avoid:

``` text
#00ff00
#ff0000
```

or equivalent component-specific color decisions.

Use semantic concepts such as:

-   positive;
-   negative;
-   neutral;
-   informational;
-   warning;
-   surface;
-   border;
-   text-primary;
-   text-secondary.

Exact token definitions belong to `04-THEME-SPECIFICATION.md`.

## 2.5 States are part of the component contract

A component is incomplete if it only specifies the happy path.

Relevant components must account for:

-   loading;
-   empty;
-   unavailable;
-   error;
-   stale;
-   pending;
-   evaluated;
-   disabled.

## 2.6 Color is never the only semantic channel

BUY, HOLD, SELL, pending, correct, incorrect, warning, and unavailable
states must remain understandable through:

-   text;
-   icons where appropriate;
-   labels;
-   structure;

not color alone.

## 2.7 Density is intentional

StockIntel is a data-heavy analytical product.

Do not increase card padding, typography, or whitespace merely because
"modern UI" is interpreted as spaciousness.

The goal is:

> high information density with strong hierarchy.

------------------------------------------------------------------------

# 3. Component Inventory

## Global / Layout

-   App Shell
-   Sidebar
-   Top Bar
-   Page Header
-   Section Header
-   Responsive Navigation

## Market / Intelligence

-   Market Index Card
-   Signal Badge
-   Confidence Display
-   Recommendation Card
-   Prediction Card
-   Stock Identity
-   Price Change
-   Tier Badge
-   Status Badge
-   Data Freshness Indicator
-   Intelligence Summary

## Tables / Filtering

-   Data Table
-   Table Row
-   Filter Bar
-   Filter Control
-   Filter Group
-   Active Filter Chips
-   Search Input
-   Sort Control
-   Pagination

## Charts

-   Market Chart
-   Price Chart
-   Distribution Chart
-   Performance Chart

## News

-   News Item
-   News List
-   Sentiment Indicator
-   Source Attribution

## States

-   Loading / Skeleton
-   Empty State
-   Error State
-   Partial Failure
-   No Results
-   Pending Evaluation
-   Evaluated Result
-   Unavailable Data
-   Demo Data Indicator

## Actions / Navigation

-   Primary Button
-   Secondary Button
-   Icon Button
-   Text Link
-   Watchlist Toggle
-   View Analysis Action
-   Context Back Navigation

------------------------------------------------------------------------

# 4. Global / Layout Components

## 4.1 App Shell

### Purpose

Provides the persistent application frame.

### Anatomy

-   Sidebar/navigation
-   Main content region
-   Optional top utility area
-   Responsive navigation behavior

### Rules

The shell must not contain page-specific business logic.

It should provide:

-   consistent page width behavior;
-   navigation;
-   theme;
-   global status surfaces where required.

### Responsive

Desktop:

-   persistent sidebar;
-   main content area.

Mobile:

-   compact top navigation;
-   drawer/sheet navigation or equivalent.

Do not permanently consume substantial mobile viewport space with
desktop navigation.

------------------------------------------------------------------------

## 4.2 Sidebar

### Purpose

Primary desktop navigation.

### Required navigation concepts

-   Dashboard
-   Recommendations
-   Stock Screener
-   News Intelligence
-   Prediction History
-   Model Intelligence

Watchlist may be represented as a navigation concept if supported by the
product.

### Anatomy

-   brand/product identity;
-   primary navigation;
-   active page state;
-   optional secondary utilities;
-   theme/user controls where approved.

### Rules

-   active page must be obvious;
-   labels must remain readable;
-   icons cannot be the only semantic identifier unless universally
    recognizable and additionally labeled accessibly;
-   avoid unnecessary nested navigation.

### Responsive

Collapse into a navigation drawer/sheet on smaller screens.

------------------------------------------------------------------------

## 4.3 Top Bar

### Purpose

Provides persistent utility context where needed.

Potential content:

-   market status;
-   global search;
-   theme control;
-   user/session controls.

Do not turn the top bar into a second navigation system.

------------------------------------------------------------------------

## 4.4 Page Header

### Purpose

Establishes page identity and purpose.

### Anatomy

-   title;
-   description;
-   optional actions;
-   optional freshness/system status.

### Rules

The title should state the actual workspace.

Examples:

-   Prediction History
-   News Intelligence
-   Stock Screener

Avoid marketing copy as the primary title.

------------------------------------------------------------------------

## 4.5 Section Header

### Purpose

Introduces a section inside a page.

### Anatomy

-   section title;
-   optional supporting text;
-   optional action.

Keep section headers visually subordinate to the page header.

------------------------------------------------------------------------

## 4.6 Responsive Navigation

Navigation must preserve:

-   current location;
-   access to all major workspaces;
-   keyboard accessibility;
-   touch usability.

Do not hide critical application navigation behind unexplained gestures.

------------------------------------------------------------------------

# 4.6A Mobile Navigation Contract

The v1 mobile navigation pattern is a compact top bar with a hamburger/menu control that opens a navigation drawer or sheet.

Primary navigation:

- Dashboard
- Recommendations
- Screener
- News Intelligence
- Prediction History
- Model Intelligence

Requirements:

- current workspace is clearly identifiable;
- all six primary workspaces remain accessible;
- the drawer/sheet has a clear close mechanism;
- keyboard focus is managed correctly while open;
- controls have usable touch targets;
- Stock Detail remains contextual rather than primary navigation;
- a bottom tab bar is not used for the six analytical workspaces in v1.

# 5. Market / Intelligence Components

## 5.1 Market Index Card

### Purpose

Compactly communicates current/latest market-index context.

### Anatomy

-   index name;
-   latest value;
-   absolute/percentage change;
-   timestamp/date;
-   optional market-status indicator.

### Variants

-   compact;
-   standard;
-   dashboard.

### States

-   normal;
-   loading;
-   unavailable;
-   stale.

### Rules

Do not imply real-time pricing if data is delayed or historical.

### Accessibility

Value and change must be readable without color.

------------------------------------------------------------------------

## 5.2 Signal Badge

### Purpose

Consistent representation of model signal.

### Values

-   BUY
-   HOLD
-   SELL

Only use values actually supported by the model/API.

### Anatomy

-   text label;
-   optional icon.

### Rules

BUY/HOLD/SELL semantics must remain identical throughout the
application.

Do not use different colors or labels for the same signal on different
pages.

Do not use the badge to communicate evaluation status.

### Accessibility

Signal must be conveyed through text, not color alone.

------------------------------------------------------------------------

## 5.3 Confidence Display

### Purpose

Displays model confidence exactly as defined by the backend.

### Anatomy

-   numeric value;
-   optional compact visual representation;
-   optional semantic label only if threshold definitions are approved.

### Rules

Do not assume confidence means:

-   probability of profit;
-   probability of price increase;
-   expected return.

Do not create arbitrary "low/medium/high" categories unless they are
defined by product requirements.

### States

-   value;
-   unavailable;
-   loading.

### Formatting

Use one consistent precision throughout the product.

Do not show excessive decimal precision.

------------------------------------------------------------------------

## 5.4 Recommendation Card

### Purpose

Compact representation of a current recommendation.

### Anatomy

-   stock identity;
-   signal;
-   confidence;
-   price;
-   price change;
-   tier where supported;
-   navigation action.

### Variants

-   compact;
-   dashboard;
-   strongest-signal.

### Interaction

Entire card may be clickable if it has a single clear destination.

Avoid multiple competing click targets.

### Rules

The card does not calculate recommendation ranking.

------------------------------------------------------------------------

## 5.5 Prediction Card

### Purpose

Compact representation of an individual historical prediction.

### Anatomy

-   stock identity;
-   signal;
-   confidence;
-   generation date;
-   horizon;
-   status;
-   outcome where legitimately available.

### Variants

-   pending;
-   evaluated;
-   unavailable.

### Pending

Display:

> Pending evaluation

Do not display an implied result.

### Evaluated

Display canonical evaluation outcome and return where supported.

### Unavailable

Display:

> Unable to evaluate

Do not label the prediction incorrect unless the evaluator explicitly
does so.

------------------------------------------------------------------------

## 5.6 Stock Identity

### Purpose

Consistent representation of a stock across the product.

### Anatomy

-   ticker;
-   company name;
-   optional exchange.

### Rules

Ticker should be visually scannable.

Company name should provide context without overwhelming the ticker.

### Interaction

When a navigation destination exists, stock identity may be a link.

------------------------------------------------------------------------

## 5.7 Price Change

### Purpose

Communicates movement in a price/value.

### Anatomy

-   absolute value where relevant;
-   percentage change;
-   optional period label.

### Rules

Do not imply a time period unless it is known.

Examples:

-   Today
-   1D
-   Since prediction

The label must reflect the actual calculation.

### Accessibility

Do not communicate positive/negative movement through color alone.

------------------------------------------------------------------------

## 5.8 Tier Badge

### Purpose

Represents a recommendation tier where the backend provides one.

### Rules

-   use only canonical tiers;
-   preserve consistent terminology;
-   do not invent tier definitions in the frontend.

If tier semantics are not documented, the component must not expose an
explanatory claim.

------------------------------------------------------------------------

## 5.9 Status Badge

### Purpose

Communicates system/data/workflow state.

Potential values include:

-   Pending
-   Evaluated
-   Unable to evaluate
-   Fresh
-   Stale
-   Unavailable
-   Demo

### Critical rule

Status and signal are different concepts.

For example:

``` text
Signal: BUY
Status: Pending
```

is valid.

Do not replace status with signal.

### Accessibility

Text must communicate the state.

------------------------------------------------------------------------

## 5.10 Data Freshness Indicator

### Purpose

Communicates when relevant data was last updated.

### Anatomy

-   state;
-   timestamp/date;
-   optional source/context.

### Variants

-   fresh;
-   stale;
-   unavailable;
-   updating.

### Rules

Do not claim freshness without a timestamp/source.

Different datasets may have different freshness.

------------------------------------------------------------------------

## 5.11 Intelligence Summary

### Purpose

Compact aggregate summary for a page or section.

### Examples

-   recommendation counts;
-   evaluated/pending counts;
-   signal distribution;
-   market sentiment distribution.

### Anatomy

-   metric label;
-   value;
-   optional comparison/context;
-   optional timestamp.

### Rules

Every metric must have a defined backend/product meaning.

Do not add decorative metrics.

------------------------------------------------------------------------

# 6. Tables / Filtering Components

## 6.1 Data Table

### Purpose

Dense analytical presentation of structured records.

### Requirements

-   clear column headers;
-   readable row density;
-   consistent numeric alignment;
-   sortable columns where supported;
-   accessible table semantics;
-   row navigation where appropriate.

### Rules

Do not color entire rows based on signal.

Do not hide critical values behind hover-only interactions.

### Responsive

Desktop:

-   full table.

Mobile:

-   transform into compact cards/rows where required.

Do not merely shrink typography until the table becomes unusable.

------------------------------------------------------------------------

## 6.2 Table Row

### Purpose

Represents a single analytical record.

### Rules

If a row has a single destination, the row can be clickable.

Avoid placing many independent buttons inside every row.

Use inline actions only where the workflow genuinely requires them.

### Accessibility

Keyboard users must be able to access clickable rows.

------------------------------------------------------------------------

## 6.3 Filter Bar

### Purpose

Provides the primary filtering workspace.

### Anatomy

-   filter controls;
-   search;
-   optional advanced-filter trigger;
-   active-filter summary;
-   clear-all action.

### Rules

Current filtering state must be obvious.

Do not hide active filters after selection.

------------------------------------------------------------------------

## 6.4 Filter Control

### Purpose

Single filter input.

Possible types:

-   select;
-   multi-select;
-   range;
-   date range;
-   numeric input;
-   toggle.

### Rules

Use the control type appropriate to the data.

Do not create custom controls when native semantics are sufficient.

------------------------------------------------------------------------

## 6.5 Filter Group

### Purpose

Groups related filters.

Examples:

``` text
Model
- Signal
- Confidence
- Horizon

Market
- Price
- Change
- Volume

Company
- Sector
- Market Cap
```

### Rules

Grouping must follow user mental models, not database table structure.

------------------------------------------------------------------------

## 6.6 Active Filter Chips

### Purpose

Makes current filter state visible.

### Anatomy

-   filter name;
-   selected value;
-   remove action.

### Rules

Each chip must be independently removable.

Show Clear All when multiple/meaningful filters are active.

------------------------------------------------------------------------

## 6.7 Search Input

### Purpose

Searches supported entities/content.

### Supported examples

-   ticker;
-   company;
-   news headline.

### Rules

Placeholder text must describe actual search scope.

Do not imply semantic/AI search when it is only string/entity search.

------------------------------------------------------------------------

## 6.8 Sort Control

### Purpose

Changes ordering of records.

### Rules

Only expose fields that can be meaningfully sorted.

If a canonical default ranking exists, preserve it as the default.

Do not imply that sorting equals recommendation ranking.

------------------------------------------------------------------------

## 6.9 Pagination

### Purpose

Navigates moderate-sized datasets.

### Requirements

-   current page;
-   total/known page context;
-   previous/next;
-   accessible controls.

Use virtualization only when dataset size warrants it.

------------------------------------------------------------------------

# 7. Chart Components

## 7.1 Chart Principles

Every chart must answer a defined analytical question.

Each chart should provide, where applicable:

-   title;
-   timeframe/context;
-   axes;
-   tooltip;
-   source/data date;
-   accessible textual summary.

Do not add charts because an empty area looks boring.

------------------------------------------------------------------------

## 7.2 Market Chart

### Purpose

Shows market/index movement.

### Required context

-   index identity;
-   timeframe;
-   data date;
-   meaningful comparison when supported.

### Controls

Only expose supported timeframes.

------------------------------------------------------------------------

## 7.3 Price Chart

### Purpose

Shows stock price history relevant to Stock Detail.

### Potential features

-   timeframe;
-   current/latest price;
-   prediction markers where supported;
-   comparison data where supported.

### Rules

Do not turn Stock Detail into a full technical-analysis terminal.

------------------------------------------------------------------------

## 7.4 Distribution Chart

### Purpose

Shows distributions such as:

-   BUY/HOLD/SELL;
-   confidence ranges;
-   sentiment.

### Rules

Category definitions must come from product/backend semantics.

Labels must remain readable.

------------------------------------------------------------------------

## 7.5 Performance Chart

### Purpose

Shows historical model performance when sufficient evaluated
observations exist.

### Examples

-   accuracy over time;
-   performance by class;
-   evaluation volume.

### Rules

Do not imply statistical significance from visual trend alone.

Small samples must be contextualized.

------------------------------------------------------------------------

# 7A. Mobile Data Card

## Purpose

Reusable mobile representation for dense desktop analytical tables.

## Required Content

Support:

- entity identity;
- primary signal/state;
- primary metric(s);
- status;
- required date/time context;
- secondary metadata;
- primary navigation action.

## Transformation Rules

Do not mechanically stack every desktop column.

### Prediction History

Prioritize:

1. Stock
2. Signal
3. Confidence
4. Generated date/time
5. Horizon
6. Evaluation status
7. Outcome/return when evaluated

Model version and lower-priority metadata may be progressively disclosed.

### Recommendations / Screener

Prioritize:

1. Stock
2. Signal
3. Confidence where available
4. Price
5. Price change
6. Canonical tier/status where available

## Semantics

The mobile card must preserve the same meaning and destination as the desktop row.

For stock-aware records, the default destination is Stock Detail.

## Prohibited

- hiding the signal;
- hiding evaluation status;
- turning unavailable values into zero;
- creating mobile-only calculations;
- inventing mobile rankings;
- rendering all desktop fields as an unreadable vertical list.

# 8. News Components

## 8.1 News Item

### Purpose

Compact representation of a news article.

### Anatomy

-   ticker/company where supported;
-   sentiment;
-   headline;
-   source;
-   timestamp;
-   source link.

### Rules

Do not reproduce article bodies.

Do not invent summaries.

Do not claim causality between article and stock/model signal without
system support.

------------------------------------------------------------------------

## 8.2 News List

### Purpose

Displays a collection of news items.

### Rules

-   consistent ordering;
-   explicit loading;
-   empty state;
-   partial failure handling;
-   pagination or controlled loading where appropriate.

Do not implement infinite scroll by default.

------------------------------------------------------------------------

## 8.3 Sentiment Indicator

### Purpose

Represents the canonical news sentiment.

### Rules

The component must use the backend-defined sentiment vocabulary.

Do not invent sentiment thresholds.

Do not imply investment advice.

Color must not be the only signal.

------------------------------------------------------------------------

## 8.4 Source Attribution

### Purpose

Clearly identifies the source of external news.

### Anatomy

-   source name;
-   timestamp;
-   external-link affordance.

### Rules

External sources must remain clearly identifiable.

Do not present third-party reporting as StockIntel-generated
information.

------------------------------------------------------------------------

# 9. State Components

## 9.1 Loading / Skeleton

### Purpose

Communicates loading while preserving expected layout.

### Rules

Use skeletons where structure is known.

Avoid full-page blocking spinners for isolated sections.

------------------------------------------------------------------------

## 9.2 Empty State

### Purpose

Communicates that a valid dataset currently contains no records.

### Anatomy

-   concise title;
-   explanation;
-   relevant next action where useful.

### Rules

Do not confuse empty with error.

Do not fill the state with fake data.

------------------------------------------------------------------------

## 9.3 Error State

### Purpose

Communicates an actual failure.

### Anatomy

-   what failed;
-   impact;
-   retry/recovery action where supported.

Do not expose technical stack traces to normal users.

------------------------------------------------------------------------

## 9.4 Partial Failure

### Purpose

Communicates that one section/source failed while the rest remains
usable.

### Rules

Keep unaffected content visible.

Clearly scope the failure to the affected area.

------------------------------------------------------------------------

## 9.5 No Results

### Purpose

Communicates that filtering/searching produced zero matches.

### Difference from Empty

Empty:

> no underlying records.

No results:

> records exist, but current criteria match none.

### Rules

Suggest removing/adjusting filters where appropriate.

------------------------------------------------------------------------

## 9.6 Pending Evaluation

### Purpose

Represents a prediction that is not yet legitimately evaluable.

### Required distinction

Pending is not:

-   incorrect;
-   zero return;
-   failed prediction.

### Display

Use a clear textual status:

> Pending evaluation

Where supported, show maturity/evaluation information from the backend.

------------------------------------------------------------------------

## 9.7 Evaluated Result

### Purpose

Represents a prediction with a completed canonical evaluation.

### Display

Where supported:

-   outcome;
-   return;
-   settlement information;
-   correctness.

Do not recompute these independently if canonical values are supplied by
the backend.

------------------------------------------------------------------------

## 9.8 Unavailable Data

### Purpose

Represents data that cannot currently be displayed.

### Rules

Use an explicit unavailable state.

Do not silently display zero.

Do not display stale data as current unless clearly labeled.

------------------------------------------------------------------------

## 9.9 Demo Data Indicator

### Purpose

Clearly distinguishes non-production data.

### Rules

Must be visible enough to prevent confusion.

Do not use subtle metadata that ordinary users will miss.

------------------------------------------------------------------------

# 10. Action / Navigation Components

## 10.1 Primary Button

Use for the primary action within a local context.

Do not make every action visually primary.

------------------------------------------------------------------------

## 10.2 Secondary Button

Use for supporting actions.

Maintain lower visual weight than the primary action.

------------------------------------------------------------------------

## 10.3 Icon Button

### Rules

Every icon-only button requires an accessible label.

Do not use unfamiliar icons without supporting labels/tooltips where
appropriate.

------------------------------------------------------------------------

## 10.4 Text Link

Use for navigation and low-emphasis actions.

Links should remain visually identifiable in both themes.

------------------------------------------------------------------------

## 10.5 Watchlist Toggle

### Purpose

Adds/removes a stock from the user's watchlist.

### States

-   not watched;
-   watched;
-   loading;
-   error.

### Rules

State must be visually and programmatically clear.

Do not confuse watchlist state with recommendation state.

------------------------------------------------------------------------

## 10.6 View Analysis Action

### Purpose

Navigates from a recommendation/prediction/news item to Stock Detail.

Prefer a consistent label and destination.

Avoid different names for the same action across pages.

------------------------------------------------------------------------

## 10.7 Context Back Navigation

### Purpose

Returns the user to the originating analytical workspace.

Where practical, preserve:

-   filters;
-   sort;
-   page;
-   relevant selected context.

Do not reset a complex workspace unnecessarily.

------------------------------------------------------------------------

# 11. Component State Matrix

Reusable components should follow a predictable state vocabulary.

  -------------------------------------------------------------------------------------
  Component           Loading      Empty   Unavailable      Error      Stale    Pending
  category                                                                   
  ---------------- ---------- ---------- ------------- ---------- ---------- ----------
  Market Index              ✓        ---             ✓          ✓          ✓        ---

  Signal                    ✓        ---             ✓        ---        ---        ---

  Confidence                ✓        ---             ✓        ---        ---        ---

  Recommendation            ✓          ✓             ✓          ✓          ✓        ---

  Prediction                ✓          ✓             ✓          ✓          ✓          ✓

  News                      ✓          ✓             ✓          ✓          ✓        ---

  Table                     ✓          ✓             ✓          ✓          ✓        ---

  Chart                     ✓          ✓             ✓          ✓          ✓        ---

  Freshness               ---        ---             ✓        ---          ✓        ---
  -------------------------------------------------------------------------------------

A component should not expose states that have no meaningful semantic
interpretation.

------------------------------------------------------------------------

# 12. Cross-Component Consistency

The following concepts must remain consistent everywhere.

## Signal

``` text
BUY
HOLD
SELL
```

## Prediction status

``` text
Pending
Evaluated
Unable to evaluate
```

## Data status

``` text
Fresh
Stale
Unavailable
Demo
```

## Navigation

Stock references should lead to Stock Detail.

Historical prediction references should lead to the relevant Stock
Detail context.

Aggregate model-performance references should lead to Model
Intelligence.

------------------------------------------------------------------------

# 13. Responsive Component Rules

## Desktop

Prioritize:

-   analytical density;
-   multi-column layouts;
-   dense tables;
-   persistent navigation.

## Tablet

Reduce:

-   simultaneous columns;
-   nonessential metadata;
-   navigation width.

## Mobile

Prioritize:

-   primary signal;
-   identity;
-   key numeric information;
-   state;
-   action.

Transform:

-   tables → cards/compact rows;
-   filter bars → drawers;
-   multi-column cards → vertical groups;
-   dense chart controls → simplified controls.

Do not simply reduce font size.

------------------------------------------------------------------------

# 14. Theme Rules

All components must work in both first-class themes:

-   light;
-   dark.

Components must consume semantic theme tokens.

No component may assume that:

-   a light surface is always white;
-   a dark surface is always black;
-   positive means a particular hard-coded color;
-   borders are always visible at the same intensity.

Theme-specific values belong in `04-THEME-SPECIFICATION.md`.

Both themes must preserve equivalent:

-   hierarchy;
-   status meaning;
-   readability;
-   interaction affordance.

------------------------------------------------------------------------

# 15. Accessibility Rules

All reusable components must:

-   expose semantic names;
-   support keyboard navigation where interactive;
-   provide visible focus;
-   preserve sufficient contrast;
-   avoid color-only semantics;
-   provide accessible labels for icons;
-   support screen-reader interpretation;
-   maintain appropriate touch targets.

Charts must provide a textual summary of important information.

Tables must use semantic headers and appropriate row/cell relationships.

Clickable rows must be keyboard accessible.

------------------------------------------------------------------------

# 16. Component Anti-Patterns

Implementation agents must not:

-   create multiple visually different SignalBadge implementations;
-   hard-code signal colors in individual pages;
-   use color as the only status indicator;
-   make every card clickable without a clear destination;
-   create giant cards for dense datasets;
-   hide important information behind hover;
-   add decorative charts;
-   add unsupported financial metrics;
-   invent confidence categories;
-   invent prediction outcomes;
-   calculate settlement independently;
-   calculate maturity independently;
-   use current recommendation data to rewrite historical prediction
    components;
-   create separate components for trivial one-off markup;
-   introduce component variants without a real UX requirement;
-   add animations merely for visual novelty.

------------------------------------------------------------------------

# 17. Component Acceptance Criteria

A reusable component is considered complete only when:

-   [ ] Purpose is documented.
-   [ ] Data inputs are defined.
-   [ ] Domain semantics are not invented.
-   [ ] Required variants are defined.
-   [ ] Relevant states are defined.
-   [ ] Loading behavior is defined.
-   [ ] Empty/unavailable/error behavior is defined where applicable.
-   [ ] Responsive behavior is defined.
-   [ ] Light theme works.
-   [ ] Dark theme works.
-   [ ] Accessibility requirements are satisfied.
-   [ ] Component does not duplicate another existing component's
    responsibility.
-   [ ] No hard-coded theme colors are introduced.
-   [ ] No page-specific business logic is hidden inside the component.
-   [ ] Visual QA has been performed in representative contexts.

------------------------------------------------------------------------

# 18. Implementation Boundary

This document defines **what reusable components must mean and how they
must behave**.

It does not prescribe:

-   exact React component names;
-   exact TypeScript interfaces;
-   CSS architecture;
-   state-management library;
-   routing library;
-   testing framework;
-   exact token values;
-   exact pixel dimensions.

Those implementation decisions belong in `07-IMPLEMENTATION-RULES.md`
and the broader project architecture.

The implementation agent must not use implementation freedom to change
approved UX semantics.

------------------------------------------------------------------------

# 19. Final Component Principle

> **Build one coherent component language for StockIntel, not seven
> collections of page-specific widgets.**

Every component should make the product more consistent, more
understandable, and more trustworthy.

The reusable system should reduce accidental variation while preserving
the analytical density StockIntel requires.

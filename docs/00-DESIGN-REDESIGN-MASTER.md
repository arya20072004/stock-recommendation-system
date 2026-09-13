# StockIntel UI Redesign --- Master Design Specification

**Document:** `00-DESIGN-REDESIGN-MASTER.md`\
**Product:** StockIntel\
**Status:** Master design direction\
**Purpose:** Establish the overall UX/UI direction, design philosophy,
boundaries, and non-negotiable decisions for the StockIntel redesign.

------------------------------------------------------------------------

# 1. Purpose

This document is the highest-level design reference for the StockIntel
UI redesign.

It establishes:

-   the product experience we are trying to create;
-   the visual/product character;
-   the fundamental UX problems being solved;
-   the principles that govern design decisions;
-   the relationship between information, intelligence, and action;
-   the boundaries of the redesign;
-   the major decisions that every subsequent design document must
    respect.

This is not a page specification.

Page-level behavior is defined in:

> `05-PAGE-SPECIFICATIONS.md`

Reusable components are defined in:

> `06-COMPONENT-SPECIFICATIONS.md`

Implementation behavior is defined in:

> `07-IMPLEMENTATION-RULES.md`

------------------------------------------------------------------------

# 2. Product Context

StockIntel is a data-heavy stock recommendation and market-intelligence
application.

The system combines multiple forms of information, including:

-   market data;
-   stock information;
-   model-generated signals;
-   model confidence;
-   historical predictions;
-   prediction evaluation;
-   news;
-   sentiment;
-   model-performance information;
-   stock screening.

The redesign must therefore solve a different problem from a typical
marketing website.

The product needs to support users who want to:

-   quickly understand the current market/model state;
-   identify stocks worth investigating;
-   inspect why a signal exists;
-   filter a stock universe;
-   understand relevant news;
-   examine historical predictions;
-   evaluate model behavior.

The UI must remain analytically useful while becoming significantly
easier to understand.

------------------------------------------------------------------------

# 3. Current UI Assessment

The existing application is functional and already contains meaningful
analytical workflows.

The problem is not simply:

> "The UI looks bad."

The deeper problem is that the current experience can feel like an
internal data application with a dark theme rather than a coherent
intelligence product.

Common issues to solve include:

-   insufficient hierarchy;
-   excessive visual equality between important and secondary
    information;
-   pages feeling like independent data views;
-   weak relationships between related workspaces;
-   limited progressive disclosure;
-   insufficiently explicit data/state communication;
-   overreliance on tables/cards without a strong narrative;
-   inconsistent emphasis;
-   insufficient distinction between current predictions and historical
    evaluation;
-   weak communication of freshness and system state.

The redesign must therefore be structural, not merely cosmetic.

------------------------------------------------------------------------

# 4. Redesign Vision

## 4.1 Core vision

> **StockIntel should feel like an intelligence system that happens to
> operate on financial markets.**

It should communicate:

-   analytical seriousness;
-   trustworthiness;
-   clarity;
-   technical competence;
-   restrained sophistication;
-   useful density.

The product should feel closer to:

> institutional research terminal + modern SaaS intelligence product

than to:

> generic stock website + crypto dashboard.

------------------------------------------------------------------------

# 5. Experience Goals

The redesign has six primary experience goals.

## 5.1 Clarity

Users should understand what a page is for almost immediately.

## 5.2 Hierarchy

Important information should visually dominate secondary information.

## 5.3 Comprehension

The interface should help users understand relationships between:

-   market conditions;
-   model signals;
-   stocks;
-   news;
-   predictions;
-   evaluations.

## 5.4 Trust

The product must be honest about:

-   uncertainty;
-   freshness;
-   incomplete data;
-   pending evaluation;
-   unavailable information;
-   model limitations.

## 5.5 Density

The product must preserve enough information density for serious
analytical use.

Density should be organized rather than removed.

## 5.6 Continuity

Pages should form a coherent investigation workflow instead of feeling
like unrelated screens.

------------------------------------------------------------------------

# 6. Core Product Mental Model

The application should support this broad workflow:

``` text
OBSERVE
   ↓
PRIORITIZE
   ↓
FILTER
   ↓
INVESTIGATE
   ↓
UNDERSTAND CONTEXT
   ↓
REVIEW EVIDENCE
   ↓
ASSESS MODEL
```

This corresponds to:

``` text
Dashboard
    ↓
Recommendations / Screener / News
    ↓
Stock Detail
    ↓
Prediction History
    ↓
Model Intelligence
```

The user should be able to move naturally between these levels.

------------------------------------------------------------------------

# 7. Page Roles

Each major page must have one dominant responsibility.

  -----------------------------------------------------------------------
  Page                                Dominant question
  ----------------------------------- -----------------------------------
  Dashboard                           What is happening right now?

  Recommendations                     What does StockIntel currently
                                      recommend?

  Stock Detail                        Why does StockIntel think this
                                      about this stock?

  Screener                            Which stocks meet my criteria?

  News Intelligence                   What is happening in the news and
                                      sentiment?

  Prediction History                  What did StockIntel predict, and
                                      what happened afterward?

  Model Intelligence                  How does the model perform overall?
  -----------------------------------------------------------------------

Avoid overlapping page responsibilities.

------------------------------------------------------------------------

# 8. Information Architecture Direction

The redesign should create a clear hierarchy between:

### Overview

Dashboard

### Current model output

Recommendations

### User-driven discovery

Screener

### External context

News Intelligence

### Individual investigation

Stock Detail

### Historical evidence

Prediction History

### Aggregate model analysis

Model Intelligence

This prevents the application from becoming a collection of duplicated
dashboards.

------------------------------------------------------------------------

# 9. Investigation Flow

Stock Detail is the primary convergence point.

``` text
Dashboard ─────────────┐
Recommendations ───────┤
Screener ───────────────┤
News Intelligence ──────┤
Watchlist ──────────────┤
Prediction History ─────┘
             ↓
       Stock Detail
             ↓
    Prediction History
             ↓
     Model Intelligence
```

A user should rarely reach a dead end after discovering something
interesting.

------------------------------------------------------------------------

# 10. Visual Direction

## 10.1 Overall character

The visual language should be:

-   restrained;
-   analytical;
-   modern;
-   premium;
-   professional;
-   information-dense.

## 10.2 Reference character

The product may draw inspiration from:

-   institutional terminals;
-   professional research platforms;
-   high-quality SaaS analytics applications.

It should not directly imitate a particular company's UI.

## 10.3 Avoid

Do not use:

-   neon/crypto aesthetics;
-   excessive gradients;
-   excessive glassmorphism;
-   giant decorative typography;
-   cartoonish illustrations;
-   excessive rounded cards;
-   unnecessary shadows;
-   excessive borders;
-   noisy animations;
-   dashboard "widget soup."

------------------------------------------------------------------------

# 11. Data Density Philosophy

StockIntel is not a presentation-only application.

Users need to inspect real data.

Therefore:

> **Do not reduce information density simply to make the interface look
> more modern.**

Instead:

``` text
Poor density
=
many unrelated things competing for attention

Good density
=
many useful things organized by hierarchy
```

Tables remain appropriate.

Charts remain appropriate.

Compact cards remain appropriate.

The redesign should improve their organization rather than eliminate
them.

------------------------------------------------------------------------

# 12. Progressive Disclosure

Not all information deserves equal prominence.

The product should expose information in layers:

### Layer 1: Immediate answer

What is happening?

What is the signal?

What deserves attention?

### Layer 2: Supporting context

Why?

What is the relevant market/news context?

### Layer 3: Detailed evidence

What prediction was generated?

What happened afterward?

How has the model performed historically?

### Layer 4: Technical detail

Model/version/evaluation details where useful.

This prevents the primary UI from becoming a dump of every available
field.

------------------------------------------------------------------------

# 13. Intelligence vs Data

The redesign should distinguish:

> **Data**

from:

> **Interpretation**

Examples:

### Data

-   stock price;
-   daily change;
-   article timestamp;
-   prediction date;
-   settlement price.

### Interpretation

-   BUY/HOLD/SELL signal;
-   confidence;
-   sentiment;
-   evaluation result;
-   model-performance metric.

The UI should make these relationships understandable without pretending
interpretation is certainty.

------------------------------------------------------------------------

# 14. Trust & Transparency

Trust is a core design requirement.

The product must communicate when information is:

-   current;
-   historical;
-   pending;
-   stale;
-   unavailable;
-   evaluated;
-   unevaluated;
-   demo.

## 14.1 No false precision

Do not display excessive decimal places merely because the backend
returns them.

## 14.2 No false certainty

Do not use language such as:

-   guaranteed;
-   certain;
-   will rise;
-   will fall;

unless the product explicitly has a defensible basis for such claims.

## 14.3 No fabricated explanations

If model explainability is unavailable, say so.

Do not generate a plausible explanation simply because the UI has an
empty section.

------------------------------------------------------------------------

# 15. Financial Semantics

Financial/model terminology must be precise.

Examples requiring canonical definitions:

-   confidence;
-   accuracy;
-   correctness;
-   return;
-   prediction horizon;
-   maturity;
-   settlement;
-   signal;
-   sentiment.

The frontend must not reinterpret these terms.

For historical predictions specifically:

``` text
Prediction
    ↓
10 valid trading sessions
    ↓
Evaluation eligibility
    ↓
Canonical settlement
    ↓
Evaluation result
```

Calendar days must not be substituted for valid trading sessions.

The canonical settlement-price basis must not be substituted by a
convenient frontend price.

------------------------------------------------------------------------

# 16. Signal Semantics

BUY/HOLD/SELL is a shared product language.

The same signal must:

-   use consistent terminology;
-   use consistent visual semantics;
-   remain recognizable across pages.

However:

> signal ≠ confidence ≠ evaluation status

For example:

``` text
Signal: BUY
Confidence: 84%
Status: Pending
```

is completely valid.

Do not collapse these concepts into one badge.

------------------------------------------------------------------------

# 17. Current vs Historical

The product must make a strong distinction between:

### Current state

What StockIntel currently recommends.

### Historical state

What StockIntel previously predicted.

Recommendations represents current output.

Prediction History represents historical output.

Stock Detail may show both, but they must remain distinguishable.

Historical prediction snapshots must not silently change because the
current model now produces a different result.

------------------------------------------------------------------------

# 18. Market State

Market state should be treated as contextual information.

Possible states include:

-   market open;
-   market closed;
-   pre-market/other supported session states;
-   latest available data;
-   stale data.

"Market closed" is not an error.

The UI should avoid implying live movement when the latest data is
historical.

------------------------------------------------------------------------

# 19. Light & Dark Themes

Light and dark are both first-class themes.

The redesign must not be:

``` text
dark design
+
inverted colors
=
light theme
```

Instead:

``` text
Shared semantic design
        ↓
 ┌───────────────┐
 │               │
Light           Dark
 │               │
 └───────┬───────┘
         ↓
Equivalent hierarchy
```

Both themes must support the same information hierarchy and semantics.

------------------------------------------------------------------------

# 20. Semantic Color Philosophy

Color should communicate meaning rather than decorate the interface.

Primary semantic categories include:

-   positive;
-   negative;
-   neutral;
-   informational;
-   warning;
-   unavailable.

Signal colors should be restrained.

Do not:

-   color entire tables green/red;
-   use excessive signal-colored backgrounds;
-   make every positive number bright green;
-   make every negative number aggressive red.

Color should guide attention, not overwhelm it.

------------------------------------------------------------------------

# 21. Typography Direction

Typography should establish hierarchy through:

-   size;
-   weight;
-   contrast;
-   grouping.

Do not rely on oversized typography to create hierarchy.

The product should feel dense but readable.

Important numbers should be visually scannable.

Secondary metadata should recede.

------------------------------------------------------------------------

# 22. Layout Direction

Use a consistent layout grid.

Pages should generally have:

``` text
Page Header
    ↓
Primary Intelligence / Summary
    ↓
Primary Workspace
    ↓
Secondary Context
    ↓
Deep Investigation
```

Not:

``` text
12 cards
12 cards
chart
table
random card
another chart
```

The user should understand why sections exist and how they relate.

------------------------------------------------------------------------

# 23. Cards

Cards are useful but must not become the universal UI primitive.

Use cards for:

-   summaries;
-   compact intelligence;
-   focused information groups;
-   market indices;
-   recommendations;
-   prediction summaries.

Use tables for:

-   large structured datasets;
-   comparisons;
-   historical records;
-   screening results.

Do not convert every table into oversized cards.

------------------------------------------------------------------------

# 24. Tables

Tables are a core StockIntel interaction pattern.

They should be:

-   dense;
-   readable;
-   sortable where meaningful;
-   filterable where appropriate;
-   keyboard accessible;
-   visually hierarchical.

Rows should not become visual noise.

Ticker/company identity should remain easy to scan.

------------------------------------------------------------------------

# 25. Charts

Charts must answer questions.

Examples:

-   How has the market moved?
-   How has the stock moved?
-   How are signals distributed?
-   How has model performance changed?

Do not add charts solely because a page has available space.

Every chart should have:

-   defined purpose;
-   meaningful timeframe;
-   readable labels;
-   data context;
-   accessible interpretation.

------------------------------------------------------------------------

# 26. News

News is not a separate generic media product.

StockIntel uses news as intelligence/context.

The experience should help answer:

-   what is being reported;
-   what sentiment is present;
-   which stocks are affected;
-   what deserves attention.

Do not imply that a news article caused a model prediction unless the
actual system establishes that relationship.

------------------------------------------------------------------------

# 27. Model Intelligence

Model Intelligence should communicate evidence about model behavior.

It should not become:

-   an ML notebook;
-   a training-console dump;
-   a collection of meaningless metrics.

The page should prioritize:

-   aggregate performance;
-   class-level performance;
-   evaluation volume;
-   model/version context;
-   supported explainability.

Detailed individual evidence belongs in Prediction History.

------------------------------------------------------------------------

# 28. Prediction History

Prediction History is an audit/investigation workspace.

It should make the prediction lifecycle understandable:

``` text
Generated
    ↓
Pending
    ↓
Mature after valid trading-session horizon
    ↓
Evaluated
```

Possible final states:

``` text
Evaluated
Unable to evaluate
```

Pending must never be presented as incorrect.

Unable to evaluate must never be presented as incorrect unless the
canonical evaluator explicitly says so.

------------------------------------------------------------------------

# 29. Empty States

Empty states are part of the product design, not implementation
leftovers.

A good empty state should:

-   explain the condition;
-   preserve the surrounding hierarchy;
-   provide a useful next action where appropriate.

Never fill an empty area with invented data.

------------------------------------------------------------------------

# 30. Error & Partial Failure Philosophy

StockIntel depends on multiple data sources and processing stages.

Therefore the UI must tolerate imperfect systems.

Prefer:

``` text
Market data: available
News: unavailable
```

over:

``` text
Entire application: broken
```

when the news source alone has failed.

Errors should be scoped to the affected information.

------------------------------------------------------------------------

# 31. Demo Data

Demo data must be unmistakable.

The user must never reasonably confuse:

-   demo predictions;
-   demo evaluations;
-   demo prices;

with production information.

Do not use demo records merely to make an empty production screen look
populated.

------------------------------------------------------------------------

# 32. Responsive Philosophy

Responsive design is not a desktop layout compressed into a phone.

Desktop prioritizes:

-   density;
-   simultaneous context;
-   tables;
-   persistent navigation.

Mobile prioritizes:

-   primary information;
-   signal;
-   stock identity;
-   key values;
-   status;
-   action.

Examples:

``` text
Desktop table
→ Mobile compact prediction/recommendation cards
```

``` text
Desktop filter sidebar
→ Mobile filter drawer
```

------------------------------------------------------------------------

# 33. Accessibility Philosophy

Accessibility is part of product quality.

The interface must remain understandable without relying on:

-   color;
-   hover;
-   animation;
-   visual position alone.

Important financial/model semantics must remain explicit in text.

------------------------------------------------------------------------

# 34. Animation Philosophy

Animation is optional and subordinate.

Use it when it helps:

-   transition between states;
-   communicate interaction;
-   improve perceived responsiveness.

Avoid:

-   decorative motion;
-   constant movement;
-   excessive chart animation;
-   flashy transitions.

Respect reduced-motion preferences.

------------------------------------------------------------------------

# 35. What This Redesign Is Not

The redesign is not intended to become:

-   a trading execution platform;
-   a portfolio-management system;
-   a social investing network;
-   a crypto-style terminal;
-   a generic financial-news website;
-   a personal trading journal;
-   an AI financial-advisor chatbot;
-   a fake backtesting product;
-   an ML research notebook.

Those are different products.

------------------------------------------------------------------------

# 36. Scope Boundaries

The redesign may improve:

-   visual hierarchy;
-   information architecture;
-   component consistency;
-   responsive behavior;
-   accessibility;
-   data-state communication;
-   navigation;
-   presentation of existing intelligence.

It must not silently expand into:

-   new ML methodology;
-   new prediction methodology;
-   new evaluation methodology;
-   new settlement methodology;
-   unrelated backend redesign;
-   unrelated authentication changes;
-   portfolio functionality;
-   trading execution;
-   unsupported financial features.

------------------------------------------------------------------------

# 37. Non-Negotiable Design Decisions

The following decisions are locked unless explicitly changed by the
user.

1.  StockIntel is an intelligence product, not a generic stock website.
2.  Redesign the UX structure, not merely the colors.
3.  Preserve analytical data density.
4.  Light and dark themes are equally important.
5.  Semantic theme tokens are mandatory.
6.  BUY/HOLD/SELL semantics remain consistent.
7.  Signal, confidence, status, and outcome remain separate concepts.
8.  Current recommendations and historical predictions remain distinct.
9.  Stock Detail is the primary deep-investigation destination.
10. Recommendations and Screener remain separate workspaces.
11. News Intelligence is an intelligence layer, not a generic feed.
12. Prediction History is an auditable historical prediction workspace.
13. Model Intelligence owns aggregate model analysis.
14. Pending predictions are not outcomes.
15. Unable-to-evaluate predictions are not automatically incorrect.
16. Prediction maturity uses valid trading sessions.
17. Evaluation uses canonical production semantics.
18. Settlement uses the canonical production settlement basis.
19. Historical prediction snapshots must remain immutable.
20. No fake data.
21. No fabricated model explanations.
22. No invented financial metrics.
23. No unsupported causal claims.
24. No unnecessary animation.
25. No unnecessary dependencies.
26. No silent scope creep.
27. Accessibility is mandatory.
28. Visual QA is mandatory.
29. Backend/domain semantics remain authoritative.
30. Implementation must follow the approved documentation rather than
    inventing product behavior.

------------------------------------------------------------------------

# 38. Design Decision Test

When a design decision is unclear, evaluate it against these questions:

### Does it improve comprehension?

If no, question it.

### Does it preserve analytical usefulness?

If no, reject it unless there is a strong reason.

### Does it represent real system data?

If no, reject it.

### Does it introduce unsupported assumptions?

If yes, reject it.

### Does it improve a genuine workflow?

If no, avoid it.

### Does it work in light and dark?

If no, it is incomplete.

### Does it work on mobile?

If no, determine the appropriate responsive transformation.

### Does it belong to this product's scope?

If no, defer it.

------------------------------------------------------------------------

# 39. Relationship to Other Documents

This document establishes overall direction.

Use:

### `01-UX-PRINCIPLES.md`

for detailed decision-making principles.

### `02-INFORMATION-ARCHITECTURE.md`

for navigation and information relationships.

### `03-DESIGN-SYSTEM.md`

for visual tokens, typography, spacing, surfaces, and shared visual
language.

### `04-THEME-SPECIFICATION.md`

for light/dark theme implementation.

### `05-PAGE-SPECIFICATIONS.md`

for page-level structure and behavior.

### `06-COMPONENT-SPECIFICATIONS.md`

for reusable component contracts.

### `07-IMPLEMENTATION-RULES.md`

for implementation workflow, validation, and engineering constraints.

------------------------------------------------------------------------

# 40. Success Criteria

The redesign is successful when a user can:

-   understand the current market/model state quickly;
-   identify relevant recommendations;
-   discover stocks using the Screener;
-   understand a stock's current model signal;
-   inspect supporting context;
-   distinguish current predictions from historical predictions;
-   understand whether historical predictions have matured;
-   understand evaluated outcomes without ambiguity;
-   inspect aggregate model performance;
-   navigate naturally between related information.

The redesign should make StockIntel feel:

> **more intelligent, more coherent, more trustworthy, and easier to
> investigate without making it less analytical.**

------------------------------------------------------------------------

# 41. Final Design Principle

> **Make the important obvious, the detailed accessible, the uncertain
> honest, and the complex coherent.**

The redesign should not hide complexity.

It should organize complexity so that users can understand it
progressively.

That is the central design direction for StockIntel.

# StockIntel UI Redesign --- Information Architecture

**Document:** `02-INFORMATION-ARCHITECTURE.md`\
**Product:** StockIntel\
**Status:** Information architecture specification\
**Purpose:** Define how StockIntel's workspaces, information,
navigation, and investigation flows are organized.

------------------------------------------------------------------------

# 1. Purpose

This document defines the information architecture for the StockIntel
redesign.

It establishes:

-   primary application structure;
-   workspace hierarchy;
-   navigation;
-   page relationships;
-   information ownership;
-   Stock Detail's role as the investigation destination;
-   current versus historical information boundaries;
-   cross-page discovery paths;
-   responsive navigation behavior;
-   deep-linking and context preservation.

The objective is to ensure StockIntel feels like **one coherent
intelligence product**, rather than a collection of unrelated pages.

------------------------------------------------------------------------

# 2. Core IA Principle

StockIntel should organize information according to the user's
analytical workflow, not according to the backend database structure.

The user should move naturally through:

``` text
OBSERVE
   ↓
PRIORITIZE
   ↓
DISCOVER
   ↓
INVESTIGATE
   ↓
UNDERSTAND CONTEXT
   ↓
REVIEW HISTORY
   ↓
ASSESS MODEL
```

This maps to the product's major workspaces.

------------------------------------------------------------------------

# 3. Primary Application Hierarchy

The primary application navigation is:

``` text
StockIntel
│
├── Dashboard
│
├── Recommendations
│
├── Screener
│
├── News Intelligence
│
├── Prediction History
│
└── Model Intelligence
```

Stock Detail is a **deep investigation destination**, not a peer
workspace that competes for primary navigation attention.

Conceptually:

``` text
Primary Workspaces
        │
        ▼
    Stock Detail
        │
        ▼
Historical / Model Evidence
```

------------------------------------------------------------------------

# 4. Workspace Responsibilities

Each workspace owns one dominant user task.

  -----------------------------------------------------------------------
  Workspace               Responsibility          Primary question
  ----------------------- ----------------------- -----------------------
  Dashboard               Market/model overview   What is happening?

  Recommendations         Current model output    What does StockIntel
                                                  recommend?

  Screener                Stock discovery         Which stocks meet my
                                                  criteria?

  News Intelligence       News/context            What is happening in
                                                  the news?

  Stock Detail            Individual              Why this stock/signal?
                          investigation           

  Prediction History      Historical evidence     What did the model
                                                  predict?

  Model Intelligence      Aggregate evaluation    How does the model
                                                  perform?
  -----------------------------------------------------------------------

No workspace should become a duplicate of another.

------------------------------------------------------------------------

# 5. Dashboard as Overview

Dashboard is the top-level orientation surface.

It should provide enough information for users to decide where to
investigate next.

Its information should be a curated subset of:

-   market state;
-   current model signals;
-   strongest recommendations;
-   news context;
-   watchlist information.

It should not become the authoritative workspace for any one of those
domains.

For example:

-   full recommendation analysis belongs in Recommendations;
-   full filtering belongs in Screener;
-   full news analysis belongs in News Intelligence;
-   individual stock analysis belongs in Stock Detail;
-   historical evaluation belongs in Prediction History;
-   aggregate model analysis belongs in Model Intelligence.

------------------------------------------------------------------------

# 6. Recommendations as Current Model Workspace

Recommendations owns the current model-output dataset.

It answers:

> What does StockIntel currently recommend?

It may expose:

-   all current recommendations;
-   signal distribution;
-   confidence;
-   ranking;
-   filtering;
-   sorting.

It should link to Stock Detail for investigation.

It should not become a historical performance page.

------------------------------------------------------------------------

# 7. Screener as Discovery Workspace

Screener owns user-driven stock discovery.

It answers:

> Which stocks meet my criteria?

It should allow users to:

-   define criteria;
-   filter the supported stock universe;
-   inspect results;
-   compare results;
-   navigate to Stock Detail.

Screener and Recommendations must remain conceptually separate.

### Recommendations

System-driven:

> Here are the model's current recommendations.

### Screener

User-driven:

> Show me stocks matching these criteria.

A filter that happens to include model signal does not make Screener
equivalent to Recommendations.

------------------------------------------------------------------------

# 8. News Intelligence as Context Workspace

News Intelligence owns the news and sentiment investigation workflow.

It answers:

> What is being reported and what market/stock context does it provide?

It should expose:

-   aggregate sentiment where supported;
-   stock-related sentiment;
-   relevant stories;
-   search;
-   filtering;
-   source information.

News items should connect to affected Stock Detail pages where a
supported stock relationship exists.

------------------------------------------------------------------------

# 9. Stock Detail as Investigation Hub

Stock Detail is the central entity-level destination.

A stock can be discovered from:

-   Dashboard;
-   Recommendations;
-   Screener;
-   News Intelligence;
-   Watchlist;
-   Prediction History.

All of these should converge on Stock Detail.

``` text
Dashboard ───────────┐
Recommendations ─────┤
Screener ─────────────┤
News Intelligence ───┤
Watchlist ────────────┤
Prediction History ──┘
             ↓
       ┌─────────────┐
       │ Stock Detail│
       └─────────────┘
```

Stock Detail should then expose paths to:

-   historical predictions;
-   related news;
-   Model Intelligence.

This makes Stock Detail the primary bridge between current information
and deeper evidence.

------------------------------------------------------------------------

# 10. Prediction History as Historical Evidence

Prediction History owns individual historical prediction records.

It answers:

> What did StockIntel predict, and what happened afterward?

It should not duplicate current Recommendations.

Its core entities are historical prediction instances.

Conceptually:

``` text
Current recommendation
        ≠
Historical prediction
```

Historical prediction records retain their historical context.

------------------------------------------------------------------------

# 11. Model Intelligence as Aggregate Evidence

Model Intelligence owns aggregate model-performance interpretation.

It answers:

> How does the model perform overall?

It should aggregate evidence from evaluated predictions.

It should not replace Prediction History.

Relationship:

``` text
Prediction History
individual evidence
        ↓
Model Intelligence
aggregate interpretation
```

Model Intelligence should provide a path back to underlying historical
evidence.

------------------------------------------------------------------------

# 12. Primary Navigation

Desktop primary navigation should expose:

1.  Dashboard
2.  Recommendations
3.  Screener
4.  News Intelligence
5.  Prediction History
6.  Model Intelligence

The active workspace must be visually obvious.

Navigation should not require users to understand backend terminology.

------------------------------------------------------------------------

# 13. Navigation Grouping

If visual grouping is used, it should reflect user tasks rather than
implementation details.

A possible conceptual grouping is:

``` text
OVERVIEW
  Dashboard

DISCOVER
  Recommendations
  Screener

CONTEXT
  News Intelligence

ANALYZE
  Prediction History
  Model Intelligence
```

Stock Detail remains contextual/deep navigation rather than a top-level
workspace.

Grouping is optional visually but the underlying conceptual hierarchy
should remain.

------------------------------------------------------------------------

# 14. Stock Detail Navigation Model

Stock Detail should be reachable from any surface where a specific stock
is identified.

Examples:

``` text
Dashboard
→ Strongest Signal
→ Stock Detail
```

``` text
Recommendations
→ Recommendation Row
→ Stock Detail
```

``` text
Screener
→ Result
→ Stock Detail
```

``` text
News
→ Article / Ticker
→ Stock Detail
```

``` text
Prediction History
→ Prediction
→ Stock Detail
```

The destination should be the same Stock Detail experience rather than
separate page variants for each origin.

------------------------------------------------------------------------

# 15. Context Preservation

When moving from a workspace into Stock Detail, preserve the originating
context where technically practical.

Example:

``` text
Recommendations
Signal = BUY
Sort = Confidence
Page = 2

↓ Stock Detail

Back

↓

Recommendations
Signal = BUY
Sort = Confidence
Page = 2
```

The same principle applies to:

-   Screener filters;
-   News filters;
-   Prediction History filters.

The user should not lose significant analytical context unnecessarily.

------------------------------------------------------------------------

# 16. Current vs Historical Information Architecture

This distinction is fundamental.

## Current

Owned primarily by:

-   Dashboard;
-   Recommendations;
-   Stock Detail current signal.

## Historical

Owned primarily by:

-   Prediction History;
-   Model Intelligence;
-   Stock Detail historical section.

The application should never make a historical prediction appear to be
the current recommendation.

------------------------------------------------------------------------

# 17. Entity Model

The core conceptual entity is the **Stock**.

Around it are related information types:

``` text
                 ┌─────────────┐
                 │    Stock    │
                 └──────┬──────┘
                        │
       ┌────────────────┼────────────────┐
       │                │                │
       ▼                ▼                ▼
  Recommendation      News          Predictions
       │                                 │
       │                                 ▼
       │                              Evaluation
       │
       ▼
  Current Signal
```

Additional relationships may include:

``` text
Stock
 ├── Market Data
 ├── News
 ├── Current Recommendation
 ├── Historical Predictions
 └── Evaluation Results
```

The exact backend schema is not defined here.

This is the UX entity model.

------------------------------------------------------------------------

# 18. Information Ownership

Avoid duplicate authoritative representations.

### Dashboard

Curates information.

### Recommendations

Owns current recommendation exploration.

### Screener

Owns filtering/discovery.

### News Intelligence

Owns news exploration.

### Stock Detail

Owns stock-level synthesis.

### Prediction History

Owns individual historical prediction exploration.

### Model Intelligence

Owns aggregate model-performance analysis.

A piece of information may appear in multiple places as a summary, but
its primary analytical ownership should remain clear.

------------------------------------------------------------------------

# 19. Summary vs Source

It is acceptable for the same information to appear as a summary in
multiple places.

Example:

A BUY recommendation can appear:

-   Dashboard;
-   Recommendations;
-   Stock Detail.

But each surface has a different purpose.

### Dashboard

Summary:

> RELIANCE --- BUY

### Recommendations

Detailed current recommendation record.

### Stock Detail

Full stock-level investigation.

This is intentional duplication of **access**, not duplication of
**responsibility**.

------------------------------------------------------------------------

# 20. Information Depth

The application should have four conceptual depth levels.

## Level 1 --- Overview

Dashboard.

## Level 2 --- Workspace

Recommendations, Screener, News Intelligence.

## Level 3 --- Entity investigation

Stock Detail.

## Level 4 --- Evidence and model analysis

Prediction History, Model Intelligence.

This structure allows users to progressively deepen their investigation.

------------------------------------------------------------------------

# 21. Discovery Paths

## 21.1 From Dashboard

Possible paths:

``` text
Dashboard
→ Recommendation
→ Stock Detail
```

``` text
Dashboard
→ News
→ News Intelligence
→ Stock Detail
```

``` text
Dashboard
→ Market context
→ relevant workspace
```

## 21.2 From Recommendations

``` text
Recommendations
→ Stock Detail
→ Prediction History
```

## 21.3 From Screener

``` text
Screener
→ Result
→ Stock Detail
```

## 21.4 From News

``` text
News Intelligence
→ Article
→ Stock Detail
```

## 21.5 From Prediction History

``` text
Prediction History
→ Historical Prediction
→ Stock Detail
```

## 21.6 From Model Intelligence

``` text
Model Intelligence
→ Performance metric
→ Prediction History
→ Stock Detail
```

------------------------------------------------------------------------

# 22. Navigation Should Follow User Intent

Do not design navigation around URL structure.

The user should think:

> "I want to investigate this stock."

not:

> "I need to go to `/prediction/history/detail/:id`."

URLs are implementation details.

The visual/navigation model should follow user tasks.

------------------------------------------------------------------------

# 23. Deep Linking

Important investigation states should be deep-linkable where technically
practical.

Potential examples:

``` text
Stock Detail
specific ticker

Recommendations
specific filters

Screener
specific filter configuration

Prediction History
specific prediction/filter state
```

Do not make every UI state part of the URL.

Only preserve state that materially improves:

-   navigation;
-   sharing;
-   refresh persistence;
-   investigation continuity.

------------------------------------------------------------------------

# 24. Filter State

Filtering workspaces should maintain clear ownership of filter state.

### Recommendations

Current recommendation filters.

### Screener

Stock-discovery filters.

### News Intelligence

News filters.

### Prediction History

Historical prediction filters.

Do not create a single global filter system that causes unrelated
workspaces to interfere with each other.

------------------------------------------------------------------------

# 25. Search Architecture

Search should be contextual unless the product explicitly supports
global search.

Examples:

### Screener

Search stocks.

### News Intelligence

Search news/tickers/company names.

### Prediction History

Search historical predictions by stock.

A future global search may exist, but it should not be assumed as part
of v1.

------------------------------------------------------------------------

# 26. Responsive Navigation Architecture

## Desktop

Persistent sidebar navigation is appropriate.

## Mobile

Use a compact navigation pattern such as:

-   top-bar navigation;
-   drawer/sheet;
-   bottom navigation if justified by the final design.

The chosen mobile pattern must preserve access to all major workspaces.

Do not simply hide half the application.

------------------------------------------------------------------------

# 27. Page-to-Page Semantic Boundaries

## Dashboard → Recommendations

Moves from overview to current model-output detail.

## Dashboard → News Intelligence

Moves from summary context to news investigation.

## Recommendations → Stock Detail

Moves from current recommendation to individual investigation.

## Screener → Stock Detail

Moves from discovery to investigation.

## News Intelligence → Stock Detail

Moves from external context to stock-level synthesis.

## Prediction History → Stock Detail

Moves from historical prediction evidence to entity-level context.

## Model Intelligence → Prediction History

Moves from aggregate performance to individual evidence.

These transitions should remain conceptually stable.

------------------------------------------------------------------------

# 28. Back Navigation

Back navigation should generally return to the user's previous
analytical context.

If a deep link is opened directly, back navigation should behave
according to normal browser/application behavior.

Do not manufacture a fake origin when none exists.

------------------------------------------------------------------------

# 29. Breadcrumbs

Breadcrumbs are optional.

They may be useful for:

``` text
Prediction History
→ Stock Detail
```

or complex nested contexts.

They should not be added merely because the product has multiple pages.

The sidebar plus page header may be sufficient for most routes.

------------------------------------------------------------------------

# 30. Modals, Drawers & Pages

Use a full page for:

-   Stock Detail;
-   major workspaces;
-   substantial historical analysis.

Use a drawer for:

-   mobile filters;
-   temporary contextual controls;
-   focused secondary information.

Use a modal only for genuinely interruptive/focused actions.

Do not turn Stock Detail into a modal.

------------------------------------------------------------------------

# 31. Data-State Architecture

Information architecture must represent state as part of the content
model.

Example:

``` text
Prediction
 ├── Current
 ├── Pending
 ├── Evaluated
 └── Unable to evaluate
```

Similarly:

``` text
Data
 ├── Fresh
 ├── Stale
 └── Unavailable
```

These are meaningful states, not incidental UI styling.

------------------------------------------------------------------------

# 32. Prediction Lifecycle in IA

The historical prediction lifecycle should be conceptually represented
as:

``` text
Generated
    ↓
Pending
    ↓
Maturity after valid trading sessions
    ↓
Evaluation
    ↓
Canonical settlement
    ↓
Evaluated outcome
```

This lifecycle belongs to Prediction History and relevant Stock Detail
historical views.

The frontend must not redefine the lifecycle.

------------------------------------------------------------------------

# 33. Model Version Context

Where multiple model versions exist, model identity should remain
attached to historical prediction/evaluation evidence.

A historical prediction should not appear as if it was generated by the
currently active model when it was not.

Model Intelligence owns comparison/aggregate interpretation.

Prediction History provides individual model-version context where
supported.

------------------------------------------------------------------------

# 34. News Relationship Model

News may relate to:

-   market;
-   sector;
-   stock/company.

The IA should distinguish these levels.

``` text
Market News
    ↓
Market context

Stock News
    ↓
Stock Detail
```

Do not force every article into a stock relationship if none exists.

------------------------------------------------------------------------

# 35. Watchlist Relationship

Watchlist is a cross-cutting personal tracking concept.

It can appear in:

-   Dashboard;
-   Stock Detail;
-   potentially Recommendations/Screener.

Watchlist state does not change:

-   recommendation;
-   prediction;
-   evaluation;
-   stock data.

It is user state layered on top of product data.

------------------------------------------------------------------------

# 36. Information Duplication Rules

Duplication is acceptable when it serves different user contexts.

For example:

``` text
BUY recommendation
```

may appear on Dashboard, Recommendations, and Stock Detail.

However, each representation should have different depth.

Do not duplicate full datasets across pages merely to avoid navigation.

------------------------------------------------------------------------

# 37. Avoid Information Silos

Important information should not exist only in one inaccessible
workspace if users naturally encounter it elsewhere.

Examples:

-   stock identity should lead to Stock Detail;
-   prediction history should lead to stock;
-   news should lead to affected stock;
-   model metrics should lead to evidence.

The IA should make relationships discoverable.

------------------------------------------------------------------------

# 38. Avoid Cross-Workspace Coupling

Workspaces should not unexpectedly change each other's state.

For example:

Changing a Screener filter must not change Recommendations filters.

Changing News filters must not alter Dashboard content.

Each workspace owns its own state unless a deliberate shared feature is
defined.

------------------------------------------------------------------------

# 39. IA Anti-Patterns

Do not:

-   duplicate the same workspace under different names;
-   make Stock Detail a modal;
-   hide major workspaces in nested menus;
-   create separate Stock Detail variants for each origin;
-   mix current recommendations with historical predictions;
-   mix aggregate model metrics with individual predictions;
-   make Screener merely another Recommendations filter panel;
-   turn News Intelligence into a generic feed;
-   make Dashboard a duplicate of every other page;
-   expose backend entities directly as navigation;
-   create navigation based solely on database tables;
-   introduce global filters that interfere with workspace-specific
    analysis.

------------------------------------------------------------------------

# 40. IA Acceptance Criteria

The information architecture is successful when:

-   [ ] Every major page has one dominant purpose.
-   [ ] Users can identify where to perform common analytical tasks.
-   [ ] Stock Detail is the consistent entity-level destination.
-   [ ] Current and historical information are clearly separated.
-   [ ] Recommendations and Screener remain distinct.
-   [ ] News Intelligence has a distinct role.
-   [ ] Prediction History and Model Intelligence have distinct roles.
-   [ ] Related information can be reached without dead ends.
-   [ ] Workspace state does not unexpectedly leak into unrelated
    workspaces.
-   [ ] Navigation works on desktop and mobile.
-   [ ] Deep investigation can preserve useful context.
-   [ ] Backend structure does not dictate user-facing navigation.
-   [ ] No major information relationship depends on hidden UI behavior.

------------------------------------------------------------------------

# 41. Final IA Principle

> **Organize StockIntel around the questions users are trying to answer,
> then connect the evidence needed to answer those questions.**

The product should feel like one connected investigation system:

``` text
Observe
  ↓
Discover
  ↓
Prioritize
  ↓
Investigate
  ↓
Understand
  ↓
Verify
  ↓
Assess
```

The user should never need to understand StockIntel's internal
architecture in order to understand StockIntel itself.

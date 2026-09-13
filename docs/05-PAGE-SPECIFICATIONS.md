# StockIntel UI Redesign --- Page Specifications

**Document:** `05-PAGE-SPECIFICATIONS.md`\
**Product:** StockIntel\
**Status:** Design specification\
**Scope:** Page-level UX/UI behavior and information architecture\
**Audience:** Product/design/implementation agents\
**Authority:** This document defines approved page-level behavior.
Reusable component behavior belongs in `06-COMPONENT-SPECIFICATIONS.md`;
implementation constraints belong in `07-IMPLEMENTATION-RULES.md`.

------------------------------------------------------------------------

## 1. Purpose & Authority

This document translates the approved StockIntel UX direction into
concrete page specifications.

It defines:

-   why each page exists;
-   what user questions it must answer;
-   information hierarchy;
-   page structure;
-   required data;
-   interactions;
-   states;
-   responsive behavior;
-   light/dark behavior;
-   cross-page navigation;
-   explicit prohibitions.

This document is **not** a backend specification and must not redefine
ML, prediction, evaluation, settlement, or market-data semantics.

### 1.1 Source-of-truth hierarchy

When implementing a page, use the following authority order:

1.  Explicit current user instruction
2.  Product requirements and approved scope
3.  This page specification
4.  Component specifications
5.  Theme/design-system specifications
6.  UX principles
7.  Existing implementation

Existing UI behavior must not override an approved redesign requirement
merely because it already exists.

### 1.2 No invented semantics

The frontend must never invent:

-   model meaning;
-   confidence meaning;
-   prediction correctness;
-   settlement methodology;
-   maturity dates;
-   trading-session calculations;
-   financial metrics;
-   news relevance methodology;
-   causal relationships;
-   missing model explanations.

If a required value is not supported by the backend/API contract, the UI
must represent the value as unavailable rather than manufacture it.

------------------------------------------------------------------------

# 2. Global Page Requirements

All pages share a common product language.

## 2.1 Product character

StockIntel should feel like:

> **An intelligence system that happens to operate on financial
> markets.**

The visual/product direction combines:

-   institutional research terminal;
-   modern SaaS;
-   restrained financial analytics.

Avoid:

-   crypto/neon aesthetics;
-   excessive gradients;
-   oversized marketing typography;
-   unnecessary glassmorphism;
-   decorative animations;
-   consumer-finance gimmicks;
-   dashboard clutter.

## 2.2 Information hierarchy

Every page should answer:

1.  What is this page for?
2.  What is the most important information?
3.  What can I investigate next?
4.  What is the freshness/state of the underlying data?

Information density is acceptable. Unstructured density is not.

## 2.3 Data freshness

Where data can become stale, display an appropriate freshness indicator.

Examples:

-   Last updated time
-   Data as of date
-   Evaluation last run
-   Model version
-   Market session/date

Do not imply real-time data when the underlying source is delayed or
historical.

Different datasets may have different timestamps. Do not collapse them
into one misleading "updated" timestamp.

## 2.4 Loading states

Loading states must preserve layout hierarchy.

Prefer:

-   skeletons for known content structures;
-   localized loading indicators for individual sections;
-   preserved page shell.

Do not replace the entire page with a generic spinner when only one
section is loading.

## 2.5 Empty states

Empty states must explain why content is absent where the reason is
known.

Examples:

-   No recommendations generated yet
-   No stocks match these criteria
-   No news available
-   No predictions have matured
-   Watchlist is empty

Never fill empty states with fake data.

## 2.6 Error states

Errors must distinguish between:

-   complete page failure;
-   individual data-source failure;
-   stale data;
-   unavailable evaluation;
-   missing historical data;
-   invalid/unknown ticker.

A failed request must not be visually represented as a valid zero.

## 2.7 Partial failure

A page may contain usable data even when one source fails.

Example:

-   recommendations load;
-   news service fails.

The recommendations remain visible while the news section communicates
its own failure.

Do not hide the entire page because one auxiliary source failed.

## 2.8 Demo/live state

Demo data must be unmistakable.

Use a persistent but restrained indicator such as:

> Demo data

Never allow demo records to appear indistinguishable from production
predictions or evaluations.

## 2.9 Market closed

"Market closed" is a legitimate market state, not automatically an
error.

Where relevant, show:

-   market status;
-   last available market timestamp;
-   data date.

Do not imply prices are changing live when the market is closed.

## 2.10 Responsive behavior

Desktop and mobile are separate layout considerations.

Do not simply shrink desktop layouts.

On mobile:

-   preserve hierarchy;
-   prioritize primary information;
-   convert dense tables into cards/compact rows where required;
-   move filters into drawers where appropriate;
-   avoid horizontal overflow unless the data genuinely requires it.

## 2.11 Accessibility

All pages must support:

-   keyboard navigation;
-   visible focus;
-   sufficient contrast;
-   semantic headings;
-   accessible table structure;
-   non-color-only status communication;
-   usable controls;
-   meaningful labels;
-   accessible charts or textual summaries.

------------------------------------------------------------------------

# 3. Dashboard

## 3.1 Purpose

The Dashboard is the high-level StockIntel intelligence overview.

It should answer within approximately ten seconds:

-   What is happening in the market?
-   What does StockIntel currently see?
-   Which stocks deserve attention?
-   How strong are the signals?
-   What news/context matters?
-   Where should I investigate next?

The Dashboard is an **overview**, not a specialist workspace.

## 3.2 Primary mental model

> **Observe → prioritize → investigate**

## 3.3 Information hierarchy

1.  Intelligence/system status
2.  Market context
3.  Model intelligence
4.  Strongest signals
5.  Market/news context
6.  Personal tracking/watchlist
7.  Deep-dive navigation

## 3.4 Structure

### Header / Intelligence Status

Include:

-   page title;
-   concise description;
-   relevant market/data status;
-   model/version context if available;
-   freshness.

### Market Context

Display major supported market indices.

Each index card should prioritize:

-   index name;
-   current/latest value;
-   absolute or percentage change;
-   timestamp/date.

Cards must not dominate the entire page.

### Model Intelligence

Show:

-   signal distribution;
-   confidence distribution where supported;
-   strongest signals.

Signal distribution should make BUY/HOLD/SELL composition immediately
understandable.

### Strongest Signals

Display approximately 3--5 stocks.

Each item should include only information supported by the backend, such
as:

-   ticker/company;
-   signal;
-   confidence;
-   price;
-   change;
-   tier;
-   link to analysis.

Ranking must use a canonical backend ranking when one exists.

The frontend must not silently redefine "strongest" as "highest
confidence" unless that is explicitly the canonical ranking.

### Market & News Intelligence

Include:

-   market trend visualization where useful;
-   aggregate sentiment;
-   a concise set of important news items.

The chart must answer a meaningful question and provide supported
timeframe/index controls.

News is context, not a complete news feed.

### Watchlist / Personal Tracking

If the watchlist is populated:

-   show a compact selection of watched stocks;
-   provide direct navigation to Stock Detail.

If empty:

-   show a compact empty state explaining how the area becomes useful.

Do not create a large empty panel.

## 3.5 Interactions

-   Market cards may navigate to relevant market detail where supported.
-   Strongest signals navigate to Stock Detail.
-   News items navigate to News Intelligence or the relevant stock.
-   Watchlist items navigate to Stock Detail.
-   Chart controls change only supported dimensions.

## 3.6 States

Support:

-   normal;
-   loading;
-   empty model data;
-   no recommendations;
-   news unavailable;
-   partial failure;
-   stale data;
-   market closed;
-   demo mode.

## 3.7 Mobile

Order:

1.  Market context
2.  Model signals
3.  Strongest opportunities
4.  News
5.  Watchlist

Avoid a desktop-style multi-column grid that becomes a long sequence of
visually equal cards.

## 3.8 Do not do

-   Do not make one giant "Top Recommendation" the entire page.
-   Do not turn the Dashboard into a Screener.
-   Do not duplicate the full Recommendations table.
-   Do not show decorative charts.
-   Do not fabricate model explanations.
-   Do not overuse BUY/SELL colors.
-   Do not imply financial certainty.

------------------------------------------------------------------------

# 4. Recommendations

## 4.1 Purpose

Recommendations is the primary workspace for reviewing **current
model-generated recommendations**.

Core question:

> **What does StockIntel currently recommend?**

## 4.2 Primary mental model

> **Understand current model output → prioritize → filter → inspect**

## 4.3 Information hierarchy

1.  Intelligence/system status
2.  Recommendation summary
3.  Strongest signals
4.  Filters
5.  Dense recommendation table
6.  Pagination/navigation

## 4.4 Recommendation Summary

Show:

-   total recommendation count;
-   BUY count;
-   HOLD count;
-   SELL count;
-   relevant freshness/status.

Use compact visual hierarchy.

## 4.5 Strongest Signals

Display approximately 3--5 important recommendations.

Each item may contain:

-   ticker/company;
-   signal;
-   confidence;
-   price;
-   change;
-   tier;
-   View Analysis action.

Use canonical ranking where available.

Do not implement frontend-only ranking logic unless explicitly required
by the API contract.

## 4.6 Filters

Core filters:

-   Signal;
-   Confidence;
-   Search.

Additional filters are permitted only when supported by backend data and
product requirements, such as:

-   sector;
-   market cap;
-   prediction horizon.

Provide basic/advanced organization when filter count becomes
substantial.

Active filters must be visible as removable chips.

Provide:

> Clear all

when filters are active.

## 4.7 Recommendation Table

Suggested columns:

-   Stock;
-   Signal;
-   Price;
-   Change;
-   Confidence;
-   Tier;
-   Sector where supported.

Ticker and signal should be visual anchors.

Do not color entire rows according to BUY/HOLD/SELL.

Rows should be clickable to Stock Detail.

## 4.8 Sorting

Sorting may be offered for fields where ordering is meaningful.

The default ordering must reflect canonical recommendation ranking if
one exists.

Do not create a new ranking methodology in the frontend.

## 4.9 Pagination

Use normal pagination when dataset size is moderate.

Use virtualization only if actual dataset size requires it.

## 4.10 States

Support:

-   loading;
-   normal;
-   no recommendations;
-   filtered no-results;
-   stale;
-   market closed;
-   pipeline/evaluation unavailable;
-   demo.

## 4.11 Mobile

Replace the desktop table with compact recommendation cards/rows
containing:

-   ticker;
-   signal;
-   confidence;
-   price/change;
-   tier;
-   navigation.

Filters become a filter drawer.

## 4.12 Do not do

-   Do not turn recommendations into a trading execution screen.
-   Do not use "BUY NOW" language.
-   Do not imply certainty.
-   Do not add unsupported target prices.
-   Do not duplicate Screener functionality.
-   Do not add an AI chat layer.
-   Do not create unexplained scoring formulas.

------------------------------------------------------------------------

# 5. Stock Detail

## 5.1 Purpose

Stock Detail is the central investigation destination.

Core question:

> **What does StockIntel currently think about this stock, and what
> evidence supports that view?**

## 5.2 Primary mental model

> **Identify → understand signal → inspect evidence → inspect context →
> inspect history**

## 5.3 Information hierarchy

1.  Stock header
2.  Model signal/intelligence summary
3.  Price/market chart
4.  Recommendation explanation
5.  Market/news context
6.  Model/prediction details
7.  Historical information

## 5.4 Stock Header

Display, where supported:

-   company name;
-   ticker;
-   exchange;
-   latest price;
-   price change;
-   timestamp/date;
-   watchlist control.

## 5.5 Model Signal Hero

Prioritize:

-   BUY/HOLD/SELL signal;
-   model confidence;
-   recommendation tier;
-   prediction horizon;
-   generation date/time;
-   model version where available.

The exact semantic meaning of confidence must come from the backend.

Do not label confidence as probability of future return unless that is
actually what the model outputs.

## 5.6 Why This Signal?

Provide evidence only when actual explainability/model-output data
exists.

Possible supported evidence:

-   model features;
-   feature contributions;
-   defined signal factors;
-   documented model rationale.

If explainability is unavailable:

> Explanation unavailable for this prediction.

Do not generate plausible-sounding AI explanations.

## 5.7 Price & Market Chart

Support the actual chart data available.

Potential controls:

-   timeframe;
-   supported data mode;
-   comparison index where available.

Preserve existing useful TradingView/StockIntel Data distinctions if
those are part of the implementation.

Signal markers may be added only when supported by the underlying data.

## 5.8 Market & News Context

Where supported:

-   stock sentiment;
-   relevant recent news;
-   sector/market comparison;
-   aggregate market context.

News items link to News Intelligence or source articles as appropriate.

Do not imply news caused a model prediction unless the system explicitly
establishes that relationship.

## 5.9 Prediction Information

Show relevant historical/current prediction information, including:

-   prediction date;
-   signal;
-   confidence;
-   horizon;
-   status;
-   evaluation result once legitimately available.

## 5.10 Historical Prediction Lifecycle

The page must distinguish:

-   current prediction;
-   pending historical prediction;
-   evaluated prediction;
-   unavailable evaluation.

Pending does not mean incorrect.

## 5.11 Model Information

Keep model context compact.

Display:

-   model/version;
-   prediction horizon;
-   relevant generation information.

Detailed model analytics belong in Model Intelligence.

Do not dump training hyperparameters into Stock Detail.

## 5.12 States

Support:

-   stock found with current prediction;
-   no current prediction;
-   pending evaluation;
-   evaluated prediction;
-   historical data unavailable;
-   news unavailable;
-   stale prediction;
-   market closed;
-   demo;
-   stock not found.

## 5.13 Navigation

Stock Detail is a convergence point for:

-   Dashboard;
-   Recommendations;
-   Screener;
-   Watchlist;
-   News Intelligence;
-   Prediction History.

Preserve originating context/back navigation where practical.

## 5.14 Mobile

Recommended order:

1.  Header
2.  Signal
3.  Key price information
4.  Chart
5.  Why this signal
6.  News/context
7.  Prediction history
8.  Model information

## 5.15 Do not do

-   Do not build a full trading terminal.
-   Do not overload with technical indicators.
-   Do not invent target prices.
-   Do not fabricate explanations.
-   Do not add unsupported fundamentals.
-   Do not create an AI financial-advisor chat.
-   Do not turn the page into a second Dashboard.

------------------------------------------------------------------------

## 5A. Watchlist V1 Scope Lock

Watchlist is **cross-cutting user state**, not a primary navigation workspace in v1.

It may be surfaced through:

- Dashboard
- Recommendations
- Screener
- Stock Detail

A dedicated Watchlist page is not required for v1 and must not be created merely because stocks can be added to a watchlist.

A future dedicated Watchlist workspace requires an explicit product and IA decision.

# 6. Screener

## 6.1 Purpose

Screener is the user-driven stock discovery workspace.

Core question:

> **Which stocks meet my criteria?**

## 6.2 Primary mental model

> **Define criteria → filter universe → compare → investigate**

## 6.3 Information hierarchy

1.  Header
2.  Result summary
3.  Filter workspace
4.  Active filter summary
5.  Results
6.  Pagination

Saved Screens are future scope and must not be implemented in v1 unless separately authorized.

## 6.4 Header

Explain that the page is for stock discovery and filtering.

Show relevant freshness/data context.

## 6.5 Result Summary

Display:

-   matching-stock count;
-   compact signal distribution;
-   relevant data status.

Example structure:

> 51 stocks match your criteria.

The exact count must come from the current result set.

## 6.6 Filter Workspace

Group filters by meaning.

### Model

-   Signal;
-   Confidence;
-   Prediction horizon.

### Market

-   Price;
-   Daily change;
-   Volume where supported.

### Company

-   Sector;
-   Market cap where supported.

Additional technical/fundamental filters may be added only when actual
backend data supports them.

## 6.7 Basic vs Advanced

Keep frequently used filters visible.

Move specialized filters into an advanced area.

Do not expose every backend field simply because it exists.

## 6.8 Active Filters

Display active filters as removable chips.

Provide Clear All.

## 6.9 Results

The results table is the dominant workspace.

Support:

-   ticker/company search;
-   supported sorting;
-   direct Stock Detail navigation.

Default ordering should have a documented reason.

## 6.10 Initial State

The initial state must be intentional.

Options include:

-   showing the full supported universe;
-   showing a defined default screen;
-   showing a clear "start filtering" state.

Do not falsely present "0 results" merely because no filters have been
chosen.

## 6.11 No Results

Explain:

> No stocks match the current criteria.

Provide useful filter-reset/adjustment guidance.

Do not fabricate alternatives.

## 6.12 Mobile

Use:

-   filter drawer;
-   active-filter chips;
-   compact result cards/rows.

Do not squeeze the full desktop table into a narrow viewport.

## 6.13 States

Support:

-   loading;
-   normal;
-   no filters/default;
-   no results;
-   stale;
-   partial data;
-   demo;
-   data unavailable.

## 6.14 Do not do

-   Do not duplicate Recommendations.
-   Do not add an "AI screener" in v1.
-   Do not add arbitrary filter types.
-   Do not create unsupported derived metrics.
-   Do not turn this into a portfolio tool.
-   Do not add saved screens until the core screener is stable.

------------------------------------------------------------------------

# 7. News Intelligence

## 7.1 Purpose

News Intelligence is a market-news intelligence workspace.

Core question:

> **What is being reported, which stocks/market areas are affected, what
> is the sentiment, and what context deserves attention?**

It is **not** simply a news feed.

## 7.2 Primary mental model

> **Observe sentiment → identify changes → inspect stories → investigate
> affected stocks**

## 7.3 Information hierarchy

1.  Header/data status
2.  Market sentiment overview
3.  Notable sentiment changes
4.  Search/filters
5.  News feed
6.  Source/article navigation
7.  Stock deep dive

## 7.4 Header

Display:

-   page title;
-   short description;
-   data freshness;
-   source status where meaningful.

## 7.5 Market Sentiment Overview

Show aggregate market sentiment when supported.

Possible representations:

-   sentiment distribution;
-   average/aggregate sentiment;
-   sentiment by market segment.

Do not invent an aggregate formula in the frontend.

## 7.6 Notable Sentiment Changes

Display sentiment changes only when historical sentiment information
exists.

If a backend relevance/change score exists, use the canonical score.

If no historical change metric exists, do not pretend that a current
article is a "sentiment shift."

## 7.7 Search & Filters

Support only meaningful backend-supported dimensions.

Suggested controls:

-   search by ticker/company/headline;
-   market vs stock scope;
-   sentiment;
-   time range where supported.

## 7.8 News Feed

Use compact editorial-style article rows/cards.

Prioritize:

-   ticker/company;
-   sentiment;
-   headline;
-   source;
-   timestamp;
-   original article link.

Do not reproduce full external articles.

## 7.9 Stock Relationship

When an article has a supported ticker relationship, provide direct
navigation to Stock Detail.

Market-level articles should remain clearly distinct from stock-specific
articles.

## 7.10 News → Model Relationship

A relationship between news and model signals may be displayed only if
the backend/system explicitly supports it.

Do not state:

> News caused the BUY signal.

unless the actual methodology establishes that causal relationship.

At most, where justified:

> Related model signal

or similar neutral language.

## 7.11 States

Support:

-   normal;
-   loading;
-   no news;
-   filtered no-results;
-   source failure;
-   partial source failure;
-   stale;
-   demo;
-   market closed.

## 7.12 Mobile

Prioritize:

1.  sentiment overview;
2.  filters;
3.  article list;
4.  affected-stock navigation.

Avoid giant article cards.

## 7.13 Do not do

-   Do not turn it into a generic news website.
-   Do not add social-media sentiment without backend support.
-   Do not invent sentiment changes.
-   Do not make causal claims.
-   Do not reproduce copyrighted article content.
-   Do not add unnecessary news categories.
-   Do not use infinite scrolling merely because news feeds
    traditionally do.

------------------------------------------------------------------------

# 8. Prediction History

## 8.1 Purpose

Prediction History is the audit and investigation workspace for
historical model predictions.

Core question:

> **What did StockIntel predict, what happened afterward, and how was
> that prediction evaluated?**

## 8.2 Primary mental model

> **Review prediction → understand lifecycle → inspect outcome → assess
> historical evidence**

## 8.3 Critical production principle

Prediction History must represent the actual production
prediction/evaluation lifecycle.

The frontend must not create a second interpretation of the prediction
system.

The canonical lifecycle is conceptually:

``` text
Prediction generated
        ↓
Prediction stored
        ↓
Waiting for evaluation maturity
        ↓
10 valid trading sessions
        ↓
Evaluation becomes eligible
        ↓
Canonical settlement price
        ↓
Evaluation result
```

The exact API contract remains the authoritative implementation source.

## 8.4 Evaluation Overview

Place summary metrics above the historical table.

Potential canonical metrics include:

-   evaluated predictions;
-   correct predictions;
-   accuracy;
-   pending predictions;
-   evaluation coverage where defined.

Only display a metric if the backend provides it or its calculation is
explicitly part of the canonical contract.

### Accuracy rule

Use the term **accuracy** only if the production evaluator defines
correctness as a classification outcome suitable for an accuracy
calculation.

Do not infer accuracy from raw returns.

## 8.5 Performance by Signal

Where supported, summarize performance by:

-   BUY;
-   HOLD;
-   SELL.

For each signal, potentially show:

-   evaluated count;
-   correct count;
-   accuracy.

Do not calculate these independently from raw records if an
authoritative backend aggregate exists.

If the backend provides only raw evaluated predictions, any frontend
aggregation must exactly reproduce documented backend semantics.

## 8.6 Historical Trend

An accuracy/performance-over-time visualization may be shown only when
enough evaluated observations exist to make it meaningful.

Do not produce a misleading trend from tiny samples.

Do not describe a trend as statistically significant without statistical
support.

## 8.7 Confidence Distribution

A confidence-range breakdown may be shown if confidence has a defined
backend meaning.

Possible grouping:

-   low;
-   medium;
-   high;

or numerical ranges.

The grouping must be documented and consistent.

Do not claim calibration solely because high-confidence predictions
appear more successful.

## 8.8 Filters

Core filters:

-   signal;
-   status;
-   prediction horizon;
-   generated date range;
-   ticker/company search.

Optional:

-   model version;
-   evaluation result;
-   sector;

only when supported and useful.

## 8.9 Prediction Table

Suggested columns:

  Field           Purpose
  --------------- -------------------------------------------
  Stock           Ticker + company
  Signal          Stored prediction output
  Confidence      Stored inference output
  Generated       Prediction generation date/time
  Horizon         Canonical prediction/evaluation horizon
  Status          Pending/evaluated/unavailable
  Outcome         Canonical evaluated outcome
  Return          Actual evaluated movement where supported
  Evaluation      Correct/incorrect where defined
  Model Version   Historical model identity where supported

The exact field names must follow the API contract.

## 8.10 Prediction Snapshot Semantics

Historical prediction records should be treated as immutable snapshots.

The UI should not rewrite historical prediction information using
current model state.

For example, historical records should retain their historical:

-   signal;
-   confidence;
-   model/version;
-   generation context;
-   prediction price/data basis;
-   evaluation result.

A current recommendation for a ticker must not overwrite the historical
prediction shown in Prediction History.

## 8.11 Confidence Semantics

The frontend must display confidence exactly as defined by the
model/API.

Do not assume:

> confidence = probability of positive return

unless the backend explicitly defines it that way.

Do not rename confidence to "probability" without confirmation.

## 8.12 Ten Valid Trading-Session Maturity

The production system uses an evaluation horizon based on **valid
trading sessions**, not a naive calendar-day difference.

Therefore:

-   weekends do not count as trading sessions;
-   market holidays do not count;
-   non-trading days must not advance maturity;
-   the frontend must not independently calculate maturity from calendar
    dates.

If the API provides:

-   maturity date;
-   evaluation eligibility;
-   remaining sessions;
-   status;

consume those canonical values.

If not provided, the UI should not implement its own trading-calendar
algorithm unless explicitly required by the backend/frontend contract.

## 8.13 Canonical Settlement Price

Evaluation outcome must use the production system's canonical
settlement-price basis.

The frontend must:

-   display the backend-provided settlement price/outcome;
-   never substitute a different market-data price;
-   never choose a convenient closing price from another source;
-   never recompute the settlement basis independently.

The UI may display the settlement date and price where available.

## 8.14 Pending Predictions

A pending prediction has been generated but does not yet have a
legitimate completed evaluation.

Display:

-   original prediction;
-   confidence;
-   generated date;
-   horizon;
-   pending status;
-   canonical maturity/evaluation information when available.

Do not display a guessed outcome.

Do not treat missing outcome as incorrect.

## 8.15 Evaluated Predictions

Once the production evaluator has completed evaluation, display:

-   original prediction;
-   confidence;
-   evaluation status;
-   canonical settlement information where supported;
-   actual outcome/return where supported;
-   correct/incorrect result where defined.

## 8.16 Unable to Evaluate

An evaluation that cannot legitimately be completed is not equivalent to
an incorrect prediction.

Display a distinct state such as:

> Unable to evaluate

Only provide a reason when supplied by the backend.

Do not classify the record as:

-   incorrect;
-   loss;
-   failed prediction;

unless the evaluator explicitly says so.

## 8.17 Failed Pipeline vs Failed Prediction

A pipeline/evaluation infrastructure failure must not be represented as
model failure.

The UI should distinguish:

``` text
Model evaluation result:
Incorrect

vs.

Evaluation status:
Unable to evaluate
```

This distinction is mandatory for trustworthy model-performance
reporting.

## 8.18 Prediction Detail Navigation

Individual historical predictions should be inspectable.

Preferred destination:

> Stock Detail with the selected prediction context.

Avoid creating a separate database-like prediction-detail page unless a
real user workflow requires it.

Stock Detail can provide:

-   prediction context;
-   chart;
-   signal;
-   outcome;
-   related news;
-   historical predictions.

## 8.19 Relationship With Model Intelligence

Prediction History is **instance-level**.

Model Intelligence is **aggregate-level**.

Prediction History answers:

> What happened to individual predictions?

Model Intelligence answers:

> How does the model perform overall?

The pages should cross-link.

## 8.20 States

Mandatory states:

### Normal

Evaluated and/or pending predictions are available.

### Pending Only

Predictions exist but none have matured for evaluation.

### No Predictions

No prediction records are available.

### No Evaluated Predictions

Predictions exist, but no completed evaluations are available.

### Filtered No Results

The dataset exists but the current filters produce zero records.

### Unable to Evaluate

Some records cannot be evaluated.

### Partial Evaluation

Some records have valid evaluations while others remain
pending/unavailable.

### Stale Evaluation

Historical predictions exist but evaluation data has not refreshed as
expected.

### Demo

Records are explicitly identified as demo data.

### Data Failure

The evaluation/prediction source is unavailable.

## 8.21 Mobile

Use a compact prediction-card representation.

Example structure:

``` text
RELIANCE
BUY · 84%

Generated
20 Aug 2026

10 trading sessions

Evaluated
+4.2% · Correct
```

Pending:

``` text
INFY
SELL · 76%

Generated
2 Sep 2026

10 trading sessions

Pending evaluation
```

Do not force the full desktop table into mobile.

## 8.22 Do not do

-   Do not create a fake backtesting interface.
-   Do not turn it into a personal trading journal.
-   Do not overwrite historical snapshots.
-   Do not independently calculate prediction maturity.
-   Do not independently calculate settlement prices.
-   Do not classify unavailable evaluation as incorrect.
-   Do not invent confidence thresholds.
-   Do not claim calibration without calibration analysis.
-   Do not duplicate aggregate Model Intelligence analytics.
-   Do not present unevaluated predictions as model wins/losses.
-   Do not use fake historical records.

------------------------------------------------------------------------

# 9. Model Intelligence

## 9.1 Purpose

Model Intelligence is the aggregate analytical view of model behavior
and performance.

Core question:

> **How well does the StockIntel model perform, under what conditions,
> and what does the historical evidence show?**

## 9.2 Primary mental model

> **Assess model → inspect metrics → understand behavior → investigate
> evidence**

## 9.3 Relationship to Prediction History

Model Intelligence consumes aggregate evidence.

Prediction History exposes individual evidence.

Do not duplicate every prediction row here.

## 9.4 Model Overview

Display:

-   active model/version;
-   supported prediction horizon;
-   evaluation status;
-   total evaluated sample;
-   aggregate performance metrics supported by the backend.

## 9.5 Performance Metrics

Potential metrics:

-   accuracy;
-   precision/recall/F1 where the prediction formulation supports them;
-   class-level performance;
-   evaluation counts;
-   return-based metrics only if formally defined by the system.

Every metric must have an unambiguous definition.

Do not label an arbitrary return calculation as "accuracy."

## 9.6 Model Versioning

If multiple production model versions exist historically, allow users to
distinguish performance by version.

Historical metrics must not silently combine incompatible model versions
unless the backend explicitly defines the aggregate.

## 9.7 Class Performance

Show BUY/HOLD/SELL performance where supported.

Use enough sample-size context to prevent misleading conclusions.

## 9.8 Explainability

Show model explainability only when actual model outputs or approved
explanations exist.

Possible content:

-   feature importance;
-   feature contribution;
-   documented model factors.

Do not generate natural-language explanations that are not grounded in
actual model data.

## 9.9 Evaluation Integrity

Model Intelligence must inherit the canonical evaluation methodology.

That includes:

-   valid trading-session maturity;
-   canonical settlement price;
-   production evaluation rules;
-   handling of unavailable records.

## 9.10 States

Support:

-   normal;
-   insufficient evaluation sample;
-   no evaluations;
-   stale evaluation;
-   partial data;
-   model unavailable;
-   demo.

## 9.11 Do not do

-   Do not claim model superiority without evidence.
-   Do not show meaningless metrics with tiny samples without context.
-   Do not mix model versions without explanation.
-   Do not invent explainability.
-   Do not turn the page into an ML training notebook.
-   Do not expose every internal training parameter unless it provides
    actual product value.

------------------------------------------------------------------------

# 10. Cross-Page Relationships

StockIntel pages must form a coherent investigation graph.

``` text
                         Dashboard
                       /    |     \
                      /     |      \
                     ▼      ▼       ▼
             Recommendations News  Watchlist
                    │         │       │
                    └────┬────┘       │
                         ▼            │
                    Stock Detail ◄────┘
                    ▲    │
                    │    │
              Screener   │
                    │    ▼
                    └─ Prediction History
                              │
                              ▼
                       Model Intelligence
```

## 10.1 Required navigation relationships

### Dashboard

Can navigate to:

-   Recommendations;
-   Stock Detail;
-   News Intelligence;
-   Watchlist/Stock Detail.

### Recommendations

Can navigate to:

-   Stock Detail.

### Screener

Can navigate to:

-   Stock Detail;
-   Recommendations where appropriate.

### News Intelligence

Can navigate to:

-   Stock Detail;
-   source article.

### Stock Detail

Can navigate to:

-   Prediction History;
-   News Intelligence;
-   Model Intelligence;
-   originating workspace.

### Prediction History

Can navigate to:

-   Stock Detail;
-   Model Intelligence.

### Model Intelligence

Can navigate to:

-   Prediction History for evidence.

------------------------------------------------------------------------

# 11. Navigation Rules

## 11.1 Stock identity

Ticker/company identity should remain visually consistent across the
application.

## 11.2 Deep-linking

Where practical, filters and selected records should be representable in
URL/state so investigation can be resumed or shared.

Do not introduce URL complexity without a real use case.

## 11.3 Back navigation

Preserve useful originating context.

Example:

``` text
Recommendations
→ RELIANCE
→ Stock Detail
→ Back
→ Recommendations with previous filters/order
```

Avoid unnecessarily resetting the user's workspace.

------------------------------------------------------------------------

# 12. Data & Backend Contract Rules

## 12.1 Backend is authoritative

For financial/model semantics, the backend is the source of truth.

The frontend is responsible for:

-   presentation;
-   interaction;
-   formatting;
-   client-side state;
-   permitted filtering/sorting;
-   responsive transformation.

The frontend is not responsible for redefining domain truth.

## 12.2 No duplicate domain logic

Avoid duplicating:

-   trading calendars;
-   maturity calculations;
-   settlement calculations;
-   evaluation algorithms;
-   model scoring;
-   canonical ranking logic.

If such logic is required client-side for a specific reason, it must be
explicitly documented and tested against the backend.

## 12.3 Missing fields

If a backend field is unavailable:

-   do not substitute a guessed value;
-   use an explicit unavailable state;
-   preserve surrounding layout where possible.

## 12.4 Zero vs unavailable

These are distinct:

``` text
0
```

means a valid numeric value.

``` text
—
```

or an unavailable state means the value is not currently available.

Do not turn missing data into zero.

## 12.5 Timestamps

Always preserve the semantic meaning of timestamps.

Where useful, display:

-   generated at;
-   evaluated at;
-   market data as of;
-   settlement date.

Do not label all timestamps simply "updated."

------------------------------------------------------------------------

# 13. Accessibility Requirements

All page implementations must:

-   use semantic headings;
-   maintain logical heading hierarchy;
-   expose table headers to assistive technology;
-   provide accessible names for icon-only controls;
-   provide keyboard-accessible interactive rows;
-   preserve visible focus;
-   avoid status communication through color alone;
-   provide text equivalents for important charts;
-   maintain readable contrast in both themes;
-   avoid rapidly moving or flashing content;
-   maintain usable touch targets on mobile.

BUY/HOLD/SELL must remain understandable without color.

Pending/evaluated/unavailable must remain distinguishable without color.

------------------------------------------------------------------------

# 14. Visual QA Requirements

Every page implementation must be reviewed in:

-   light desktop;
-   dark desktop;
-   light mobile;
-   dark mobile.

## 14.1 Validate

Check:

-   hierarchy;
-   spacing;
-   typography;
-   alignment;
-   table density;
-   card density;
-   signal semantics;
-   state visibility;
-   responsive transformations;
-   empty states;
-   loading states;
-   error states;
-   freshness indicators;
-   accessibility;
-   navigation.

## 14.2 Financial-data QA

Explicitly verify:

-   no stale value presented as current;
-   no missing value shown as zero;
-   no pending evaluation shown as a result;
-   no unavailable evaluation shown as incorrect;
-   no invented confidence semantics;
-   no invented target price;
-   no incorrect trading-day calculation;
-   no alternate settlement price;
-   no accidental current-state overwrite of historical prediction data.

------------------------------------------------------------------------

# 15. Page-Level Acceptance Criteria

A page is not complete merely because it renders.

## 15.1 Dashboard

-   [ ] Purpose is immediately clear.
-   [ ] Market context is visible.
-   [ ] Model signal distribution is visible where data exists.
-   [ ] Strongest signals are prioritized.
-   [ ] News is presented as context, not a generic feed.
-   [ ] Watchlist has intentional populated/empty behavior.
-   [ ] Data state is visible.
-   [ ] No unsupported analytics are fabricated.

## 15.2 Recommendations

-   [ ] Current recommendations are clearly distinguished from
    historical predictions.
-   [ ] Summary metrics are visible.
-   [ ] Strongest signals are available.
-   [ ] Filters are usable.
-   [ ] Active filters are visible.
-   [ ] Table is dense but readable.
-   [ ] Rows navigate to Stock Detail.
-   [ ] No frontend-only ranking semantics are invented.

## 15.3 Stock Detail

-   [ ] Stock identity is obvious.
-   [ ] Current model signal is the hero.
-   [ ] Confidence semantics are correct.
-   [ ] Price context is clear.
-   [ ] Explanation uses only supported evidence.
-   [ ] News/context is connected where supported.
-   [ ] Prediction history is inspectable.
-   [ ] Current and historical states are distinguished.

## 15.4 Screener

-   [ ] User can understand what the page filters.
-   [ ] Result count is clear.
-   [ ] Filters are grouped logically.
-   [ ] Active filters are visible.
-   [ ] Results dominate the workspace.
-   [ ] No-results state is useful.
-   [ ] Results navigate to Stock Detail.
-   [ ] No unsupported screening metrics are introduced.

## 15.5 News Intelligence

-   [ ] Market sentiment context is visible where supported.
-   [ ] News can be searched/filtered.
-   [ ] Stock-specific and market-level news are distinguishable.
-   [ ] Article metadata is clear.
-   [ ] Source navigation works.
-   [ ] Sentiment changes are only shown when supported.
-   [ ] No causal claims are invented.

## 15.6 Prediction History

-   [ ] Historical predictions are distinguishable from current
    recommendations.
-   [ ] Evaluation overview is visible.
-   [ ] Pending predictions are explicitly pending.
-   [ ] Evaluated predictions display canonical outcomes.
-   [ ] Unable-to-evaluate records are distinct from incorrect
    predictions.
-   [ ] Ten valid trading-session maturity is respected.
-   [ ] Canonical settlement price is respected.
-   [ ] Historical snapshots are not overwritten.
-   [ ] Confidence semantics are preserved.
-   [ ] Individual records can be investigated.
-   [ ] Model Intelligence provides aggregate context.
-   [ ] No frontend evaluation methodology is invented.

## 15.7 Model Intelligence

-   [ ] Aggregate model performance is clearly presented.
-   [ ] Evaluation methodology is respected.
-   [ ] Model versions are distinguishable where required.
-   [ ] Sample sizes/context are visible where necessary.
-   [ ] Explainability is grounded in actual model data.
-   [ ] No unsupported performance claims are made.

------------------------------------------------------------------------

# 16. Implementation Boundary

This document intentionally does **not** specify:

-   exact React component APIs;
-   CSS implementation;
-   exact token values;
-   component variants;
-   frontend folder structure;
-   state-management implementation;
-   API client implementation;
-   testing framework configuration.

Those belong to:

-   `03-DESIGN-SYSTEM.md`
-   `04-THEME-SPECIFICATION.md`
-   `06-COMPONENT-SPECIFICATIONS.md`
-   `07-IMPLEMENTATION-RULES.md`

The implementation agent must treat this document as the page-level
contract and must not "improve" it by silently changing information
hierarchy, domain semantics, or scope.

------------------------------------------------------------------------

# 17. Design Principle Summary

Across all pages:

> **Show the user what matters first, preserve the density required for
> serious analysis, expose uncertainty honestly, and never manufacture
> intelligence that the system does not actually possess.**

The redesign succeeds when StockIntel feels less like a collection of
database views and more like a coherent intelligence product while
remaining faithful to the production system underneath it.

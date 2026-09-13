# StockIntel UI Redesign --- UX Principles

**Document:** `01-UX-PRINCIPLES.md`\
**Product:** StockIntel\
**Status:** UX design principles\
**Purpose:** Define the rules used to make, evaluate, and reject UX
decisions across StockIntel.

------------------------------------------------------------------------

# 1. Purpose

This document turns the high-level direction in
`00-DESIGN-REDESIGN-MASTER.md` into practical UX decision rules.

When a proposed interface decision is ambiguous, the team should use
these principles to determine what the product should do.

These principles apply across:

-   Dashboard;
-   Recommendations;
-   Stock Detail;
-   Screener;
-   News Intelligence;
-   Prediction History;
-   Model Intelligence.

They also guide:

-   component design;
-   information hierarchy;
-   interaction design;
-   responsive behavior;
-   error handling;
-   data presentation;
-   model transparency.

The purpose is not to make StockIntel simpler by removing information.

The purpose is to make **complex information easier to understand and
investigate**.

------------------------------------------------------------------------

# 2. Principle 1 --- Answer the User's Question First

Every page should have one dominant question.

The interface should answer that question before exposing secondary
information.

Examples:

  -----------------------------------------------------------------------
  Page                                Primary question
  ----------------------------------- -----------------------------------
  Dashboard                           What is happening right now?

  Recommendations                     What does StockIntel recommend?

  Stock Detail                        Why does StockIntel think this
                                      about this stock?

  Screener                            Which stocks meet my criteria?

  News Intelligence                   What is happening in the news?

  Prediction History                  What did StockIntel predict, and
                                      what happened afterward?

  Model Intelligence                  How does the model perform overall?
  -----------------------------------------------------------------------

### Rule

If a section does not help answer the page's primary question or support
a clear next step, question whether it belongs on that page.

------------------------------------------------------------------------

# 3. Principle 2 --- Hierarchy Before Decoration

Visual hierarchy must communicate importance.

Use:

-   size;
-   weight;
-   contrast;
-   spacing;
-   grouping;
-   position;

to distinguish primary from secondary information.

Do not use:

-   gradients;
-   oversized cards;
-   excessive color;
-   shadows;
-   animation;

as substitutes for hierarchy.

A well-designed page should remain understandable if decorative effects
are removed.

------------------------------------------------------------------------

# 4. Principle 3 --- Show the Important Before the Detailed

Users should encounter information progressively.

Preferred structure:

``` text
Immediate answer
      ↓
Key supporting context
      ↓
Detailed evidence
      ↓
Technical detail
```

Example on Stock Detail:

``` text
BUY
84% confidence
      ↓
Why this signal?
      ↓
Price/news context
      ↓
Historical predictions
      ↓
Model details
```

Do not expose every backend field at the same visual level.

------------------------------------------------------------------------

# 5. Principle 4 --- Preserve Analytical Density

StockIntel is a serious data application.

The goal is not to make it resemble a minimalist marketing site.

Use density when density is useful.

The distinction is:

``` text
Bad density
= everything competes for attention

Good density
= lots of useful information with clear hierarchy
```

Tables, compact cards, filters, and charts remain valid.

Do not remove useful information merely to create whitespace.

------------------------------------------------------------------------

# 6. Principle 5 --- Reduce Cognitive Load, Not Information

When a page feels overwhelming, first ask:

-   Can information be grouped?
-   Can secondary details be visually de-emphasized?
-   Can progressive disclosure be used?
-   Can related controls be consolidated?
-   Can repeated information be removed?
-   Can terminology be clarified?

Do not immediately solve overload by deleting data.

------------------------------------------------------------------------

# 7. Principle 6 --- One Concept, One Visual Language

If two pages communicate the same concept, they should use the same
component and semantics.

Examples:

-   BUY/HOLD/SELL → same signal component;
-   pending → same pending treatment;
-   stock identity → same stock identity pattern;
-   freshness → same freshness indicator.

Users should not have to relearn the interface on every page.

------------------------------------------------------------------------

# 8. Principle 7 --- Separate Signal, Confidence, Status, and Outcome

These are different dimensions.

``` text
Signal
BUY / HOLD / SELL

Confidence
How confident the model says it is, according to its defined semantics

Status
Pending / Evaluated / Unable to evaluate

Outcome
What happened after evaluation
```

Never collapse them into one "score."

Example:

``` text
BUY
84% confidence
Pending evaluation
```

is a valid state.

The interface must preserve these distinctions.

------------------------------------------------------------------------

# 9. Principle 8 --- The Backend Is the Source of Truth

The UI presents system intelligence.

It does not invent it.

For any domain-semantic value:

1.  identify the backend/API source;
2.  understand its definition;
3.  display it consistently;
4.  avoid creating a competing frontend interpretation.

This applies especially to:

-   model signals;
-   confidence;
-   recommendation ranking;
-   prediction maturity;
-   evaluation;
-   settlement;
-   sentiment;
-   performance metrics.

------------------------------------------------------------------------

# 10. Principle 9 --- Never Invent Missing Intelligence

If the system does not know something, the UI must not pretend it does.

Examples:

If no model explanation exists:

> Explanation unavailable.

If evaluation has not matured:

> Pending evaluation.

If required data is missing:

> Data unavailable.

This is preferable to plausible-looking nonsense.

------------------------------------------------------------------------

# 11. Principle 10 --- Uncertainty Must Be Visible

Stock prediction is inherently uncertain.

The interface must not turn uncertainty into false confidence.

Avoid language such as:

-   guaranteed;
-   certain;
-   will rise;
-   will fall;
-   risk-free.

unless explicitly justified by the actual product semantics.

Confidence should not be presented as certainty.

------------------------------------------------------------------------

# 12. Principle 11 --- Status Is Information

System state should be visible where it affects interpretation.

Examples:

-   market closed;
-   data stale;
-   prediction pending;
-   evaluation unavailable;
-   demo data;
-   model unavailable.

Do not make users infer system state from missing content.

------------------------------------------------------------------------

# 13. Principle 12 --- Differentiate Empty, Error, and Unavailable

These states mean different things.

### Empty

There is valid data infrastructure, but no records exist.

### No results

Records exist, but the current filter/search matches none.

### Error

A request or operation failed.

### Unavailable

The required information cannot currently be provided.

### Stale

Information exists but may no longer represent the current state.

Use distinct UX treatment where the distinction matters.

------------------------------------------------------------------------

# 14. Principle 13 --- Do Not Turn Failures Into Zeros

A missing value is not zero.

For example:

``` text
Evaluated: 0
```

means there are zero evaluated predictions.

Whereas:

``` text
Accuracy: unavailable
```

means accuracy cannot currently be determined.

Never substitute zero for missing data.

------------------------------------------------------------------------

# 15. Principle 14 --- Pending Is Not Failed

This is particularly important for Prediction History.

A prediction that has not reached its evaluation horizon is not a failed
prediction.

The UI must communicate:

``` text
Generated
   ↓
Pending
   ↓
Maturity
   ↓
Evaluation
```

Do not show pending records as wins/losses.

------------------------------------------------------------------------

# 16. Principle 15 --- Trading Sessions Are Not Calendar Days

Prediction maturity must respect the actual production definition of the
evaluation horizon.

If the system specifies ten valid trading sessions:

-   weekends do not count;
-   market holidays do not count;
-   other non-trading sessions do not count.

The frontend should consume canonical maturity/evaluation information.

Do not use naive date subtraction to decide whether a prediction is
mature.

------------------------------------------------------------------------

# 17. Principle 16 --- Preserve Canonical Settlement

Historical evaluation must use the production system's canonical
settlement-price basis.

The UI should display canonical results.

It must not:

-   substitute another price;
-   choose a different data source;
-   calculate an alternative settlement;
-   reinterpret the settlement date.

If a settlement value is unavailable, display it as unavailable.

------------------------------------------------------------------------

# 18. Principle 17 --- Historical Data Is Evidence

Historical predictions should help users understand how the model
behaved.

Historical data should not be used as decorative proof.

Provide enough context to understand:

-   what was predicted;
-   when it was predicted;
-   under which model/version where supported;
-   confidence;
-   evaluation horizon;
-   what happened;
-   how it was evaluated.

Do not selectively surface only successful predictions.

------------------------------------------------------------------------

# 19. Principle 18 --- Avoid Survivorship Bias in Presentation

When showing historical model performance, do not present only:

-   strongest predictions;
-   successful predictions;
-   high-confidence predictions;

unless the UI clearly labels the selection criteria.

Performance views should represent the actual evaluated dataset.

------------------------------------------------------------------------

# 20. Principle 19 --- Sample Size Matters

A metric without context can mislead.

For example:

``` text
Accuracy: 100%
```

means very different things with:

``` text
2 evaluated predictions
```

versus:

``` text
2,000 evaluated predictions
```

Where performance metrics are displayed, provide relevant evaluation
counts.

Do not imply statistical confidence without appropriate evidence.

------------------------------------------------------------------------

# 21. Principle 20 --- Do Not Confuse Correlation With Causation

News, sentiment, market movement, and model signals may appear together.

That does not automatically mean one caused another.

Avoid statements such as:

> This article caused the BUY signal.

unless the system explicitly establishes that causal relationship.

Prefer neutral relationships such as:

> Related news

when appropriate.

------------------------------------------------------------------------

# 22. Principle 21 --- Make Relationships Discoverable

Related information should be easy to navigate.

Examples:

``` text
Recommendation
      ↓
Stock Detail
```

``` text
News
      ↓
Stock Detail
```

``` text
Prediction History
      ↓
Stock Detail
      ↓
Model Intelligence
```

Users should not need to manually reconstruct relationships that the
product already knows.

------------------------------------------------------------------------

# 23. Principle 22 --- Every Deep Dive Needs a Way Back

Investigation should not trap users.

When navigating:

``` text
Screener
→ Stock Detail
```

or:

``` text
Recommendations
→ Stock Detail
```

preserve useful originating context where technically practical.

For filtered analytical workspaces, avoid unnecessarily resetting:

-   filters;
-   sort;
-   page.

------------------------------------------------------------------------

# 24. Principle 23 --- Prefer Direct Manipulation

When a user needs to:

-   filter;
-   sort;
-   search;
-   inspect;
-   navigate;

the control should be obvious and direct.

Avoid unnecessary intermediate dialogs.

Do not hide ordinary analytical operations behind complex interactions.

------------------------------------------------------------------------

# 25. Principle 24 --- Progressive Disclosure Beats Information Hiding

Secondary information may be collapsed.

Important information should not be hidden.

Good:

``` text
Prediction
  ↓
View details
```

Bad:

``` text
Hover over tiny icon
  ↓
Discover critical evaluation state
```

Critical financial/model information must remain discoverable.

------------------------------------------------------------------------

# 26. Principle 25 --- Use Tables for Comparison

Tables are appropriate when users need to compare many structured
records.

Use tables for:

-   recommendations;
-   screener results;
-   prediction history;
-   model evaluation records.

Do not replace useful tables with oversized cards simply because cards
look more modern.

------------------------------------------------------------------------

# 27. Principle 26 --- Use Cards for Focused Intelligence

Cards are appropriate for:

-   market summaries;
-   recommendation highlights;
-   prediction summaries;
-   compact intelligence;
-   focused metric groups.

A card should represent a coherent concept.

Do not create one card for every tiny metric.

------------------------------------------------------------------------

# 28. Principle 27 --- Charts Must Answer Questions

Before adding a chart, state the question it answers.

Examples:

> How has NIFTY moved?

> How are signals distributed?

> How has accuracy changed?

If the question cannot be stated clearly, the chart probably does not
belong.

------------------------------------------------------------------------

# 29. Principle 28 --- Avoid Decorative Analytics

Do not add:

-   random trend lines;
-   meaningless percentages;
-   decorative sparklines;
-   fake correlations;
-   arbitrary scores;

to fill empty space.

Every analytical visualization needs a defensible meaning.

------------------------------------------------------------------------

# 30. Principle 29 --- Use Color Sparingly

Color should direct attention.

Do not color:

-   entire tables;
-   entire cards;
-   every number;
-   every section.

Use semantic color for meaningful states.

Signal colors should be restrained.

------------------------------------------------------------------------

# 31. Principle 30 --- Color Is Never the Only Signal

Important distinctions must survive grayscale.

For example:

``` text
BUY
HOLD
SELL
```

must remain distinguishable through text.

Likewise:

``` text
Pending
Evaluated
Unable to evaluate
```

must not depend solely on color.

------------------------------------------------------------------------

# 32. Principle 31 --- Light and Dark Are Equal Citizens

Neither theme is the fallback.

Both must preserve:

-   hierarchy;
-   readability;
-   semantic meaning;
-   focus;
-   chart interpretation;
-   state visibility.

Do not design one theme and mechanically invert it.

------------------------------------------------------------------------

# 33. Principle 32 --- Responsive Design Changes Priorities

Responsive design is not merely resizing.

Desktop can expose:

-   more columns;
-   persistent navigation;
-   multiple context panels.

Mobile should prioritize:

-   identity;
-   primary signal;
-   key values;
-   status;
-   next action.

Secondary information can move lower or into progressive disclosure.

------------------------------------------------------------------------

# 34. Principle 33 --- Mobile Must Remain Analytical

Do not reduce mobile to a marketing experience.

Users must still be able to:

-   inspect recommendations;
-   filter stocks;
-   inspect predictions;
-   read news;
-   navigate to Stock Detail.

The interaction model may change, but analytical capability should
remain.

------------------------------------------------------------------------

# 35. Principle 34 --- Minimize Interaction Cost

Common actions should require few steps.

Examples:

-   opening Stock Detail from a stock row;
-   removing a filter;
-   clearing filters;
-   opening a news source;
-   switching a chart timeframe.

Avoid unnecessary confirmation dialogs for reversible low-risk UI
actions.

------------------------------------------------------------------------

# 36. Principle 35 --- Do Not Overuse Modals

Use modals/drawers when the user needs focused temporary context.

Do not use them for:

-   normal page navigation;
-   large datasets;
-   deep stock analysis;
-   historical investigation.

Stock Detail should generally remain a page-level destination.

------------------------------------------------------------------------

# 37. Principle 36 --- Preserve Context While Changing Scope

If a user moves from:

``` text
All recommendations
```

to:

``` text
BUY recommendations
```

the interface should preserve the user's mental context.

Similarly, navigating from a filtered Screener to Stock Detail should
not unexpectedly discard the search/filter context.

------------------------------------------------------------------------

# 38. Principle 37 --- Search Should Tell the Truth

Search labels and placeholders must describe what the search actually
supports.

If search matches:

-   ticker;
-   company name;

say so.

Do not call it:

> AI Search

unless it actually performs semantic AI search.

------------------------------------------------------------------------

# 39. Principle 38 --- Filters Should Reflect User Mental Models

Filters should be grouped by concepts users understand.

Prefer:

``` text
Model
Market
Company
```

over:

``` text
prediction_table.column_17
prediction_table.column_22
```

The UI should organize data according to user tasks, not database
schema.

------------------------------------------------------------------------

# 40. Principle 39 --- Defaults Matter

Default sorting, filtering, timeframe, and displayed metrics should have
a clear reason.

Good defaults:

-   show useful information immediately;
-   reduce repetitive setup;
-   reflect common investigation behavior.

Bad defaults:

-   arbitrary;
-   optimized for visual appearance;
-   designed to make metrics look better;
-   silently selective.

------------------------------------------------------------------------

# 41. Principle 40 --- Never Optimize the UI for a Better-looking Metric

The interface must not manipulate presentation to make model performance
appear stronger.

Do not:

-   hide poor-performing classes;
-   default to favorable time windows;
-   omit unsuccessful predictions;
-   show only high-confidence predictions;
-   exclude unavailable records without explanation.

The product should help users investigate the evidence, not sell them a
conclusion.

------------------------------------------------------------------------

# 42. Principle 41 --- Distinguish Product Intelligence From Marketing

StockIntel should communicate intelligence without marketing hype.

Prefer:

> 187 of 312 evaluated predictions were classified as correct.

over:

> Our AI is crushing the market.

Prefer:

> Pending evaluation

over:

> Prediction loading...

when the prediction exists but maturity has not occurred.

------------------------------------------------------------------------

# 43. Principle 42 --- Explain Technical Concepts in Product Language

The interface should not expose implementation terminology
unnecessarily.

For example:

Backend terminology:

``` text
evaluation_window_sessions
```

User-facing concept:

> 10 trading sessions

Backend terminology:

``` text
settlement_price_basis
```

User-facing concept:

> Settlement price

The user should understand what matters without knowing the codebase.

------------------------------------------------------------------------

# 44. Principle 43 --- Do Not Hide Important Limitations

If a limitation affects interpretation, surface it.

Examples:

-   insufficient evaluation sample;
-   stale market data;
-   unavailable news;
-   pending evaluation;
-   unavailable explainability.

Trust improves when limitations are visible.

------------------------------------------------------------------------

# 45. Principle 44 --- Design for Imperfect Data

Real systems fail.

Components should expect:

-   missing fields;
-   delayed data;
-   stale data;
-   API failures;
-   partial results;
-   empty datasets.

The interface should remain coherent rather than collapsing.

------------------------------------------------------------------------

# 46. Principle 45 --- Preserve User Agency

StockIntel provides information and model outputs.

The interface should help users investigate rather than pressure them
into action.

Avoid:

-   aggressive BUY CTAs;
-   execution-oriented language;
-   urgency created purely by visual design;
-   claims that users should act immediately.

------------------------------------------------------------------------

# 47. Principle 46 --- The Interface Should Encourage Investigation

Good UI creates useful next questions.

For example:

``` text
BUY
84% confidence
    ↓
Why?
    ↓
Stock Detail
    ↓
What else is happening?
    ↓
News
    ↓
Was the model right before?
    ↓
Prediction History
    ↓
How good is the model overall?
    ↓
Model Intelligence
```

This is the intended StockIntel investigation loop.

------------------------------------------------------------------------

# 48. Principle 47 --- Avoid Dead Ends

A page should provide meaningful next destinations where a user is
likely to need them.

Examples:

-   recommendation → Stock Detail;
-   stock → prediction history;
-   prediction → stock;
-   news → affected stock;
-   model metric → underlying prediction evidence.

------------------------------------------------------------------------

# 49. Principle 48 --- Build for Auditability

Because StockIntel produces model predictions that are later evaluated,
the UI should support traceability.

A user should be able to understand:

``` text
What was predicted?
When?
By which model/version?
At what confidence?
For what horizon?
What happened?
How was it evaluated?
```

where the underlying system supports those fields.

Auditability does not mean exposing internal implementation details
everywhere.

It means preserving enough context to trust and investigate the result.

------------------------------------------------------------------------

# 50. Principle 49 --- Historical Records Should Not Mutate Semantically

A current recommendation may change.

A historical prediction should not.

The UI must preserve the distinction between:

``` text
Current model state
```

and:

``` text
Historical model output
```

Do not re-render historical records using current values.

------------------------------------------------------------------------

# 51. Principle 50 --- Consistency Beats Novelty

When choosing between:

-   a familiar pattern already used in StockIntel;
-   a novel interaction with marginal benefit;

prefer consistency unless the new interaction solves a real problem.

Users should learn the product once.

------------------------------------------------------------------------

# 52. Principle 51 --- Reuse Patterns, Not Mistakes

Existing UI can provide useful implementation context.

It is not automatically correct.

If the existing interface has:

-   inconsistent spacing;
-   duplicated components;
-   unclear hierarchy;
-   weak states;

do not preserve those problems merely for consistency.

Consistency should mean consistency with the approved redesign system.

------------------------------------------------------------------------

# 53. Principle 52 --- Complexity Should Be Deliberate

Complexity is justified when it corresponds to real analytical needs.

Avoid complexity caused by:

-   excessive configuration;
-   unnecessary controls;
-   redundant metrics;
-   decorative sections;
-   implementation leakage.

The user should experience the complexity of the problem, not the
complexity of the codebase.

------------------------------------------------------------------------

# 54. Principle 53 --- Design the Failure Path

For every major workflow, ask:

> What happens when this fails?

Examples:

### Recommendation API fails

Show a scoped error.

### News API fails

Keep recommendations visible.

### Evaluation has not matured

Show pending.

### Settlement data is unavailable

Show unavailable evaluation.

### Screener returns no matches

Show no-results guidance.

Failure behavior is part of the experience.

------------------------------------------------------------------------

# 55. Principle 54 --- Do Not Confuse System Failure With Model Failure

This is critical.

Examples:

``` text
API unavailable
≠
model performed badly
```

``` text
evaluation data missing
≠
prediction incorrect
```

``` text
market closed
≠
market data broken
```

The UI must preserve these distinctions.

------------------------------------------------------------------------

# 56. Principle 55 --- Optimize for Scanning

Users should be able to scan:

-   tickers;
-   signals;
-   confidence;
-   price changes;
-   statuses;
-   timestamps;

without reading every word.

Use:

-   alignment;
-   hierarchy;
-   compact metadata;
-   predictable placement;
-   consistent formatting.

------------------------------------------------------------------------

# 57. Principle 56 --- Numbers Need Context

A number without a label or period can be misleading.

Avoid displaying:

``` text
+4.2%
```

without making clear what it represents when ambiguity exists.

Prefer:

``` text
+4.2%
Since prediction
```

when that is the actual metric.

Likewise, always contextualize:

-   confidence;
-   accuracy;
-   returns;
-   price changes;
-   evaluation counts.

------------------------------------------------------------------------

# 58. Principle 57 --- Do Not Overformat Data

Avoid unnecessary:

-   decimal places;
-   prefixes;
-   labels;
-   icons;
-   badges.

Formatting should improve scanning.

It should not turn every number into a UI ornament.

------------------------------------------------------------------------

# 59. Principle 58 --- Keep Secondary Metadata Quiet

Information such as:

-   timestamps;
-   model version;
-   source;
-   exchange;

is important but usually secondary.

It should be available without competing with:

-   signal;
-   stock identity;
-   key metrics;
-   primary action.

------------------------------------------------------------------------

# 60. Principle 59 --- Every Visual Pattern Needs a Reason

Before introducing a visual pattern, ask:

-   What does it communicate?
-   Why is it necessary?
-   Is there an existing pattern?
-   Does it improve comprehension?
-   Does it work in both themes?
-   Does it work responsively?

If the answer is mostly aesthetic preference, do not add it.

------------------------------------------------------------------------

# 61. Principle 60 --- Do Not Let the Design System Become the Product

Reusable components exist to support the product.

Do not force every piece of information into:

-   a card;
-   a badge;
-   a chip;
-   a modal;

simply because those components exist.

Choose the presentation based on the user's task.

------------------------------------------------------------------------

# 62. UX Decision Checklist

When evaluating any new UX proposal, ask:

### User

-   What user question does this solve?
-   What task does it improve?

### Information

-   Is the information real?
-   Is it canonical?
-   Is its meaning defined?

### Hierarchy

-   Is this primary, secondary, or tertiary information?
-   Is its visual prominence appropriate?

### Interaction

-   Is the interaction obvious?
-   Does it reduce effort?

### Consistency

-   Does an existing pattern already solve this?
-   Does it preserve shared semantics?

### Trust

-   Could this presentation mislead the user?
-   Does it communicate uncertainty and limitations?

### Responsive

-   How does this work on mobile?

### Theme

-   Does it work equally well in light and dark?

### Accessibility

-   Can the meaning be understood without color or hover?

### Scope

-   Is this actually part of StockIntel's product?

If a proposal fails multiple questions, reject or redesign it.

------------------------------------------------------------------------

# 63. Priority Order for Trade-offs

When principles conflict, prioritize approximately in this order:

1.  Data correctness
2.  User comprehension
3.  Trust/transparency
4.  Core task completion
5.  Accessibility
6.  Consistency
7.  Analytical density
8.  Performance
9.  Visual polish
10. Novelty

Visual polish must never override data correctness.

Novelty must never override usability.

------------------------------------------------------------------------

# 64. Final UX Principle

> **StockIntel should never make users work harder to understand
> information merely because the underlying system is complex.**

The product should preserve complexity where it provides analytical
value, remove accidental complexity, communicate uncertainty honestly,
and guide users naturally from observation to investigation.

The best StockIntel experience is not the one with the most impressive
visuals.

It is the one where the user can quickly understand:

> **What is happening, what StockIntel thinks, why it thinks it, what
> happened before, and how much confidence the evidence deserves.**

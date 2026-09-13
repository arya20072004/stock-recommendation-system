# StockIntel UI Redesign — Design System

**Document:** `03-DESIGN-SYSTEM.md`  
**Status:** Design authority  
**Scope:** Theme-agnostic visual and interaction system  
**Applies to:** Dashboard, Recommendations, Screener, News Intelligence, Stock Detail, Prediction History, Model Intelligence, and shared application chrome

---

## 1. Purpose

This document defines the reusable design system for the StockIntel interface.

It translates the product vision, UX principles, and information architecture into a coherent visual language that can be implemented consistently across the application.

This document is intentionally **theme-agnostic**.

It defines:

- design language
- semantic token architecture
- typography
- spacing
- layout
- density
- surfaces
- borders
- elevation
- radii
- controls
- cards
- tables
- badges
- charts
- news presentation
- state presentation
- responsive behavior
- accessibility
- motion
- component consistency

The concrete values and appearance of the **light and dark themes** belong in:

`04-THEME-SPECIFICATION.md`

Therefore:

> `03-DESIGN-SYSTEM.md` defines **what roles exist and how the system behaves**.  
> `04-THEME-SPECIFICATION.md` defines **how those roles are rendered in each theme**.

---

# 2. Design-System Philosophy

StockIntel is not a marketing website.

It is a data-heavy analytical application where users need to:

- scan information quickly
- compare securities
- identify model signals
- understand confidence
- inspect evidence
- navigate between related information
- distinguish current information from historical information
- understand uncertainty and system state

The design system therefore prioritizes:

1. **Hierarchy**
2. **Comprehension**
3. **Information density**
4. **Trust**
5. **Consistency**
6. **Accessibility**
7. **Efficient interaction**
8. **Visual restraint**

Visual polish is useful only when it improves one of the above.

The interface must not become decorative at the expense of analytical utility.

---

# 3. Visual Direction

The target visual language is:

> **Institutional research terminal + modern SaaS product**

It should feel:

- professional
- analytical
- precise
- restrained
- information-rich
- contemporary
- trustworthy

It should not feel:

- crypto-themed
- casino-like
- neon-heavy
- gaming-oriented
- excessively glassmorphic
- overly rounded
- consumer-finance-gimmicky
- artificially futuristic
- visually noisy

The application should communicate intelligence through **organization and evidence**, not visual effects.

---

# 4. Core Design Rules

## 4.1 Hierarchy Before Decoration

Every visual decision should help users answer:

> What is this?  
> Why does it matter?  
> What should I inspect next?

If a visual treatment does not improve comprehension, it is probably unnecessary.

---

## 4.2 Important Information Comes First

Visual hierarchy must reflect analytical importance.

Typical order:

1. page purpose
2. current state
3. primary decision-relevant information
4. supporting evidence
5. secondary metadata
6. technical/system details

Do not give low-value metadata the same visual weight as model signals.

---

## 4.3 Dense Does Not Mean Crowded

StockIntel should retain high information density.

Density should be achieved through:

- strong alignment
- compact metadata
- predictable spacing
- structured tables
- concise labels
- restrained cards
- progressive disclosure

Do not achieve density by:

- tiny unreadable text
- excessive borders
- overlapping elements
- cramped controls
- unnecessary abbreviations

---

## 4.4 One Visual Language

The following must behave consistently throughout the product:

- signal badges
- confidence displays
- prices
- percentage changes
- status indicators
- timestamps
- table rows
- filters
- buttons
- cards
- charts
- empty states
- errors

A component should not acquire a different visual grammar merely because it appears on another page.

---

# 5. Semantic Token Architecture

The application must use semantic design tokens.

Components should consume roles rather than hard-coded visual values.

For example:

```text
background.primary
background.secondary
surface.default
surface.elevated
border.default
text.primary
text.secondary
text.muted
signal.buy
signal.hold
signal.sell
status.success
status.warning
status.error
status.info
focus.ring
```

Components must not directly depend on arbitrary color literals.

Bad:

```css
color: #16a34a;
```

Preferred:

```css
color: var(--color-signal-buy);
```

This architecture is mandatory for theme support.

---

## 5.1 Complete Token Inventory

The token families listed in Section 5 are the complete baseline semantic inventory for the redesign.

An implementation agent must not invent component-local equivalents for these roles.

If implementation reveals a genuinely missing semantic role, stop and document the proposed new role before introducing it. The new role must then be reflected in both the design system and theme specification.

# 6. Token Categories

The system should maintain semantic tokens for:

### Color

- background
- surface
- elevated surface
- overlay
- border
- divider
- primary text
- secondary text
- muted text
- disabled text
- link
- focus
- signal
- status
- chart series

### Typography

- font family
- font size
- line height
- weight
- letter spacing

### Spacing

- page padding
- section spacing
- component spacing
- control gaps
- table cell padding

### Shape

- radius
- border width

### Elevation

- surface depth
- overlay depth
- modal depth

### Motion

- duration
- easing

### Layout

- container widths
- sidebar width
- content gutters
- breakpoint behavior

---

# 7. Color System

The design system distinguishes **semantic meaning** from raw color.

Primary semantic groups:

### Neutral

Used for:

- page backgrounds
- surfaces
- borders
- text
- disabled states
- secondary information

### Signal

Used for:

- BUY
- HOLD
- SELL

### Status

Used for:

- success
- warning
- error
- informational states

### Interaction

Used for:

- links
- selected controls
- focus
- active navigation

### Data Visualization

Used for:

- charts
- distributions
- historical series
- comparison series

Chart colors must remain distinguishable in both themes.

The actual palette belongs in `04-THEME-SPECIFICATION.md`.

---

# 8. Signal Semantics

StockIntel uses three core model signals:

- BUY
- HOLD
- SELL

Their visual semantics must remain consistent everywhere.

A signal should normally be represented using:

1. text label
2. restrained color cue
3. optional icon or visual marker

Never communicate signal using color alone.

### BUY

Positive/opportunity-oriented semantic.

### HOLD

Neutral/watch semantic.

### SELL

Negative/risk-oriented semantic.

Signal colors must not overpower the rest of the interface.

Do not:

- color entire table rows
- flood cards with signal colors
- use giant colored backgrounds
- use gradients to exaggerate signals

The signal is important, but the application must not visually imply certainty beyond what the model provides.

---

# 9. Status Semantics

Signal and system status are different concepts.

Examples:

- BUY + Pending Evaluation
- HOLD + Evaluated
- SELL + Evaluation Unavailable
- BUY + Stale
- HOLD + Demo Data

These must not be collapsed into one badge.

Use separate semantic treatments for:

### Success

Confirmed successful operation or valid result.

### Warning

Attention required but operation/data remains usable.

### Error

Operation or data retrieval failed.

### Info

Neutral contextual information.

### Pending

Expected future completion.

### Unavailable

Data cannot currently be provided.

### Stale

Data exists but freshness requirements are not satisfied.

---

# 10. Typography

Typography must support rapid scanning and hierarchy.

Use a modern, highly legible sans-serif family already compatible with the project where possible.

Do not introduce a new font dependency without a clear reason.

Recommended hierarchy:

| Role | Purpose |
|---|---|
| Display | Major analytical headline or hero metric |
| Page Title | Primary page identity |
| Section Title | Major content section |
| Subsection | Secondary grouping |
| Body | Explanatory content |
| Metadata | Timestamps, source, model version |
| Label | Form/control labels |
| Table | Dense analytical data |

Typography should distinguish:

- primary information
- supporting information
- metadata

without requiring excessive size differences.

---

# 11. Numeric Typography

Financial numbers require special treatment.

Prices, percentages, confidence values, counts, and model metrics should be visually easy to compare.

Use:

- tabular numerals where supported
- consistent decimal precision
- consistent percentage formatting
- consistent alignment

For dense tables:

- numeric columns should generally align right
- text columns should generally align left
- categorical badges should remain visually compact

Do not randomly mix:

```text
57.4%
57%
0.574
57.40 percent
```

for the same underlying metric.

Formatting must follow the backend's canonical semantics.

---

# 12. Font Weight

Use weight to establish hierarchy, not decoration.

Suggested semantic usage:

- regular: body and secondary content
- medium: labels and controls
- semibold: headings and important values
- bold: only for major emphasis

Avoid excessive bold text.

If everything is bold, nothing is important. Humanity has somehow managed to rediscover this principle approximately every five years.

---

# 13. Spacing System

Use a consistent spacing scale.

Recommended base unit:

**4 px**

Example scale:

```text
4
8
12
16
20
24
32
40
48
64
80
```

The exact implementation may use the project's existing spacing system if compatible.

Spacing should communicate hierarchy:

- 4–8 px: tightly related elements
- 12–16 px: component internals
- 20–24 px: component sections
- 32–40 px: major sections
- 48+ px: page-level separation

Avoid arbitrary values unless required by a specific layout constraint.

---

# 14. Layout System

The application uses a persistent analytical workspace model.

Desktop structure:

```text
┌──────────────┬──────────────────────────────────────────┐
│              │                                          │
│   Sidebar    │              Main Content                │
│              │                                          │
│              │                                          │
└──────────────┴──────────────────────────────────────────┘
```

The main content area should have:

- predictable horizontal padding
- readable maximum content width where appropriate
- full-width treatment for data-heavy tables
- consistent section alignment

Do not force every page into the same fixed-width container.

### Content Width Principle

Use:

- constrained width for narrative/detail content
- wider width for dashboards
- near-full width for tables and analytical workspaces

The content width should follow the task.

---

# 15. Grid

Use a consistent responsive grid.

Desktop layouts may use:

- 12-column conceptual grid
- flexible CSS grid
- responsive flex layouts

depending on the component.

The important requirement is consistent alignment.

Major elements should share common:

- left edges
- right edges
- column boundaries
- section widths

Avoid independently positioned cards that create visual drift.

---

# 16. Page-Level Rhythm

Every page should follow a predictable vertical rhythm:

```text
Page Header
↓
Context / Status
↓
Primary Intelligence
↓
Supporting Analysis
↓
Detailed Data
```

Not every page requires every layer.

The information architecture determines the actual content order.

The design system determines how those layers are visually expressed.

---

# 17. Surfaces

Use a limited surface hierarchy.

Conceptual levels:

1. Application background
2. Standard surface
3. Elevated surface
4. Overlay/modal surface

Surfaces should differ primarily through:

- background tone
- border
- elevation
- contrast

Do not create ten different shades of cards.

A component should not look elevated simply because someone got bored with flat backgrounds.

---

# 18. Borders

Borders should be subtle and purposeful.

Use borders for:

- table separation
- control boundaries
- card boundaries
- section separation
- selected states

Do not outline every nested element.

Avoid:

```text
card
  └── bordered section
       └── bordered box
            └── bordered metric
```

unless the nested hierarchy genuinely requires it.

---

# 19. Elevation

Elevation communicates hierarchy.

Recommended conceptual levels:

### Level 0

Flat page content.

### Level 1

Cards and standard surfaces.

### Level 2

Menus, popovers, elevated controls.

### Level 3

Dialogs and major overlays.

Avoid dramatic shadows.

Institutional analytical software should not look like every component is hovering three centimeters above the screen.

---

# 20. Border Radius

Use restrained corner radii.

Suggested semantic levels:

```text
radius-sm
radius-md
radius-lg
radius-pill
```

Use:

- small radius for controls
- medium radius for cards
- larger radius sparingly for prominent containers
- pill radius for badges/tags

Avoid excessive pill-shaped UI.

Buttons, filters, cards, and entire panels should not all look like capsules.

---

# 21. Iconography

Use one consistent icon family.

Icons should:

- support recognition
- reinforce meaning
- remain visually secondary to text
- use consistent stroke/weight
- have consistent dimensions

Icons should not replace text where the meaning would become ambiguous.

Examples:

Good:

```text
↗ View Analysis
```

Less useful:

```text
↗
```

for an action users have to guess.

---

# 22. Buttons

Buttons are for actions.

### Primary Button

Use for the dominant action within a context.

Examples:

- Apply Filters
- View Analysis
- Retry
- Save Changes

Saved Screens are future scope and are not a v1 button example.

### Secondary Button

Use for supporting actions.

Examples:

- Clear Filters
- Reset
- Compare

### Tertiary/Text Action

Use for lightweight navigation or low-emphasis actions.

Examples:

- View Details
- Open Source

### Icon Button

Use only when the icon's meaning is widely understood or accompanied by an accessible label.

Avoid multiple competing primary buttons in one section.

---

# 23. Form Controls

Controls should prioritize:

- clear labels
- predictable interaction
- adequate hit areas
- visible focus
- readable values
- clear selected state

Controls include:

- inputs
- selects
- checkboxes
- radio buttons
- toggles
- date/time selectors
- range inputs

Financial filtering should not require users to decipher clever UI.

---

# 24. Filters

Filters are a major interaction pattern in:

- Recommendations
- Screener
- News Intelligence
- Prediction History

Filters should:

- expose important criteria
- group related criteria
- support clear active states
- allow reset
- preserve user context
- avoid unnecessary complexity

Recommended pattern:

```text
[Search] [Signal] [Confidence] [More Filters]
                         ↓
Active filters:
[BUY ×] [Confidence > 60% ×] [Clear All]
```

Advanced filters should be progressively disclosed.

---

# 25. Cards

Cards are appropriate for:

- focused intelligence
- summary metrics
- recommendations
- compact contextual information
- small analytical modules

Cards are not appropriate for:

- every piece of information
- large dense datasets
- long tables
- content that users need to compare line-by-line

A card should represent a meaningful unit.

A collection of unrelated cards is not an information architecture.

---

# 26. Intelligence Cards

Intelligence cards should answer one question.

Example:

```text
Strongest Signals

INFY
BUY
Confidence 72.1%
₹1,842.50
+1.8%

View Analysis
```

The card hierarchy should be:

1. entity
2. signal
3. primary metric
4. supporting context
5. action

Do not bury the signal below metadata.

---

# 27. Tables

Tables are the primary component for high-density comparison.

Use tables when users need to compare:

- stocks
- signals
- confidence
- prices
- outcomes
- dates
- model versions
- performance metrics

### Table Rules

- clear column headers
- stable column ordering
- consistent numeric alignment
- compact but readable rows
- restrained separators
- visible hover state where appropriate
- sortable columns only where meaningful
- predictable row interaction

Do not add columns simply because the backend exposes fields.

The table serves the user, not the database.

---

# 28. Table Interaction

Clickable rows should provide one clear destination.

For stock-related tables:

> Row → Stock Detail

Avoid filling rows with:

```text
View
Analyze
Open
Details
More
```

when the entire row already performs the same action.

Use explicit row actions only when multiple distinct actions are genuinely required.

---

# 29. Table Density

Desktop analytical tables should be dense enough to support scanning.

However:

- minimum readable text size must be maintained
- row height must remain touch-friendly where appropriate
- important information should not be vertically centered in huge empty rows

Mobile should not simply shrink desktop tables until they become unusable.

Use transformation into:

- compact cards
- stacked rows
- prioritized fields

when necessary.

---

# 30. Badges

Badges are for short categorical states.

Appropriate:

- BUY
- HOLD
- SELL
- Pending
- Evaluated
- Demo
- Stale

Not appropriate:

- full sentences
- paragraphs
- multiple unrelated metrics

Badges should remain compact and scannable.

---

# 31. Confidence Display

Confidence must be visually subordinate to the signal while remaining prominent enough to inspect.

Possible representations:

- percentage
- compact meter
- progress-like indicator
- numeric value with semantic context

Do not imply:

> 72% confidence = 72% probability of making money

unless the backend explicitly defines it that way.

The UI must preserve the actual model meaning.

---

# 32. Price and Change Display

Price presentation should be consistent.

Example:

```text
₹1,842.50
+1.84%
```

or:

```text
₹1,842.50
+₹33.25 (+1.84%)
```

depending on context.

Daily change should use the established market-data semantics.

Do not mix:

- daily change
- prediction return
- evaluation return
- expected return

without explicit labels.

These are different concepts.

---

# 33. Charts

Charts must answer a question.

Bad:

> Here is a chart because charts look analytical.

Good:

> How has this stock moved over the selected period?

or:

> How are BUY/HOLD/SELL predictions distributed?

Every chart needs:

- meaningful title/context
- understandable axes
- units
- timeframe where applicable
- tooltip or equivalent inspection mechanism where useful
- legend when multiple series exist
- accessible fallback or textual summary where practical

---

# 34. Chart Principles

Charts should prioritize:

1. readability
2. comparison
3. context
4. accurate representation

Avoid unnecessary:

- gradients
- 3D effects
- decorative fills
- excessive gridlines
- animated drawing effects
- chartjunk

Never visually distort scale to make performance appear more dramatic.

---

# 35. Market Charts

Market/price charts may support:

- timeframe selection
- index/stock selection
- TradingView/Data mode where already supported
- tooltips
- contextual annotations where backend data supports them

Signal markers should only appear when actual signal history exists.

Do not manufacture historical markers from current state.

---

# 36. Distribution Charts

Distribution charts are useful for:

- BUY/HOLD/SELL distribution
- confidence distribution
- class performance

They should show:

- category
- count/percentage
- sample context where relevant

Avoid implying statistical significance from tiny samples.

---

# 37. Performance Charts

Performance charts must distinguish:

- prediction outcomes
- model metrics
- market performance
- benchmark performance

Do not combine unrelated series into one chart merely because they share an axis.

---

# 38. News Components

News is supporting intelligence.

News items should be compact and scannable.

Recommended structure:

```text
[TICKER]  [Sentiment]

Headline

Source · Timestamp

Open Source
```

Optional:

- relevance indicator if backend provides one
- company name
- market/stock scope

Do not reproduce entire external articles.

---

# 39. Sentiment

Sentiment is contextual information.

The UI must distinguish:

- positive
- neutral
- negative
- unavailable

If historical sentiment exists, changes can be shown.

If historical sentiment does not exist, do not invent:

> Sentiment increased 18%

Use only actual backend-supported information.

---

# 40. Stock Identity

Stock Identity is a shared pattern.

Recommended hierarchy:

```text
INFY
Infosys Limited
NSE
```

Depending on context, price may follow:

```text
INFY
Infosys Limited

₹1,842.50   +1.84%
```

Ticker should remain visually identifiable.

---

# 41. Watchlist Control

Watchlist state is user state, not market/model state.

It must be visually distinct from:

- BUY/HOLD/SELL
- confidence
- evaluation status

Recommended interaction:

- compact star/bookmark control
- clear selected/unselected state
- accessible label

Do not make the watchlist control look like a recommendation.

---

# 42. Data Freshness

Freshness is part of analytical trust.

Where relevant, show:

- data timestamp
- last updated time
- model generation time
- evaluation state
- stale indicator

Do not display a timestamp merely as decoration.

Users should be able to determine whether information is current enough for the task.

---

# 43. Loading States

Loading states should preserve layout.

Use skeletons or reserved structure for:

- cards
- tables
- charts
- news lists

Avoid replacing the entire interface with a generic spinner when only one section is loading.

Preferred:

```text
Header          ✓
Market Context  ✓
Signals         [loading]
News            ✓
```

This communicates partial progress.

---

# 44. Empty States

Empty states must explain why the area is empty.

Examples:

### No Watchlist

> Your watchlist is empty.

Provide the relevant action or navigation.

### No Results

> No stocks match the selected filters.

### No Evaluated Predictions

> Predictions exist, but none have reached their evaluation maturity window yet.

Do not use generic:

> No data.

when the actual state is known.

---

# 45. Error States

Errors must distinguish:

- request failure
- unavailable backend
- partial failure
- stale data
- missing historical data
- evaluation unavailable

Do not silently render zeros.

Do not convert:

```text
API failure
```

into:

```text
0 recommendations
```

That destroys trust.

---

# 46. Pending Evaluation

Pending evaluation is a legitimate state.

It means:

> A prediction exists, but its evaluation window has not completed.

It does **not** mean:

- incorrect
- failed
- zero return
- missing prediction

The visual system must communicate this clearly.

---

# 47. Evaluated Results

Evaluated predictions may show:

- outcome
- settlement price where canonical
- return where supported
- correctness where formally defined
- evaluation date
- model version

Use backend-provided semantics.

Do not calculate a competing frontend result.

---

# 48. Unavailable Data

Unavailable data should be visually different from zero.

Examples:

```text
Confidence: —
Historical return: Unavailable
Sentiment: Not available
```

Use an explicit explanation where useful.

Never imply:

```text
0
```

when the system actually means:

```text
Unknown
```

---

# 49. Demo Data

Demo data must be unmistakable.

Possible treatments:

- Demo Data badge
- environment indicator
- contextual banner
- clearly labeled sample content

Do not make demo data visually indistinguishable from production data.

Never use fake production-looking values to fill an empty production state.

---

# 50. Navigation

Navigation must follow the information architecture.

Primary workspaces:

- Dashboard
- Recommendations
- Screener
- News Intelligence
- Prediction History
- Model Intelligence

Stock Detail is an investigation destination reached from stock-aware surfaces.

Navigation styling should communicate:

- current location
- available destinations
- hierarchy

Do not make every route appear equally important.

---

# 51. Active Navigation

Active navigation should use a semantic selected state.

It should be recognizable through more than color alone.

Possible cues:

- background
- border/accent
- weight
- icon treatment

Avoid giant saturated navigation blocks.

---

# 52. Responsive Design

Responsive behavior is structural, not merely proportional.

Desktop:

- persistent navigation
- multi-column layouts
- dense tables
- visible filters
- richer chart controls

Tablet:

- reduced columns
- tighter spacing
- adaptive cards
- collapsible filters

Mobile:

- compact navigation
- stacked sections
- filter drawer
- prioritized fields
- transformed tables
- full-width controls

Do not simply scale desktop down.

---

# 53. Mobile Information Priority

When space is constrained, preserve:

1. Stock identity
2. Signal
3. Primary metric
4. Confidence
5. Critical status
6. Primary action

Secondary metadata may collapse or move below.

Never sacrifice the primary analytical signal to preserve decorative structure.

---

# 54. Touch Targets

Interactive elements should provide sufficiently large touch targets.

Small visual icons may exist inside larger hit areas.

Avoid tiny:

- filter buttons
- close buttons
- pagination controls
- watchlist controls

especially on mobile.

---

# 55. Accessibility

Accessibility is part of the design system.

Requirements include:

- semantic HTML
- keyboard navigation
- visible focus
- adequate contrast
- meaningful labels
- accessible tables
- accessible form controls
- screen-reader-friendly state descriptions
- no color-only semantics
- reduced-motion support

Every interactive control must have a discernible purpose.

---

# 56. Focus States

Focus must be visible in both themes.

Focus should use a dedicated semantic token.

Do not remove browser focus indicators without replacing them with an equally clear treatment.

---

# 57. Color Accessibility

Never rely exclusively on:

- green vs red
- light vs dark
- saturation

to communicate meaning.

For example:

```text
BUY ↑
HOLD •
SELL ↓
```

may combine:

- text
- icon
- color

where appropriate.

The exact iconography should remain consistent across the product.

---

# 58. Motion

Motion should communicate state or relationship.

Appropriate:

- menu opening
- drawer transitions
- filter expansion
- subtle hover states
- route transitions where useful
- loading progression

Avoid:

- constant floating animations
- decorative particles
- excessive chart animation
- bouncing cards
- attention-grabbing transitions

Recommended motion principles:

```text
fast   → micro-interaction
normal → component transition
slow   → major structural transition
```

Respect `prefers-reduced-motion`.

---

# 59. Hover States

Hover should clarify interactivity.

Useful for:

- clickable table rows
- buttons
- links
- cards that navigate

Hover should not radically transform the component.

The interface should remain usable without hover.

---

# 60. Selection States

Selected elements must be distinguishable from:

- hover
- focus
- disabled
- default

This applies to:

- navigation
- tabs
- filters
- chart controls
- table selections

Do not use the same visual state for multiple meanings.

---

# 61. Disabled States

Disabled controls should clearly communicate unavailable interaction without becoming unreadable.

Do not use disabled styling for:

> Data unavailable

unless the actual control is disabled.

Data state and interaction state are different.

---

# 62. Tabs

Tabs should be used only when switching between closely related views.

Examples:

- chart/data view
- TradingView/StockIntel Data

Avoid using tabs to hide major information architecture problems.

If two areas represent fundamentally different user tasks, they should probably be separate navigation destinations.

---

# 63. Drawers, Modals, and Pages

Use a modal when:

- the task is short
- context should remain visible
- interruption is appropriate

Use a drawer when:

- supporting context is useful
- filtering or inspection is secondary
- the user benefits from retaining the underlying page

Use a page when:

- the content is analytical
- the task is substantial
- the user needs navigation/history
- the content deserves deep linking

Stock Detail should remain a page-level destination, not a modal.

---

# 64. Progressive Disclosure

Show the minimum necessary information first.

Reveal deeper information through:

- expandable sections
- detail panels
- secondary controls
- navigation
- contextual drawers

Do not hide information merely to make the interface look cleaner.

The purpose is to reduce cognitive load while retaining analytical depth.

---

# 65. Data vs Intelligence

The design system should visually distinguish:

### Raw/Reference Data

Examples:

- price
- timestamp
- volume
- article source

### Model Intelligence

Examples:

- signal
- confidence
- model prediction
- evaluation outcome

Model intelligence should receive stronger hierarchy where it is the page's primary purpose.

Raw data should remain available as supporting evidence.

---

# 66. Current vs Historical

Current state and historical evidence must not look interchangeable.

Current:

- current signal
- current price
- current market state

Historical:

- prior prediction
- evaluated outcome
- historical return
- previous model version

Use labels, timestamps, section hierarchy, and context rather than arbitrary visual effects.

---

# 67. Model Version Context

Where model version materially affects interpretation, expose it.

Example:

```text
Model
v2.4

Prediction horizon
10 trading sessions

Generated
05 Aug 2026
```

Do not expose implementation details that users cannot interpret.

Optuna configuration, internal training logs, and raw technical parameters do not belong in the main product UI unless specifically useful.

---

# 68. Component Composition

Components should compose predictably.

Example:

```text
Page
 ├── PageHeader
 ├── IntelligenceSummary
 ├── Section
 │    ├── SectionHeader
 │    └── DataTable
 └── Section
      ├── SectionHeader
      └── NewsList
```

Avoid components that secretly introduce unrelated spacing, navigation, data fetching, or domain logic.

---

# 69. Component Responsibilities

A component should own:

- presentation
- interaction state appropriate to that component
- accessibility behavior
- visual consistency

A component should not independently decide:

- model evaluation methodology
- trading-session maturity
- settlement calculation
- prediction ranking
- backend semantics

Domain rules belong in the appropriate data/service layer.

---

# 70. State Architecture

Every data-driven component should be capable of representing relevant states:

```text
idle
loading
success
empty
partial
error
stale
unavailable
demo
```

Not every component needs every state, but the state model must be deliberate.

Do not assume:

```text
data.length === 0
```

means the same thing in every context.

---

# 71. Cross-Page Consistency

Shared entities should look the same everywhere.

For example, `INFY` should retain consistent:

- ticker treatment
- company identity
- signal semantics
- confidence treatment
- price formatting
- navigation behavior

across:

- Dashboard
- Recommendations
- Screener
- News Intelligence
- Stock Detail
- Prediction History

---

# 72. Design-System Boundaries

This design system does not define:

- backend architecture
- ML algorithms
- prediction generation
- evaluation methodology
- database schema
- API implementation
- trading logic
- investment advice
- unsupported financial metrics

Those remain outside the UI design system.

---

# 73. Implementation Requirements

Implementation must:

1. Use semantic tokens.
2. Avoid hard-coded theme colors in components.
3. Reuse shared components.
4. Preserve light and dark themes.
5. Preserve backend semantics.
6. Keep state handling explicit.
7. Avoid duplicating domain logic.
8. Maintain responsive behavior.
9. Maintain keyboard accessibility.
10. Avoid unnecessary dependencies.
11. Avoid unnecessary animation.
12. Avoid unrelated refactoring.

Existing project conventions should be inspected before introducing replacements.

---

# 74. Token Naming Convention

Use names based on **meaning**, not appearance.

Preferred:

```text
--color-surface-default
--color-text-primary
--color-text-secondary
--color-border-default
--color-signal-buy
--color-signal-hold
--color-signal-sell
--color-status-warning
```

Avoid:

```text
--green
--dark-gray
--blue-500
--card-gray
```

Raw palette tokens may exist underneath semantic tokens, but components should consume semantic tokens.

---

# 75. Component Naming Convention

Names should represent UI responsibility.

Preferred:

```text
SignalBadge
ConfidenceDisplay
MarketIndexCard
NewsItem
DataFreshnessIndicator
PredictionStatus
```

Avoid vague names:

```text
Box
Thing
InfoCard2
GreenCard
GenericPanel
```

---

# 76. Design-System Anti-Patterns

The following are explicitly prohibited unless a documented exception exists:

- hard-coded theme colors
- inconsistent signal colors
- color-only status communication
- giant gradients
- excessive glassmorphism
- excessive rounded cards
- excessive shadows
- decorative chart effects
- oversized empty whitespace
- unreadably dense tables
- unnecessary modals
- duplicated component implementations
- fake data
- fake model explanations
- fake metrics
- fabricated confidence meanings
- frontend reimplementation of backend evaluation
- silent API failures
- zeros used for unavailable values
- generic “No data” for known states
- excessive animation
- arbitrary iconography
- route-specific visual languages
- unrelated redesign during implementation

---

# 77. Quality Hierarchy

When design decisions conflict, prioritize:

1. Data correctness
2. User comprehension
3. Trust and transparency
4. Task completion
5. Accessibility
6. Consistency
7. Information density
8. Performance
9. Visual polish
10. Novelty

A visually impressive interface that obscures whether a prediction is pending or evaluated is a design failure.

---

# 78. Design Review Checklist

Before accepting a component or page, verify:

### Visual

- [ ] Hierarchy is obvious.
- [ ] Spacing follows the system.
- [ ] Typography is consistent.
- [ ] Surfaces are restrained.
- [ ] Borders are purposeful.
- [ ] Icons are consistent.

### Semantic

- [ ] Signal meaning is correct.
- [ ] Status meaning is separate from signal.
- [ ] Confidence semantics are accurate.
- [ ] Current and historical states are distinguishable.
- [ ] Missing data is not represented as zero.

### Interaction

- [ ] Primary action is obvious.
- [ ] Controls have clear labels.
- [ ] Hover/selection/focus states are distinct.
- [ ] Navigation destination is predictable.

### Responsive

- [ ] Desktop layout works.
- [ ] Tablet behavior is intentional.
- [ ] Mobile layout prioritizes critical information.
- [ ] Tables transform appropriately.
- [ ] Touch targets remain usable.

### Accessibility

- [ ] Keyboard navigation works.
- [ ] Focus is visible.
- [ ] Contrast is adequate.
- [ ] Color is not the sole semantic.
- [ ] Screen-reader labels are meaningful.

### Data integrity

- [ ] Backend/API semantics are preserved.
- [ ] No frontend evaluation logic was introduced.
- [ ] No fake values were added.
- [ ] Loading/error/empty/stale states are explicit.

---

# 79. Definition of Done

A design-system implementation is complete only when:

- shared semantic tokens exist
- components consume semantic tokens
- light and dark themes use the same semantic architecture
- typography is consistent
- spacing is consistent
- surfaces/borders/elevation are consistent
- signal/status semantics are consistent
- tables and cards follow defined rules
- charts follow defined rules
- responsive behavior is intentional
- accessibility requirements are met
- loading/empty/error/unavailable states are supported
- no fake production data has been introduced
- no backend/ML semantics have been changed
- visual QA has been performed across representative pages

---

# 80. Relationship to Other Design Documents

This document should be interpreted together with:

### `00-DESIGN-REDESIGN-MASTER.md`

Defines the overall redesign direction and non-negotiable product vision.

### `01-UX-PRINCIPLES.md`

Defines the reasoning and user-experience principles behind the design.

### `02-INFORMATION-ARCHITECTURE.md`

Defines page responsibilities, navigation, information ownership, and user flow.

### `04-THEME-SPECIFICATION.md`

Defines the concrete light and dark theme implementation.

### `05-PAGE-SPECIFICATIONS.md`

Defines page-level structure and behavior.

### `06-COMPONENT-SPECIFICATIONS.md`

Defines component-level contracts.

### `07-IMPLEMENTATION-RULES.md`

Defines how coding agents should implement and validate the system.

---

# 81. Final Principle

The StockIntel design system should make complex financial and model information feel **organized rather than simplified**.

The goal is not to hide complexity.

The goal is to give complexity a structure that humans can actually use.

StockIntel should look like a serious intelligence product because its interface demonstrates:

- hierarchy
- evidence
- consistency
- uncertainty
- context
- analytical discipline

The visual system is successful when the user can scan quickly, investigate deeply, and understand exactly what the system knows, what it predicts, what has been evaluated, and what remains uncertain.

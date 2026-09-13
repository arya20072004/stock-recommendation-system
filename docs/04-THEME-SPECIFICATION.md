# StockIntel UI Redesign — Theme Specification

**Document:** `04-THEME-SPECIFICATION.md`  
**Status:** Theme authority  
**Scope:** Concrete light and dark theme tokens and theme behavior  
**Applies to:** Entire StockIntel application

---

# 1. Purpose

This document defines the concrete visual implementation of StockIntel's two first-class themes:

- **Light Theme**
- **Dark Theme**

The preceding design-system document defines semantic roles and component behavior.

This document defines the actual theme values assigned to those roles.

The central requirement is:

> **Light and dark mode must be designed as equal products, not as one theme plus an inverted color palette.**

Both themes must preserve:

- hierarchy
- readability
- analytical density
- signal semantics
- accessibility
- visual emphasis
- information architecture
- trust

A theme switch must not change meaning.

---

# 2. Theme Philosophy

StockIntel should feel like a serious analytical workspace in either theme.

The visual character should be:

- restrained
- high-clarity
- professional
- data-oriented
- modern
- calm
- precise

The themes should differ in atmosphere while preserving the same hierarchy.

### Light Theme

The light theme should feel:

- clean
- crisp
- research-oriented
- highly readable
- suitable for extended daytime analysis

### Dark Theme

The dark theme should feel:

- focused
- controlled
- information-dense
- comfortable for prolonged screen use
- appropriate for a terminal-like analytical environment

Neither theme is the "correct" theme.

---

# 3. Non-Negotiable Rules

1. Components must consume semantic theme tokens.
2. Components must not contain arbitrary theme-specific color literals.
3. Light and dark themes must use the same semantic token names.
4. Signal meanings must remain identical between themes.
5. Status meanings must remain identical between themes.
6. Text must remain readable at every hierarchy level.
7. Borders must remain visible but restrained.
8. Charts must remain distinguishable in both themes.
9. Focus indicators must remain obvious in both themes.
10. Disabled states must remain distinguishable without becoming unreadable.
11. Theme switching must not change layout or information hierarchy.
12. Theme switching must not change data semantics.
13. No component should require a separate "dark version" unless genuinely necessary.
14. Demo/live status must remain unmistakable in both themes.
15. Stale, unavailable, pending, error, and empty states must remain distinguishable.
16. No full-page black backgrounds unless required by the existing product architecture.
17. No pure-white card explosion in light mode.
18. No pure-black surface explosion in dark mode.
19. Avoid excessive saturation.
20. Avoid gradients as a primary UI treatment.

---

# 4. Token Architecture

Theme tokens should be organized by semantic role.

Recommended structure:

```text
color.background.*
color.surface.*
color.border.*
color.text.*
color.link.*
color.interaction.*
color.signal.*
color.status.*
color.chart.*
color.overlay.*
```

Example:

```css
--color-background-primary
--color-surface-default
--color-text-primary
--color-signal-buy
--color-status-warning
```

The same token exists in both themes.

Only the assigned value changes.

---

# 5. Base Palette Philosophy

The theme palette should be built from restrained neutrals rather than large collections of decorative colors.

Primary visual categories:

```text
Neutral
Signal
Status
Interaction
Visualization
```

Neutral colors form the majority of the interface.

Signal and status colors should be used as semantic accents, not as page decoration.

---

# 6. Light Theme

## 6.1 Light Theme Character

The light theme should use a slightly tinted neutral application background rather than a harsh pure white canvas.

Conceptually:

```text
Application background
    ↓
Soft neutral surface
    ↓
White / near-white analytical surface
    ↓
Subtle border
```

This creates separation without requiring heavy shadows.

---

## 6.2 Light Theme Base Tokens

Recommended starting values:

| Token | Light Value | Purpose |
|---|---|---|
| `background.primary` | `#F5F7FA` | Main application canvas |
| `background.secondary` | `#EEF1F5` | Secondary workspace areas |
| `surface.default` | `#FFFFFF` | Standard cards/panels |
| `surface.elevated` | `#FFFFFF` | Menus/overlays |
| `surface.subtle` | `#F8FAFC` | Low-emphasis content areas |
| `border.default` | `#D9DEE7` | Standard borders |
| `border.subtle` | `#E7EBF0` | Light separators |
| `text.primary` | `#17202A` | Primary text |
| `text.secondary` | `#4B5563` | Supporting text |
| `text.muted` | `#6B7280` | Metadata |
| `text.disabled` | `#9CA3AF` | Disabled content |

These values are starting design tokens, not permission for individual components to hard-code them.

---

# 7. Dark Theme

## 7.1 Dark Theme Character

The dark theme should not use absolute black as the default background.

Use layered charcoal/slate surfaces.

Conceptually:

```text
Deep application background
    ↓
Dark analytical surface
    ↓
Slightly elevated panel
    ↓
Subtle border
```

This preserves depth without relying on bright borders or shadows.

---

## 7.2 Dark Theme Base Tokens

Recommended starting values:

| Token | Dark Value | Purpose |
|---|---|---|
| `background.primary` | `#0F141A` | Main application canvas |
| `background.secondary` | `#151B23` | Secondary workspace areas |
| `surface.default` | `#18202A` | Standard cards/panels |
| `surface.elevated` | `#1D2631` | Menus/overlays |
| `surface.subtle` | `#141B23` | Low-emphasis content areas |
| `border.default` | `#303A46` | Standard borders |
| `border.subtle` | `#252E39` | Light separators |
| `text.primary` | `#F1F5F9` | Primary text |
| `text.secondary` | `#B6C0CC` | Supporting text |
| `text.muted` | `#8995A3` | Metadata |
| `text.disabled` | `#5E6976` | Disabled content |

The dark theme should retain visible hierarchy without turning every boundary into a bright line.

---

# 8. Text Hierarchy

Both themes must use the same semantic hierarchy.

### Primary

Used for:

- page titles
- primary metrics
- stock tickers
- important values
- major headings

### Secondary

Used for:

- descriptions
- supporting values
- explanatory labels

### Muted

Used for:

- timestamps
- sources
- model metadata
- secondary context

### Disabled

Used only for:

- unavailable interaction
- disabled controls

Do not use muted text for information that users actually need to read.

---

# 9. Link Colors

Links should be visually identifiable without becoming visually dominant.

Semantic tokens:

```text
link.default
link.hover
link.visited
```

Use the same conceptual hue family in both themes, with adjusted luminance.

Links should not be confused with:

- BUY
- success
- selected navigation

A link is an interaction semantic, not a financial signal.

---

# 10. Interaction Colors

Interaction tokens:

```text
interaction.primary
interaction.primary-hover
interaction.primary-active
interaction.secondary
interaction.selected
interaction.focus
```

These tokens control:

- buttons
- selected navigation
- active filters
- tabs
- focused controls
- interactive states

The interaction palette should remain restrained.

---

# 11. Signal Colors

The signal palette is shared semantically across the entire application.

Recommended conceptual palette:

### BUY

Base:

```text
#16A34A
```

Use for:

- BUY badges
- positive signal indicators
- positive model classification

### HOLD

Base:

```text
#D97706
```

Use for:

- HOLD badges
- neutral/watch signal indicators

### SELL

Base:

```text
#DC2626
```

Use for:

- SELL badges
- negative signal indicators

These colors must be adjusted for theme contrast where necessary.

The exact light/dark variants should remain semantically aligned.

---

# 12. Signal Color Usage

Signal color should generally appear in:

- badge text/background
- small indicator
- icon
- selected signal control
- chart category

Signal color should generally **not** fill:

- entire cards
- table rows
- full sections
- page backgrounds
- large chart regions

The interface should communicate:

> "This is a signal."

not:

> "THE SCREEN IS NOW GREEN BECAUSE BUY."

---

# 13. Status Colors

Recommended semantic categories:

| Status | Base |
|---|---|
| Success | `#16A34A` |
| Warning | `#D97706` |
| Error | `#DC2626` |
| Info | `#2563EB` |
| Pending | `#7C3AED` |
| Unavailable | Neutral |
| Stale | `#B45309` |

Status colors should be visually distinct from one another.

Where signal and status share a semantic family, the UI must still distinguish the label/context.

Example:

```text
BUY
Pending Evaluation
```

must not look like one combined "green success" state.

---

# 14. Status Backgrounds

Status backgrounds should use low-opacity or very light semantic tints.

Do not use fully saturated status backgrounds for large components.

Preferred conceptual treatment:

```text
status color
    +
low-intensity background
    +
clear text
```

This is especially important for:

- warnings
- errors
- pending states
- demo indicators

---

# 15. Demo Data Theme Treatment

Demo data should be visually explicit.

Recommended semantic treatment:

```text
status.demo
```

It may use a neutral or informational accent.

Example:

```text
DEMO DATA
```

The indicator should remain noticeable in both themes without competing with BUY/HOLD/SELL.

Do not use a subtle gray label that users can easily mistake for ordinary metadata.

---

# 16. Stale Data Theme Treatment

Stale data should communicate:

> Data exists, but freshness may be insufficient.

Recommended treatment:

- amber/warning semantic
- explicit text
- optional freshness icon

Example:

```text
Stale · Updated 3h ago
```

Do not style stale data as an error unless the backend says it is invalid.

---

# 17. Pending Evaluation Theme Treatment

Pending evaluation should use its own semantic treatment.

Recommended:

- informational/purple-neutral accent
- explicit `Pending` label
- no success/failure implication

Example:

```text
Pending Evaluation
10 trading-session horizon
```

Pending is not failure.

Pending is not incorrect.

Pending is not zero return.

---

# 18. Error Theme Treatment

Errors should be obvious but not visually overwhelming.

Use:

- error icon
- short explanation
- recovery action where available
- error semantic token

Example:

```text
Unable to load recommendations
The recommendation service did not return valid data.

Retry
```

Do not make an entire page red because one API request failed.

---

# 19. Empty State Theme Treatment

Empty states should be visually quiet.

Use:

- neutral illustration/icon where useful
- clear heading
- concise explanation
- relevant action

Avoid giant illustrations that consume analytical workspace.

---

# 20. Unavailable Data

Unavailable values should use neutral semantics.

Example:

```text
Historical Return
—
Unavailable
```

Avoid using red unless the unavailable state itself represents an error.

This distinction matters.

---

# 21. Surface Hierarchy

### Light

```text
background.primary
    ↓
surface.subtle
    ↓
surface.default
    ↓
surface.elevated
```

### Dark

```text
background.primary
    ↓
surface.subtle
    ↓
surface.default
    ↓
surface.elevated
```

The hierarchy is identical conceptually.

The luminance relationships differ.

---

# 22. Cards in Light Theme

Cards should generally use:

- near-white surface
- subtle border
- minimal shadow
- restrained radius

The background should provide enough separation that a card does not require a thick border.

Avoid:

```text
white card
white card
white card
white card
```

on a pure white background.

That produces visual soup.

---

# 23. Cards in Dark Theme

Cards should use:

- slightly lighter surface than the page background
- subtle border
- minimal elevation
- restrained radius

Avoid making cards dramatically brighter than the background.

The goal is layered depth, not glowing panels.

---

# 24. Border Contrast

Borders should be visible enough to define structure.

They should not compete with content.

Light theme:

- subtle gray borders
- stronger border for active controls

Dark theme:

- muted slate borders
- slightly stronger border for active/selected controls

Avoid bright white borders in dark mode.

---

# 25. Elevation and Shadows

### Light Theme

Use subtle shadows primarily for:

- menus
- dropdowns
- popovers
- dialogs

Cards should generally rely on surface + border before shadow.

### Dark Theme

Use even more restrained shadows.

Surface contrast and borders should do most of the work.

Large glowing shadows are prohibited.

---

# 26. Navigation Theme

Sidebar/navigation must visually recede behind primary analytical content.

### Inactive

- neutral text
- neutral icon
- transparent/low-emphasis background

### Hover

- subtle surface change

### Active

- clear selected background
- accent or border cue
- stronger text/icon

The active state must be visible without relying solely on color.

---

# 27. Header Theme

The top header should remain visually lightweight.

Use:

- appropriate surface
- subtle bottom border if needed
- strong page-title hierarchy
- compact utility controls

Do not turn the header into a giant decorative banner.

---

# 28. Table Theme

Tables require excellent contrast and subtle separation.

### Light

Use:

- light surface
- subtle row separators
- stronger header text
- restrained hover background

### Dark

Use:

- dark surface
- low-contrast separators
- readable header text
- subtle hover surface change

Avoid zebra striping unless testing demonstrates a real readability benefit.

---

# 29. Table Signal Treatment

Signal cells should use:

- badge
- icon
- text
- small accent

Do not tint the entire row according to signal.

A table containing 51 securities should remain visually scannable, not resemble a Christmas tree.

---

# 30. Filter Theme

Filter controls should be visually distinct from ordinary text.

Use:

- clear border
- readable selected state
- visible focus
- compact dimensions

Active filters should use a selected semantic token.

Example:

```text
Signal: BUY
Confidence: >60%
```

should be visibly active but not aggressively saturated.

---

# 31. Search Input Theme

Search fields should:

- have a clear boundary
- contain readable placeholder text
- show focus clearly
- preserve sufficient internal padding
- support keyboard interaction

Placeholder text must remain visibly secondary to entered text.

---

# 32. Button Theme

### Primary

Strongest interaction emphasis.

### Secondary

Moderate emphasis.

### Tertiary

Minimal visual weight.

The hierarchy must remain equivalent between light and dark modes.

Do not make dark-mode buttons disproportionately bright merely because the background is dark.

---

# 33. Focus Ring

Focus must be clearly visible in both themes.

Use:

```text
interaction.focus
```

with sufficient contrast against the current surface.

Focus should generally appear as:

- outline
- ring
- or equivalent non-destructive visual cue

Do not rely on browser-default focus if it becomes visually inconsistent with the product.

Do not remove focus without replacement.

---

# 34. Disabled Controls

Disabled controls should:

- reduce contrast
- retain readable labels
- clearly appear unavailable
- remain distinguishable from normal controls

Do not reduce disabled text until it becomes impossible to read.

Disabled does not mean invisible.

---

# 35. Charts: Theme Strategy

Charts should not simply reuse the same literal colors in both themes.

Each chart series requires a semantic visualization token.

Example:

```text
chart.primary
chart.secondary
chart.tertiary
chart.buy
chart.hold
chart.sell
chart.benchmark
chart.grid
chart.axis
chart.tooltip
```

Theme values should be adjusted for:

- contrast
- saturation
- readability
- visual weight

---

# 36. Chart Backgrounds

Charts should visually belong to their containing surface.

Do not automatically place charts on a different dark/white rectangle.

The chart should generally inherit:

- surface background
- text semantics
- border semantics

This keeps the analytical surface cohesive.

---

# 37. Chart Gridlines

Gridlines should be subtle.

They exist to help users read values, not to dominate the chart.

Use lower-emphasis neutral tokens.

Avoid:

- dense grids
- high-contrast grids
- unnecessary vertical/horizontal lines

---

# 38. Chart Tooltips

Tooltips should use an elevated surface.

They need:

- readable text
- clear labels
- adequate contrast
- appropriate numeric formatting

Tooltips should not rely on tiny text.

---

# 39. Chart Accessibility

Where practical:

- provide textual summaries
- preserve accessible labels
- use distinguishable series
- do not rely only on color
- provide equivalent data tables where appropriate

Charts are analytical aids, not inaccessible decorations.

---

# 40. News Theme

News cards should remain visually subordinate to model intelligence.

Use:

- neutral surface
- compact metadata
- subtle sentiment accent
- readable headline

Sentiment color should appear as a supporting cue.

Do not make every positive article green and every negative article red from edge to edge.

---

# 41. Sentiment Theme

Sentiment tokens:

```text
sentiment.positive
sentiment.neutral
sentiment.negative
sentiment.unavailable
```

Positive/negative should be distinguishable in both themes.

Neutral should remain genuinely neutral rather than looking like an error.

---

# 42. Stock Detail Theme

Stock Detail is the central investigation page.

Its theme hierarchy should emphasize:

1. stock identity
2. current signal
3. confidence
4. market/price context
5. supporting evidence
6. historical evidence
7. model metadata

The signal should be prominent without turning the page into a giant colored dashboard.

---

# 43. Prediction History Theme

Prediction History should visually distinguish:

```text
Prediction
↓
Pending
↓
Evaluated
```

Use:

- neutral styling for historical records
- explicit signal badge
- explicit evaluation status
- outcome styling only after evaluation

Do not visually treat pending predictions as failed outcomes.

---

# 44. Model Intelligence Theme

Model Intelligence should feel analytical rather than decorative.

Metrics should use:

- strong typography
- restrained cards
- clear labels
- sample-size context
- chart support where useful

Avoid giant colorful KPI tiles that make every model statistic look equally important.

---

# 45. Confidence Visualization Theme

Confidence meters should use a neutral base and a semantic accent.

The meter should communicate magnitude without implying:

- profitability probability
- guaranteed success
- certainty

A 57.4% confidence value should look like a model metric, not a 57.4% guaranteed outcome.

---

# 46. Data Freshness Theme

Freshness should be subtle but discoverable.

Recommended levels:

### Fresh

Neutral or positive subtle indicator.

### Aging

Informational/warning treatment.

### Stale

Warning treatment with explicit label.

### Unknown

Neutral unavailable treatment.

The UI should not imply freshness if the backend does not provide enough information to establish it.

---

# 47. Theme-Specific Contrast Requirements

Every theme must be tested for:

- normal text contrast
- large text contrast
- control contrast
- border visibility
- focus visibility
- selected state visibility
- signal distinguishability
- status distinguishability

Do not approve a theme because the palette looks attractive.

Approve it because users can reliably read and interpret it.

---

# 48. Avoiding Pure Extremes

Avoid defaulting to:

```text
#000000
#FFFFFF
```

for most surfaces.

Exceptions may exist for:

- specific imagery
- source content
- chart/export requirements
- accessibility-driven contrast
- external content

But application chrome should use layered neutrals.

---

# 49. Theme Switching

Theme switching must:

- preserve current page
- preserve filters
- preserve scroll position where practical
- preserve selected records
- preserve form input
- preserve analytical context

Only visual presentation changes.

Do not reload or reset application state unnecessarily.

---

# 50. Theme Persistence

The selected theme should persist according to the application's existing settings architecture.

Supported conceptual modes:

```text
Light
Dark
System
```

If `System` is supported, it should resolve to the user's operating-system preference.

The theme implementation should avoid flash-of-incorrect-theme during application startup where technically practical.

---

# 51. System Theme

If the product supports system preference:

```text
System → OS Light → Light theme
System → OS Dark → Dark theme
```

Manual Light/Dark selections override the system preference.

The implementation should not create a third independently designed theme.

System mode is a selector, not a separate visual system.

---

# 52. Responsive Theme Behavior

Theme semantics must remain unchanged across:

- desktop
- tablet
- mobile

Do not create a "mobile dark mode" or other divergent palette.

Responsive design changes layout, not meaning.

---

# 53. Accessibility Requirements

Both themes must support:

- WCAG-appropriate text contrast
- visible focus
- keyboard navigation
- color-independent semantics
- readable disabled states
- accessible charts where applicable
- accessible form controls

Signal/status colors must be tested for distinguishability.

Particular attention should be paid to:

- red vs green confusion
- amber on light backgrounds
- purple on dark surfaces
- muted text contrast
- thin border visibility

---

# 54. Theme QA Matrix

Every major shared component should be tested in:

| Component | Light | Dark |
|---|---:|---:|
| App Shell | ✓ | ✓ |
| Sidebar | ✓ | ✓ |
| Page Header | ✓ | ✓ |
| Buttons | ✓ | ✓ |
| Inputs | ✓ | ✓ |
| Filters | ✓ | ✓ |
| Cards | ✓ | ✓ |
| Tables | ✓ | ✓ |
| Signal Badges | ✓ | ✓ |
| Status Badges | ✓ | ✓ |
| Charts | ✓ | ✓ |
| News Items | ✓ | ✓ |
| Empty States | ✓ | ✓ |
| Error States | ✓ | ✓ |
| Loading States | ✓ | ✓ |
| Stock Detail | ✓ | ✓ |
| Prediction History | ✓ | ✓ |
| Model Intelligence | ✓ | ✓ |

---

# 55. Theme QA Procedure

For each major page:

1. Render in light theme.
2. Inspect hierarchy.
3. Inspect text contrast.
4. Inspect signal/status semantics.
5. Inspect borders and surfaces.
6. Inspect charts.
7. Inspect interactive states.
8. Switch to dark theme.
9. Repeat all checks.
10. Compare hierarchy, not literal colors.
11. Test keyboard focus.
12. Test mobile.
13. Verify no state-specific meaning changed.

The goal is not pixel equality between themes.

The goal is **semantic equivalence**.

---

# 56. Semantic Equivalence

A user switching themes should still understand:

- what is primary
- what is secondary
- what is interactive
- what is a BUY/HOLD/SELL signal
- what is pending
- what is evaluated
- what is stale
- what is unavailable
- what is an error
- what is demo data

without relearning the interface.

---

# 57. Forbidden Theme Patterns

Do not introduce:

- pure black full-page UI by default
- pure white card stacks
- neon signal colors
- glowing borders
- gradient backgrounds
- gradient buttons
- excessive glassmorphism
- translucent everything
- huge shadows
- saturated chart backgrounds
- color-filled table rows
- red/green-only semantics
- theme-specific component duplicates
- arbitrary color literals
- theme-specific information hierarchy
- animated theme transitions that reduce readability

---

# 58. Implementation Contract

Implementation agents must:

1. Inspect existing styling architecture first.
2. Reuse existing token infrastructure where compatible.
3. Introduce semantic tokens rather than scattered literals.
4. Define both light and dark values together.
5. Refactor shared components to consume semantic tokens.
6. Preserve existing behavior unless explicitly redesigned.
7. Verify all major states in both themes.
8. Avoid changing backend/API/ML behavior.
9. Avoid introducing unnecessary styling libraries.
10. Document any intentional deviation from this specification.

---

# 59. Suggested Token Structure

A practical implementation may use a structure similar to:

```css
:root {
  --color-background-primary: ...;
  --color-background-secondary: ...;

  --color-surface-default: ...;
  --color-surface-subtle: ...;
  --color-surface-elevated: ...;

  --color-border-default: ...;
  --color-border-subtle: ...;

  --color-text-primary: ...;
  --color-text-secondary: ...;
  --color-text-muted: ...;
  --color-text-disabled: ...;

  --color-link-default: ...;

  --color-interaction-primary: ...;
  --color-interaction-selected: ...;
  --color-interaction-focus: ...;

  --color-signal-buy: ...;
  --color-signal-hold: ...;
  --color-signal-sell: ...;

  --color-status-success: ...;
  --color-status-warning: ...;
  --color-status-error: ...;
  --color-status-info: ...;
  --color-status-pending: ...;

  --color-chart-primary: ...;
  --color-chart-secondary: ...;
  --color-chart-grid: ...;
}
```

Then the dark theme overrides the same semantic roles.

Do not create separate component-specific colors such as:

```css
--recommendation-card-green
--dark-table-border
--news-card-gray
```

unless a genuinely unique semantic role exists.

---

# 60. Theme Token Checklist

Before adding a new color token, ask:

1. What semantic meaning does this represent?
2. Is an existing token already appropriate?
3. Does the meaning need different light/dark values?
4. Is this truly global or component-specific?
5. Can the component consume an existing semantic role?
6. Does the new token create unnecessary palette complexity?

If the answer is merely:

> "It looked slightly better with another blue."

do not create the token.

---

# 61. Definition of Done

The theme system is complete when:

- Light theme is fully defined.
- Dark theme is fully defined.
- Both use identical semantic token roles.
- Text hierarchy is equivalent.
- Surface hierarchy is equivalent.
- Signal semantics are equivalent.
- Status semantics are equivalent.
- Charts remain readable in both themes.
- Tables remain readable in both themes.
- Focus states remain visible.
- Disabled states remain understandable.
- Demo/live state remains explicit.
- Stale/pending/unavailable/error states remain distinct.
- Responsive layouts preserve theme semantics.
- Theme switching preserves application state.
- No major component relies on hard-coded theme colors.
- Accessibility checks pass for representative screens.
- Visual QA has been performed on all major workspaces.

---

# 62. Relationship to Other Documents

### `00-DESIGN-REDESIGN-MASTER.md`

Defines the overall product vision and redesign direction.

### `01-UX-PRINCIPLES.md`

Defines the UX principles governing decisions.

### `02-INFORMATION-ARCHITECTURE.md`

Defines information hierarchy and navigation.

### `03-DESIGN-SYSTEM.md`

Defines theme-agnostic component and visual-system roles.

### `05-PAGE-SPECIFICATIONS.md`

Defines how individual pages use the system.

### `06-COMPONENT-SPECIFICATIONS.md`

Defines reusable component contracts.

### `07-IMPLEMENTATION-RULES.md`

Defines implementation and validation discipline.

---

# 63. Final Principle

StockIntel should never feel like:

> "The dark version of the light website."

It should feel like the **same analytical product expressed through two equally deliberate visual environments**.

Light mode should maximize clarity.

Dark mode should maximize focus.

Both must preserve the same:

- hierarchy
- semantics
- density
- trust
- analytical workflow

The theme is successful when changing it changes the atmosphere, but **nothing about the user's understanding of the data**.


# Appendix A. Concrete Token Completeness

The following token families have concrete baseline values in this document:

- semantic colors
- typography
- spacing
- shape/radii
- border widths
- elevation
- motion
- layout

Implementation agents must consume these values through the semantic token layer.

The baseline values are approved for implementation. They may be adjusted only through a deliberate design-system change followed by accessibility and visual validation.

No component may introduce an arbitrary replacement for a token that already exists.



# Appendix B. Complete Concrete Typography Tokens

| Token | Value |
|---|---:|
| `font.family-sans` | Existing project-compatible sans-serif family |
| `font.size-xs` | `0.75rem` / 12px |
| `font.size-sm` | `0.875rem` / 14px |
| `font.size-md` | `1rem` / 16px |
| `font.size-lg` | `1.125rem` / 18px |
| `font.size-xl` | `1.25rem` / 20px |
| `font.size-2xl` | `1.5rem` / 24px |
| `font.size-3xl` | `1.875rem` / 30px |
| `font.weight-regular` | 400 |
| `font.weight-medium` | 500 |
| `font.weight-semibold` | 600 |
| `font.weight-bold` | 700 |
| `font.line-height-tight` | 1.2 |
| `font.line-height-normal` | 1.5 |
| `font.line-height-relaxed` | 1.65 |
| `font.letter-spacing-tight` | -0.01em |
| `font.letter-spacing-normal` | 0 |

If the repository already uses a compatible font family, preserve it. Do not add a new font dependency merely to satisfy this token.

# Appendix C. Complete Concrete Spacing / Shape / Layout Tokens

## Spacing

| Token | Value |
|---|---:|
| `space-1` | 4px |
| `space-2` | 8px |
| `space-3` | 12px |
| `space-4` | 16px |
| `space-5` | 20px |
| `space-6` | 24px |
| `space-8` | 32px |
| `space-10` | 40px |
| `space-12` | 48px |
| `space-16` | 64px |
| `space-20` | 80px |

## Shape

| Token | Value |
|---|---:|
| `radius-sm` | 6px |
| `radius-md` | 8px |
| `radius-lg` | 12px |
| `radius-pill` | 999px |
| `border-width-default` | 1px |
| `border-width-strong` | 2px |

## Layout

| Token | Value |
|---|---:|
| `layout-sidebar-width` | 240px |
| `layout-content-max` | 1440px |
| `layout-content-wide-max` | 1600px |
| `layout-page-gutter` | 32px |
| `layout-mobile-gutter` | 16px |

These are baseline layout constraints. They may adapt responsively, but components must not introduce arbitrary spacing scales.

# Appendix D. Complete Concrete Motion Tokens

| Token | Value |
|---|---:|
| `motion-duration-fast` | 120ms |
| `motion-duration-normal` | 180ms |
| `motion-duration-slow` | 260ms |
| `motion-ease-standard` | `cubic-bezier(0.2, 0, 0, 1)` |
| `motion-ease-emphasized` | `cubic-bezier(0.2, 0, 0, 1)` |

All motion must respect `prefers-reduced-motion`.

# Appendix E. Complete Concrete Semantic Color Tokens

## Light Theme

| Semantic Token | Value |
|---|---|
| `background.primary` | `#F5F7FA` |
| `background.secondary` | `#EEF1F5` |
| `surface.default` | `#FFFFFF` |
| `surface.subtle` | `#F8FAFC` |
| `surface.elevated` | `#FFFFFF` |
| `border.default` | `#D9DEE7` |
| `border.subtle` | `#E7EBF0` |
| `text.primary` | `#17202A` |
| `text.secondary` | `#4B5563` |
| `text.muted` | `#6B7280` |
| `text.disabled` | `#9CA3AF` |
| `link.default` | `#2563EB` |
| `interaction.primary` | `#2563EB` |
| `interaction.primary-hover` | `#1D4ED8` |
| `interaction.primary-active` | `#1E40AF` |
| `interaction.secondary` | `#FFFFFF` |
| `interaction.selected` | `#E8F0FE` |
| `interaction.focus` | `#2563EB` |
| `interaction.disabled` | `#E5E7EB` |
| `signal.buy` | `#15803D` |
| `signal.hold` | `#B45309` |
| `signal.sell` | `#B91C1C` |
| `status.success` | `#15803D` |
| `status.warning` | `#B45309` |
| `status.error` | `#B91C1C` |
| `status.info` | `#1D4ED8` |
| `status.pending` | `#6D28D9` |
| `status.demo` | `#475569` |
| `status.stale` | `#B45309` |
| `status.unavailable` | `#6B7280` |
| `sentiment.positive` | `#15803D` |
| `sentiment.neutral` | `#64748B` |
| `sentiment.negative` | `#B91C1C` |
| `sentiment.unavailable` | `#6B7280` |
| `chart.primary` | `#2563EB` |
| `chart.secondary` | `#7C3AED` |
| `chart.tertiary` | `#0891B2` |
| `chart.benchmark` | `#64748B` |
| `chart.buy` | `#15803D` |
| `chart.hold` | `#B45309` |
| `chart.sell` | `#B91C1C` |
| `chart.grid` | `#E2E8F0` |
| `chart.axis` | `#64748B` |
| `chart.tooltip` | `#FFFFFF` |
| `overlay.scrim` | `rgba(15, 23, 42, 0.48)` |

## Dark Theme

| Semantic Token | Value |
|---|---|
| `background.primary` | `#0F141A` |
| `background.secondary` | `#151B23` |
| `surface.default` | `#18202A` |
| `surface.subtle` | `#141B23` |
| `surface.elevated` | `#1D2631` |
| `border.default` | `#303A46` |
| `border.subtle` | `#252E39` |
| `text.primary` | `#F1F5F9` |
| `text.secondary` | `#B6C0CC` |
| `text.muted` | `#8995A3` |
| `text.disabled` | `#5E6976` |
| `link.default` | `#60A5FA` |
| `interaction.primary` | `#60A5FA` |
| `interaction.primary-hover` | `#93C5FD` |
| `interaction.primary-active` | `#BFDBFE` |
| `interaction.secondary` | `#18202A` |
| `interaction.selected` | `#1E3A5F` |
| `interaction.focus` | `#60A5FA` |
| `interaction.disabled` | `#27313C` |
| `signal.buy` | `#4ADE80` |
| `signal.hold` | `#FBBF24` |
| `signal.sell` | `#F87171` |
| `status.success` | `#4ADE80` |
| `status.warning` | `#FBBF24` |
| `status.error` | `#F87171` |
| `status.info` | `#60A5FA` |
| `status.pending` | `#A78BFA` |
| `status.demo` | `#94A3B8` |
| `status.stale` | `#FBBF24` |
| `status.unavailable` | `#8995A3` |
| `sentiment.positive` | `#4ADE80` |
| `sentiment.neutral` | `#94A3B8` |
| `sentiment.negative` | `#F87171` |
| `sentiment.unavailable` | `#8995A3` |
| `chart.primary` | `#60A5FA` |
| `chart.secondary` | `#A78BFA` |
| `chart.tertiary` | `#22D3EE` |
| `chart.benchmark` | `#94A3B8` |
| `chart.buy` | `#4ADE80` |
| `chart.hold` | `#FBBF24` |
| `chart.sell` | `#F87171` |
| `chart.grid` | `#2A3440` |
| `chart.axis` | `#94A3B8` |
| `chart.tooltip` | `#1D2631` |
| `overlay.scrim` | `rgba(0, 0, 0, 0.60)` |

These values are the baseline approved palette for implementation. Accessibility and visual QA may justify controlled revisions, but individual components must not independently alter them.

# Appendix F. Confidence Tier Rule

Confidence tiers are displayable only when supplied as canonical backend/API data or when a formally approved backend/product mapping exists.

If no canonical confidence tier exists:

```text
Display confidence percentage.
Do not invent a tier.
```

The frontend must not independently define thresholds such as Low / Medium / High.


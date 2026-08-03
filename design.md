# Stock Recommendation System — Frontend Design Specification

## 1. Purpose

This document is the source of truth for the frontend UI/UX of the Stock Recommendation System.

The application should feel like a modern AI-powered investment research terminal rather than a generic stock-market dashboard.

The interface must make three things immediately understandable:

1. What does the system recommend?
2. Why does the system recommend it?
3. How confident and risky is that recommendation?

The frontend must also expose relevant market news and, when available, news sentiment without implying that news sentiment influences model predictions unless the backend model actually uses those features.

---

# 2. Product Principles

## 2.1 Recommendation First

The primary product differentiator is the recommendation engine.

Price charts, technical indicators, market information, news, and model statistics exist to support the recommendation rather than compete with it.

The hierarchy should generally be:

Recommendation → Confidence → Expected Return → Evidence → Market Data → News

## 2.2 Explainability

Never present only:

`BUY — 87%`

Where backend information is available, explain why the recommendation was generated.

Possible supporting information includes:

* Technical indicators
* Momentum
* Moving averages
* Volatility
* Volume trends
* Model probabilities
* Feature importance
* Relevant news
* News sentiment

Do not fabricate explanations.

## 2.3 Financial Semantics

Colors must carry consistent meaning.

Green represents positive financial movement or bullish signals.

Red represents negative financial movement or bearish signals.

Yellow/amber represents neutral, hold, caution, or moderate risk.

Blue represents general application interaction and selection.

Do not use green simply as a decorative application accent.

## 2.4 Data Integrity

Never present mock values as real system output.

When APIs are unavailable, the frontend may use mock data during development, but mock data must:

* Live outside UI components.
* Be clearly separated from production API services.
* Be easy to remove.
* Match expected API contracts.
* Never silently replace failed production requests.

---

# 3. Technology Direction

Recommended frontend stack:

* React
* Vite
* React Router
* Axios
* TanStack Query
* Recharts
* Lucide React
* Standard CSS or CSS Modules

Do not introduce Tailwind CSS.

Do not introduce Redux unless application state becomes complex enough to justify it.

Server state should primarily be managed using TanStack Query.

---

# 4. Visual Identity

## 4.1 Theme

Primary interface:

Dark financial research terminal.

The UI should feel:

* Professional
* Data-driven
* Dense without becoming cluttered
* Modern
* Precise
* Technical
* Trustworthy

Avoid:

* Excessive gradients
* Glassmorphism everywhere
* Neon cyberpunk effects
* Giant glowing shadows
* Excessive animations
* Meme-style fintech visuals
* Crypto-dashboard aesthetics
* Oversized cards containing very little information

---

# 5. Design Tokens

## 5.1 Colors

```css
:root {
    --bg-primary: #080B10;

    --surface-primary: #10151D;
    --surface-secondary: #171D27;
    --surface-hover: #1D2531;

    --border-primary: #252D3A;
    --border-secondary: #303A49;

    --text-primary: #F4F7FB;
    --text-secondary: #8B95A7;
    --text-muted: #667085;

    --positive: #20C77A;
    --negative: #F05252;
    --warning: #F4B740;

    --accent: #4C8DFF;
    --accent-hover: #3979E8;
}
```

---

# 6. Typography

Primary font:

Inter

Optional financial/numeric font:

JetBrains Mono

Use Inter for:

* Navigation
* Titles
* Labels
* Buttons
* General text

Use JetBrains Mono selectively for:

* Prices
* Percentages
* Technical values
* Model metrics
* Table numeric columns

Do not overuse monospace typography.

---

# 7. Typography Scale

Suggested hierarchy:

```text
Page title              28px / 700
Stock price             28–32px / 700
Section heading         16–18px / 600
Card heading            14–16px / 600
Body                    14px / 400
Secondary               13px / 400
Metadata                12px / 400
```

---

# 8. Spacing

Use an 8px-based spacing system.

Preferred values:

```text
4px
8px
12px
16px
24px
32px
40px
48px
```

Primary page gap:

24px

Card internal padding:

16px–24px

---

# 9. Cards

Default card:

```css
.card {
    background: var(--surface-primary);
    border: 1px solid var(--border-primary);
    border-radius: 12px;
}
```

Avoid heavy box shadows.

Hoverable cards may use:

```css
.card:hover {
    background: var(--surface-hover);
    border-color: var(--border-secondary);
}
```

---

# 10. Application Layout

Desktop layout consists of:

```text
┌──────────────┬────────────────────────────────────────┐
│              │ Top Header                             │
│              ├────────────────────────────────────────┤
│   Sidebar    │                                        │
│              │ Main Content                           │
│              │                                        │
│              │                                        │
└──────────────┴────────────────────────────────────────┘
```

Suggested dimensions:

Sidebar width:

`230–250px`

Header height:

`64px`

Main content:

Fluid width with approximately `24px–32px` page padding.

---

# 11. Navigation

Sidebar hierarchy:

```text
OVERVIEW

Dashboard


DISCOVER

Stocks
Screener
Watchlist


INTELLIGENCE

Recommendations
News Intelligence
Prediction History
Model Intelligence


PORTFOLIO

Portfolio


SYSTEM

Settings
```

Each item should use a Lucide icon.

Active navigation item:

* Subtle blue background
* Accent-colored icon
* Primary text

Do not use large glowing navigation effects.

---

# 12. Header

Header contains:

* Global stock search
* Notification button
* Optional market-status indicator
* User/profile control

Example:

```text
Search stocks...                         🔔    Profile
```

Search should support ticker and company-name lookup when backend functionality exists.

---

# 13. Dashboard

Route:

`/dashboard`

Purpose:

Provide an immediate summary of market conditions and the system's strongest actionable intelligence.

The dashboard must not attempt to display every tracked stock.

## Dashboard Structure

```text
Greeting / Overview

Market Summary

Top Model Recommendation

Market Overview                Watchlist

Recommendation Snapshot        Important News
```

---

# 14. Market Summary

Display major market indices relevant to the system.

Example:

```text
NIFTY 50
24,842
▲ 0.72%

SENSEX
81,903
▲ 0.58%

BANK NIFTY
54,312
▼ 0.14%

MARKET
● OPEN
```

Values must come from actual data services when available.

---

# 15. Top Recommendation

This is one of the strongest visual elements on the dashboard.

Example:

```text
TOP MODEL RECOMMENDATION

RELIANCE INDUSTRIES

₹1,412.30
▲ 1.42%

BUY

Confidence
87%

Target Price
₹1,530

Expected Return
+8.35%
```

Include a small sparkline if data is available.

Clicking the card navigates to the Stock Details page.

---

# 16. Recommendation Snapshot

Display distribution of current recommendation signals.

Example:

```text
21
BUY

17
HOLD

13
SELL
```

Use semantic colors.

Do not hardcode these values.

---

# 17. Dashboard Watchlist

Show approximately 4–6 watchlisted stocks.

Each row contains:

* Ticker
* Current price
* Daily percentage movement
* Optional recommendation
* Optional miniature sparkline

Provide navigation to the complete Watchlist page.

---

# 18. Dashboard News

Display approximately 3–5 high-impact recent stories.

This section should prioritize:

* Market-moving events
* News affecting tracked companies
* Major sector developments

Each news item may display:

```text
Headline

Source • time

Affected ticker(s)

Sentiment badge
```

Sentiment must only appear when sentiment analysis exists.

---

# 19. Stock Details

Route:

`/stocks/:ticker`

This is the most important analytical page.

The page must answer:

* What is happening with this stock?
* What does the model recommend?
* Why?
* What does the model expect?
* What risks exist?
* What relevant news exists?

---

# 20. Stock Header

Example:

```text
RELIANCE

Reliance Industries Limited
NSE

₹1,412.30

+₹19.80 (+1.42%) Today
```

Actions:

* Add/remove Watchlist
* Optional share
* Optional refresh

---

# 21. Price Chart

Provide selectable time ranges:

```text
1D
1W
1M
3M
6M
1Y
```

Chart should prioritize readability.

Avoid unnecessary animation.

---

# 22. Model Signal Card

Prominent card containing:

```text
MODEL SIGNAL

STRONG BUY

87%

Confidence
█████████████████░░░

Target Price
₹1,530

Expected Return
+8.35%

Risk
MEDIUM
```

Signal options may include:

* Strong Buy
* Buy
* Hold
* Sell
* Strong Sell

Only expose signal types supported by backend logic.

---

# 23. Recommendation Explanation

Section heading:

`Why This Recommendation?`

Possible display:

```text
✓ Strong 20-day momentum
✓ RSI remains below overbought territory
✓ Price above 50-day moving average
✓ Positive volume trend
✓ ML probability exceeds BUY threshold
```

These explanations must originate from actual model logic or deterministic frontend interpretation of supplied model features.

Never fabricate explanation text merely to make the interface look complete.

---

# 24. Technical Indicators

Suggested table/card:

```text
RSI          61.4       Bullish
MACD         +8.42      Bullish
SMA 20       ₹1,389     Above
SMA 50       ₹1,341     Above
Volatility   18.3%      Moderate
```

Only display indicators actually available from the backend.

---

# 25. Stock-Specific News

The Stock Details page should contain relevant news associated with the selected ticker.

Example:

```text
LATEST NEWS

POSITIVE

Reliance announces expansion of renewable energy...

Source • 42 minutes ago

Sentiment
Positive

Confidence
82%
```

Each news card should support:

* Headline
* Source
* Published time
* Short summary where available
* Related ticker
* Sentiment
* Sentiment confidence
* Link to original article when legally/technically appropriate

Do not reproduce full copyrighted news articles.

---

# 26. Recommendations Explorer

Route:

`/recommendations`

Purpose:

Allow users to inspect and rank current model recommendations.

Filters:

```text
All
Buy
Hold
Sell
```

Additional filters:

* Risk
* Sector
* Minimum confidence
* Expected return

Sorting:

* Confidence
* Expected return
* Price movement
* Alphabetical

---

# 27. Recommendations Table

Suggested columns:

```text
Stock
Signal
Price
Target
Expected Return
Confidence
Risk
```

Example:

```text
RELIANCE   BUY    ₹1,412   ₹1,530   +8.35%   87%   Medium
TCS        BUY    ₹3,087   ₹3,320   +7.52%   82%   Low
HDFCBANK   HOLD   ₹2,011   ₹2,060   +2.41%   68%   Medium
SBIN       SELL   ₹812     ₹765     -5.80%   76%   High
```

Clicking a row opens Stock Details.

---

# 28. Confidence Visualization

Do not display confidence only as text.

Example:

```text
87%

█████████████████░░░
```

The bar should use semantic signal color where appropriate.

---

# 29. Stock Screener

Route:

`/screener`

Purpose:

Allow users to discover stocks matching model and financial criteria.

Filters may include:

```text
Recommendation

☐ Strong Buy
☐ Buy
☐ Hold
☐ Sell


Minimum Confidence

────────●────────
       70%


Minimum Expected Return

──────────●──────
          5%


Risk

☐ Low
☐ Medium
☐ High


Sector

[ All Sectors ▼ ]
```

Provide:

`Reset`

and

`Apply Filters`

controls.

---

# 30. Screener Results

Suggested columns:

```text
Stock
Signal
Expected Return
Confidence
Risk
Sector
```

Desktop should use a dense table.

Mobile should transform results into cards.

---

# 31. Watchlist

Route:

`/watchlist`

Display watchlisted stocks as cards or compact rows.

Each card:

```text
RELIANCE
Reliance Industries Ltd.

₹1,412.30

▲ 1.42%

[ sparkline ]

Model

BUY • 87%
```

Actions:

* Remove from watchlist
* Open stock details

---

# 32. News Intelligence

Route:

`/news`

Purpose:

Expose collected financial news and sentiment intelligence.

This is not merely a generic news page.

It should connect articles to:

* Stocks
* Sectors
* Market events
* Sentiment

---

# 33. News Filters

Suggested controls:

```text
Sentiment

All
Positive
Neutral
Negative


Stock

[ All Stocks ▼ ]


Source

[ All Sources ▼ ]


Period

24 Hours
7 Days
30 Days
```

Optional search:

`Search news...`

---

# 34. News Cards

Each card:

```text
RELIANCE                       POSITIVE

Reliance announces...

Source • 42 minutes ago

Short article summary...

Sentiment confidence

82%
████████████████░░░░
```

Clicking should open article details or the original source depending on implementation.

---

# 35. News Sentiment

Sentiment categories:

```text
Positive
Neutral
Negative
```

Colors:

Positive → Green

Neutral → Amber or muted gray

Negative → Red

Never infer sentiment merely from stock price movement.

---

# 36. News and Recommendation Separation

Until sentiment is actually integrated into the recommendation model:

Do NOT display language implying:

`Positive news caused BUY recommendation.`

Instead present news as supplementary intelligence.

Acceptable:

```text
Model Recommendation
BUY

Recent News Sentiment
72% Positive
```

Not acceptable:

```text
BUY because recent news is positive
```

unless the model genuinely uses news sentiment features.

---

# 37. Future Sentiment Integration

The UI architecture should support future metrics such as:

```text
Technical Score       84 / 100

News Sentiment        78 / 100

Momentum              Strong

Volatility            Moderate

Overall Confidence    87%
```

These components should not appear until corresponding backend data exists.

---

# 38. Prediction History

Route:

`/predictions/history`

Purpose:

Show whether historical recommendations were correct.

Top metrics:

```text
Overall Accuracy

81.7%

Total Predictions
389

Correct
318

Incorrect
71
```

---

# 39. Prediction Performance Chart

Display historical model performance over time.

Possible ranges:

```text
1M
3M
6M
1Y
ALL
```

Chart should visualize actual stored prediction outcomes.

---

# 40. Prediction History Table

Suggested columns:

```text
Date
Stock
Prediction
Expected Return
Actual Return
Outcome
```

Example:

```text
01 Aug   RELIANCE   BUY    +6.2%   +7.1%   Correct

01 Aug   INFY       BUY    +4.8%   +2.9%   Correct

31 Jul   SBIN       SELL   -3.1%   +1.2%   Incorrect
```

---

# 41. Model Intelligence

Route:

`/model`

Purpose:

Expose model status and performance.

This page is technical and should be designed for transparency.

---

# 42. Model Status

Example:

```text
MODEL STATUS

● Operational

Model Version
v2.4.1

Last Training
01 Aug 2026

Stocks Trained
51

Features
34

Training Records
126,420
```

Do not hardcode production values.

---

# 43. Model Performance

Possible metrics:

```text
Accuracy
Precision
Recall
F1 Score
```

Only display metrics that are meaningful for the actual model architecture.

If the production model uses regression rather than classification, replace inappropriate classification metrics with suitable metrics such as:

* MAE
* RMSE
* R²
* Directional accuracy

The frontend must reflect actual model semantics rather than blindly displaying common ML metrics.

---

# 44. Prediction Distribution

Visualize recommendation distribution.

Example:

```text
BUY     21
HOLD    17
SELL    13
```

Preferred visualization:

Donut chart.

Always accompany charts with numeric values.

---

# 45. Feature Importance

Display model feature importance when available.

Example:

```text
RSI                18.2%
MACD               15.7%
20D Return         13.4%
Volume Change      11.1%
SMA Ratio           9.8%
```

Preferred visualization:

Horizontal bar chart.

Feature importance must come from actual model metadata.

---

# 46. Portfolio

Route:

`/portfolio`

This page should be treated as optional until portfolio functionality exists.

Do not build fake portfolio functionality solely to populate the navigation.

If the backend does not support portfolios yet, either:

* Hide the route during early development, or
* Display a clearly marked development/coming-soon state.

Future functionality may include:

* Holdings
* Invested amount
* Current value
* Profit/loss
* Portfolio allocation
* Model recommendations for held stocks

---

# 47. Settings

Route:

`/settings`

Potential settings:

* Theme
* Default market
* Refresh frequency
* Notification preferences
* News preferences

Only implement settings supported by the application.

---

# 48. Reusable Components

Recommended component architecture:

```text
components/

layout/
    AppLayout
    Sidebar
    Header
    PageHeader

stocks/
    StockCard
    StockRow
    StockSearch
    StockPrice
    StockChart
    StockSparkline

recommendations/
    RecommendationBadge
    RecommendationCard
    ConfidenceBar
    RiskBadge
    RecommendationExplanation

news/
    NewsCard
    NewsList
    SentimentBadge
    SentimentBar
    NewsFilters

model/
    ModelStatus
    MetricCard
    FeatureImportance
    PredictionDistribution

charts/
    PriceChart
    PerformanceChart
    DistributionChart

common/
    Card
    Badge
    Button
    Select
    SearchInput
    LoadingState
    EmptyState
    ErrorState
    Skeleton
```

Do not create abstractions solely for the sake of abstraction.

Reuse components where meaningful.

---

# 49. Suggested Frontend Structure

```text
frontend/

├── public/
│
├── src/
│
│   ├── components/
│   │   ├── layout/
│   │   ├── stocks/
│   │   ├── recommendations/
│   │   ├── news/
│   │   ├── model/
│   │   ├── charts/
│   │   └── common/
│   │
│   ├── pages/
│   │   ├── Dashboard/
│   │   ├── Stocks/
│   │   ├── StockDetails/
│   │   ├── Screener/
│   │   ├── Recommendations/
│   │   ├── Watchlist/
│   │   ├── News/
│   │   ├── PredictionHistory/
│   │   ├── ModelIntelligence/
│   │   ├── Portfolio/
│   │   └── Settings/
│   │
│   ├── services/
│   │   ├── api.js
│   │   ├── stocks.js
│   │   ├── recommendations.js
│   │   ├── news.js
│   │   ├── predictions.js
│   │   └── model.js
│   │
│   ├── hooks/
│   │
│   ├── mocks/
│   │   ├── stocks.js
│   │   ├── recommendations.js
│   │   ├── news.js
│   │   └── model.js
│   │
│   ├── utils/
│   │
│   ├── styles/
│   │   ├── tokens.css
│   │   ├── global.css
│   │   └── utilities.css
│   │
│   ├── App.jsx
│   └── main.jsx
│
├── package.json
└── vite.config.js
```

---

# 50. Data Architecture

UI components should never make raw Axios requests directly.

Bad:

```text
Dashboard.jsx
    ↓
axios.get(...)
```

Preferred:

```text
Dashboard
    ↓
React Query Hook
    ↓
Service Layer
    ↓
API Client
    ↓
Backend
```

Example conceptual structure:

```text
useRecommendations()
        ↓
recommendationsService
        ↓
apiClient
        ↓
/api/recommendations
```

This allows the backend implementation to change without rewriting presentation components.

---

# 51. Mock Data Strategy

Some backend systems are still under development.

Therefore frontend development may initially use mock data.

Mocks belong inside:

```text
src/mocks/
```

Never:

```text
Dashboard.jsx

const recommendation = {
    ticker: "RELIANCE",
    confidence: 87
}
```

Mock objects should follow the expected backend response schema.

The frontend should make switching between mock and live services straightforward.

---

# 52. Expected Recommendation Data Shape

Conceptual example only:

```json
{
    "ticker": "RELIANCE.NS",
    "companyName": "Reliance Industries Limited",
    "signal": "BUY",
    "confidence": 0.87,
    "currentPrice": 1412.30,
    "targetPrice": 1530.00,
    "expectedReturn": 0.0835,
    "risk": "MEDIUM",
    "explanations": [
        "Strong 20-day momentum",
        "Price above 50-day moving average"
    ]
}
```

The actual API contract should ultimately be determined by backend implementation.

---

# 53. Expected News Data Shape

Conceptual example:

```json
{
    "id": "news_001",
    "headline": "Example headline",
    "source": "Example Source",
    "publishedAt": "2026-08-03T08:30:00Z",
    "summary": "Short summary",
    "tickers": [
        "RELIANCE.NS"
    ],
    "sentiment": "POSITIVE",
    "sentimentConfidence": 0.82,
    "url": "original article URL"
}
```

Fields that are unavailable should not be fabricated.

---

# 54. Loading States

Every asynchronous page must have loading states.

Use skeleton components rather than blank pages or generic text such as:

`Loading...`

Skeletons should roughly match the final component geometry.

---

# 55. Empty States

Examples:

Watchlist:

```text
Your watchlist is empty.

Search for stocks and add companies you want to monitor.
```

News:

```text
No relevant news found for the selected filters.
```

Recommendations:

```text
No recommendations match the selected filters.
```

---

# 56. Error States

API failures must not result in broken layouts.

Example:

```text
Unable to load recommendations.

The recommendation service could not be reached.

[ Try Again ]
```

Do not silently display mock data after production API failure.

---

# 57. Data Freshness

Where relevant, show:

```text
Last updated 2 minutes ago
```

This is particularly important for:

* Stock prices
* Recommendations
* News
* Market indices

The user should be able to distinguish live/recent data from stale information.

---

# 58. Responsive Design

## Desktop

Primary target.

Use sidebar + header layout.

Dense tables are acceptable.

## Tablet

Sidebar may collapse to icon navigation.

Cards should reorganize into fewer columns.

## Mobile

Use a drawer or bottom-accessible navigation pattern.

Tables should transform into cards rather than forcing horizontal scrolling wherever practical.

Important information order:

```text
Stock
Price
Recommendation
Confidence
Expected Return
Risk
```

Charts must remain readable.

---

# 59. Accessibility

Minimum requirements:

* Sufficient text contrast
* Visible keyboard focus states
* Semantic HTML
* Buttons must be actual buttons
* Navigation must use appropriate navigation elements
* Charts must have text equivalents where important
* Color must not be the sole indicator of state

For example:

Do not show only a green badge.

Show:

`BUY`

with green styling.

---

# 60. Motion

Animations should be subtle.

Allowed examples:

* 150–250ms hover transitions
* Small card elevation changes
* Skeleton loading animations
* Smooth chart rendering
* Sidebar collapse transition

Avoid:

* Page elements flying into view
* Excessive GSAP animations
* Animated glowing borders
* Constantly moving backgrounds
* Decorative particles

This is an analytical product, not a marketing landing page.

---

# 61. Number Formatting

Indian market values should use appropriate formatting.

Examples:

```text
₹1,412.30

₹24,842.50

+8.35%

-1.42%
```

Large Indian financial values may use:

```text
₹1.2L
₹4.8Cr
```

where appropriate.

Avoid excessive decimal precision.

---

# 62. Signal Components

Recommended visual semantics:

```text
STRONG BUY    Green
BUY           Green
HOLD          Amber
SELL          Red
STRONG SELL   Red
```

Use textual labels in addition to colors.

---

# 63. Risk Components

```text
LOW       Green or muted positive
MEDIUM    Amber
HIGH      Red
```

Risk color should never overpower recommendation information.

---

# 64. Frontend Development Phases

Do not implement the entire frontend in one uncontrolled pass.

## Phase 1 — Foundation

Implement:

* React/Vite setup
* Global CSS
* Design tokens
* Routing
* AppLayout
* Sidebar
* Header
* Reusable Card
* Badge
* Button
* Loading/error/empty components

Goal:

Establish the design system and application shell.

---

## Phase 2 — Dashboard

Implement:

* Market summary
* Top recommendation
* Market overview
* Recommendation snapshot
* Watchlist preview
* Important news
* Mock service layer where backend endpoints do not yet exist

---

## Phase 3 — Recommendations + Stock Details

Implement:

* Recommendations Explorer
* Recommendation filters
* Recommendation table
* Stock Details
* Price chart
* Model signal
* Confidence
* Forecast
* Recommendation explanation
* Technical indicators
* Stock-specific news

---

## Phase 4 — Discovery

Implement:

* Stocks
* Screener
* Watchlist
* Search functionality

---

## Phase 5 — News Intelligence

Implement:

* News feed
* Stock filtering
* Sentiment filtering
* Source filtering
* Time filtering
* Sentiment visualization

Do not imply sentiment affects recommendations unless backend integration exists.

---

## Phase 6 — ML Transparency

Implement:

* Prediction History
* Model Intelligence
* Model metrics
* Prediction distribution
* Feature importance
* Training metadata

---

## Phase 7 — Backend Integration

Replace mocks endpoint-by-endpoint.

For every integration:

1. Define API contract.
2. Connect service.
3. Validate loading state.
4. Validate success state.
5. Validate empty state.
6. Validate failure state.
7. Remove corresponding mock dependency.

---

## Phase 8 — Responsive and Accessibility Pass

Validate:

* Desktop
* Tablet
* Mobile
* Keyboard navigation
* Contrast
* Loading states
* Empty states
* Error states
* Long stock/company names
* Large values
* Missing data

---

# 65. Backend Independence

Frontend implementation must not modify ML logic merely to satisfy UI requirements.

If required backend information does not exist:

1. Document the expected API requirement.
2. Use mock data where appropriate during development.
3. Mark the integration as pending.
4. Continue frontend development.

Do not create fake backend behavior without explicit architectural justification.

---

# 66. Current vs Future Features

The frontend must distinguish between:

### Current

Features supported by existing backend/data systems.

### Mocked

UI being developed against a proposed API contract.

### Future

Features requiring substantial backend functionality that does not yet exist.

Do not blur these categories.

---

# 67. News Architecture

Initial architecture:

```text
News Sources
      ↓
News Collector
      ↓
Article Normalization
      ↓
Ticker Association
      ↓
Database
      ↓
News API
      ↓
Frontend
```

Future architecture:

```text
News Collector
      ↓
Ticker Association
      ↓
Sentiment Model
      ↓
Sentiment Features
      ↓
Model Evaluation
      ↓
Recommendation Model
```

Sentiment must only enter the recommendation model after testing demonstrates that it improves model performance.

---

# 68. Product Information Hierarchy

The frontend should communicate information in this order:

```text
What should I look at?
        ↓
What does the model recommend?
        ↓
How confident is it?
        ↓
What return does it expect?
        ↓
What is the risk?
        ↓
Why does it believe this?
        ↓
What is happening technically?
        ↓
What relevant news exists?
        ↓
How has the model performed historically?
```

This hierarchy should influence page layout and component prominence.

---

# 69. UI Anti-Patterns

Do NOT:

* Display every tracked stock on Dashboard.
* Hardcode model confidence values inside components.
* Present mock data as live data.
* Generate fake recommendation explanations.
* Generate fake news sentiment.
* Use red/green decoratively.
* Introduce excessive animation.
* Add Redux without justification.
* Add Tailwind CSS.
* Create unnecessary dependencies.
* Duplicate components across pages.
* Create huge monolithic page components.
* Put direct API requests inside presentation components.
* Hide API errors by automatically falling back to mocks.
* Claim news sentiment affects recommendations before integration exists.
* Show inappropriate ML metrics simply because they look impressive.

---

# 70. Target User Experience

A user should be able to open the application and within seconds understand:

```text
Market condition

        ↓

Best current opportunities

        ↓

Model recommendation

        ↓

Confidence + expected return + risk

        ↓

Reasoning

        ↓

Technical evidence

        ↓

Relevant news

        ↓

Historical model reliability
```

The system should feel like an AI investment research product rather than a collection of disconnected charts.

---

# 71. Final Design Rule

Every frontend element must answer at least one of these questions:

* Does this help understand the market?
* Does this help understand a stock?
* Does this help understand a recommendation?
* Does this help evaluate model reliability?
* Does this help understand relevant news?
* Does this help the user take a meaningful next action?

If an element answers none of these questions, it probably does not belong in the interface.

---

# 72. Source of Truth

This file is the authoritative frontend design specification.

When implementing the frontend:

1. Read this document before modifying UI architecture.
2. Preserve established design tokens and information hierarchy.
3. Reuse existing components before creating new ones.
4. Do not redesign pages independently without updating this specification.
5. Do not invent unavailable backend functionality.
6. Keep mock and live data clearly separated.
7. Maintain visual consistency across all application pages.

When implementation requirements conflict with this document because of actual backend constraints, document the conflict and adapt the frontend deliberately rather than silently deviating from the design.

# Portfolio Growth & Cashflow Modeling — Implementation Notes (Oct 2025)

> Final setup: **Option 2 (Nominal)** — *dividends are modeled as cash inflows and reinvested; portfolio price growth compounds at a **nominal ex‑div** rate*. This keeps portfolio growth **nominal** to align with `ret_outflows` which are emitted in **nominal** dollars.

---

## 1) Data Artifacts (Power BI)

**Tables**
- `Cashflows` — projected/actual inflows (+) and outflows (−). Includes *dividends as inflows* if you load them there; otherwise see §3.B.
- `DivFlowsYear` — yearly dividend projections per account. Required columns:
  - `year` (number)
  - `dividends_net_by_year` (number, *net* of taxes used for cashflows)
  - `portfolio_value` (number)
  - `dividend_yield_weighted` (number, gross; optional)
- `YearTable` — canonical year dimension.
- `TypeDim` — disconnected 5‑row dimension for Matrix columns: Inflows, Outflows, Dividends, Beg Balance, End Balance (with `SortKey` to order columns).

**Relationships**
- `YearTable[Year]` 1→* `Cashflows[Year]`
- `YearTable[Year]` 1→* `DivFlowsYear[year]`
- `TypeDim` remains **disconnected** (used only to drive Matrix headers).

**Data types**
- Ensure `DivFlowsYear` numeric columns are **Decimal Number** (Power Query) to avoid DAX text→number coercion.

---

## 2) Return, Inflation, Dividend Yield (DAX)

```DAX
-- Inputs already in model
[Real Return Rate]     -- e.g., 0.04 ; TOTAL real return (includes dividends)
[Inflation Rate]       -- e.g., 0.03

-- 2.1 Nominal total return
Nominal Total Return :=
VAR r = COALESCE([Real Return Rate], 0)
VAR i = COALESCE([Inflation Rate], 0)
RETURN (1 + r) * (1 + i) - 1

-- 2.2 Portfolio‑weighted NET dividend yield (from DivFlowsYear)
Dividend Yield (Net) :=
DIVIDE(
    SUM(DivFlowsYear[dividends_net_by_year]),
    SUM(DivFlowsYear[portfolio_value])
)

-- 2.3 Price‑only nominal return (ex‑div)
Return ex Div (Nominal) :=
VAR r_nom = COALESCE([Nominal Total Return], 0)
VAR y_net = COALESCE([Dividend Yield (Net)], 0)
RETURN (1 + r_nom) / (1 + y_net) - 1
```

> Rationale: with dividends treated as cashflows, compounding must use a **price‑only** rate to avoid double‑counting. Using **nominal** keeps growth consistent with `ret_outflows` (which grow with inflation).

---

## 3) End‑of‑Year Balance with Reinvested Dividends (DAX)

We compound **Start Balance** at the **nominal ex‑div** rate and add **all signed investable flows**, each compounded forward. Two variants below; pick one and **do not** use both.

### A) Dividends **live in Cashflows** already (preferred)
```DAX
End Balance (EOY) :=
VAR y = MAX(YearTable[Year])
VAR r = COALESCE([Return ex Div (Nominal)], 0)

VAR baseYear =
    VAR cy = CALCULATE(MAX(ret_globals[Value]), ret_globals[Variable] = "current_year")
    RETURN IF(NOT ISBLANK(cy), cy, YEAR(TODAY()))

VAR StartCompounded =
    [Start Balance] * POWER(1 + r, y - baseYear)

VAR FlowsCompoundedSigned =
    SUMX(
        FILTER(
            ALL(Cashflows),
            Cashflows[IsInvestable] = TRUE()
                && VALUE(Cashflows[Year]) <= y
        ),
        Cashflows[SignedValue] * POWER(1 + r, y - VALUE(Cashflows[Year]))
    )

RETURN StartCompounded + FlowsCompoundedSigned
```

### B) Dividends **do NOT** live in Cashflows (add them explicitly)
```DAX
End Balance (EOY) :=
VAR y = MAX(YearTable[Year])
VAR r = COALESCE([Return ex Div (Nominal)], 0)

VAR baseYear =
    VAR cy = CALCULATE(MAX(ret_globals[Value]), ret_globals[Variable] = "current_year")
    RETURN IF(NOT ISBLANK(cy), cy, YEAR(TODAY()))

VAR StartCompounded =
    [Start Balance] * POWER(1 + r, y - baseYear)

VAR Flows_NoDiv =
    SUMX(
        FILTER(
            ALL(Cashflows),
            Cashflows[IsInvestable] = TRUE()
                && VALUE(Cashflows[Year]) <= y
        ),
        Cashflows[SignedValue] * POWER(1 + r, y - VALUE(Cashflows[Year]))
    )

VAR Divs_Compounded =
    SUMX(
        FILTER(ALL(DivFlowsYear), VALUE(DivFlowsYear[year]) <= y),
        DivFlowsYear[dividends_net_by_year] * POWER(1 + r, y - VALUE(DivFlowsYear[year]))
    )

RETURN StartCompounded + Flows_NoDiv + Divs_Compounded
```

**Timing tweak (optional):** if you want *current‑year* flows to earn ~half a year, add `+ 0.5` to the exponent in the SUMX terms.

---

## 4) Matrix Setup (Combined view)

- **Rows:** `YearTable[Year]`
- **Columns:** `TypeDim[Type]` (sorted by `SortKey`: Inflows, Outflows, Dividends, Beg Balance, End Balance)
- **Values:** one branching measure:
```DAX
Amount By Type :=
VAR sel = SELECTEDVALUE(TypeDim[Type])
RETURN
SWITCH(
    sel,
    "Inflows",   CALCULATE(SUM(Cashflows[Value]), Cashflows[Type] = "Inflows"),
    "Outflows",  CALCULATE(SUM(Cashflows[Value]), Cashflows[Type] = "Outflows"),
    "Dividends", SUM(DivFlowsYear[dividends_net_by_year]),
    "Beg Balance", [Beg Balance],
    "End Balance", [End Balance (EOY)],
    BLANK()
)
```
Turn **Column subtotals** off (totals are not meaningful across mixed “types”).

---

## 5) Validation & Diagnostics

```DAX
Net Flows (Investable) := [Inflows] + [Dividends] - [Outflows]

Avg Invested Capital (midyear) := [Beg Balance] + 0.5 * [Net Flows (Investable)]

Implied Price Return (Nominal) :=
DIVIDE(
    [End Balance (EOY)] - ( [Beg Balance] + [Net Flows (Investable)] ),
    [Avg Invested Capital (midyear)]
)
```
Expect `Implied Price Return (Nominal)` ≈ `[Return ex Div (Nominal)]` pre‑ and post‑withdrawals. If an Excel ratio “jumps” when withdrawals begin, it’s the denominator shrinking (math, not modeling error).

---

## 6) Switching Views (Nominal ↔ Real)

- Model is **nominal** by default to align with `ret_outflows`.
- To show a **real** view, either:
  1) Deflate flows in Power Query: `RealValue = Value / (1+Inflation)^(Year-BaseYear)` and use `[Return ex Div (Real)]`, **or**
  2) Keep nominal data and deflate visuals with a real‑dollar wrapper.

Use one frame per visual to avoid mismatches.

---

## 7) Common Pitfalls

- **Double‑counting dividends:** If dividends are in `Cashflows`, do **not** add `Divs_Compounded`. If not, add §3.B and keep them out of `Cashflows` filters.
- **Text typed as numbers:** Coerce numeric CSV columns in Power Query to Decimal; else wrap with `VALUE()` in DAX.
- **Column order in Matrix:** sort `TypeDim[Type]` by `TypeDim[SortKey]` (Model view → Properties).

---

## 8) Changelog

- **2025‑10‑15** — Finalized **Nominal + ex‑div** approach; added dividend yield from `DivFlowsYear` and EOY variants; documented validation checks and timing option.

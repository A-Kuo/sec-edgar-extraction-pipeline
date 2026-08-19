"""
Synthetic filing generator.

Why this exists
---------------
Training and evaluating the detector must work in a CI runner that has no
database, no SEC credentials, and no network egress to sec.gov. Generating a
plausible corpus locally makes the whole ML path — train, evaluate, register,
gate — runnable from a clean checkout, which is the difference between a model
pipeline that is genuinely tested and one that is only tested where someone
already has production data.

The generator is seeded, so a given seed always yields the same corpus and the
resulting model artifact hashes reproducibly.

What "plausible" means here
---------------------------
Company scale is drawn log-uniformly across five orders of magnitude, because
real filers span corner shops to megacaps. Margins, leverage and asset turnover
are drawn from sector-flavoured ranges and then held roughly stable per company
across years with a random walk, so the year-over-year continuity features see
realistic drift rather than white noise. Facts are emitted in exactly the row
shape ``financial_facts`` uses, so the same ``build_features`` path serves
synthetic and real data with no branching.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_SEED = 7

#: Sector archetypes: (name, net margin, operating margin, leverage, asset turnover).
#: Ranges are (low, high) and get sampled per company, then walked per year.
_SECTORS: tuple[
    tuple[str, tuple[float, float], tuple[float, float], tuple[float, float], tuple[float, float]],
    ...,
] = (
    ("software", (0.10, 0.35), (0.15, 0.40), (0.20, 0.50), (0.30, 0.70)),
    ("retail", (0.01, 0.06), (0.02, 0.09), (0.45, 0.75), (1.50, 3.00)),
    ("industrial", (0.04, 0.12), (0.06, 0.16), (0.40, 0.70), (0.60, 1.20)),
    ("bank", (0.15, 0.30), (0.20, 0.38), (0.80, 0.93), (0.03, 0.09)),
    ("biotech", (-0.60, 0.10), (-0.70, 0.12), (0.10, 0.40), (0.05, 0.40)),
)


@dataclass
class SyntheticCorpus:
    """Fact rows plus the filing metadata ``build_features`` needs for continuity."""

    facts: list[dict[str, Any]]
    filings: dict[str, dict[str, Any]]

    def __len__(self) -> int:
        return len(self.filings)


def _accession(cik: int, year: int, seq: int) -> str:
    """Format an EDGAR-shaped accession number: ``NNNNNNNNNN-YY-NNNNNN``."""
    return f"{cik:010d}-{year % 100:02d}-{seq:06d}"


def _fact_row(
    accession: str,
    name: str,
    value: float,
    unit: str,
    period_start: date | None,
    period_end: date,
) -> dict[str, Any]:
    return {
        "accession_number": accession,
        "fact_name": f"us-gaap:{name}",
        "fact_value": value,
        "unit": unit,
        "period_start": period_start.isoformat() if period_start else None,
        "period_end": period_end.isoformat(),
        "segment": None,
    }


def generate_corpus(
    n_companies: int = 120,
    years_per_company: int = 5,
    *,
    seed: int = DEFAULT_SEED,
    start_year: int = 2019,
) -> SyntheticCorpus:
    """
    Generate ``n_companies × years_per_company`` well-formed annual filings.

    Every filing carries the full target fact set with internally consistent
    values — net income really is margin × revenue, and EPS really does
    reconcile with shares outstanding. Corruptions are the evaluation harness's
    job (:mod:`src.ml.evaluation`), not this generator's; keeping the two apart
    means a corruption the detector catches is one that was genuinely absent
    from training.
    """
    if n_companies < 1 or years_per_company < 1:
        raise ValueError("n_companies and years_per_company must both be >= 1")

    rng = np.random.default_rng(seed)
    facts: list[dict[str, Any]] = []
    filings: dict[str, dict[str, Any]] = {}

    for company_index in range(n_companies):
        cik = 100000 + company_index
        sector, margin_r, op_r, lev_r, turn_r = _SECTORS[company_index % len(_SECTORS)]

        # Company scale: revenue spanning ~$10M to ~$500B.
        revenue = float(10 ** rng.uniform(7.0, 11.7))
        net_margin = float(rng.uniform(*margin_r))
        op_margin = float(rng.uniform(*op_r))
        leverage = float(rng.uniform(*lev_r))
        turnover = float(rng.uniform(*turn_r))
        shares = float(10 ** rng.uniform(7.0, 9.9))

        for year_offset in range(years_per_company):
            year = start_year + year_offset
            period_end = date(year, 12, 31)
            period_start = date(year, 1, 1)
            filing_date = period_end + timedelta(days=int(rng.integers(30, 75)))
            accession = _accession(cik, year, year_offset + 1)

            # Random walk so year-over-year features see realistic movement.
            revenue *= float(np.exp(rng.normal(0.06, 0.14)))
            net_margin = float(np.clip(net_margin + rng.normal(0, 0.015), -0.9, 0.6))
            op_margin = float(np.clip(op_margin + rng.normal(0, 0.015), -0.9, 0.6))
            leverage = float(np.clip(leverage + rng.normal(0, 0.02), 0.05, 0.95))
            turnover = float(np.clip(turnover * np.exp(rng.normal(0, 0.06)), 0.02, 4.0))
            shares *= float(np.exp(rng.normal(0.0, 0.03)))

            assets = revenue / turnover
            net_income = revenue * net_margin
            operating_income = revenue * op_margin
            liabilities = assets * leverage
            eps = net_income / shares

            filings[accession] = {
                "accession_number": accession,
                "cik": str(cik),
                "ticker": f"SY{company_index:04d}",
                "filing_type": "10-K",
                "filing_date": filing_date.isoformat(),
                "period_of_report": period_end.isoformat(),
                "sector": sector,
            }

            facts.extend(
                [
                    _fact_row(accession, "Revenues", revenue, "USD", period_start, period_end),
                    _fact_row(
                        accession, "NetIncomeLoss", net_income, "USD", period_start, period_end
                    ),
                    _fact_row(
                        accession,
                        "OperatingIncomeLoss",
                        operating_income,
                        "USD",
                        period_start,
                        period_end,
                    ),
                    # Balance-sheet concepts are instant, not duration.
                    _fact_row(accession, "Assets", assets, "USD", None, period_end),
                    _fact_row(accession, "Liabilities", liabilities, "USD", None, period_end),
                    _fact_row(
                        accession,
                        "EarningsPerShareBasic",
                        eps,
                        "USD/shares",
                        period_start,
                        period_end,
                    ),
                    _fact_row(
                        accession,
                        "CommonStockSharesOutstanding",
                        shares,
                        "shares",
                        None,
                        period_end,
                    ),
                ]
            )

    logger.info(
        "Generated %d synthetic filings (%d companies × %d years, seed=%d)",
        len(filings),
        n_companies,
        years_per_company,
        seed,
    )
    return SyntheticCorpus(facts=facts, filings=filings)

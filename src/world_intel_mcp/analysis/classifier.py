"""Keyword-based event threat classification.

Fast classifier with 14 categories and severity scoring. No ML deps.
Designed as the first pass; an LLM can refine confidence later.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Category keyword patterns (ordered by specificity)
# ---------------------------------------------------------------------------

CATEGORIES: dict[str, dict] = {
    "nuclear": {
        "keywords": [
            "nuclear", "atomic", "uranium", "plutonium", "warhead",
            "icbm", "ballistic missile", "nuclear test", "radiation",
            "enrichment", "nonproliferation", "iaea", "dirty bomb",
        ],
        "severity_base": 9,
    },
    "terrorism": {
        "keywords": [
            "terrorist", "terrorism", "suicide bomb", "car bomb", "ied",
            "hostage", "kidnapping", "beheading", "mass shooting",
            "lone wolf", "radicalization", "jihad", "extremist",
        ],
        "severity_base": 9,
    },
    "military": {
        "keywords": [
            "military", "troops", "soldier", "artillery", "airstrike",
            "bombing", "invasion", "offensive", "deployed", "nato",
            "warship", "submarine", "fighter jet", "drone strike",
            "mobilization", "ceasefire", "frontline", "casualties",
        ],
        "severity_base": 8,
    },
    "cyber": {
        "keywords": [
            "cyber", "hack", "ransomware", "malware", "phishing",
            "data breach", "ddos", "vulnerability", "exploit", "cve-",
            "zero-day", "apt", "botnet", "credential", "encryption",
        ],
        "severity_base": 7,
    },
    "political": {
        "keywords": [
            "election", "coup", "protest", "demonstration", "sanctions",
            "embargo", "diplomatic", "parliament", "congress",
            "impeachment", "referendum", "regime", "martial law",
            "authoritarian", "democracy", "opposition",
        ],
        "severity_base": 6,
    },
    "economic": {
        "keywords": [
            "recession", "inflation", "default", "debt crisis",
            "currency collapse", "bank run", "stock crash", "layoffs",
            "trade war", "tariff", "supply chain", "shortage",
        ],
        "severity_base": 6,
    },
    "health": {
        "keywords": [
            "pandemic", "epidemic", "outbreak", "virus", "pathogen",
            "vaccine", "quarantine", "who emergency", "mortality",
            "infection", "ebola", "bird flu", "h5n1", "mpox",
        ],
        "severity_base": 7,
    },
    "climate": {
        "keywords": [
            "hurricane", "typhoon", "cyclone", "earthquake", "tsunami",
            "flood", "drought", "wildfire", "heatwave", "glacier",
            "sea level", "emission", "carbon", "climate change",
        ],
        "severity_base": 6,
    },
    "infrastructure": {
        "keywords": [
            "power outage", "blackout", "pipeline", "cable cut",
            "internet outage", "grid failure", "dam collapse",
            "bridge collapse", "port blockade", "supply disruption",
        ],
        "severity_base": 7,
    },
    "maritime": {
        "keywords": [
            "shipping", "strait", "piracy", "naval blockade", "vessel",
            "cargo ship", "tanker", "port", "chokepoint", "seafarer",
            "coast guard", "maritime security",
        ],
        "severity_base": 5,
    },
    "aviation": {
        "keywords": [
            "airspace", "no-fly zone", "intercept", "air defense",
            "airline", "crash", "hijack", "flight ban",
        ],
        "severity_base": 6,
    },
    "energy": {
        "keywords": [
            "oil price", "opec", "gas pipeline", "lng", "refinery",
            "energy crisis", "power grid", "renewable", "solar",
            "wind farm", "battery", "hydrogen",
        ],
        "severity_base": 5,
    },
    "social_unrest": {
        "keywords": [
            "riot", "looting", "tear gas", "water cannon", "curfew",
            "state of emergency", "civil disobedience", "strike",
            "labor dispute", "food riot", "uprising",
        ],
        "severity_base": 7,
    },
    "space": {
        "keywords": [
            "satellite", "space debris", "solar flare", "geomagnetic",
            "space weather", "orbit", "launch", "rocket", "asteroid",
        ],
        "severity_base": 4,
    },
}

# Severity modifiers — keywords that bump severity up
_HIGH_SEVERITY: list[str] = [
    "killed", "dead", "death toll", "massacre", "war crime", "genocide",
    "nuclear", "catastrophic", "critical", "emergency", "mass casualty",
    "destroyed", "collapse", "unprecedented", "worst",
]

_MODERATE_SEVERITY: list[str] = [
    "injured", "wounded", "escalation", "crisis", "threat", "warning",
    "attack", "strike", "explosion", "breach", "violation",
]

# ---------------------------------------------------------------------------
# Keyword compilation
#
# Default semantics: boundary-anchored prefix (\b + keyword + \w*), so stems
# keep matching their inflections ("launch" → "launched", "protest" →
# "protesters") but nothing fires mid-word ("riot" no longer matches inside
# "patriotic", "strike" inside "airstrike", "ied" inside "denied").
# ---------------------------------------------------------------------------

# Keywords that are themselves prefixes of unrelated words match only as a
# standalone word (plus plural): "port" must not fire on "portfolio", "coup"
# on "couple", "regime" on "regimen", "carbon" on "carbonated".
_WORD_ONLY_KEYWORDS: frozenset[str] = frozenset({"coup", "port", "regime", "carbon"})

# Keywords whose valid suffixes are narrower than \w*: "apt" only as APT or
# a numbered group id (never "aptitude"), "dead" only bare or "deadly"
# (never "deadline").
_PATTERN_OVERRIDES: dict[str, str] = {
    "apt": r"\bapt\d*\b",
    "dead": r"\bdead(?:ly)?\b",
}


def _compile_keyword(kw: str) -> re.Pattern[str]:
    override = _PATTERN_OVERRIDES.get(kw)
    if override is not None:
        return re.compile(override)
    if kw in _WORD_ONLY_KEYWORDS:
        return re.compile(r"\b" + re.escape(kw) + r"s?\b")
    return re.compile(r"\b" + re.escape(kw) + r"\w*")


def _matched_keywords(
    patterns: list[tuple[str, re.Pattern[str]]], text_lower: str
) -> list[str]:
    """Keywords whose boundary pattern matches, in table order.

    Every pattern requires its literal keyword as a substring (overrides
    included), so the C-speed `in` check gates the regex and keeps
    classification at roughly substring-scan cost.
    """
    return [kw for kw, pat in patterns if kw in text_lower and pat.search(text_lower)]


_CATEGORY_PATTERNS: dict[str, list[tuple[str, re.Pattern[str]]]] = {
    name: [(kw, _compile_keyword(kw)) for kw in info["keywords"]]
    for name, info in CATEGORIES.items()
}
_HIGH_SEVERITY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (kw, _compile_keyword(kw)) for kw in _HIGH_SEVERITY
]
_MODERATE_SEVERITY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (kw, _compile_keyword(kw)) for kw in _MODERATE_SEVERITY
]


def classify_event(text: str) -> dict:
    """Classify text into threat categories with severity scoring.

    Returns dict with primary category, severity (1-10), confidence,
    all matched categories, and matched keywords.
    """
    text_lower = text.lower()
    matches: list[dict] = []

    for cat_name, cat_info in CATEGORIES.items():
        matched_kw = _matched_keywords(_CATEGORY_PATTERNS[cat_name], text_lower)
        if matched_kw:
            # Confidence scales with number of keyword hits
            confidence = min(1.0, len(matched_kw) * 0.2 + 0.3)
            matches.append(
                {
                    "category": cat_name,
                    "keywords_matched": matched_kw,
                    "keyword_count": len(matched_kw),
                    "severity_base": cat_info["severity_base"],
                    "confidence": round(confidence, 2),
                }
            )

    # Sort by keyword count (most specific match first)
    matches.sort(key=lambda m: m["keyword_count"], reverse=True)

    # Compute severity modifier
    severity_mod = 0
    high_hits = _matched_keywords(_HIGH_SEVERITY_PATTERNS, text_lower)
    mod_hits = _matched_keywords(_MODERATE_SEVERITY_PATTERNS, text_lower)
    if high_hits:
        severity_mod = 2
    elif mod_hits:
        severity_mod = 1

    primary = matches[0] if matches else None
    severity = min(10, (primary["severity_base"] + severity_mod)) if primary else 0

    return {
        "primary_category": primary["category"] if primary else "unclassified",
        "severity": severity,
        "confidence": primary["confidence"] if primary else 0.0,
        "all_categories": [
            {
                "category": m["category"],
                "confidence": m["confidence"],
                "keywords": m["keywords_matched"],
            }
            for m in matches
        ],
        "category_count": len(matches),
        "severity_modifiers": high_hits + mod_hits,
        "source": "keyword-classifier",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Hostility categories (headline heuristic for news_signals)
#
# Four kinds of hostile coverage between or toward countries. English
# keywords over a headline, boundary-anchored like the categories above.
# A keyword heuristic: it does not parse who acts on whom, negation
# ("rules out strikes"), quotation, or sports and weather idiom beyond the
# patterns pinned below. Non-English headlines match only through
# spellings they share with English ("sanctions", "missile").
# ---------------------------------------------------------------------------

HOSTILITY_CATEGORIES: dict[str, list[str]] = {
    "threat": [
        "threat", "warns", "warned", "ultimatum", "retaliat", "vows revenge",
        "vow revenge",
    ],
    "sanction": [
        "sanction", "embargo", "blacklist", "export control", "export ban",
        "asset freeze", "tariff", "trade war",
    ],
    "force": [
        "airstrike", "air strike", "missile", "drone strike", "drone attack",
        "attack", "bombing", "bombard", "shelling", "invasion", "invade",
        "troops", "clash", "artillery", "offensive", "war", "raid",
        "military strike", "strikes on", "strike on", "strike kill",
        "strikes kill",
    ],
    "criticism": [
        "condemn", "criticis", "criticiz", "denounce", "rebuke", "slams",
        "slammed", "accuse", "accusation", "summons ambassador",
        "summoned ambassador", "lambast",
    ],
}

# Narrower than the default \bkw\w* stem where the stem would fire on
# unrelated words: "war" not inside "warns"/"warrant"/"Warsaw", and not
# a trade, price or culture war (a trade war counts as sanction); "raid"
# not as "raiders" (sports teams); "attack" not as "attacker" (football);
# "troops" not "troopship"-style compounds; "offensive" not before
# remarks, comments or posts. Other idioms ("war on drugs", a "heart
# attack") still match: a named limit.
_HOSTILITY_OVERRIDES: dict[str, str] = {
    "war": r"(?<!trade )(?<!price )(?<!culture )(?<!tariff )\bwars?\b(?! of words)",
    "trade war": r"\btrade wars?\b",
    "offensive": (
        r"\boffensive\b(?! (?:remarks?|comments?|language|posts?|tweets?|jokes?|content))"
    ),
    "raid": r"\braid(?:s|ed)?\b",
    "attack": r"\battack(?:s|ed|ing)?\b",
    "clash": r"\bclash(?:es|ed)?\b",
    "troops": r"\btroops\b",
}

_HOSTILITY_PATTERNS: dict[str, list[tuple[str, re.Pattern[str]]]] = {
    cat: [
        (
            kw,
            re.compile(_HOSTILITY_OVERRIDES[kw])
            if kw in _HOSTILITY_OVERRIDES
            else _compile_keyword(kw),
        )
        for kw in kws
    ]
    for cat, kws in HOSTILITY_CATEGORIES.items()
}

HOSTILITY_METHOD = (
    "English keyword heuristic over headlines (classifier.HOSTILITY_CATEGORIES): "
    "boundary-anchored stems, no parsing of who acts on whom, negation, "
    "quotation or idiom; a headline can count in several categories. Written "
    "for English: a non-English headline matches only through shared "
    "spellings (French 'sanctions', 'missile'), so non-English coverage is "
    "undercounted."
)


def classify_hostility(text: str) -> dict[str, list[str]]:
    """Hostility categories a headline matches, with the keywords that hit.

    Returns {category: [matched keywords]} for matching categories only;
    an empty dict means no hostility keyword matched.
    """
    text_lower = (text or "").lower()
    out: dict[str, list[str]] = {}
    for cat, patterns in _HOSTILITY_PATTERNS.items():
        hits = _matched_keywords(patterns, text_lower)
        if hits:
            out[cat] = hits
    return out


async def fetch_classify_event(fetcher, text: str) -> dict:
    """Classify a text event. Thin async wrapper for MCP dispatch."""
    return classify_event(text)

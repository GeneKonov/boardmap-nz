"""
BoardMap NZ — Data Pipeline
============================
Fetches current board of directors for each NZX top 30 company from the
NZBN API (api.business.govt.nz), resolves duplicate director names via fuzzy
matching, and outputs a single graph.json consumed by the frontend.

Usage:
    python pipeline.py

Requirements:
    pip install requests thefuzz python-Levenshtein

Environment:
    Set MBIE_API_KEY in your environment or edit the API_KEY line below.
    Register at: https://portal.api.business.govt.nz
"""

import os
import json
import time
import logging
from datetime import date
from collections import defaultdict

import requests
from thefuzz import process as fuzz_process

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("MBIE_API_KEY", "1746d694dea54fd8a0c428a850324cf4")
BASE_URL = "https://api.business.govt.nz/gateway/nzbn/v5/entities"
OUTPUT_FILE = "graph.json"
FUZZY_THRESHOLD = 90        # Similarity score (0-100) to merge director names
REQUEST_DELAY = 0.4         # Seconds between API calls — be polite to the API
MAX_RETRIES = 3

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NZX Top 30 — manually curated seed list
# nzbn: NZBN number (preferred — skip the search step if provided)
# If nzbn is None, pipeline will search by name automatically.
# Update this list when the top 30 composition changes.
# ---------------------------------------------------------------------------

NZX_TOP_30 = [
    {"id": "c01", "ticker": "FPH", "name": "Fisher & Paykel Healthcare Corporation Limited", "nzbn": None,              "sector": "Healthcare",      "cap": 16.0},
    {"id": "c02", "ticker": "ANZ", "name": "ANZ Bank New Zealand Limited",                   "nzbn": None,              "sector": "Financials",      "cap": 14.0},
    {"id": "c03", "ticker": "WBC", "name": "Westpac New Zealand Limited",                    "nzbn": None,              "sector": "Financials",      "cap": 13.0},
    {"id": "c04", "ticker": "CEN", "name": "Contact Energy Limited",                         "nzbn": None,              "sector": "Energy",          "cap": 5.2},
    {"id": "c05", "ticker": "MEL", "name": "Meridian Energy Limited",                        "nzbn": None,              "sector": "Energy",          "cap": 9.8},
    {"id": "c06", "ticker": "MCY", "name": "Mercury NZ Limited",                             "nzbn": None,              "sector": "Energy",          "cap": 5.6},
    {"id": "c07", "ticker": "GNE", "name": "Genesis Energy Limited",                         "nzbn": None,              "sector": "Energy",          "cap": 2.1},
    {"id": "c08", "ticker": "AIR", "name": "Air New Zealand Limited",                        "nzbn": None,              "sector": "Industrials",     "cap": 1.8},
    {"id": "c09", "ticker": "SPK", "name": "Spark New Zealand Limited",                      "nzbn": "9429039661098",   "sector": "Technology",      "cap": 4.3},
    {"id": "c10", "ticker": "TPW", "name": "Manawa Energy Limited",                          "nzbn": "9429038917912",   "sector": "Utilities",       "cap": 1.5},
    {"id": "c11", "ticker": "SUM", "name": "Summerset Group Holdings Limited",               "nzbn": None,              "sector": "Healthcare",      "cap": 2.4},
    {"id": "c12", "ticker": "RYM", "name": "Ryman Healthcare Limited",                       "nzbn": None,              "sector": "Healthcare",      "cap": 3.1},
    {"id": "c13", "ticker": "PCT", "name": "Precinct Properties New Zealand Limited",        "nzbn": None,              "sector": "Real Estate",     "cap": 1.9},
    {"id": "c14", "ticker": "KPG", "name": "Kiwi Property Group Limited",                   "nzbn": None,              "sector": "Real Estate",     "cap": 1.7},
    {"id": "c15", "ticker": "ARG", "name": "Argosy Property Limited",                        "nzbn": None,              "sector": "Real Estate",     "cap": 1.2},
    {"id": "c16", "ticker": "VCT", "name": "Vector Limited",                                 "nzbn": None,              "sector": "Utilities",       "cap": 3.2},
    {"id": "c17", "ticker": "SKC", "name": "SkyCity Entertainment Group Limited",            "nzbn": None,              "sector": "Consumer",        "cap": 1.4},
    {"id": "c18", "ticker": "SKT", "name": "Sky Network Television Limited",                 "nzbn": None,              "sector": "Consumer",        "cap": 0.9},
    {"id": "c19", "ticker": "IFT", "name": "Infratil Limited",                               "nzbn": None,              "sector": "Infrastructure",  "cap": 6.8},
    {"id": "c20", "ticker": "ATM", "name": "The a2 Milk Company Limited",                    "nzbn": None,              "sector": "Consumer",        "cap": 4.1},
    {"id": "c21", "ticker": "FCG", "name": "Fonterra Co-operative Group Limited",            "nzbn": None,              "sector": "Consumer",        "cap": 3.8},
    {"id": "c22", "ticker": "NZR", "name": "Channel Infrastructure NZ Limited",              "nzbn": "9429040663333",   "sector": "Materials",       "cap": 0.7},
    {"id": "c23", "ticker": "PFI", "name": "Property for Industry Limited",                  "nzbn": None,              "sector": "Real Estate",     "cap": 1.1},
    {"id": "c24", "ticker": "HLG", "name": "Hallenstein Glasson Holdings Limited",           "nzbn": None,              "sector": "Consumer",        "cap": 0.5},
    {"id": "c25", "ticker": "MHJ", "name": "Michael Hill International Limited",             "nzbn": None,              "sector": "Consumer",        "cap": 0.6},
    {"id": "c26", "ticker": "SCL", "name": "Scales Corporation Limited",                     "nzbn": None,              "sector": "Consumer",        "cap": 0.8},
    {"id": "c27", "ticker": "THL", "name": "Tourism Holdings Limited",                       "nzbn": None,              "sector": "Consumer",        "cap": 0.9},
    {"id": "c28", "ticker": "NHF", "name": "nib nz limited",                                 "nzbn": None,              "sector": "Financials",      "cap": 0.7},
    {"id": "c29", "ticker": "AIA", "name": "Auckland International Airport Limited",         "nzbn": None,              "sector": "Infrastructure",  "cap": 8.1},
    {"id": "c30", "ticker": "MFT", "name": "Mainfreight Limited",                            "nzbn": None,              "sector": "Industrials",     "cap": 5.5},
]

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def get_headers():
    return {
        "Ocp-Apim-Subscription-Key": API_KEY,
        "Accept": "application/json",
    }

def api_get(url, params=None, retries=MAX_RETRIES):
    """GET with retry logic and rate limiting."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=get_headers(), params=params, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            log.error(f"HTTP {resp.status_code} for {url}: {resp.text[:200]}")
            return None
        except requests.RequestException as e:
            log.warning(f"Request error (attempt {attempt+1}): {e}")
            time.sleep(1)
    return None

# ---------------------------------------------------------------------------
# NZBN lookup — find NZBN by company name
# ---------------------------------------------------------------------------

def find_nzbn(company_name):
    """
    Search the NZBN API for a company by name and return its NZBN.
    Prefers exact name matches among registered (entityStatusCode=50) entities.
    """
    params = {"search-term": company_name}
    data = api_get(BASE_URL, params=params)
    time.sleep(REQUEST_DELAY)

    if not data or not data.get("items"):
        log.warning(f"  No search results for '{company_name}'")
        return None

    # Prefer exact name match (case-insensitive) among registered entities
    for item in data["items"]:
        if item.get("entityStatusCode") != "50":
            continue
        if item.get("entityName", "").upper() == company_name.upper():
            return item["nzbn"]

    # Fall back to first registered result
    for item in data["items"]:
        if item.get("entityStatusCode") == "50":
            log.warning(
                f"  No exact match for '{company_name}' — "
                f"using '{item['entityName']}' ({item['nzbn']})"
            )
            return item["nzbn"]

    log.warning(f"  No registered entity found for '{company_name}'")
    return None

# ---------------------------------------------------------------------------
# Fetch directors for a single company
# ---------------------------------------------------------------------------

def fetch_directors(company):
    """
    Fetch current directors for a company from the NZBN API.
    Returns a list of dicts: {name, appointment_date}

    API structure confirmed from live response:
      entity["roles"] -> list of role objects
        role["roleType"]   -> "Director" (filter on this)
        role["roleStatus"] -> "ACTIVE" | "INACTIVE" (only keep ACTIVE)
        role["rolePerson"]["firstName"]   -> str
        role["rolePerson"]["middleNames"] -> str | null
        role["rolePerson"]["lastName"]    -> str
        role["startDate"]  -> ISO datetime string
    """
    # Step 1: resolve NZBN
    nzbn = company.get("nzbn")
    if not nzbn:
        log.info(f"  Searching NZBN for {company['ticker']} — {company['name']}")
        nzbn = find_nzbn(company["name"])
        if nzbn:
            # Cache it back so we only search once per run
            company["nzbn"] = nzbn
    if not nzbn:
        log.warning(f"  Could not find NZBN for {company['ticker']} — skipping")
        return []

    # Step 2: fetch full entity record
    url = f"{BASE_URL}/{nzbn}"
    data = api_get(url)
    time.sleep(REQUEST_DELAY)

    if not data:
        log.warning(f"  No data returned for {company['ticker']} (NZBN {nzbn})")
        return []

    directors = []
    for role in data.get("roles", []):
        # Only current directors
        if role.get("roleType") != "Director":
            continue
        if role.get("roleStatus") != "ACTIVE":
            continue

        person = role.get("rolePerson") or {}
        first  = (person.get("firstName")   or "").strip()
        middle = (person.get("middleNames") or "").strip()
        last   = (person.get("lastName")    or "").strip()

        parts = [p for p in [first, middle, last] if p]
        full_name = " ".join(parts).strip()

        if not full_name:
            continue

        directors.append({
            "name": full_name,
            "appointment_date": role.get("startDate", ""),
        })

    log.info(f"  {company['ticker']}: {len(directors)} active director(s)")
    return directors

# ---------------------------------------------------------------------------
# Name normalisation — fuzzy deduplication
# ---------------------------------------------------------------------------

def normalise_names(raw_name_set):
    """
    Given a set of raw director name strings, return a mapping of
    raw_name -> canonical_name using fuzzy matching.

    Directors who appear across multiple companies may have minor
    inconsistencies (e.g. middle initial present/absent). This clusters
    near-identical names under a single canonical form.
    """
    names = sorted(raw_name_set)
    canonical = {}   # raw_name -> canonical_name
    clusters = {}    # canonical_name -> list of raw names

    for name in names:
        if name in canonical:
            continue

        # Find best match among already-established canonical names
        if clusters:
            match, score = fuzz_process.extractOne(name, clusters.keys())
            if score >= FUZZY_THRESHOLD:
                # Merge — keep the shorter/simpler name as canonical
                canonical[name] = match
                clusters[match].append(name)
                log.debug(f"Merged '{name}' → '{match}' (score {score})")
                continue

        # No match — becomes a new canonical name
        canonical[name] = name
        clusters[name] = [name]

    return canonical

# ---------------------------------------------------------------------------
# Build graph data structure
# ---------------------------------------------------------------------------

def build_graph(company_directors):
    """
    company_directors: dict of company_id -> list of canonical director names

    Returns:
        nodes: list of node dicts (company + director)
        links: list of edge dicts
    """
    # Collect all unique director names
    all_names = set()
    for dirs in company_directors.values():
        all_names.update(dirs)

    # Assign stable IDs to directors
    dir_list = sorted(all_names)
    dir_id_map = {name: f"d{str(i+1).zfill(3)}" for i, name in enumerate(dir_list)}

    # Count board seats per director
    board_counts = defaultdict(int)
    for dirs in company_directors.values():
        for name in dirs:
            board_counts[name] += 1

    # Build company nodes
    co_nodes = []
    for co in NZX_TOP_30:
        co_nodes.append({
            "id": co["id"],
            "type": "company",
            "ticker": co["ticker"],
            "name": co["name"],
            "sector": co["sector"],
            "cap": co["cap"],
        })

    # Build director nodes
    dir_nodes = []
    for name in dir_list:
        dir_nodes.append({
            "id": dir_id_map[name],
            "type": "director",
            "name": name,
            "boardCount": board_counts[name],
        })

    # Build edges
    links = []
    for co in NZX_TOP_30:
        for dir_name in company_directors.get(co["id"], []):
            links.append({
                "source": dir_id_map[dir_name],
                "target": co["id"],
            })

    return co_nodes + dir_nodes, links

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=== BoardMap NZ Pipeline ===")
    log.info(f"Fetching directors for {len(NZX_TOP_30)} companies…")

    # Step 1: Fetch raw directors per company
    raw_by_company = {}    # company_id -> list of raw name strings
    all_raw_names = set()

    for co in NZX_TOP_30:
        log.info(f"Fetching {co['ticker']} — {co['name']}")
        directors = fetch_directors(co)
        names = [d["name"] for d in directors]
        raw_by_company[co["id"]] = names
        all_raw_names.update(names)

    log.info(f"\nRaw director names collected: {len(all_raw_names)}")

    # Step 2: Fuzzy deduplication
    log.info("Running fuzzy name normalisation…")
    canonical_map = normalise_names(all_raw_names)

    # Apply canonical names
    canonical_by_company = {}
    for co_id, names in raw_by_company.items():
        canonical_by_company[co_id] = list({canonical_map[n] for n in names})

    # Report merges
    merges = [(raw, canon) for raw, canon in canonical_map.items() if raw != canon]
    if merges:
        log.info(f"Name merges applied ({len(merges)}):")
        for raw, canon in merges:
            log.info(f"  '{raw}' → '{canon}'")
    else:
        log.info("No name merges required.")

    # Step 3: Build graph
    log.info("\nBuilding graph…")
    nodes, links = build_graph(canonical_by_company)

    cos  = [n for n in nodes if n["type"] == "company"]
    dirs = [n for n in nodes if n["type"] == "director"]
    multi = [d for d in dirs if d["boardCount"] >= 2]

    log.info(f"  Companies:              {len(cos)}")
    log.info(f"  Unique directors:       {len(dirs)}")
    log.info(f"  Edges:                  {len(links)}")
    log.info(f"  Directors on 2+ boards: {len(multi)}")

    if multi:
        top = max(dirs, key=lambda d: d["boardCount"])
        log.info(f"  Most connected: {top['name']} ({top['boardCount']} boards)")

    # Step 4: Write output
    output = {
        "meta": {
            "refreshed": str(date.today()),
            "companies": len(cos),
            "directors": len(dirs),
            "edges": len(links),
        },
        "nodes": nodes,
        "links": links,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    log.info(f"\nWritten to {OUTPUT_FILE} — {len(nodes)} nodes, {len(links)} edges.")
    log.info("Done. Commit graph.json and push to Vercel to redeploy.")

if __name__ == "__main__":
    main()

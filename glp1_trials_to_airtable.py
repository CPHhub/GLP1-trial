"""
glp1_trials_to_airtable.py
---------------------------
Pulls GLP-1 / incretin clinical trials from ClinicalTrials.gov v2 API
and pushes them into Airtable via direct REST API (no extra libraries).

Fields pushed: NCTId, DrugName, BriefTitle, OfficialTitle, Phase,
               OverallStatus, Conditions, StartDate, PrimaryCompletionDate,
               Enrollment, LeadSponsor, InterventionNames, LastUpdated

Run locally:
  pip install requests
  export AIRTABLE_API_KEY="patXXXXXX"
  export AIRTABLE_BASE_ID="appXXXXXX"
  python glp1_trials_to_airtable.py
"""

import os
import time
import requests

# ---------------------------------------------------------------------------
# 1. DRUG DICTIONARY — add / remove drugs here anytime
# ---------------------------------------------------------------------------
DRUG_TERMS = {
    "Semaglutide":  ["semaglutide", "ozempic", "wegovy", "rybelsus"],
    "Liraglutide":  ["liraglutide", "victoza", "saxenda"],
    "Dulaglutide":  ["dulaglutide", "trulicity"],
    "Exenatide":    ["exenatide", "byetta", "bydureon"],
    "Tirzepatide":  ["tirzepatide", "mounjaro", "zepbound", "ly3298176"],
    "Retatrutide":  ["retatrutide", "ly3437943"],
    "Orforglipron": ["orforglipron", "ly3502970"],
    "Danuglipron":  ["danuglipron", "pf-06882961"],
    "Cotadutide":   ["cotadutide", "medi0382"],
    "Survodutide":  ["survodutide", "bi456906"],
}

# ---------------------------------------------------------------------------
# 2. CONFIG — from environment variables / GitHub Secrets
# ---------------------------------------------------------------------------
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE   = os.environ.get("AIRTABLE_TABLE", "GLP1_Trials")

CT_BASE    = "https://clinicaltrials.gov/api/v2/studies"
AT_BASE    = "https://api.airtable.com/v0"
PAGE_SIZE  = 1000
MAX_PAGES  = 50
BATCH_SIZE = 10

# ---------------------------------------------------------------------------
# 3. BUILD CLINICALTRIALS QUERY
# ---------------------------------------------------------------------------
def build_query():
    def _q(s):
        return f'"{s}"' if (" " in s or "-" in s) else s
    all_syns = sorted({syn for syns in DRUG_TERMS.values() for syn in syns})
    return " OR ".join(_q(s) for s in all_syns)

# ---------------------------------------------------------------------------
# 4. FETCH ALL STUDIES
# ---------------------------------------------------------------------------
def fetch_studies(query_term):
    studies, page_token = [], None
    params = {"query.term": query_term, "pageSize": PAGE_SIZE, "format": "json"}

    for page_num in range(1, MAX_PAGES + 1):
        if page_token:
            params["pageToken"] = page_token
        else:
            params.pop("pageToken", None)

        resp = requests.get(CT_BASE, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("studies", [])
        studies.extend(batch)
        print(f"  Page {page_num}: +{len(batch)} (total: {len(studies)})")

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return studies

# ---------------------------------------------------------------------------
# 5. DETECT DRUG NAME
# ---------------------------------------------------------------------------
def detect_drug(study):
    ps   = study.get("protocolSection", {}) or {}
    aim  = ps.get("armsInterventionsModule", {}) or {}
    invs = aim.get("interventions") or []
    idm  = ps.get("identificationModule", {}) or {}

    full = " ".join(
        (inv.get("name", "") + " " + inv.get("description", "")).lower()
        for inv in invs if isinstance(inv, dict)
    ) + " " + (idm.get("briefTitle") or "").lower()

    matched = [name for name, syns in DRUG_TERMS.items() if any(s.lower() in full for s in syns)]
    return ", ".join(matched) if matched else "Unknown"

# ---------------------------------------------------------------------------
# 6. FLATTEN STUDY TO DICT
# ---------------------------------------------------------------------------
def flatten(study):
    ps   = study.get("protocolSection", {}) or {}
    idm  = ps.get("identificationModule", {}) or {}
    sm   = ps.get("statusModule", {}) or {}
    des  = ps.get("designModule", {}) or {}
    cm   = ps.get("conditionsModule", {}) or {}
    aim  = ps.get("armsInterventionsModule", {}) or {}
    spon = ps.get("sponsorsCollaboratorsModule", {}) or {}

    phase_raw = des.get("phases") or des.get("phase") or ""
    phase = "; ".join(phase_raw) if isinstance(phase_raw, list) else str(phase_raw)

    conditions = cm.get("conditions") or []
    conditions_str = "; ".join(c.strip() for c in conditions if isinstance(c, str) and c.strip())

    invs = aim.get("interventions") or []
    inv_names = sorted({inv["name"] for inv in invs if isinstance(inv, dict) and inv.get("name")})

    enroll = des.get("enrollmentInfo", {}).get("count") or ""

    def _date(struct_key, flat_key):
        return (sm.get(struct_key) or {}).get("date") or sm.get(flat_key) or ""

    return {
        "NCTId":                 idm.get("nctId") or "",
        "DrugName":              detect_drug(study),
        "BriefTitle":            idm.get("briefTitle") or "",
        "OfficialTitle":         idm.get("officialTitle") or "",
        "Phase":                 phase,
        "OverallStatus":         sm.get("overallStatus") or "",
        "Conditions":            conditions_str,
        "StartDate":             _date("startDateStruct", "startDate"),
        "PrimaryCompletionDate": _date("primaryCompletionDateStruct", "primaryCompletionDate"),
        "Enrollment":            str(enroll) if enroll else "",
        "LeadSponsor":           (spon.get("leadSponsor") or {}).get("name") or "",
        "InterventionNames":     "; ".join(inv_names),
        "LastUpdated":           _date("lastUpdatePostDateStruct", "lastUpdatePostDate"),
    }

# ---------------------------------------------------------------------------
# 7. PUSH TO AIRTABLE (direct REST — zero extra dependencies)
# ---------------------------------------------------------------------------
def push_to_airtable(records):
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        print("WARNING: Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID")
        return

    url     = f"{AT_BASE}/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type":  "application/json",
    }

    total  = len(records)
    pushed = 0
    errors = 0

    for i in range(0, total, BATCH_SIZE):
        batch   = records[i : i + BATCH_SIZE]
        payload = {"records": [{"fields": r} for r in batch], "typecast": True}

        for attempt in range(3):
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=30)

                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    print(f"\n  Rate limited - waiting {wait}s...")
                    time.sleep(wait)
                    continue

                if resp.status_code == 422:
                    print(f"\n  ERROR 422 on batch {i}: {resp.text[:400]}")
                    errors += len(batch)
                    break

                if resp.status_code == 404:
                    print(f"\n  ERROR 404 - table not found. Check AIRTABLE_TABLE secret matches your table name exactly.")
                    print(f"  Current value: '{AIRTABLE_TABLE}'")
                    print(f"  URL attempted: {url}")
                    errors += len(batch)
                    break

                resp.raise_for_status()
                pushed += len(batch)
                print(f"  Pushed {pushed}/{total}...", end="\r")
                break

            except Exception as e:
                if attempt == 2:
                    errors += len(batch)
                    print(f"\n  Batch {i} failed: {e}")
                else:
                    time.sleep(5)

        time.sleep(0.22)

    print(f"\nDone: {pushed} pushed, {errors} errors.")

# ---------------------------------------------------------------------------
# 8. MAIN
# ---------------------------------------------------------------------------
def main():
    print("=== GLP-1 ClinicalTrials to Airtable ===")
    print(f"  Target table: '{AIRTABLE_TABLE}'")
    print(f"  Base ID: '{AIRTABLE_BASE_ID[:8]}...' (truncated)")

    print("\n[1/3] Building query...")
    query = build_query()
    print(f"  {len(DRUG_TERMS)} drugs, {sum(len(v) for v in DRUG_TERMS.values())} synonyms")

    print("\n[2/3] Fetching from ClinicalTrials.gov...")
    raw = fetch_studies(query)
    print(f"  Total fetched: {len(raw)}")

    print("\n[3/3] Pushing to Airtable...")
    records = [flatten(s) for s in raw]

    for r in records[:3]:
        print(f"  -> {r['NCTId']} | {r['DrugName']} | {r['Phase']} | {r['OverallStatus']}")

    push_to_airtable(records)
    print(f"\nComplete. {len(records)} records processed.")

if __name__ == "__main__":
    main()

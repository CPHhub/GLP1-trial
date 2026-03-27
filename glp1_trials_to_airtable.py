"""
glp1_trials_to_airtable.py
---------------------------
Pulls GLP-1 / incretin clinical trials from ClinicalTrials.gov v2 API
and upserts them into an Airtable base.

Fields pushed to Airtable:
  NCTId, DrugName, BriefTitle, OfficialTitle, Phase,
  OverallStatus, Conditions, StartDate, PrimaryCompletionDate,
  Enrollment, LeadSponsor, InterventionNames, LastUpdated

Run locally:
  pip install requests pyairtable
  export AIRTABLE_API_KEY="patXXXXXX"
  export AIRTABLE_BASE_ID="appXXXXXX"
  python glp1_trials_to_airtable.py

Run via GitHub Actions: see .github/workflows/glp1_sync.yml
"""

import os
import sys
import time
import requests
from pyairtable import Api

# ---------------------------------------------------------------------------
# 1. DRUG DICTIONARY  — add / remove drugs here anytime
# ---------------------------------------------------------------------------
DRUG_TERMS = {
    "Semaglutide":    ["semaglutide", "ozempic", "wegovy", "rybelsus"],
    "Liraglutide":    ["liraglutide", "victoza", "saxenda"],
    "Dulaglutide":    ["dulaglutide", "trulicity"],
    "Exenatide":      ["exenatide", "byetta", "bydureon"],
    "Tirzepatide":    ["tirzepatide", "mounjaro", "zepbound", "ly3298176"],
    "Retatrutide":    ["retatrutide", "ly3437943"],
    "Orforglipron":   ["orforglipron", "ly3502970"],
    "Danuglipron":    ["danuglipron", "pf-06882961"],
    "Cotadutide":     ["cotadutide", "medi0382"],
    "Survodutide":    ["survodutide", "bi456906"],
}

# ---------------------------------------------------------------------------
# 2. CONFIG  — pulled from environment variables (safe for GitHub Secrets)
# ---------------------------------------------------------------------------
AIRTABLE_API_KEY   = os.environ.get("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID   = os.environ.get("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE     = os.environ.get("AIRTABLE_TABLE", "GLP1_Trials")

CT_BASE            = "https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE          = 1000
MAX_PAGES          = 50        # safety cap (~50 000 studies max)
AIRTABLE_BATCH     = 10        # Airtable max records per request

# ---------------------------------------------------------------------------
# 3. BUILD QUERY
# ---------------------------------------------------------------------------
def _quote(s):
    return f'"{s}"' if (" " in s or "-" in s) else s

def build_query():
    all_syns = sorted({syn for syns in DRUG_TERMS.values() for syn in syns})
    return " OR ".join(_quote(s) for s in all_syns)

# ---------------------------------------------------------------------------
# 4. FETCH ALL STUDIES FROM CLINICALTRIALS.GOV
# ---------------------------------------------------------------------------
def fetch_studies(query_term: str) -> list[dict]:
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
        print(f"  Page {page_num}: +{len(batch)} studies (total so far: {len(studies)})")

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return studies

# ---------------------------------------------------------------------------
# 5. DETECT WHICH DRUG(S) MATCH A STUDY
# ---------------------------------------------------------------------------
def detect_drug(study: dict) -> str:
    """Return comma-separated drug names found in this study's intervention names."""
    ps   = study.get("protocolSection", {}) or {}
    aim  = ps.get("armsInterventionsModule", {}) or {}
    invs = aim.get("interventions") or []

    inv_text = " ".join(
        (inv.get("name", "") + " " + inv.get("description", "")).lower()
        for inv in invs if isinstance(inv, dict)
    )
    # also check title
    idm   = ps.get("identificationModule", {}) or {}
    title = (idm.get("briefTitle") or "").lower()
    full  = inv_text + " " + title

    matched = []
    for drug_name, synonyms in DRUG_TERMS.items():
        if any(syn.lower() in full for syn in synonyms):
            matched.append(drug_name)

    return ", ".join(matched) if matched else "Unknown"

# ---------------------------------------------------------------------------
# 6. FLATTEN A STUDY INTO THE FIELDS WE CARE ABOUT
# ---------------------------------------------------------------------------
def flatten(study: dict) -> dict:
    ps   = study.get("protocolSection", {}) or {}
    idm  = ps.get("identificationModule", {}) or {}
    sm   = ps.get("statusModule", {}) or {}
    des  = ps.get("designModule", {}) or {}
    cm   = ps.get("conditionsModule", {}) or {}
    aim  = ps.get("armsInterventionsModule", {}) or {}
    spon = ps.get("sponsorsCollaboratorsModule", {}) or {}

    # Phase: can be list or string
    phase_raw = des.get("phases") or des.get("phase") or ""
    if isinstance(phase_raw, list):
        phase = "; ".join(phase_raw)
    else:
        phase = str(phase_raw)

    # Conditions
    conditions = cm.get("conditions") or []
    conditions_str = "; ".join(c.strip() for c in conditions if isinstance(c, str) and c.strip())

    # Intervention names
    invs = aim.get("interventions") or []
    inv_names = sorted({inv["name"] for inv in invs if isinstance(inv, dict) and inv.get("name")})

    # Enrollment
    enroll = des.get("enrollmentInfo", {}).get("count") or ""

    # Lead sponsor
    lead_sponsor = (spon.get("leadSponsor") or {}).get("name") or ""

    # Dates
    def _date(struct_key, flat_key):
        return (sm.get(struct_key) or {}).get("date") or sm.get(flat_key) or ""

    return {
        "NCTId":               idm.get("nctId") or "",
        "DrugName":            detect_drug(study),
        "BriefTitle":          idm.get("briefTitle") or "",
        "OfficialTitle":       idm.get("officialTitle") or "",
        "Phase":               phase,
        "OverallStatus":       sm.get("overallStatus") or "",
        "Conditions":          conditions_str,
        "StartDate":           _date("startDateStruct", "startDate"),
        "PrimaryCompletionDate": _date("primaryCompletionDateStruct", "primaryCompletionDate"),
        "Enrollment":          str(enroll) if enroll else "",
        "LeadSponsor":         lead_sponsor,
        "InterventionNames":   "; ".join(inv_names),
        "LastUpdated":         _date("lastUpdatePostDateStruct", "lastUpdatePostDate"),
    }

# ---------------------------------------------------------------------------
# 7. UPSERT TO AIRTABLE
# ---------------------------------------------------------------------------
def upsert_to_airtable(records: list[dict]):
    """
    Upsert records into Airtable using NCTId as the unique key.
    pyairtable batch_upsert handles create + update automatically.
    """
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        print("⚠️  AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set — skipping Airtable push.")
        print(f"    Would have pushed {len(records)} records.")
        return

    api   = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE)

    total   = len(records)
    pushed  = 0
    errors  = 0

    for i in range(0, total, AIRTABLE_BATCH):
        batch = records[i : i + AIRTABLE_BATCH]
        try:
            table.batch_upsert(
                batch,
                key_fields=["NCTId"],   # match on NCTId; create if missing, update if exists
            )
            pushed += len(batch)
            print(f"  Upserted {pushed}/{total}...", end="\r")
        except Exception as e:
            errors += len(batch)
            print(f"\n  ⚠️  Batch {i}–{i+AIRTABLE_BATCH} failed: {e}")

        time.sleep(0.22)   # stay under Airtable rate limit (5 req/s)

    print(f"\n✅ Airtable upsert complete: {pushed} ok, {errors} errors.")

# ---------------------------------------------------------------------------
# 8. MAIN
# ---------------------------------------------------------------------------
def main():
    print("=== GLP-1 ClinicalTrials → Airtable MVP ===")

    print("\n[1/3] Building query...")
    query = build_query()
    print(f"  Query terms: {len(DRUG_TERMS)} drugs, {sum(len(v) for v in DRUG_TERMS.values())} synonyms")

    print("\n[2/3] Fetching studies from ClinicalTrials.gov...")
    raw_studies = fetch_studies(query)
    print(f"  Total studies fetched: {len(raw_studies)}")

    print("\n[3/3] Flattening + pushing to Airtable...")
    records = [flatten(s) for s in raw_studies]

    # Quick sanity print — first 3 records
    for r in records[:3]:
        print(f"  → {r['NCTId']} | {r['DrugName'][:30]} | {r['Phase']} | {r['OverallStatus']}")

    upsert_to_airtable(records)
    print(f"\nDone. {len(records)} records processed.")

if __name__ == "__main__":
    main()

import os
import json
import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Request

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# -------------------------
# CONFIG
# -------------------------
DB_PATH = os.getenv("DB_PATH", "leads.db")

# Email destination (where digest is sent)
DEFAULT_OWNER_EMAIL = os.getenv("DEFAULT_OWNER_EMAIL", "demontez@lassiterllc.services")

# Gmail SMTP sender (must be a real Gmail you own)
GMAIL_SMTP_USER = os.getenv("GMAIL_SMTP_USER", "")
GMAIL_SMTP_APP_PASSWORD = os.getenv("GMAIL_SMTP_APP_PASSWORD", "")
GMAIL_FROM_NAME = os.getenv("GMAIL_FROM_NAME", "Lassiter Lead Engine")

# SerpAPI key (search provider)
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")
SERPAPI_ENGINE = os.getenv("SERPAPI_ENGINE", "google")

MAX_RESULTS_PER_QUERY = int(os.getenv("MAX_RESULTS_PER_QUERY", "10"))
DEFAULT_MIN_SCORE = int(os.getenv("DEFAULT_MIN_SCORE", "70"))

CITY_SEEDS = [
    "Phoenix AZ", "Dallas TX", "Houston TX", "Atlanta GA", "Charlotte NC",
    "Denver CO", "Las Vegas NV", "Orlando FL", "Tampa FL", "Nashville TN",
    "Columbus OH", "Indianapolis IN", "Kansas City MO", "St. Louis MO",
    "Chicago IL", "Minneapolis MN", "Seattle WA", "Portland OR",
    "San Diego CA", "Los Angeles CA", "San Jose CA", "San Francisco CA",
    "New York NY", "Philadelphia PA", "Boston MA", "Washington DC",
    "Detroit MI", "Cleveland OH", "Pittsburgh PA", "Baltimore MD",
]

app = FastAPI()

# -------------------------
# DB
# -------------------------
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS criteria (
        id INTEGER PRIMARY KEY,
        criteria_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    """)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        niche TEXT NOT NULL,
        name TEXT,
        website TEXT,
        email TEXT,
        phone TEXT,
        contact_url TEXT,
        location TEXT,
        notes TEXT,
        score INTEGER,
        run_date TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(niche, website, email, run_date)
      )
    """)
    conn.commit()
    conn.close()

init_db()

def set_criteria(criteria: Dict[str, Any]) -> None:
    conn = db_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute("DELETE FROM criteria")
    cur.execute(
        "INSERT INTO criteria (id, criteria_json, updated_at) VALUES (1, ?, ?)",
        (json.dumps(criteria), now),
    )
    conn.commit()
    conn.close()

def get_criteria() -> Dict[str, Any]:
    conn = db_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT criteria_json FROM criteria WHERE id = 1").fetchone()
    conn.close()
    if not row:
        return {
            "ownerEmail": DEFAULT_OWNER_EMAIL,
            "niches": ["hvac", "dispensary", "gym"],
            "dailyQuotas": {"hvac": 10, "dispensary": 5, "gym": 5},
            "geo": "United States (nationwide)",
            "mustHave": [],
            "avoid": [],
            "prioritySignals": [],
            "minScore": DEFAULT_MIN_SCORE,
        }
    return json.loads(row["criteria_json"])

def save_leads(leads: List[Dict[str, Any]], run_date: str) -> int:
    conn = db_conn()
    cur = conn.cursor()
    added = 0
    now = datetime.utcnow().isoformat()
    for lead in leads:
        cur.execute("""
          INSERT OR IGNORE INTO leads
          (niche, name, website, email, phone, contact_url, location, notes, score, run_date, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead.get("niche"),
            lead.get("name"),
            lead.get("website"),
            lead.get("email"),
            lead.get("phone"),
            lead.get("contact_url"),
            lead.get("location"),
            lead.get("notes"),
            int(lead.get("score") or 0),
            run_date,
            now
        ))
        if cur.rowcount == 1:
            added += 1
    conn.commit()
    conn.close()
    return added

def get_todays_leads(run_date: str, limit: int = 100, min_score: int = 0) -> List[Dict[str, Any]]:
    conn = db_conn()
    cur = conn.cursor()
    rows = cur.execute("""
      SELECT niche, name, website, email, phone, contact_url, location, notes, score, created_at
      FROM leads
      WHERE run_date = ? AND score >= ?
      ORDER BY score DESC, created_at DESC
      LIMIT ?
    """, (run_date, min_score, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# -------------------------
# UTIL
# -------------------------
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(r"(\+?1[\s\-\.]?)?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}")

def extract_email(text: str) -> Optional[str]:
    if not text:
        return None
    m = EMAIL_RE.search(text)
    return m.group(0) if m else None

def extract_phone(text: str) -> Optional[str]:
    if not text:
        return None
    m = PHONE_RE.search(text)
    return m.group(0) if m else None

def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"\?.*$", "", url)
    return url.lower()

def today_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

# -------------------------
# SEARCH (SerpAPI)
# -------------------------
def serpapi_search(query: str, num: int = 10) -> List[Dict[str, Any]]:
    if not SERPAPI_KEY:
        return []
    url = "https://serpapi.com/search.json"
    params = {
        "engine": SERPAPI_ENGINE,
        "q": query,
        "num": min(num, MAX_RESULTS_PER_QUERY),
        "api_key": SERPAPI_KEY,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    organic = data.get("organic_results", []) or []
    results = []
    for item in organic:
        results.append({
            "name": (item.get("title") or "")[:140],
            "website": item.get("link") or "",
            "notes": (item.get("snippet") or "")[:500],
        })
    return results

def build_queries(niche: str, city: str) -> List[str]:
    if niche == "hvac":
        return [
            f"HVAC contractor {city} contact",
            f"air conditioning repair {city} contact",
            f"heating and cooling company {city} contact",
            f"HVAC company {city} services contact",
        ]
    if niche == "dispensary":
        return [
            f"cannabis dispensary {city} contact",
            f"dispensary {city} contact us",
            f"marijuana dispensary {city} website contact",
        ]
    if niche == "gym":
        return [
            f"gym {city} contact",
            f"fitness center {city} contact us",
            f"strength and conditioning gym {city} contact",
        ]
    return [f"{niche} {city} contact"]

# -------------------------
# LIGHT ENRICHMENT
# -------------------------
def try_fetch_contact_page(website: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not website:
        return None, None, None
    try:
        r = requests.get(website, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code >= 400:
            return None, None, None
        html = r.text[:200000]
        email = extract_email(html)
        phone = extract_phone(html)
        m = re.search(r'href=["\']([^"\']*contact[^"\']*)["\']', html, re.IGNORECASE)
        contact_url = None
        if m:
            href = m.group(1)
            if href.startswith("http"):
                contact_url = href
            elif href.startswith("/"):
                contact_url = website.rstrip("/") + href
        return contact_url, email, phone
    except Exception:
        return None, None, None

# -------------------------
# SCORING
# -------------------------
def score_lead(lead: Dict[str, Any], criteria: Dict[str, Any]) -> int:
    must_have = criteria.get("mustHave") or []
    avoid = criteria.get("avoid") or []
    priority = criteria.get("prioritySignals") or []

    text = " ".join([
        lead.get("name") or "",
        lead.get("website") or "",
        lead.get("notes") or "",
        lead.get("contact_url") or "",
    ]).lower()

    score = 0
    if lead.get("website"):
        score += 25
    if lead.get("email"):
        score += 25
    if lead.get("phone"):
        score += 15
    if lead.get("contact_url"):
        score += 15

    for kw in must_have:
        if kw and kw.lower() in text:
            score += 15

    for kw in priority:
        if kw and kw.lower() in text:
            score += 10

    for kw in avoid:
        if kw and kw.lower() in text:
            score -= 60

    bad_domains = ["yelp.", "angi.", "homeadvisor.", "thumbtack.", "bbb.", "yellowpages.", "mapquest.", "facebook.com"]
    if any(b in (lead.get("website") or "").lower() for b in bad_domains):
        score -= 70

    return score

def qualifies(lead: Dict[str, Any], min_score: int) -> bool:
    has_contact = bool(lead.get("email") or lead.get("phone") or lead.get("contact_url"))
    return has_contact and int(lead.get("score") or 0) >= min_score

# -------------------------
# DAILY RUN
# -------------------------
def run_daily(criteria: Dict[str, Any], run_date: str) -> Dict[str, Any]:
    niches: List[str] = criteria.get("niches") or ["hvac", "dispensary", "gym"]
    quotas: Dict[str, int] = criteria.get("dailyQuotas") or {"hvac": 10, "dispensary": 5, "gym": 5}
    min_score = int(criteria.get("minScore") or DEFAULT_MIN_SCORE)

    day_index = int(datetime.utcnow().strftime("%j"))
    rotated = CITY_SEEDS[day_index % len(CITY_SEEDS):] + CITY_SEEDS[:day_index % len(CITY_SEEDS)]

    results_by_niche: Dict[str, List[Dict[str, Any]]] = {n: [] for n in niches}
    seen_urls = set()

    for niche in niches:
        target = int(quotas.get(niche, 0))
        if target <= 0:
            continue

        for city in rotated:
            if len(results_by_niche[niche]) >= target:
                break

            for q in build_queries(niche, city):
                if len(results_by_niche[niche]) >= target:
                    break

                for item in serpapi_search(q, num=MAX_RESULTS_PER_QUERY):
                    website = item.get("website") or ""
                    norm = normalize_url(website)
                    if not norm or norm in seen_urls:
                        continue

                    lead = {
                        "niche": niche,
                        "name": item.get("name"),
                        "website": website,
                        "location": city,
                        "notes": item.get("notes"),
                        "email": extract_email(item.get("notes") or ""),
                        "phone": extract_phone(item.get("notes") or ""),
                        "contact_url": None,
                    }

                    if not (lead["email"] or lead["phone"]):
                        contact_url, email, phone = try_fetch_contact_page(website)
                        lead["contact_url"] = contact_url
                        if email:
                            lead["email"] = email
                        if phone:
                            lead["phone"] = phone

                    lead["score"] = score_lead(lead, criteria)

                    if qualifies(lead, min_score):
                        results_by_niche[niche].append(lead)
                        seen_urls.add(norm)
                        if len(results_by_niche[niche]) >= target:
                            break

    all_leads = []
    for niche in niches:
        all_leads.extend(results_by_niche.get(niche, []))

    added = save_leads(all_leads, run_date)

    return {
        "runDate": run_date,
        "added": added,
        "counts": {n: len(results_by_niche.get(n, [])) for n in niches},
        "leadsByNiche": results_by_niche,
    }

# -------------------------
# EMAIL (Gmail SMTP)
# -------------------------
def send_gmail(to_email: str, subject: str, html: str) -> None:
    if not GMAIL_SMTP_USER or not GMAIL_SMTP_APP_PASSWORD:
        raise RuntimeError("Gmail SMTP not configured. Set GMAIL_SMTP_USER and GMAIL_SMTP_APP_PASSWORD.")

    msg = MIMEMultipart("alternative")
    msg["From"] = f"{GMAIL_FROM_NAME} <{GMAIL_SMTP_USER}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_SMTP_USER, GMAIL_SMTP_APP_PASSWORD)
        server.sendmail(GMAIL_SMTP_USER, [to_email], msg.as_string())

def leads_to_html(run_date: str, leads: List[Dict[str, Any]]) -> str:
    def esc(s: Any) -> str:
        return (str(s) if s is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows = []
    for l in leads:
        rows.append(f"""
        <tr>
          <td>{esc(l.get("niche"))}</td>
          <td>{esc(l.get("score"))}</td>
          <td>{esc(l.get("name"))}</td>
          <td><a href="{esc(l.get("website"))}">{esc(l.get("website"))}</a></td>
          <td>{esc(l.get("email") or "")}</td>
          <td>{esc(l.get("phone") or "")}</td>
          <td>{("<a href='%s'>contact</a>" % esc(l.get("contact_url"))) if l.get("contact_url") else ""}</td>
          <td>{esc(l.get("location") or "")}</td>
        </tr>
        """)

    return f"""
    <h2>Daily Qualified Leads — {esc(run_date)}</h2>
    <p>Split target: 10 HVAC / 5 Dispensary / 5 Gym (nationwide). Minimum score applied.</p>
    <table border="1" cellpadding="6" cellspacing="0">
      <thead>
        <tr>
          <th>Niche</th><th>Score</th><th>Name</th><th>Website</th><th>Email</th><th>Phone</th><th>Contact</th><th>City Seed</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
    """

# -------------------------
# VAPI WEBHOOK
# -------------------------
@app.post("/vapi/webhook")
async def vapi_webhook(req: Request):
    payload = await req.json()
    msg = (payload or {}).get("message") or {}

    # Accept both formats
    if msg.get("type") != "tool-calls":
        return {"ok": True}

    tool_calls = (
        msg.get("toolCallList")
        or msg.get("toolCalls")
        or msg.get("tool_calls")
        or []
    )

    results = []

    for tc in tool_calls:
        # Accept multiple shapes
        tool_name = tc.get("name") or tc.get("function", {}).get("name")
        tool_call_id = tc.get("id") or tc.get("toolCallId") or tc.get("tool_call_id")
        params = tc.get("parameters") or tc.get("args") or tc.get("function", {}).get("arguments") or {}

        # If params is a JSON string, parse it
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except Exception:
                params = {}

        try:
            if tool_name == "setLeadCriteria":
                crit = get_criteria()
                crit.update(params)

                crit["ownerEmail"] = crit.get("ownerEmail") or DEFAULT_OWNER_EMAIL
                crit["niches"] = crit.get("niches") or ["hvac", "dispensary", "gym"]
                crit["dailyQuotas"] = crit.get("dailyQuotas") or {"hvac": 10, "dispensary": 5, "gym": 5}
                crit["geo"] = crit.get("geo") or "United States (nationwide)"
                crit["minScore"] = int(crit.get("minScore") or DEFAULT_MIN_SCORE)

                set_criteria(crit)

                results.append({
                    "name": tool_name,
                    "toolCallId": tool_call_id,
                    "result": json.dumps({"status": "saved", "criteria": crit})
                })

            elif tool_name == "runDailyLeadRun":
                criteria = get_criteria()
                run_date = (params.get("date") or today_utc())
                out = run_daily(criteria, run_date)
                results.append({
                    "name": tool_name,
                    "toolCallId": tool_call_id,
                    "result": json.dumps(out)
                })

            elif tool_name == "previewTodaysLeads":
                criteria = get_criteria()
                run_date = today_utc()
                limit = int(params.get("limit") or 10)
                leads = get_todays_leads(run_date, limit=limit, min_score=int(criteria.get("minScore") or DEFAULT_MIN_SCORE))
                results.append({
                    "name": tool_name,
                    "toolCallId": tool_call_id,
                    "result": json.dumps({"runDate": run_date, "count": len(leads), "leads": leads})
                })

            elif tool_name == "sendDailyLeadEmail":
                criteria = get_criteria()
                run_date = today_utc()
                owner_email = criteria.get("ownerEmail") or DEFAULT_OWNER_EMAIL
                subject = params.get("subject") or f"Daily Qualified Leads — {run_date} (HVAC Priority)"
                leads = get_todays_leads(run_date, limit=100, min_score=int(criteria.get("minScore") or DEFAULT_MIN_SCORE))
                html = leads_to_html(run_date, leads)
                send_gmail(owner_email, subject, html)
                results.append({
                    "name": tool_name,
                    "toolCallId": tool_call_id,
                    "result": json.dumps({"status": "emailed", "to": owner_email, "count": len(leads), "runDate": run_date})
                })

            else:
                results.append({
                    "name": tool_name or "UNKNOWN",
                    "toolCallId": tool_call_id,
                    "result": json.dumps({"status": "ignored", "reason": f"Unknown tool: {tool_name}"})
                })

        except Exception as e:
            results.append({
                "name": tool_name or "UNKNOWN",
                "toolCallId": tool_call_id,
                "result": json.dumps({"status": "error", "error": str(e)})
            })

    return {"results": results}

#!/usr/bin/env python3
"""
Jira → Teams PMO Status report

- Detects whether ROOT is a PMO Initiative or a Project
- Uses the provided JQL to gather all relevant items
- Formats per the user's required structure
"""
import os, sys, json
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import requests

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

def env(name: str, default: Optional[str]=None) -> Optional[str]:
    return os.getenv(name, default)

def parse_bool(v: Optional[str], default=False) -> bool:
    if v is None: return default
    return v.strip().lower() in {"1","true","yes","y","on"}

def jira_auth(email: str, token: str):
    return {"Accept":"application/json","Content-Type":"application/json"}, (email, token)

def fmt_date(v: Optional[str], tzname: str, fmt: str="%Y-%m-%d") -> str:
    if not v: return ""
    try:
        s = v.replace("+0000","+00:00")
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if tzname and ZoneInfo:
            dt = dt.astimezone(ZoneInfo(tzname))
        return dt.strftime(fmt)
    except Exception:
        return v

def fetch_fields(base: str, email: str, token: str) -> List[Dict[str,Any]]:
    url = f"{base}/rest/api/3/field"
    h,a = jira_auth(email, token)
    r = requests.get(url, headers=h, auth=a, timeout=60)
    r.raise_for_status()
    return r.json()

def detect_fields(requested: List[str], all_fields: List[Dict[str,Any]]) -> List[str]:
    known_ids = {f["id"] for f in all_fields}
    known_names = {(f.get("name") or "") for f in all_fields}
    out = []
    for f in requested:
        if not f: continue
        if f in known_ids or f in known_names:
            out.append(f)
        else:
            out.append(f)  # let server decide; Jira usually ignores unknowns
    return out

def jira_search(base: str, email: str, token: str, jql: str, fields: List[str], max_issues: int=1000, batch_size: int=200) -> List[Dict[str,Any]]:
    url = f"{base}/rest/api/3/search/jql"
    h,a = jira_auth(email, token)
    out = []
    start_at = 0
    while len(out) < max_issues:
        payload = {"jql": jql, "startAt": start_at, "maxResults": min(batch_size, max_issues-len(out)), "fields": fields}
        r = requests.post(url, headers=h, auth=a, data=json.dumps(payload), timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Jira error {r.status_code}: {r.text[:400]}")
        data = r.json()
        batch = data.get("issues", [])
        out.extend(batch)
        if not batch: break
        start_at += len(batch)
    return out

def field(issue: Dict[str,Any], key: str):
    f = issue.get("fields", {}) or {}
    if key in f: return f.get(key)
    if key.lower() in f: return f.get(key.lower())
    return None

def field_text(issue: Dict[str,Any], key: str) -> str:
    v = field(issue, key)
    if v is None: return ""
    if isinstance(v, dict):
        return v.get("displayName") or v.get("name") or v.get("value") or v.get("id") or ""
    if isinstance(v, list):
        parts = []
        for it in v:
            if isinstance(it, dict):
                parts.append(it.get("name") or it.get("value") or it.get("displayName") or it.get("key") or "")
            else:
                parts.append(str(it))
        return ", ".join([p for p in parts if p])
    return str(v)

def issue_type(issue: Dict[str,Any]) -> str:
    it = (issue.get("fields") or {}).get("issuetype") or {}
    return (it.get("name") or "").strip()

def issue_status_name(issue: Dict[str,Any]) -> str:
    st = (issue.get("fields") or {}).get("status") or {}
    return (st.get("name") or st.get("statusCategory",{}).get("name") or "").strip()

def issue_key(issue: Dict[str,Any]) -> str:
    return issue.get("key") or ""

def parent_key(issue: Dict[str,Any]) -> str:
    p = (issue.get("fields") or {}).get("parent")
    if isinstance(p, dict):
        return p.get("key") or ""
    return ""

def determine_root(issues: List[Dict[str,Any]], hint_key: Optional[str]) -> Optional[Dict[str,Any]]:
    by_key = {issue_key(i): i for i in issues}
    if hint_key and hint_key in by_key:
        return by_key[hint_key]
    for it in issues:
        if not parent_key(it):
            return it
    return issues[0] if issues else None

def is_open(issue: Dict[str,Any]) -> bool:
    st = issue_status_name(issue).lower()
    return st not in {"done", "closed", "resolved"}

def build_report(issues: List[Dict[str,Any]], root: Dict[str,Any], cfg: Dict[str,str], tzname: str) -> Tuple[str, str]:
    root_type = issue_type(root)
    root_key = issue_key(root)
    root_summary = field_text(root, "summary")

    weekly_field = cfg["WEEKLY_UPDATE_FIELD"]
    milestone_type = cfg["MILESTONE_TYPE_NAME"]
    milestone_target_end = cfg["MILESTONE_TARGET_END_FIELD"]
    milestone_rag = cfg["MILESTONE_RAG_FIELD"]
    init_risk_type = cfg["INITIATIVE_RISK_TYPE_NAME"]
    assumption_type = cfg["ASSUMPTION_TYPE_NAME"]
    issue_t = cfg["ISSUE_TYPE_NAME"]
    decision_t = cfg["DECISION_TYPE_NAME"]
    change_req_t = cfg["CHANGE_REQ_TYPE_NAME"]

    by_type: Dict[str, List[Dict[str,Any]]] = {}
    for it in issues:
        by_type.setdefault(issue_type(it), []).append(it)

    lines: List[str] = []

    title = f"{root_key} — {root_summary}".strip(" —")

    def bullet_or_none(items: List[str]) -> List[str]:
        return items if items else ["None"]

    def format_item(it: Dict[str,Any], extra_bits: List[str]) -> str:
        bits = [f"{issue_key(it)} — {field_text(it,'summary')}"] + [b for b in extra_bits if b]
        return f"- " + " | ".join(bits)

    # Weekly updates
    lines.append("Weekly updates:")
    weekly_lines: List[str] = []
    if root_type.lower() == cfg["INITIATIVE_TYPE_NAME"].lower():
        projects = [it for it in by_type.get(cfg["PROJECT_TYPE_NAME"], []) if is_open(it)]
        for p in projects:
            wu = field_text(p, weekly_field)
            if wu:
                weekly_lines.append(f"- **{issue_key(p)} — {field_text(p,'summary')}**: {wu}")
        if not weekly_lines:
            weekly_lines = ["- None"]
    else:
        wu = field_text(root, weekly_field)
        weekly_lines = [f"- {wu}"] if wu else ["- None"]
    lines.extend(weekly_lines)
    lines.append("")

    # Upcoming Milestones
    lines.append("Upcoming Milestones:")
    milestones = [it for it in by_type.get(milestone_type, []) if is_open(it)]
    milestone_lines: List[str] = []
    for m in milestones:
        key = issue_key(m)
        summ = field_text(m, "summary")
        tgt = fmt_date(field_text(m, milestone_target_end), tzname)
        rag = field_text(m, milestone_rag)
        milestone_lines.append(f"- {key} — {summ} | Target end: {tgt or '-'} | RAG: {rag or '-'}")
    lines.extend(bullet_or_none(milestone_lines))
    lines.append("")

    def section(header: str, type_name: str, extra_cols: List[str]) -> None:
        lines.append(header + ":")
        rows: List[str] = []
        items = [it for it in by_type.get(type_name, []) if is_open(it)]
        for it in items:
            extra = []
            for col in extra_cols:
                if col == "__status":
                    extra.append(f"Status: {issue_status_name(it)}")
                elif col == "__assignee":
                    who = field_text(it, "assignee")
                    if who: extra.append(f"Assignee: {who}")
                elif col == "__risk_score":
                    rs = field_text(it, cfg["RISK_SCORE_FIELD"])
                    if rs: extra.append(f"Aristocrat risk score: {rs}")
                else:
                    val = field_text(it, col)
                    if val: extra.append(f"{col}: {val}")
            rows.append(format_item(it, extra))
        rows = rows or ["None"]
        lines.extend(rows)
        lines.append("")

    if root_type.lower() == cfg["INITIATIVE_TYPE_NAME"].lower():
        section("Risks", init_risk_type, ["__status","__risk_score","__assignee"])
        section("Assumptions", assumption_type, ["__status","__assignee"])
        section("Issues", issue_t, ["__status","__assignee"])
        section("Decisions", decision_t, ["__status","__assignee"])
        section("Change requests", change_req_t, ["__status","__assignee"])
    else:
        # For now, project-level uses same pool but filtered by open status (hierarchy parent filtering can be added if needed)
        section("Risks", init_risk_type, ["__status","__risk_score","__assignee"])
        section("Assumptions", assumption_type, ["__status","__assignee"])
        section("Issues", issue_t, ["__status","__assignee"])
        section("Decisions", decision_t, ["__status","__assignee"])
        section("Change requests", change_req_t, ["__status","__assignee"])

    text = f"# {title}\n\n" + "\n".join(lines).rstrip() + "\n"
    return title, text

def post_to_teams(webhook_url: str, title: str, text: str) -> None:
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": title,
        "themeColor": "0076D7",
        "title": title,
        "text": text
    }
    r = requests.post(webhook_url, json=payload, timeout=45)
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"Teams webhook error {r.status_code}: {r.text[:400]}")

def main():
    base = env("JIRA_BASE_URL"); email = env("JIRA_EMAIL"); token = env("JIRA_API_TOKEN")
    jql = env("JIRA_JQL")
    fields_csv = env("JIRA_FIELDS","").strip()
    tzname = env("TIMEZONE","UTC")
    webhook = env("TEAMS_WEBHOOK_URL")
    title_override = env("TITLE","Weekly PMO Jira Status Report")
    root_hint = env("ROOT_ISSUE_KEY","").strip() or None
    max_issues = int(env("MAX_ISSUES","1000"))
    batch_size = int(env("BATCH_SIZE","200"))

    missing = [n for n,v in [("JIRA_BASE_URL",base),("JIRA_EMAIL",email),("JIRA_API_TOKEN",token),("JIRA_JQL",jql),("TEAMS_WEBHOOK_URL",webhook)] if not v]
    if missing:
        print("Missing required configuration values: " + ", ".join(missing), file=sys.stderr)
        sys.exit(2)

    cfg = {
        "WEEKLY_UPDATE_FIELD": env("WEEKLY_UPDATE_FIELD","customfield_weekly_update"),
        "MILESTONE_TYPE_NAME": env("MILESTONE_TYPE_NAME","Milestone"),
        "MILESTONE_TARGET_END_FIELD": env("MILESTONE_TARGET_END_FIELD","duedate"),
        "MILESTONE_RAG_FIELD": env("MILESTONE_RAG_FIELD","customfield_rag"),
        "INITIATIVE_RISK_TYPE_NAME": env("INITIATIVE_RISK_TYPE_NAME","InitiativeRisk"),
        "ASSUMPTION_TYPE_NAME": env("ASSUMPTION_TYPE_NAME","Assumption"),
        "ISSUE_TYPE_NAME": env("ISSUE_TYPE_NAME","Issue"),
        "DECISION_TYPE_NAME": env("DECISION_TYPE_NAME","Decision"),
        "CHANGE_REQ_TYPE_NAME": env("CHANGE_REQ_TYPE_NAME","Change req"),
        "PROJECT_TYPE_NAME": env("PROJECT_TYPE_NAME","Project"),
        "INITIATIVE_TYPE_NAME": env("INITIATIVE_TYPE_NAME","PMO Initiative"),
        "RISK_SCORE_FIELD": env("RISK_SCORE_FIELD","customfield_riskscore"),
    }

    base_fields = ["summary","issuetype","status","assignee","parent",
                   cfg["MILESTONE_TARGET_END_FIELD"], cfg["MILESTONE_RAG_FIELD"],
                   cfg["WEEKLY_UPDATE_FIELD"], cfg["RISK_SCORE_FIELD"]
                  ]
    if fields_csv:
        req_fields = [s.strip() for s in fields_csv.split(",") if s.strip()]
        # de-dup while preserving order
        seen = set()
        req_fields = [x for x in req_fields + base_fields if (x not in seen and not seen.add(x))]
    else:
        req_fields = base_fields

    all_fields = fetch_fields(base, email, token)
    req_fields = detect_fields(req_fields, all_fields)

    print(f"Querying Jira with JQL: {jql}")
    issues = jira_search(base, email, token, jql, req_fields, max_issues=max_issues, batch_size=batch_size)
    print(f"Fetched {len(issues)} issues")

    if not issues:
        post_to_teams(webhook, title_override, f"No issues matched the JQL.\\n\\nJQL: `{jql}`")
        print("Posted: empty result notice")
        return

    root = determine_root(issues, root_hint)
    if not root:
        root_title = title_override
        text = f"Could not determine a root issue for this PMO report. Posting a flat summary of milestones and RAIDs.\\n\\n_Total issues fetched: {len(issues)}_"
        post_to_teams(webhook, root_title, text)
        print("Posted: no-root fallback")
        return

    title, text = build_report(issues, root, cfg, tzname)
    post_title = f"{title_override} — {title}" if title_override.strip() else title
    post_to_teams(webhook, post_title, text)
    print("Posted PMO report")

if __name__ == "__main__":
    main()

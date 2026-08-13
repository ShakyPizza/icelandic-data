"""Samgöngustofa bifreiðatölur — vehicle-registration statistics via the
reverse-engineered Power BI query API.

The dashboard at https://bifreidatolur.samgongustofa.is/ is a thin SPA that
embeds a separate Power BI report per section, each with its OWN resource key.
Two reports carry the useful numbers:

  * nyskraningar  ("Nýskráningar" section, #nyskraningar)
        NEW registrations = nyskraningar = first Icelandic registration, i.e.
        imports (brand-new AND imported-used). Filterable by year, month and
        new-vs-used. This is the FLOW of vehicles entering the fleet.

  * onroad        ("Tölfræði ökutækja" section, #tolfraedi)
        The CURRENT fleet actually on the road ("í umferð"): every vehicle with
        an active registration status. A snapshot, not a flow — no year filter.
        This is the STOCK of vehicles.

Both break down by four dimensions:
  make   (Tegund)        brand: TOYOTA, KIA, BYD, TESLA, ...
  fuel   (Orkugjafi)     energy source: Rafmagn (electric), Bensín, Dísel,
                         Bensín/Rafmagn (PHEV), Metan, Vetni, ... — the cleanest
                         read on Iceland's EV transition
  class  (Ökutækisflokkur) vehicle class: Fólksbifreið M1, Sendibifreið N1,
                         Vörubifreið N2/N3, bifhjól, dráttarvél, ...
  model  (Undirtegund)   model: MODEL Y, DUSTER, ID.4, ...

HOW THE DATA IS FETCHED (and why the obvious way fails)
-------------------------------------------------------
Visuals POST a SemanticQueryDataShapeCommand to

    https://wabi-europe-north-b-api.analysis.windows.net/public/reports/querydata?synchronous=true

with header `x-powerbi-resourcekey: <key>` and NO bearer. The report is public,
so a plain httpx POST works — but only for a handful of requests: the anonymous
grant is session- and origin-bound and rate-limited (`retry-after` is exposed),
so a cold client soon gets 401 PowerBINotAuthorizedException, and a POST from
any origin other than the app.powerbi.com iframe is rejected outright.

The robust method, used here, REPLAYS the query with `fetch()` executed *inside
the app.powerbi.com iframe* via Playwright — reusing the report's own live
session, origin and pacing. Every variant then returns 200.

Baked-in gotchas:
  * the POST body must keep top-level `modelId`/`version`/`cancelQueries` or the
    API answers 400 "ModelId must be between 1 and 9.2e18";
  * resource keys and the model id are discovered live (decoded from the active
    iframe's embed token and read off the section's own requests) — never
    hardcoded, because they rotate;
  * response rows come in two shapes: nyskraningar uses C:[dim,count]; onroad
    uses G0 + X[0].M0 (in-traffic count) + X[1].M0 (new). _parse handles both.

GEO-FENCE: the host answers Icelandic IPs in ~50 ms and times out from
datacenter address space. Run this from an Icelandic connection.

Usage:
    uv run python scripts/samgongustofa.py list
    uv run python scripts/samgongustofa.py fetch --report onroad --dimension fuel
    uv run python scripts/samgongustofa.py fetch --report onroad --dimension make
    uv run python scripts/samgongustofa.py fetch --dimension make  --years 2020-2026
    uv run python scripts/samgongustofa.py fetch --dimension fuel  --years 2020-2026
    uv run python scripts/samgongustofa.py fetch --dimension make  --years 2025,2026 --monthly
    uv run python scripts/samgongustofa.py fetch --dimension fuel  --years 2024-2026 --import-state new
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import copy
import csv
import datetime as dt
import json
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BASE_SPA = "https://bifreidatolur.samgongustofa.is/"
API = "https://wabi-europe-north-b-api.analysis.windows.net/public/reports/querydata?synchronous=true"
OUT_DIR = Path(__file__).parent.parent / "data" / "processed" / "samgongustofa"

YEAR_COL = "Ár - ísl."
MONTH_COL = "Mánuður - ísl."
IMPORT_COL = "Innflutningsástand"

# Month slicer labels, verbatim from the dropdown (Icelandic, zero-padded).
MONTHS = [
    "01-janúar", "02-febrúar", "03-mars", "04-apríl", "05-maí", "06-júní",
    "07-júlí", "08-ágúst", "09-september", "10-október", "11-nóvember", "12-desember",
]

# Two reports. `dims` maps a friendly name -> the Power BI column that visual
# groups by (the make/brand column is confusingly named "Tegund" = kind).
# `temporal` reports carry the year/month slicers; the snapshot report does not.
REPORTS = {
    "nyskraningar": {
        "anchor": "#nyskraningar",
        "temporal": True,
        "blurb": "new registrations / imports (flow) — year, month, new/used",
        "dims": {"make": "Tegund", "class": "Ökutækisflokkur",
                 "fuel": "Orkugjafi", "model": "Undirtegund"},
    },
    "onroad": {
        "anchor": "#tolfraedi",
        "temporal": False,
        "blurb": "current fleet on the road / í umferð (snapshot)",
        "dims": {"make": "Tegund", "class": "Ökutækjaflokkur",
                 "fuel": "Orkugjafi (groups)", "model": "Undirtegund"},
    },
}

# fetch() executed inside the powerbi iframe; returns parsed JSON or {__status}.
_JS_FETCH = """async ({url, key, payload}) => {
    const r = await fetch(url, {
        method: 'POST',
        headers: {
            'content-type': 'application/json;charset=UTF-8',
            'accept': 'application/json, text/plain, */*',
            'x-powerbi-resourcekey': key,
        },
        body: JSON.stringify(payload),
    });
    if (!r.ok) return {__status: r.status};
    return await r.json();
}"""


# ---------------------------------------------------------------------------
# discovery + replay
# ---------------------------------------------------------------------------
def _key_from_token(view_url):
    """Decode the report resource key from an app.powerbi.com/view?r=<token>."""
    token = view_url.split("r=", 1)[1].split("&", 1)[0]
    token += "=" * (-len(token) % 4)
    try:
        return json.loads(base64.b64decode(token))["k"]
    except (binascii.Error, KeyError, json.JSONDecodeError, ValueError):
        return None


async def _discover(page, report):
    """Open a section; return (frame, key, templates_by_dim) for THAT report.

    templates_by_dim maps a dimension column -> the full request body the report
    fired for that visual (complete with modelId). Requests are filtered to the
    active iframe's key so a neighbouring report's identically-named visual
    cannot leak in.
    """
    captured: list[tuple[str, dict]] = []  # (key, body)

    def on_request(req):
        if "querydata" in req.url and req.post_data:
            try:
                captured.append((req.headers.get("x-powerbi-resourcekey"), json.loads(req.post_data)))
            except json.JSONDecodeError:
                pass

    page.on("request", on_request)
    await page.goto(BASE_SPA, wait_until="networkidle", timeout=90_000)
    await asyncio.sleep(3)
    await page.eval_on_selector(f'a[href="{REPORTS[report]["anchor"]}"]', "e => e.click()")
    await asyncio.sleep(10)

    frame = next((f for f in page.frames if "app.powerbi.com/view" in f.url), None)
    if frame is None:
        raise RuntimeError("Power BI iframe never appeared — SPA layout changed?")
    key = _key_from_token(frame.url)

    templates: dict[str, dict] = {}
    for k, body in captured:
        if k != key:
            continue
        try:
            q = body["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
            dims = [s["Column"]["Property"] for s in q.get("Select", []) if "Column" in s]
            if dims:
                templates[dims[0]] = body
        except (KeyError, IndexError):
            pass
    page.remove_listener("request", on_request)
    return frame, key, templates


def _in_condition(prop, value):
    return {"Condition": {"In": {
        "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "q"}}, "Property": prop}}],
        "Values": [[{"Literal": {"Value": value}}]],
    }}}


def _rewrite(body, *, year=None, month=None, import_state="all"):
    """Clone a captured template, setting the requested slicer filters.

    year/month are applied only when given (temporal report). import_state
    replaces the new/used filter on either report. Other conditions (e.g. the
    on-road status filter P_CURRENT_REGI_STATUS) are preserved untouched.
    """
    b = copy.deepcopy(body)
    q = b["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
    kept, saw_year = [], False
    for w in q.get("Where", []):
        prop = (w.get("Condition", {}).get("In", {}).get("Expressions", [{}])[0]
                .get("Column", {}).get("Property"))
        if prop == YEAR_COL and year is not None:
            for vals in w["Condition"]["In"]["Values"]:
                for v in vals:
                    if "Literal" in v:
                        v["Literal"]["Value"] = f"{year}L"
            kept.append(w)
            saw_year = True
        elif prop == MONTH_COL:
            continue  # re-added below when a month is requested
        elif prop == IMPORT_COL and import_state != "all":
            continue  # re-added below
        else:
            kept.append(w)
    if year is not None and not saw_year:
        kept.append(_in_condition(YEAR_COL, f"{year}L"))
    if month:
        kept.append(_in_condition(MONTH_COL, f"'{month}'"))
    if import_state == "new":
        kept.append(_in_condition(IMPORT_COL, "'Nýtt'"))
    elif import_state == "used":
        kept.append(_in_condition(IMPORT_COL, "'Notað'"))
    q["Where"] = kept
    return b


def _parse_where(specs):
    """['Orkugjafi=Rafmagn', 'Innflutningsástand=Nýtt;Notað'] -> [(col, [vals])].

    Values are OR'd within a column (';'-separated) and AND'd across --where.
    """
    out = []
    for spec in specs or []:
        col, sep, val = spec.partition("=")
        if not sep:
            raise SystemExit(f"--where must be COL=VALUE, got {spec!r}")
        vals = [v.strip() for v in val.split(";") if v.strip()]
        out.append((col.strip(), vals))
    return out


def _apply_where(payload, wheres):
    """Append arbitrary In(column, values) filters to a built payload.

    Values are sent as text literals — the right form for the categorical
    columns you cross-filter on (Orkugjafi, Ökutækisflokkur, Tegund, ...). The
    year slicer has its own numeric-literal path (--years).
    """
    if not wheres:
        return payload
    q = payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
    for col, vals in wheres:
        q.setdefault("Where", []).append({"Condition": {"In": {
            "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "q"}}, "Property": col}}],
            "Values": [[{"Literal": {"Value": f"'{v}'"}}] for v in vals],
        }}})
    return payload


def _parse(body):
    """DM0 rows -> {dimension_value: count}.

    Two shapes: C:[dim, measure] (nyskraningar) and G0 + X[0].M0 (onroad, where
    X[0] is the in-traffic count and X[1] the new-registration count)."""
    rows: dict[str, float] = {}
    for res in body.get("results", []):
        dsr = (res.get("result") or {}).get("data", {}).get("dsr", {})
        for ds in dsr.get("DS", []):
            for ph in ds.get("PH", []):
                for row in ph.get("DM0", []):
                    if "C" in row and len(row["C"]) >= 2:
                        name, cnt = row["C"][0], row["C"][1]
                    else:
                        name = row.get("G0")
                        x = row.get("X", [{}])
                        cnt = (x[0].get("M0") if x else 0) or 0
                    if name is not None:
                        rows[name] = rows.get(name, 0) + (cnt or 0)
    return rows


async def _replay(frame, key, payload):
    out = await frame.evaluate(_JS_FETCH, {"url": API, "key": key, "payload": payload})
    if isinstance(out, dict) and out.get("__status"):
        raise RuntimeError(f"querydata HTTP {out['__status']}")
    return _parse(out)


async def _replay_retry(frame, key, payload):
    try:
        return await _replay(frame, key, payload)
    except RuntimeError as e:  # transient 401/429 — pause and retry once
        print(f"    {e}; retrying in 5s", file=sys.stderr)
        await asyncio.sleep(5)
        return await _replay(frame, key, payload)


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------
def _parse_years(spec):
    years = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            years.update(range(int(a), int(b) + 1))
        elif part:
            years.add(int(part))
    return sorted(years)


async def _run_fetch(args):
    from playwright.async_api import async_playwright

    cfg = REPORTS[args.report]
    dim_col = cfg["dims"][args.dimension]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1600, "height": 1200})
        frame, key, templates = await _discover(page, args.report)
        if dim_col not in templates:
            raise SystemExit(
                f"dimension '{args.dimension}' ({dim_col}) not among {args.report} "
                f"visuals: {sorted(templates)}"
            )
        template = templates[dim_col]
        wheres = _parse_where(args.where)
        records, header = [], [args.dimension]

        if cfg["temporal"]:
            years = _parse_years(args.years)
            months = MONTHS[: args.through] if args.monthly else [None]
            header += ["year"] + (["month"] if args.monthly else []) + ["count"]
            print(f"report={args.report} key={key} dim={dim_col} years={years} "
                  f"months={'Jan..' + months[-1] if args.monthly else 'all'}", file=sys.stderr)
            for year in years:
                for month in months:
                    payload = _apply_where(_rewrite(template, year=year, month=month, import_state=args.import_state), wheres)
                    rows = await _replay_retry(frame, key, payload)
                    for name, cnt in sorted(rows.items(), key=lambda kv: -kv[1]):
                        rec = {args.dimension: name, "year": year, "count": int(cnt)}
                        if args.monthly:
                            rec["month"] = int(month[:2])
                        records.append(rec)
                    print(f"  {year}{' ' + month if month else ''}: {len(rows)} "
                          f"{args.dimension}s, {int(sum(rows.values())):,}", file=sys.stderr)
                    await asyncio.sleep(2.5)
        else:  # snapshot
            header += ["count"]
            print(f"report={args.report} key={key} dim={dim_col} (current fleet snapshot)", file=sys.stderr)
            rows = await _replay_retry(frame, key, _apply_where(_rewrite(template, import_state=args.import_state), wheres))
            for name, cnt in sorted(rows.items(), key=lambda kv: -kv[1]):
                records.append({args.dimension: name, "count": int(cnt)})
            print(f"  {len(rows)} {args.dimension}s on road, {int(sum(rows.values())):,} total", file=sys.stderr)

        await browser.close()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    parts = [args.report, args.dimension]
    if cfg["temporal"]:
        parts.append("by_year_month" if args.monthly else "by_year")
    if args.import_state != "all":
        parts.append(args.import_state)
    for col, vals in wheres:
        tag = col.split(" ")[0].split("-")[0][:6] + "-" + "+".join(vals)
        parts.append("".join(ch for ch in tag if ch.isalnum() or ch in "-+"))
    out = Path(args.out) if args.out else OUT_DIR / ("_".join(parts) + ".csv")
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(records)
    print(f"→ {out} ({len(records)} rows)", file=sys.stderr)


async def _run_list(args):
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1600, "height": 1200})
        for name, cfg in REPORTS.items():
            frame, key, templates = await _discover(page, name)
            inv = {v: k for k, v in cfg["dims"].items()}
            print(f"\n=== {name}  ({cfg['anchor']}) — {cfg['blurb']}")
            print(f"    resource key: {key}")
            print("    dimensions (all groupable columns the report exposes):")
            for col in sorted(templates):
                alias = inv.get(col)
                tag = f"--dimension {alias}" if alias else "(bonus column, no alias)"
                print(f"      {col:<26} {tag}")
        print("\nSlicers (nyskraningar): --years / --monthly (" + MONTH_COL + ") / --import-state new|used")
        print("Cross-filter any report by any column: --where 'COL=VALUE' (repeatable; ';' = OR)")
        print("Months:", ", ".join(MONTHS))
        print("\nExamples:")
        print("  fetch --report onroad --dimension fuel               # current EV/petrol/diesel fleet split")
        print("  fetch --dimension make --years 2020-2026             # imports by brand per year")
        print("  fetch --dimension fuel --years 2025,2026 --monthly")
        print("  fetch --dimension make --years 2026 --where 'Orkugjafi=Rafmagn'   # BEV imports by brand")
        await browser.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    pl = sub.add_parser("list", help="discover both reports, their keys and dimensions")
    pl.set_defaults(func=lambda a: asyncio.run(_run_list(a)))

    pf = sub.add_parser("fetch", help="pull registration counts to a tidy CSV")
    pf.add_argument("--report", choices=list(REPORTS), default="nyskraningar",
                    help="nyskraningar = new registrations/imports (flow); onroad = current fleet (snapshot)")
    pf.add_argument("--dimension", choices=["make", "class", "fuel", "model"], default="make")
    pf.add_argument("--years", default=f"2020-{dt.date.today().year}",
                    help="temporal reports only, e.g. '2020-2026' or '2025,2026'")
    pf.add_argument("--monthly", action="store_true", help="break each year down by month")
    pf.add_argument("--through", type=int, default=12, metavar="N",
                    help="with --monthly, only months 1..N (default 12)")
    pf.add_argument("--import-state", choices=["all", "new", "used"], default="all")
    pf.add_argument("--where", action="append", metavar="COL=VALUE",
                    help="cross-filter by any model column, repeatable; ';' OR-joins values, "
                         "e.g. --where 'Orkugjafi=Rafmagn' (see column names in `list`)")
    pf.add_argument("--out", help="output CSV path (default data/processed/samgongustofa/)")
    pf.set_defaults(func=lambda a: asyncio.run(_run_fetch(a)))

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

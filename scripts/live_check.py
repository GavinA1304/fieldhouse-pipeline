"""Cheap pre-check for live runs (Python standard library only, ~5 seconds).

Asks TrackMan for today's + yesterday's sessions and compares them with the
pipeline's state file on Hugging Face. Only if something is new (or newly
verified) does the workflow spend time setting up R. On a private repo every
Actions minute counts, so quiet runs should cost one minute, not three.

Writes new=true|false to $GITHUB_OUTPUT. Any error -> new=true, so the full
pipeline runs and reports the problem properly. Prints counts only.
"""
import datetime as dt
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request


def out(new, why):
    print(f"live check: {why}")
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            f.write(f"new={'true' if new else 'false'}\n")
    sys.exit(0)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


def http(method, url, data=None, headers=None):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=60) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        if e.code in (301, 302, 303, 307, 308):  # signed CDN URL: follow WITHOUT auth
            return http("GET", urllib.parse.urljoin(url, e.headers["Location"]))
        return e.code, e.read()


def config_value(text, key, default):
    m = re.search(rf"^\s*{key}:\s*([^\s#]+)", text, re.M)
    return m.group(1).strip("\"'") if m else default


try:
    hf_auth = {"Authorization": f"Bearer {os.environ['HF_TOKEN']}"}
    hf_base = f"https://huggingface.co/datasets/{os.environ['HF_REPO']}/resolve/main"
    if os.path.exists("config.yml"):
        cfg = open("config.yml").read()
    else:  # team config lives in the private dataset, not in this repo
        st, body = http("GET", f"{hf_base}/config/config.yml", headers=hf_auth)
        if st != 200:
            out(True, f"config read HTTP {st}")
        cfg = body.decode()
    trackman_block = cfg.split("trackman:", 1)[1].split("\ncleaning:", 1)[0]
    source = config_value(trackman_block, "source", "api")
    if source != "api":
        out(True, f"source={source}, can't pre-check cheaply; running full pipeline")
    lookback = int(config_value(trackman_block, "live_lookback_days", "1"))
    practice = config_value(trackman_block, "include_practice", "false").lower() == "true"

    # TrackMan token
    st, body = http("POST", "https://login.trackmanbaseball.com/connect/token",
                    urllib.parse.urlencode({"client_id": os.environ["TRACKMAN_CLIENT_ID"],
                                            "client_secret": os.environ["TRACKMAN_CLIENT_SECRET"],
                                            "grant_type": "client_credentials"}).encode(),
                    {"Content-Type": "application/x-www-form-urlencoded"})
    if st != 200:
        out(True, f"TrackMan login HTTP {st}")
    token = json.loads(body)["access_token"]

    today = dt.datetime.utcnow().date()
    frm, to = today - dt.timedelta(days=lookback), today
    sessions = []
    for kind in ["game"] + (["practice"] if practice else []):
        st, body = http("POST", f"https://dataapi.trackmanbaseball.com/api/v1/discovery/{kind}/sessions",
                        json.dumps({"sessionType": "All",
                                    "utcDateFrom": f"{frm}T00:00:00.000Z",
                                    "utcDateTo": f"{to}T23:59:59.999Z"}).encode(),
                        {"Authorization": f"Bearer {token}", "accept": "text/plain",
                         "Content-Type": "application/json-patch+json"})
        if st != 200:
            out(True, f"TrackMan discovery HTTP {st}")
        sessions += json.loads(body)

    # Pipeline state on Hugging Face
    st, body = http("GET", f"{hf_base}/state/trackman.json", headers=hf_auth)
    if st == 404:
        out(True, "no state yet")
    if st != 200:
        out(True, f"state read HTTP {st}")
    seen = json.loads(body).get("api") or {}

    new = 0
    for s in sessions:
        prev = seen.get(s.get("sessionId"))
        if prev is None or (not prev.get("verified") and s.get("verified")):
            new += 1
    out(new > 0, f"{len(sessions)} sessions in window, {new} new or newly verified")
except SystemExit:
    raise
except Exception as e:  # fail open: let the real pipeline run and report
    out(True, f"check error ({type(e).__name__}); running full pipeline")

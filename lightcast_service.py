import requests
import pandas as pd
import time
from pathlib import Path

LIGHTCAST_CLIENT_ID = "m1nj9z4goarfb68i"
LIGHTCAST_CLIENT_SECRET = "Dn68Zy7B"

TOKEN_URL = "https://auth.emsicloud.com/connect/token"
SKILL_URL = "https://emsiservices.com/skills/versions/latest/skills"

STORE_FILE = Path("normalized_skills.csv")

def get_lightcast_token():
    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": LIGHTCAST_CLIENT_ID,
            "client_secret": LIGHTCAST_CLIENT_SECRET,
            "scope": "emsi_open",
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def normalize_file(file_path: Path) -> Path:
    token = get_lightcast_token()
    if file_path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
    elif file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path)
    else:
        raise ValueError("Unsupported file format. Upload CSV or Excel.")

    results = []

    for _, row in df.iterrows():
        skill = str(row["skill_name"]).strip()
        if not skill:
            continue

        try:
            r = requests.get(
                SKILL_URL,
                headers={"Authorization": f"Bearer {token}"},
                params={"q": skill, "limit": 1},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json().get("data", [])

            if data:
                s = data[0]
                results.append({
                    "remote_skill_id": row["remote_skill_id"],
                    "workday_skill": skill,
                    "lightcast_skill": s["name"],
                    "lightcast_skill_id": s["id"],
                    "skill_type": s.get("type"),
                    "category": s.get("category"),
                    "status": "SUCCESS",
                })
            else:
                results.append({
                    "remote_skill_id": row["remote_skill_id"],
                    "workday_skill": skill,
                    "lightcast_skill": "",
                    "lightcast_skill_id": "",
                    "skill_type": "",
                    "category": "",
                    "status": "NO_MATCH",
                })

            time.sleep(0.2)

        except Exception as e:
            results.append({
                "remote_skill_id": row["remote_skill_id"],
                "workday_skill": skill,
                "lightcast_skill": "",
                "lightcast_skill_id": "",
                "skill_type": "",
                "category": "",
                "status": f"ERROR",
            })

    out_df = pd.DataFrame(results)
    out_df.to_csv(STORE_FILE, index=False)
    out_df.to_excel("uploads/normalized_skills.xlsx", index=False)

    return STORE_FILE
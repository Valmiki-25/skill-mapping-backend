import requests
import pandas as pd
import time
import shutil
from pathlib import Path
#lightcast credentials (use env vars in production!)
LIGHTCAST_CLIENT_ID = "m1nj9z4goarfb68i"
LIGHTCAST_CLIENT_SECRET = "Dn68Zy7B"

TOKEN_URL = "https://auth.emsicloud.com/connect/token"
SKILL_URL = "https://emsiservices.com/skills/versions/latest/skills"


def get_lightcast_token():
    try:
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
    except Exception:
        return None  # Token failed


def normalize_file(file_path: Path) -> Path:
    try:
        token = get_lightcast_token()

        # If token failed → return original file immediately
        if not token:
            print("⚠ Lightcast token failed. Returning original file.")
            return file_path

        # Read file
        if file_path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        elif file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path)
        else:
            raise ValueError("Unsupported file format. Upload CSV or Excel.")

        results = []

        for _, row in df.iterrows():
            skill = str(row.get("skill_name", "")).strip()
            remote_id = row.get("remote_skill_id", "")

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
                        "remote_skill_id": remote_id,
                        "workday_skill": skill,
                        "lightcast_skill": s.get("name", ""),
                        "lightcast_skill_id": s.get("id", ""),
                        "skill_type": s.get("type", ""),
                        "category": s.get("category", ""),
                        "status": "SUCCESS",
                    })
                else:
                    results.append({
                        "remote_skill_id": remote_id,
                        "workday_skill": skill,
                        "lightcast_skill": "",
                        "lightcast_skill_id": "",
                        "skill_type": "",
                        "category": "",
                        "status": "NO_MATCH",
                    })

                time.sleep(0.2)

            except Exception:
                # If API fails mid-process → return original file
                print("⚠ Lightcast API failed during processing. Returning original file.")
                return file_path

        # Save normalized file with SAME NAME (overwrite original)
        if file_path.suffix.lower() == ".csv":
            pd.DataFrame(results).to_csv(file_path, index=False)
        else:
            pd.DataFrame(results).to_excel(file_path, index=False)

        return file_path

    except Exception:
        print("⚠ Unexpected error. Returning original file.")
        return file_path

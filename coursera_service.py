import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from urllib.parse import quote_plus, urljoin
from pathlib import Path

# ==================================================
# CONFIG
# ==================================================

BASE_URL = "https://www.coursera.org"
SEARCH_URL = "https://www.coursera.org/search?query="
HEADERS = {"User-Agent": "Mozilla/5.0"}

STORE_FILE = Path("normalized_skills.csv")
OUTPUT_FILE = Path("uploads/coursera_mapped_skills.xlsx")

MAX_COURSES_PER_SKILL = 1
REQUEST_DELAY = 1.0

# ==================================================
# FETCH COURSE SKILLS (STRICT – NO UI NOISE)
# ==================================================

def fetch_course_skills(course_url: str) -> str:
    try:
        r = requests.get(course_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        skills = []

        # ✅ Only extract from "Skills you'll gain" section
        for h2 in soup.find_all("h2"):
            if "skill" in h2.get_text(strip=True).lower():
                ul = h2.find_next("ul")
                if not ul:
                    continue

                for a in ul.find_all("a"):
                    skill = a.get_text(strip=True)
                    if skill:
                        skills.append(skill)

        return ", ".join(sorted(set(skills)))

    except Exception:
        return ""

# ==================================================
# FETCH COURSES FOR A LIGHTCAST SKILL
# ==================================================

def fetch_courses_for_skill(lightcast_skill: str) -> list[dict]:
    url = f"{SEARCH_URL}{quote_plus(lightcast_skill)}&sortBy=BEST_MATCH"

    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")

    courses = []
    seen = set()

    for a in soup.select("a[href^='/learn/']"):
        href = a.get("href", "").split("?")[0]
        slug = href.replace("/learn/", "").strip()

        if not slug or slug in seen:
            continue

        seen.add(slug)

        course_link = urljoin(BASE_URL, href)

        courses.append({
            "course_name": a.get_text(strip=True),
            "course_slug": slug,
            "course_link": course_link,
            "course_skills": fetch_course_skills(course_link),
        })

        if len(courses) >= MAX_COURSES_PER_SKILL:
            break

        time.sleep(REQUEST_DELAY)

    return courses

# ==================================================
# MAIN COURSERA PROCESS
# ==================================================
def process_coursera() -> Path:
    if not STORE_FILE.exists():
        raise ValueError("normalized_skills.csv not found")

    df = pd.read_csv(STORE_FILE, dtype=str).fillna("")

    rows = []

    for _, row in df.iterrows():
        lightcast_skill = row.get("lightcast_skill", "").strip()
        workday_skill = row.get("workday_skill", "").strip()

        # ----------------------------------
        # 🔑 Skill selection logic
        # ----------------------------------
        if lightcast_skill:
            search_skill = lightcast_skill
            skill_source = "LIGHTCAST"
        elif workday_skill:
            search_skill = workday_skill
            skill_source = "WORKDAY"
        else:
            # Skip rows where both skills are missing
            continue

        courses = fetch_courses_for_skill(search_skill)

        for c in courses:
            rows.append({
                "remote_skill_id": row.get("remote_skill_id", ""),
                "workday_skill": workday_skill,
                "lightcast_skill": lightcast_skill,
                "lightcast_skill_id": row.get("lightcast_skill_id", ""),
                "search_skill_used": search_skill,
                "search_skill_source": skill_source,
                "course_name": c["course_name"],
                "course_slug": c["course_slug"],
                "course_link": c["course_link"],
                "course_skills": c["course_skills"],
            })

        time.sleep(REQUEST_DELAY)

    if not rows:
        raise ValueError("No Coursera courses found")

    out_df = pd.DataFrame(rows)

    final_df = (
        out_df
        .groupby(
            [
                "remote_skill_id",
                "workday_skill",
                "lightcast_skill",
                "lightcast_skill_id",
                "search_skill_used",
                "search_skill_source",
            ],
            as_index=False
        )
        .agg({
            "course_name": lambda x: ", ".join(dict.fromkeys(x)),
            "course_slug": lambda x: ", ".join(dict.fromkeys(x)),
            "course_link": lambda x: ", ".join(dict.fromkeys(x)),
            "course_skills": lambda x: ", ".join(dict.fromkeys(
                ", ".join(x).split(", ")
            )),
        })
    )

    final_df.to_excel(OUTPUT_FILE, index=False)

    return OUTPUT_FILE




# ============================
# READ / FILTER COURSERA DATA
# ============================

def list_coursera_mapped_skills(
    remote_skill_id: str | None = None,
    workday_skill: str | None = None,
    lightcast_skill: str | None = None,
):
    if not OUTPUT_FILE.exists():
        raise ValueError("Coursera mapping file not found. Run /process/coursera first")

    df = pd.read_excel(OUTPUT_FILE, dtype=str).fillna("")

    if df.empty:
        raise ValueError("No Coursera data available")

    # ----------------------------
    # OPTIONAL VALIDATION
    # ----------------------------
    if remote_skill_id:
        if not (df["remote_skill_id"].str.strip() == remote_skill_id.strip()).any():
            raise ValueError(f"remote_skill_id '{remote_skill_id}' not found")

    if workday_skill:
        if not df["workday_skill"].str.lower().str.contains(workday_skill.lower()).any():
            raise ValueError(f"workday_skill '{workday_skill}' not found")

    if lightcast_skill:
        if not df["lightcast_skill"].str.lower().str.contains(lightcast_skill.lower()).any():
            raise ValueError(f"lightcast_skill '{lightcast_skill}' not found")

    # ----------------------------
    # FILTERING
    # ----------------------------
    if remote_skill_id:
        df = df[df["remote_skill_id"].str.strip() == remote_skill_id.strip()]

    if workday_skill:
        df = df[
            df["workday_skill"]
            .str.lower()
            .str.contains(workday_skill.lower(), na=False)
        ]

    if lightcast_skill:
        df = df[
            df["lightcast_skill"]
            .str.lower()
            .str.contains(lightcast_skill.lower(), na=False)
        ]

    return df

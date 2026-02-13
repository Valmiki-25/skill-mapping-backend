import pandas as pd
from pathlib import Path

FILE_PATH = Path("normalized_skills.csv")

COLUMNS = [
    "remote_skill_id",
    "workday_skill",
    "lightcast_skill",
    "lightcast_skill_id",
    "skill_type",
    "category",
    "status",
]

# ============================
# LOAD / SAVE
# ============================

def load_df():
    if FILE_PATH.exists():
        df = pd.read_csv(FILE_PATH, dtype=str)
    else:
        df = pd.DataFrame(columns=COLUMNS)

    return df.fillna("")

def save_df(df):
    df.to_csv(FILE_PATH, index=False)
    df.to_excel("uploads/normalized_skills.xlsx", index=False)

# ============================
# LIST (ALL OPTIONAL FILTERS)
# ============================
def list_skills(
    remote_skill_id=None,
    workday_skill=None,
    lightcast_skill=None,
):
    df = load_df()

    # ---- Validate existence FIRST (before filtering) ----
    if remote_skill_id:
        exists = (
            df["remote_skill_id"]
            .str.strip()
            == remote_skill_id.strip()
        ).any()

        if not exists:
            raise ValueError(
                f"remote_skill_id '{remote_skill_id}' not found"
            )

    if workday_skill:
        exists = (
            df["workday_skill"]
            .str.lower()
            .str.contains(workday_skill.lower(), na=False)
        ).any()

        if not exists:
            raise ValueError(
                f"workday_skill '{workday_skill}' not found"
            )

    if lightcast_skill:
        exists = (
            df["lightcast_skill"]
            .str.lower()
            .str.contains(lightcast_skill.lower(), na=False)
        ).any()

        if not exists:
            raise ValueError(
                f"lightcast_skill '{lightcast_skill}' not found"
            )

    # ---- Apply filters AFTER validation ----
    filtered = df.copy()

    if remote_skill_id:
        filtered = filtered[
            filtered["remote_skill_id"].str.strip()
            == remote_skill_id.strip()
        ]

    if workday_skill:
        filtered = filtered[
            filtered["workday_skill"]
            .str.lower()
            .str.contains(workday_skill.lower(), na=False)
        ]

    if lightcast_skill:
        filtered = filtered[
            filtered["lightcast_skill"]
            .str.lower()
            .str.contains(lightcast_skill.lower(), na=False)
        ]

    return filtered.to_dict(orient="records")


# ============================
# UPDATE
# ============================

def update_lightcast_skill(workday_skill: str, new_lightcast_skill: str):
    df = load_df()

    mask = (
        df["workday_skill"]
        .str.lower()
        .str.strip()
        == workday_skill.lower().strip()
    )

    if not mask.any():
        raise ValueError("Workday skill not found")

    df.loc[mask, "lightcast_skill"] = new_lightcast_skill
    save_df(df)

# ============================
# DELETE
# ============================
def delete_skill(
    remote_skill_id: str | None = None,
    workday_skill: str | None = None,
):
    if not remote_skill_id and not workday_skill:
        raise ValueError(
            "Either remote_skill_id or workday_skill must be provided"
        )

    df = load_df()

    if remote_skill_id:
        mask = df["remote_skill_id"].str.strip() == remote_skill_id.strip()

        if not mask.any():
            raise ValueError(
                f"remote_skill_id '{remote_skill_id}' not found"
            )

        df = df[~mask]

    elif workday_skill:
        mask = (
            df["workday_skill"]
            .str.lower()
            .str.strip()
            == workday_skill.lower().strip()
        )

        if not mask.any():
            raise ValueError(
                f"workday_skill '{workday_skill}' not found"
            )

        df = df[~mask]

    save_df(df)
    
# ============================
# LIST LIGHTCAST-READY RECORDS
# ============================
def get_lightcast_ready_df(
    remote_skill_id: str | None = None,
    workday_skill: str | None = None,
):
    df = load_df()

    # Base condition: Lightcast-ready only
    filtered = df[df["lightcast_skill"].str.strip() != ""]

    if filtered.empty:
        raise ValueError("No records with valid lightcast_skill found")

    # ----------------------------
    # OPTIONAL VALIDATION
    # ----------------------------
    if remote_skill_id:
        exists = (
            filtered["remote_skill_id"]
            .str.strip()
            == remote_skill_id.strip()
        ).any()

        if not exists:
            raise ValueError(
                f"remote_skill_id '{remote_skill_id}' not found in lightcast-ready records"
            )

    if workday_skill:
        exists = (
            filtered["workday_skill"]
            .str.lower()
            .str.contains(workday_skill.lower(), na=False)
        ).any()

        if not exists:
            raise ValueError(
                f"workday_skill '{workday_skill}' not found in lightcast-ready records"
            )

    # ----------------------------
    # OPTIONAL FILTERING
    # ----------------------------
    if remote_skill_id:
        filtered = filtered[
            filtered["remote_skill_id"].str.strip()
            == remote_skill_id.strip()
        ]

    if workday_skill:
        filtered = filtered[
            filtered["workday_skill"]
            .str.lower()
            .str.contains(workday_skill.lower(), na=False)
        ]

    return filtered

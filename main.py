import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

import lightcast_service
import coursera_service
import skill_store
from schemas import SkillUpdateRequest

# ==================================================
# LOAD ENV
# ==================================================
load_dotenv()

APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", 8000))

FRONTEND_ORIGINS = os.getenv("FRONTEND_ORIGINS", "")
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in FRONTEND_ORIGINS.split(",")
    if origin.strip()
]

# ==================================================
# CONFIG
# ==================================================
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

NORMALIZED_FILE = UPLOAD_DIR / "normalized_skills.xlsx"

app = FastAPI(
    title="Skill Normalization & Mapping API",
    description="""
    1. Lightcast skill normalization
    2. CRUD on normalized skills
    3. Coursera course mapping
    """
)

# ==================================================
# CORS CONFIG (ENV-BASED)
# ==================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================================================
# 1️⃣ LIGHTCAST NORMALIZATION (DOWNLOAD)
# ==================================================
@app.post("/process/lightcast")
def process_lightcast(file: UploadFile = File(...)):
    try:
        input_path = UPLOAD_DIR / file.filename
        input_path.write_bytes(file.file.read())

        lightcast_service.normalize_file(input_path)

        return FileResponse(
            path=NORMALIZED_FILE,
            filename="normalized_skills.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================
# 2️⃣ CRUD – LIST (NO DOWNLOAD)
# ==================================================
@app.get("/skills")
def list_skills(
    remote_skill_id: Optional[str] = Query(None),
    workday_skill: Optional[str] = Query(None),
    lightcast_skill: Optional[str] = Query(None),
):
    try:
        return skill_store.list_skills(
            remote_skill_id=remote_skill_id,
            workday_skill=workday_skill,
            lightcast_skill=lightcast_skill,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================
# 2️⃣ CRUD – UPDATE (DOWNLOAD)
# ==================================================
@app.put("/skills/update")
def update_skill(payload: SkillUpdateRequest):
    try:
        skill_store.update_lightcast_skill(
            workday_skill=payload.workday_skill,
            new_lightcast_skill=payload.lightcast_skill,
        )

        return FileResponse(
            path=NORMALIZED_FILE,
            filename="normalized_skills.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================
# 2️⃣ CRUD – DELETE (DOWNLOAD)
# ==================================================
@app.delete("/skills/delete")
def delete_skill(
    remote_skill_id: Optional[str] = Query(None),
    workday_skill: Optional[str] = Query(None),
):
    try:
        skill_store.delete_skill(
            remote_skill_id=remote_skill_id,
            workday_skill=workday_skill,
        )

        return FileResponse(
            path=NORMALIZED_FILE,
            filename="normalized_skills.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================
# 3️⃣ LIST – LIGHTCAST READY SKILLS
# ==================================================
@app.get("/skills/lightcast-ready")
def list_lightcast_ready_skills(
    remote_skill_id: Optional[str] = Query(None),
    workday_skill: Optional[str] = Query(None),
    download: bool = Query(False),
):
    try:
        df = skill_store.get_lightcast_ready_df(
            remote_skill_id=remote_skill_id,
            workday_skill=workday_skill,
        )

        if download:
            output = UPLOAD_DIR / "lightcast_ready_skills.xlsx"
            df.to_excel(output, index=False)

            return FileResponse(
                path=output,
                filename="lightcast_ready_skills.xlsx",
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        return df.to_dict(orient="records")

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================
# 4️⃣ COURSERA PROCESS (DOWNLOAD)
# ==================================================
@app.post("/process/coursera")
def process_coursera():
    try:
        output = coursera_service.process_coursera()

        return FileResponse(
            output,
            filename=output.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================
# 5️⃣ COURSERA MAPPED SKILLS (FILTER + DOWNLOAD)
# ==================================================
@app.get("/coursera/mapped-skills")
def list_coursera_mapped_skills(
    remote_skill_id: Optional[str] = Query(None),
    workday_skill: Optional[str] = Query(None),
    lightcast_skill: Optional[str] = Query(None),
    download: bool = Query(False),
):
    try:
        df = coursera_service.list_coursera_mapped_skills(
            remote_skill_id=remote_skill_id,
            workday_skill=workday_skill,
            lightcast_skill=lightcast_skill,
        )

        if download:
            output = UPLOAD_DIR / "coursera_mapped_filtered.xlsx"
            df.to_excel(output, index=False)

            return FileResponse(
                path=output,
                filename="coursera_mapped_filtered.xlsx",
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        return df.to_dict(orient="records")

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================
# RUN SERVER (ENV-BASED PORT)
# ==================================================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=APP_HOST,
        port=APP_PORT,
        reload=True,
    )

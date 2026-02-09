from fastapi import FastAPI, UploadFile, File, Form, HTTPException, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import os
import requests
import httpx
import json
import io


TRAJECTORY_API_URL = os.getenv(
    "TRAJECTORY_API_URL",
    "http://trajectory_service:8080"
)

ANALYSIS_API_URL = os.getenv(
    "ANALYSIS_API_URL",
    "http://save_image_service:9000"
)


app = FastAPI(title="Gateway Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api = APIRouter()


@api.post("/trajectory")
async def get_trajectory():
    r = requests.get(f"{TRAJECTORY_API_URL}/get_normalized_trajectory", timeout=10)
    data = r.json()
    points = data["normalized_points"]
    return {
        "trajectory": [{"x": p["x"] * 2 - 1, "y": p["y"] * 2 - 1} for p in points]
    }


@api.post("/analyze")
async def analyze(
    video: UploadFile = File(...),
    trajectory: str = Form(...),
    username: str = Form(...)
):
    trajectory_json = json.loads(trajectory)

    traj_file = io.BytesIO(json.dumps(trajectory_json).encode())
    traj_file.name = "trajectory.json"

    files = {
        "video": (video.filename, video.file, "video/webm"),
        "trajectory": ("trajectory.json", traj_file, "application/json")
    }

    data = {"username": username}

    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            f"{ANALYSIS_API_URL}/upload_video_data",
            files=files,
            data=data
        )

    if r.status_code != 200:
        raise HTTPException(r.status_code, r.text)

    return r.json()
    

@api.get("/health")
async def health():
    return {
        "status": "ok",
        "trajectory_api": TRAJECTORY_API_URL,
        "analysis_api": ANALYSIS_API_URL
    }

app.include_router(api)

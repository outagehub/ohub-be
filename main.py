from fastapi import FastAPI
from fastapi.responses import FileResponse
from pathlib import Path

app = FastAPI()

# Serve the HTML file from ohub-fe
@app.get("/", response_class=FileResponse)
async def read_root():
    return FileResponse(Path("../ohub-fe/index.html"))


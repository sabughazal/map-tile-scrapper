import sys
import pathlib
import argparse
import requests
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi import HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

current_dir = pathlib.Path(__file__).parent
sys.path.append(str(current_dir.parent))

from src.config import settings



BASE_DIR = pathlib.Path(__file__).parent.resolve()
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
DEFAULT_SOURCE_URL = "https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"


class Extent(BaseModel):
    minX: float
    minY: float
    maxX: float
    maxY: float


class TileCountRequest(BaseModel):
    extent: Extent
    z: int


def get_inline_arguments():
    parser = argparse.ArgumentParser(description="Map Tile Scrapper")
    parser.add_argument("--source-url", type=str, default=DEFAULT_SOURCE_URL, help="URL template for the map tiles")
    parser.add_argument("--output-dir", type=str, default=str(settings.OUTPUT_DIR), help="Directory to save the scraped tiles")
    parser.add_argument("--host", type=str, default=settings.HOST, help="Host to run the server on")
    parser.add_argument("--port", type=int, default=settings.PORT, help="Port to run the server on")
    return parser.parse_args()


def create_app(args) -> FastAPI:

    app = FastAPI(title="Map Tile Scrapper")

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse("index.html", {"request": request})


    @app.get("/scrapper/{z}/{x}/{y}")
    async def scrape_tile(z: int, x: int, y: int):
        collection = str(hash(args.source_url))

        output_path = pathlib.Path(args.output_dir) / collection / str(z) / str(x)
        output_path.mkdir(parents=True, exist_ok=True)
        tile_path = output_path / f"{y}.png"

        if tile_path.exists():
            return Response(content=tile_path.read_bytes(), media_type="image/png")

        url = args.source_url.format(x=x, y=y, z=z)
        response = requests.get(url, timeout=15)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=f"Failed to fetch tile from {url}")

        with open(tile_path, "wb") as f:
            f.write(response.content)

        content_type: Optional[str] = response.headers.get("content-type")
        if not content_type or "image" not in content_type:
            content_type = "image/png"

        return Response(content=response.content, media_type=content_type)


    @app.post("/scrapper/get-tile-count")
    async def get_tile_count(request: TileCountRequest):
        collection = str(hash(args.source_url))
        output_path = pathlib.Path(args.output_dir) / collection / str(request.z)

        if not output_path.exists():
            return {"tile_count": 0}

        tile_count = sum(1 for _ in output_path.rglob("*.png"))
        return {"tile_count": tile_count}

    return app


if __name__ == "__main__":
    import uvicorn

    args = get_inline_arguments()
    app = create_app(args)
    uvicorn.run(app, host=args.host, port=args.port)

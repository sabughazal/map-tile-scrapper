import sys
import hashlib
import pathlib
import requests
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
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


class Extent(BaseModel):
    minX: float
    minY: float
    maxX: float
    maxY: float


class TileCountRequest(BaseModel):
    extent: Extent
    z: int


def create_app() -> FastAPI:

    app = FastAPI(title="Map Tile Scrapper")
    cache_hits = 0
    cache_misses = 0

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse("index.html", {"request": request})


    @app.get("/auto", response_class=HTMLResponse)
    async def auto(request: Request) -> HTMLResponse:
        return templates.TemplateResponse("auto.html", {"request": request})


    @app.get("/scrapper/{z}/{x}/{y}")
    async def scrape_tile(z: int, x: int, y: int):
        nonlocal cache_hits, cache_misses
        collection = hashlib.md5((settings.SOURCE_URL).encode()).hexdigest()

        output_path = pathlib.Path(settings.OUTPUT_DIR) / collection / str(z) / str(x)
        output_path.mkdir(parents=True, exist_ok=True)
        tile_path = output_path / f"{y}.png"

        # Serve from local cache first if tile is already stored.
        if tile_path.is_file():
            cache_hits += 1
            total_requests = cache_hits + cache_misses
            hit_rate = cache_hits / total_requests if total_requests else 0.0
            return FileResponse(
                path=tile_path,
                media_type="image/png",
                headers={
                    "X-Cache": "HIT",
                    "X-Cache-Hit-Rate": f"{hit_rate:.6f}",
                    "X-Cache-Hits": str(cache_hits),
                    "X-Cache-Misses": str(cache_misses),
                },
            )

        cache_misses += 1

        url = settings.SOURCE_URL.format(x=x, y=y, z=z)
        response = requests.get(url, timeout=15)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=f"Failed to fetch tile from {url}")

        tile_path.write_bytes(response.content)

        content_type: Optional[str] = response.headers.get("content-type")
        if not content_type or "image" not in content_type:
            content_type = "image/png"

        total_requests = cache_hits + cache_misses
        hit_rate = cache_hits / total_requests if total_requests else 0.0
        return Response(
            content=response.content,
            media_type=content_type,
            headers={
                "X-Cache": "MISS",
                "X-Cache-Hit-Rate": f"{hit_rate:.6f}",
                "X-Cache-Hits": str(cache_hits),
                "X-Cache-Misses": str(cache_misses),
            },
        )


    @app.get("/scrapper/cache-stats")
    async def cache_stats():
        total_requests = cache_hits + cache_misses
        hit_rate = cache_hits / total_requests if total_requests else 0.0
        return {
            "hits": cache_hits,
            "misses": cache_misses,
            "total": total_requests,
            "hit_rate": hit_rate,
        }


    @app.post("/scrapper/get-tile-count")
    async def get_tile_count(request: TileCountRequest):
        collection = hashlib.md5((settings.SOURCE_URL).encode()).hexdigest()
        output_path = pathlib.Path(settings.OUTPUT_DIR) / collection / str(request.z)

        if not output_path.exists():
            return {"tile_count": 0}

        tile_count = sum(1 for _ in output_path.rglob("*.png"))
        return {"tile_count": tile_count}

    return app


if __name__ == "__main__":
    import uvicorn

    app = create_app()
    uvicorn.run(app, host=settings.HOST, port=settings.PORT)

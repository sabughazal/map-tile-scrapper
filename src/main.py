import sys
import hashlib
import pathlib
import requests
from typing import Optional
from pydantic import BaseModel
from fastapi import FastAPI, Query, Request

from fastapi.responses import HTMLResponse, FileResponse
from fastapi import HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


current_dir = pathlib.Path(__file__).parent
sys.path.append(str(current_dir.parent))

from src.config import settings
from src.geotiff_utils import GeotiffExportManager
from src.prefetch_utils import TilePrefetchManager, BackoffConfig, PrefetchJobConflict
from src.coverage_utils import CoverageIndex



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


class GeotiffExportRequest(BaseModel):
    extent: Extent
    z: int


class BackoffModel(BaseModel):
    enabled: bool = True
    window: int = 40
    threshold: int = 10
    step_ms: int = 250
    max_delay_ms: int = 2000


class AutoDownloadRequest(BaseModel):
    extent: Extent
    zoom_levels: list[int]
    backoff: Optional[BackoffModel] = None


def create_app() -> FastAPI:

    app = FastAPI(title="Map Tile Scrapper")
    cache_hits = 0
    cache_misses = 0
    geotiff_manager = GeotiffExportManager(
        output_dir=settings.OUTPUT_DIR,
        source_url=settings.SOURCE_URL,
        max_retries=3,
        disconnect_timeout_sec=5.0,
        max_download_workers=settings.GEOTIFF_MAX_WORKERS,
    )
    prefetch_manager = TilePrefetchManager(
        output_dir=settings.OUTPUT_DIR,
        source_url=settings.SOURCE_URL,
        max_retries=3,
        max_workers=settings.PREFETCH_MAX_WORKERS,
    )
    coverage_index = CoverageIndex(output_dir=settings.OUTPUT_DIR)
    # md5 is one-way, so record which URL owns this collection while we still know.
    coverage_index.write_source_url(
        hashlib.md5(settings.SOURCE_URL.encode()).hexdigest(), settings.SOURCE_URL
    )

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request, "index.html")


    @app.get("/auto", response_class=HTMLResponse)
    async def auto(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request, "auto.html")


    @app.get("/geotiff", response_class=HTMLResponse)
    async def geotiff(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request, "geotiff.html")


    @app.get("/datasets", response_class=HTMLResponse)
    async def datasets(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request, "datasets.html")


    # Coverage takes `collection` as a query param rather than sitting at
    # /scrapper/datasets/{collection}/coverage, which would have the same segment
    # count as /scrapper/{z}/{x}/{y} and get shadowed by it (422 on the int parse).
    @app.get("/scrapper/datasets")
    async def list_datasets():
        return {
            "datasets": [
                coverage_index.stats(collection)
                for collection in coverage_index.list_collections()
            ]
        }


    @app.get("/scrapper/datasets/coverage")
    async def dataset_coverage(collection: str):
        if collection not in coverage_index.list_collections():
            raise HTTPException(status_code=404, detail="Dataset not found")
        return coverage_index.coverage(collection)


    @app.get("/scrapper/{z}/{x}/{y}")
    async def scrape_tile(z: int, x: int, y: int):
        nonlocal cache_hits, cache_misses
        collection = hashlib.md5((settings.SOURCE_URL).encode()).hexdigest()

        output_path = pathlib.Path(settings.OUTPUT_DIR) / collection / str(z) / str(x)
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

        # Created only once the fetch succeeded, so failures leave no empty dirs.
        output_path.mkdir(parents=True, exist_ok=True)
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


    @app.get("/scrapper/geotiff-export")
    async def geotiff_export(
        z: int = Query(..., ge=0, le=24),
        min_x: float = Query(..., alias="extent.minX"),
        min_y: float = Query(..., alias="extent.minY"),
        max_x: float = Query(..., alias="extent.maxX"),
        max_y: float = Query(..., alias="extent.maxY"),
    ):
        grid = geotiff_manager.estimate_grid(
            z=z,
            min_x=min_x,
            min_y=min_y,
            max_x=max_x,
            max_y=max_y,
        )
        return grid


    @app.post("/scrapper/geotiff-export/start")
    async def geotiff_export_start(request: GeotiffExportRequest):
        job = geotiff_manager.start_job(
            z=request.z,
            min_x=request.extent.minX,
            min_y=request.extent.minY,
            max_x=request.extent.maxX,
            max_y=request.extent.maxY,
        )
        return job


    @app.get("/scrapper/geotiff-export/status")
    async def geotiff_export_status(job_id: str, last_seq: int = 0):
        status = geotiff_manager.poll_status(job_id=job_id, last_seq=last_seq)
        if status is None:
            raise HTTPException(status_code=404, detail="GeoTIFF export job not found")
        return status


    @app.post("/scrapper/geotiff-export/cancel")
    async def geotiff_export_cancel(job_id: str):
        cancelled = geotiff_manager.cancel_job(job_id)
        if not cancelled:
            raise HTTPException(status_code=404, detail="GeoTIFF export job not found")
        return {"cancelled": True, "job_id": job_id}


    @app.get("/scrapper/geotiff-file/{job_id}")
    async def geotiff_export_file(job_id: str):
        geotiff_path = geotiff_manager.get_geotiff_path(job_id)
        if geotiff_path is None or not geotiff_path.is_file():
            raise HTTPException(status_code=404, detail="GeoTIFF file not found")

        return FileResponse(
            path=geotiff_path,
            media_type="image/tiff",
            filename=geotiff_path.name,
        )

    @app.post("/scrapper/auto-download/start")
    async def auto_download_start(request: AutoDownloadRequest):
        if not request.zoom_levels:
            raise HTTPException(status_code=400, detail="Select at least one zoom level.")
        backoff = request.backoff or BackoffModel()
        try:
            job = prefetch_manager.start_job(
                zoom_levels=request.zoom_levels,
                min_x=request.extent.minX,
                min_y=request.extent.minY,
                max_x=request.extent.maxX,
                max_y=request.extent.maxY,
                backoff=BackoffConfig(**backoff.model_dump()),
            )
        except PrefetchJobConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return job

    @app.post("/scrapper/auto-download/pause")
    async def auto_download_pause(job_id: str):
        if not prefetch_manager.pause_job(job_id):
            raise HTTPException(status_code=409, detail="Job cannot be paused.")
        return {"paused": True, "job_id": job_id}

    @app.post("/scrapper/auto-download/resume")
    async def auto_download_resume(job_id: str):
        if not prefetch_manager.resume_job(job_id):
            raise HTTPException(status_code=409, detail="Job cannot be resumed.")
        return {"resumed": True, "job_id": job_id}

    @app.post("/scrapper/auto-download/cancel")
    async def auto_download_cancel(job_id: str):
        if not prefetch_manager.cancel_job(job_id):
            raise HTTPException(status_code=404, detail="Prefetch job not found.")
        return {"cancelled": True, "job_id": job_id}

    @app.get("/scrapper/auto-download/status")
    async def auto_download_status(job_id: str, last_seq: int = 0):
        status = prefetch_manager.poll_status(job_id=job_id, last_seq=last_seq)
        if status is None:
            raise HTTPException(status_code=404, detail="Prefetch job not found.")
        return status

    @app.get("/scrapper/auto-download/active")
    async def auto_download_active():
        return prefetch_manager.get_active_job() or {"job_id": None}

    return app


if __name__ == "__main__":
    import uvicorn

    app = create_app()
    uvicorn.run(app, host=settings.HOST, port=settings.PORT)

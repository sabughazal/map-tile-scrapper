import hashlib
import math
import os
import pathlib
import threading
import time
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

import numpy as np
import requests
import rasterio
from PIL import Image
from geoiters.tiles import TilesIterator
from geoiters.utils import Extent as GeoItersExtent
from rasterio.transform import from_bounds


@dataclass(frozen=True)
class TileTask:
    index: int
    x: int
    y: int
    z: int
    attempt: int = 0


def _tile_cache_path(output_dir: str, collection: str, z: int, x: int, y: int) -> pathlib.Path:
    return pathlib.Path(output_dir) / collection / str(z) / str(x) / f"{y}.png"


def _web_mercator_tile_bounds(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    """
    Calculate the geographic bounds (lat/lon) of a Web Mercator tile.
    Returns (min_lon, min_lat, max_lon, max_lat)
    """
    n = 2.0 ** z

    # Calculate longitude from tile x
    min_lon = (x / n) * 360.0 - 180.0
    max_lon = ((x + 1) / n) * 360.0 - 180.0

    # Calculate latitude from tile y (tile y increases downward)
    # Using Web Mercator projection
    lat_rad_min = math.atan(math.sinh(math.pi * (1.0 - 2.0 * (y + 1) / n)))
    lat_rad_max = math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n)))
    min_lat = math.degrees(lat_rad_min)
    max_lat = math.degrees(lat_rad_max)

    return (min_lon, min_lat, max_lon, max_lat)


def _calculate_tile_grid_bounds(tasks: list[TileTask]) -> tuple[float, float, float, float]:
    """
    Calculate the geographic bounds of a tile grid based on tile coordinates.
    Returns (min_lon, min_lat, max_lon, max_lat) of the combined tile extent.

    In Web Mercator tiles:
    - x increases from west (0) to east (2^z - 1)
    - y increases from north (0) to south (2^z - 1)
    """
    if not tasks:
        raise ValueError("No tasks provided")

    # All tasks should be at the same zoom level
    z = tasks[0].z

    # Find min/max tile coordinates
    x_coords = [t.x for t in tasks]
    y_coords = [t.y for t in tasks]

    min_x, max_x = min(x_coords), max(x_coords)
    min_y, max_y = min(y_coords), max(y_coords)

    # Get bounds from the actual tiles in the grid:
    # min_lon: western edge (left) = min_lon of leftmost tile (min_x)
    # max_lon: eastern edge (right) = max_lon of rightmost tile (max_x)
    # max_lat: northern edge (top) = max_lat of northernmost tile (min_y, since y=0 is north)
    # min_lat: southern edge (bottom) = min_lat of southernmost tile (max_y, since y increases southward)

    min_lon, _, _, _ = _web_mercator_tile_bounds(min_x, 0, z)
    _, _, max_lon, _ = _web_mercator_tile_bounds(max_x, 0, z)
    _, _, _, max_lat = _web_mercator_tile_bounds(0, min_y, z)
    _, min_lat, _, _ = _web_mercator_tile_bounds(0, max_y, z)

    return (min_lon, min_lat, max_lon, max_lat)


def _download_tile_worker(task: TileTask, source_url: str, output_dir: str, collection: str) -> dict[str, Any]:
    tile_path = _tile_cache_path(output_dir, collection, task.z, task.x, task.y)
    tile_path.parent.mkdir(parents=True, exist_ok=True)

    if tile_path.is_file():
        return {
            "ok": True,
            "cached": True,
            "index": task.index,
            "x": task.x,
            "y": task.y,
            "z": task.z,
            "path": str(tile_path),
        }

    try:
        url = source_url.format(x=task.x, y=task.y, z=task.z)
        response = requests.get(url, timeout=20)
        if response.status_code != 200:
            return {
                "ok": False,
                "index": task.index,
                "x": task.x,
                "y": task.y,
                "z": task.z,
                "status_code": response.status_code,
                "error": f"Upstream responded with {response.status_code}",
            }

        tile_path.write_bytes(response.content)
        return {
            "ok": True,
            "cached": False,
            "index": task.index,
            "x": task.x,
            "y": task.y,
            "z": task.z,
            "path": str(tile_path),
        }
    except Exception as exc:  # pragma: no cover - defensive worker path
        return {
            "ok": False,
            "index": task.index,
            "x": task.x,
            "y": task.y,
            "z": task.z,
            "status_code": 0,
            "error": str(exc),
        }


def _build_geotiff(
    *,
    output_dir: str,
    collection: str,
    z: int,
    rows: int,
    cols: int,
    tasks: list[TileTask],
) -> pathlib.Path:
    if not tasks:
        raise ValueError("No tiles to export.")

    first_path = _tile_cache_path(output_dir, collection, tasks[0].z, tasks[0].x, tasks[0].y)
    if not first_path.exists():
        raise FileNotFoundError(f"Missing tile file: {first_path}")

    with Image.open(first_path) as first_img:
        tile_array = np.asarray(first_img.convert("RGBA"), dtype=np.uint8)
        tile_height, tile_width, channels = tile_array.shape

    out_height = rows * tile_height
    out_width = cols * tile_width
    mosaic = np.zeros((channels, out_height, out_width), dtype=np.uint8)

    for task in tasks:
        tile_path = _tile_cache_path(output_dir, collection, task.z, task.x, task.y)
        if not tile_path.exists():
            raise FileNotFoundError(f"Missing tile file: {tile_path}")

        row = task.index // cols
        col = task.index % cols

        y0 = row * tile_height
        y1 = y0 + tile_height
        x0 = col * tile_width
        x1 = x0 + tile_width

        with Image.open(tile_path) as tile_img:
            arr = np.asarray(tile_img.convert("RGBA"), dtype=np.uint8)

        mosaic[:, y0:y1, x0:x1] = np.transpose(arr, (2, 0, 1))

    # Calculate the actual geographic bounds of the tile grid (not user's drawn extent)
    min_lon, min_lat, max_lon, max_lat = _calculate_tile_grid_bounds(tasks)
    transform = from_bounds(min_lon, min_lat, max_lon, max_lat, out_width, out_height)

    geotiff_dir = pathlib.Path(output_dir) / "geotiffs"
    geotiff_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"geotiff_z{z}_{int(time.time())}_{uuid.uuid4().hex[:10]}.tif"
    geotiff_path = geotiff_dir / file_name

    with rasterio.open(
        geotiff_path,
        "w",
        driver="GTiff",
        width=out_width,
        height=out_height,
        count=channels,
        dtype=np.uint8,
        crs="EPSG:4326",
        transform=transform,
        compress="deflate",
    ) as dst:
        for band in range(channels):
            dst.write(mosaic[band], band + 1)

    return geotiff_path


class GeotiffExportManager:
    def __init__(
        self,
        *,
        output_dir: str,
        source_url: str,
        max_retries: int = 3,
        disconnect_timeout_sec: float = 5.0,
        max_download_workers: int | None = None,
    ):
        self._output_dir = output_dir
        self._source_url = source_url
        self._collection = hashlib.md5(source_url.encode()).hexdigest()
        self._max_retries = max(1, max_retries)
        self._disconnect_timeout_sec = disconnect_timeout_sec
        self._max_download_workers = max_download_workers or min(8, max(2, os.cpu_count() or 2))

        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._runner = ThreadPoolExecutor(max_workers=2)

    def _new_extent(self, min_x: float, min_y: float, max_x: float, max_y: float) -> GeoItersExtent:
        return GeoItersExtent(min_x=min_x, min_y=min_y, max_x=max_x, max_y=max_y, crs="EPSG:4326")

    def _append_event(self, job: dict[str, Any], payload: dict[str, Any]) -> None:
        job["seq"] += 1
        event = {"seq": job["seq"], **payload}
        job["events"].append(event)

    def estimate_grid(self, *, z: int, min_x: float, min_y: float, max_x: float, max_y: float) -> dict[str, int]:
        ext = self._new_extent(min_x=min_x, min_y=min_y, max_x=max_x, max_y=max_y)
        itr = TilesIterator(ext, zoom_level=z)
        return {
            "rows": int(itr._height_in_tiles),
            "cols": int(itr._width_in_tiles),
        }

    def start_job(self, *, z: int, min_x: float, min_y: float, max_x: float, max_y: float) -> dict[str, Any]:
        ext = self._new_extent(min_x=min_x, min_y=min_y, max_x=max_x, max_y=max_y)
        itr = TilesIterator(ext, zoom_level=z)
        rows = int(itr._height_in_tiles)
        cols = int(itr._width_in_tiles)

        tile_coords = list(itr)
        tasks = [
            TileTask(index=i, x=int(x), y=int(y), z=int(zoom))
            for i, (x, y, zoom) in enumerate(tile_coords)
        ]

        job_id = uuid.uuid4().hex
        now = time.monotonic()
        job = {
            "id": job_id,
            "state": "queued",
            "error": None,
            "created_at": now,
            "last_seen": now,
            "seq": 0,
            "events": [],
            "completed": 0,
            "failed": 0,
            "total": len(tasks),
            "rows": rows,
            "cols": cols,
            "z": z,
            "extent": ext,
            "tasks": tasks,
            "geotiff_path": None,
            "cancel_event": threading.Event(),
        }

        with self._lock:
            self._jobs[job_id] = job

        self._runner.submit(self._run_job, job_id)

        return {
            "job_id": job_id,
            "rows": rows,
            "cols": cols,
            "total": len(tasks),
        }

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            job["cancel_event"].set()
            if job["state"] in {"queued", "running"}:
                job["state"] = "cancelled"
        return True

    def poll_status(self, *, job_id: str, last_seq: int) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None

            job["last_seen"] = time.monotonic()
            events = [event for event in job["events"] if event["seq"] > last_seq]
            geotiff_name = pathlib.Path(job["geotiff_path"]).name if job["geotiff_path"] else None

            return {
                "job_id": job_id,
                "state": job["state"],
                "error": job["error"],
                "rows": job["rows"],
                "cols": job["cols"],
                "total": job["total"],
                "completed": job["completed"],
                "failed": job["failed"],
                "last_seq": job["seq"],
                "events": events,
                "geotiff_name": geotiff_name,
                "download_url": f"/scrapper/geotiff-file/{job_id}" if geotiff_name else None,
            }

    def get_geotiff_path(self, job_id: str) -> pathlib.Path | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or not job.get("geotiff_path"):
                return None
            return pathlib.Path(job["geotiff_path"])

    def _set_job_error(self, job_id: str, message: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job["error"] = message
            if job["state"] not in {"completed", "cancelled"}:
                job["state"] = "failed"

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job["state"] = "running"
            tasks: list[TileTask] = list(job["tasks"])

        pending: deque[TileTask] = deque(tasks)
        in_flight: dict[Future, TileTask] = {}
        failed_indices: set[int] = set()
        cancelled = False

        pool = ProcessPoolExecutor(max_workers=self._max_download_workers)
        try:
            while pending or in_flight:
                with self._lock:
                    job = self._jobs.get(job_id)
                    if not job:
                        cancelled = True
                        break
                    no_client = (time.monotonic() - job["last_seen"]) > self._disconnect_timeout_sec
                    cancelled = bool(job["cancel_event"].is_set() or no_client)
                    if no_client:
                        job["state"] = "cancelled"

                if cancelled:
                    break

                while pending and len(in_flight) < (self._max_download_workers * 3):
                    task = pending.popleft()
                    fut = pool.submit(
                        _download_tile_worker,
                        task,
                        self._source_url,
                        self._output_dir,
                        self._collection,
                    )
                    in_flight[fut] = task

                if not in_flight:
                    continue

                done, _ = wait(set(in_flight.keys()), timeout=0.25, return_when=FIRST_COMPLETED)
                for fut in done:
                    task = in_flight.pop(fut)
                    try:
                        result = fut.result()
                    except Exception as exc:  # pragma: no cover - defensive future path
                        result = {
                            "ok": False,
                            "index": task.index,
                            "x": task.x,
                            "y": task.y,
                            "z": task.z,
                            "status_code": 0,
                            "error": str(exc),
                        }

                    if result.get("ok"):
                        event_status = "cached" if result.get("cached") else "downloaded"
                        with self._lock:
                            job = self._jobs.get(job_id)
                            if not job:
                                continue
                            job["completed"] += 1
                            self._append_event(
                                job,
                                {
                                    "type": "tile",
                                    "status": event_status,
                                    "index": task.index,
                                    "x": task.x,
                                    "y": task.y,
                                    "z": task.z,
                                },
                            )
                        continue

                    next_attempt = task.attempt + 1
                    if next_attempt < self._max_retries:
                        pending.append(
                            TileTask(index=task.index, x=task.x, y=task.y, z=task.z, attempt=next_attempt)
                        )
                        with self._lock:
                            job = self._jobs.get(job_id)
                            if not job:
                                continue
                            self._append_event(
                                job,
                                {
                                    "type": "tile",
                                    "status": "retrying",
                                    "index": task.index,
                                    "attempt": next_attempt,
                                },
                            )
                    else:
                        failed_indices.add(task.index)
                        with self._lock:
                            job = self._jobs.get(job_id)
                            if not job:
                                continue
                            job["failed"] += 1
                            self._append_event(
                                job,
                                {
                                    "type": "tile",
                                    "status": "failed",
                                    "index": task.index,
                                    "x": task.x,
                                    "y": task.y,
                                    "z": task.z,
                                    "error": result.get("error"),
                                },
                            )
        finally:
            pool.shutdown(wait=not cancelled, cancel_futures=cancelled)

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            if cancelled:
                if job["state"] != "cancelled":
                    job["state"] = "cancelled"
                job["error"] = "Export cancelled (client disconnected or request cancelled)."
                return

            if failed_indices:
                job["state"] = "failed"
                job["error"] = f"Failed to download {len(failed_indices)} tiles after retries."
                return

        try:
            with self._lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                if job["cancel_event"].is_set():
                    job["state"] = "cancelled"
                    job["error"] = "Export cancelled."
                    return

                rows = int(job["rows"])
                cols = int(job["cols"])
                z = int(job["z"])
                extent = job["extent"]
                ordered_tasks = sorted(job["tasks"], key=lambda tile_task: tile_task.index)

            geotiff_path = _build_geotiff(
                output_dir=self._output_dir,
                collection=self._collection,
                z=z,
                rows=rows,
                cols=cols,
                tasks=ordered_tasks,
            )

            with self._lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                job["geotiff_path"] = str(geotiff_path)
                job["state"] = "completed"
                self._append_event(job, {"type": "job", "status": "completed"})
        except Exception as exc:  # pragma: no cover - defensive geotiff build path
            self._set_job_error(job_id, f"Failed to build GeoTIFF: {exc}")

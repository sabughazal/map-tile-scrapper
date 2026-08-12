import os
import time
import uuid
import hashlib
import pathlib
import threading
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from geoiters.tiles import TilesIterator
from geoiters.utils import Extent as GeoItersExtent

from src.geotiff_utils import TileTask, _download_tile_worker


ACTIVE_STATES = {"queued", "running", "paused"}


@dataclass
class BackoffConfig:
    enabled: bool = True
    window: int = 40
    threshold: int = 10
    step_ms: int = 250
    max_delay_ms: int = 2000


class PrefetchJobConflict(RuntimeError):
    """Raised when a prefetch job is started while another is still active."""


class TilePrefetchManager:
    """Runs a single background tile-prefetch job with pause/resume/cancel and 404 backoff."""

    def __init__(
        self,
        *,
        output_dir: str,
        source_url: str,
        max_retries: int = 3,
        max_workers: int | None = None,
    ):
        self._output_dir = output_dir
        self._source_url = source_url
        self._collection = hashlib.md5(source_url.encode()).hexdigest()
        self._max_retries = max(1, max_retries)
        self._max_workers = max_workers or min(8, max(2, os.cpu_count() or 2))

        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._active_job_id: str | None = None
        self._runner = ThreadPoolExecutor(max_workers=1)

    def _new_extent(self, min_x: float, min_y: float, max_x: float, max_y: float) -> GeoItersExtent:
        return GeoItersExtent(min_x=min_x, min_y=min_y, max_x=max_x, max_y=max_y, crs="EPSG:4326")

    def _append_event(self, job: dict[str, Any], payload: dict[str, Any]) -> None:
        job["seq"] += 1
        job["events"].append({"seq": job["seq"], **payload})

    def _build_tasks(
        self, ext: GeoItersExtent, zoom_levels: list[int]
    ) -> tuple[list[TileTask], dict[int, int]]:
        tasks: list[TileTask] = []
        per_zoom: dict[int, int] = {}
        index = 0
        for z in zoom_levels:
            itr = TilesIterator(ext, zoom_level=z)
            count = 0
            for x, y, zoom in itr:
                tasks.append(TileTask(index=index, x=int(x), y=int(y), z=int(zoom)))
                index += 1
                count += 1
            per_zoom[z] = count
        return tasks, per_zoom

    def start_job(
        self,
        *,
        zoom_levels: list[int],
        min_x: float,
        min_y: float,
        max_x: float,
        max_y: float,
        backoff: BackoffConfig,
    ) -> dict[str, Any]:
        job_id = uuid.uuid4().hex
        # Atomically reserve the single active slot before the slow task build.
        with self._lock:
            active = self._jobs.get(self._active_job_id) if self._active_job_id else None
            if active and active["state"] in ACTIVE_STATES:
                raise PrefetchJobConflict("A prefetch job is already running. Cancel it before starting a new one.")
            job = {
                "id": job_id,
                "state": "queued",
                "error": None,
                "seq": 0,
                "events": [],
                "completed": 0,
                "failed": 0,
                "total": 0,
                "zoom_levels": list(zoom_levels),
                "per_zoom": {},
                "tasks": [],
                "backoff": backoff,
                "current_delay_ms": 0,
                "cancel_event": threading.Event(),
                "pause_event": threading.Event(),
            }
            self._jobs[job_id] = job
            self._active_job_id = job_id

        ext = self._new_extent(min_x=min_x, min_y=min_y, max_x=max_x, max_y=max_y)
        tasks, per_zoom = self._build_tasks(ext, zoom_levels)

        with self._lock:
            job["tasks"] = tasks
            job["total"] = len(tasks)
            job["per_zoom"] = per_zoom

        self._runner.submit(self._run_job, job_id)

        return {
            "job_id": job_id,
            "total": len(tasks),
            "zoom_levels": list(zoom_levels),
            "per_zoom": per_zoom,
        }

    def pause_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job["state"] not in {"queued", "running"}:
                return False
            job["pause_event"].set()
        return True

    def resume_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job["state"] != "paused":
                return False
            job["pause_event"].clear()
        return True

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            job["cancel_event"].set()
            job["pause_event"].clear()
            if job["state"] in ACTIVE_STATES:
                job["state"] = "cancelled"
        return True

    def _serialize(self, job: dict[str, Any], last_seq: int) -> dict[str, Any]:
        events = [event for event in job["events"] if event["seq"] > last_seq]
        return {
            "job_id": job["id"],
            "state": job["state"],
            "error": job["error"],
            "total": job["total"],
            "completed": job["completed"],
            "failed": job["failed"],
            "zoom_levels": job["zoom_levels"],
            "per_zoom": job["per_zoom"],
            "current_delay_ms": job["current_delay_ms"],
            "last_seq": job["seq"],
            "events": events,
        }

    def poll_status(self, *, job_id: str, last_seq: int) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return self._serialize(job, last_seq)

    def get_active_job(self) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(self._active_job_id) if self._active_job_id else None
            if job is None:
                return None
            return self._serialize(job, 0)

    def _recompute_delay(self, job: dict[str, Any], recent: deque[int]) -> None:
        cfg: BackoffConfig = job["backoff"]
        previous = job["current_delay_ms"]
        if not cfg.enabled:
            delay = 0
        else:
            count_404 = sum(recent)
            delay = previous
            if count_404 >= cfg.threshold:
                delay = min(previous + cfg.step_ms, cfg.max_delay_ms)
            elif count_404 <= cfg.threshold // 2:
                delay = max(previous - cfg.step_ms, 0)

        if delay != previous:
            with self._lock:
                job["current_delay_ms"] = delay
                self._append_event(job, {"type": "backoff", "delay_ms": delay})

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if job["cancel_event"].is_set():
                job["state"] = "cancelled"
                return
            job["state"] = "running"
            cfg: BackoffConfig = job["backoff"]
            tasks: list[TileTask] = list(job["tasks"])

        pending: deque[TileTask] = deque(tasks)
        in_flight: dict[Future, TileTask] = {}
        failed_indices: set[int] = set()
        recent: deque[int] = deque(maxlen=max(1, cfg.window))
        cancelled = False

        pool = ThreadPoolExecutor(max_workers=self._max_workers)
        try:
            while pending or in_flight:
                with self._lock:
                    job = self._jobs.get(job_id)
                    if not job:
                        cancelled = True
                        break
                    if job["cancel_event"].is_set():
                        cancelled = True
                    paused = job["pause_event"].is_set()
                    delay_ms = job["current_delay_ms"]

                if cancelled:
                    break

                # Reflect pause/resume in job state; keep draining in-flight while paused.
                with self._lock:
                    if paused and job["state"] == "running":
                        job["state"] = "paused"
                        self._append_event(job, {"type": "job", "status": "paused"})
                    elif not paused and job["state"] == "paused":
                        job["state"] = "running"
                        self._append_event(job, {"type": "job", "status": "resumed"})

                if not paused:
                    cap = self._max_workers if delay_ms > 0 else self._max_workers * 3
                    while pending and len(in_flight) < cap:
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
                    time.sleep(0.1)
                    continue

                done, _ = wait(set(in_flight.keys()), timeout=0.25, return_when=FIRST_COMPLETED)
                for fut in done:
                    task = in_flight.pop(fut)
                    try:
                        result = fut.result()
                    except Exception as exc:  # pragma: no cover - defensive future path
                        result = {"ok": False, "status_code": 0, "error": str(exc)}

                    status = 200 if result.get("ok") else int(result.get("status_code") or 0)
                    recent.append(1 if status == 404 else 0)
                    self._recompute_delay(job, recent)

                    if result.get("ok"):
                        event_status = "cached" if result.get("cached") else "downloaded"
                        with self._lock:
                            job = self._jobs.get(job_id)
                            if not job:
                                continue
                            job["completed"] += 1
                            self._append_event(
                                job,
                                {"type": "tile", "status": event_status, "x": task.x, "y": task.y, "z": task.z},
                            )
                        continue

                    next_attempt = task.attempt + 1
                    if next_attempt < self._max_retries:
                        pending.append(
                            TileTask(index=task.index, x=task.x, y=task.y, z=task.z, attempt=next_attempt)
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
                                    "x": task.x,
                                    "y": task.y,
                                    "z": task.z,
                                    "error": result.get("error"),
                                },
                            )

                if delay_ms > 0 and not paused:
                    time.sleep(delay_ms / 1000.0)
        finally:
            pool.shutdown(wait=not cancelled, cancel_futures=cancelled)

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if cancelled:
                job["state"] = "cancelled"
                job["error"] = "Prefetch cancelled."
                self._append_event(job, {"type": "job", "status": "cancelled"})
            elif failed_indices:
                job["state"] = "completed"
                job["error"] = f"Completed with {len(failed_indices)} failed tiles."
                self._append_event(job, {"type": "job", "status": "completed"})
            else:
                job["state"] = "completed"
                self._append_event(job, {"type": "job", "status": "completed"})

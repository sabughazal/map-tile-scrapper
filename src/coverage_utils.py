import json
import os
import pathlib
import threading
from typing import Any, Optional

from src.geotiff_utils import _web_mercator_tile_bounds


INDEX_FILE = ".coverage-index.json"
SOURCE_FILE = "source.json"
INDEX_VERSION = 1

# Upper bound on polygons emitted per zoom level. Coverage is rolled up to a
# coarser zoom until it fits, so the map never receives more than this.
MAX_COVERAGE_CELLS = 4000

# Tile x-columns further apart than this are treated as separate regions.
CLUSTER_GAP = 50


def _encode_runs(values: set[int]) -> list[list[int]]:
    """Collapse a set of ints into inclusive [start, end] runs."""
    runs: list[list[int]] = []
    for value in sorted(values):
        if runs and value == runs[-1][1] + 1:
            runs[-1][1] = value
        else:
            runs.append([value, value])
    return runs


def _decode_runs(runs: list[list[int]]) -> set[int]:
    values: set[int] = set()
    for start, end in runs:
        values.update(range(start, end + 1))
    return values


class CoverageIndex:
    """
    Tracks which tiles exist on disk per collection.

    The directory layout (collection/z/x/y.png) is the index, so no tile is ever
    opened. Freshness comes from x-directory mtimes: adding or removing a tile
    bumps its parent directory, so only changed directories are re-read. The
    index is persisted per collection to avoid a cold full scan on restart.
    """

    def __init__(self, *, output_dir: str):
        self._output_dir = pathlib.Path(output_dir)
        self._lock = threading.Lock()
        # collection -> {"dirs": {"z/x": {"m": mtime_ns, "b": bytes, "y": runs}}}
        self._indexes: dict[str, dict[str, Any]] = {}
        # Bumped whenever a refresh sees a change; derived results memoize on it so
        # a poll that finds nothing new costs only the x-directory stat pass.
        self._revisions: dict[str, int] = {}
        self._derived: dict[str, tuple[int, str, Any]] = {}

    # ---------------------------------------------------------------- discovery

    def _collection_dir(self, collection: str) -> pathlib.Path:
        return self._output_dir / collection

    def list_collections(self) -> list[str]:
        """Directories holding numeric zoom subdirectories (excludes geotiffs/)."""
        if not self._output_dir.is_dir():
            return []

        collections = []
        for entry in os.scandir(self._output_dir):
            if not entry.is_dir() or entry.name.startswith("."):
                continue
            if any(sub.is_dir() and sub.name.isdigit() for sub in os.scandir(entry.path)):
                collections.append(entry.name)
        return sorted(collections)

    def read_source_url(self, collection: str) -> Optional[str]:
        source_path = self._collection_dir(collection) / SOURCE_FILE
        try:
            with source_path.open("r", encoding="utf-8") as handle:
                url = json.load(handle).get("source_url")
            return url if isinstance(url, str) else None
        except (OSError, ValueError):
            return None

    def write_source_url(self, collection: str, source_url: str) -> None:
        """Record which URL produced a collection; md5 alone is not reversible."""
        collection_dir = self._collection_dir(collection)
        try:
            collection_dir.mkdir(parents=True, exist_ok=True)
            if self.read_source_url(collection) == source_url:
                return
            self._atomic_write(collection_dir / SOURCE_FILE, {"source_url": source_url})
        except OSError:
            pass

    # ------------------------------------------------------------- persistence

    @staticmethod
    def _atomic_write(path: pathlib.Path, payload: dict[str, Any]) -> None:
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
        os.replace(tmp_path, path)

    def _load_index(self, collection: str) -> dict[str, Any]:
        cached = self._indexes.get(collection)
        if cached is not None:
            return cached

        index: dict[str, Any] = {"dirs": {}}
        index_path = self._collection_dir(collection) / INDEX_FILE
        try:
            with index_path.open("r", encoding="utf-8") as handle:
                stored = json.load(handle)
            if stored.get("version") == INDEX_VERSION and isinstance(stored.get("dirs"), dict):
                index["dirs"] = stored["dirs"]
        except (OSError, ValueError):
            pass

        self._indexes[collection] = index
        return index

    def _save_index(self, collection: str, index: dict[str, Any]) -> None:
        payload = {
            "version": INDEX_VERSION,
            "source_url": self.read_source_url(collection),
            "dirs": index["dirs"],
        }
        try:
            self._atomic_write(self._collection_dir(collection) / INDEX_FILE, payload)
        except OSError:
            pass

    # ------------------------------------------------------------------ refresh

    def refresh(self, collection: str) -> dict[str, Any]:
        """
        Bring the index up to date. Costs one stat per x-directory when nothing
        changed; only directories whose mtime moved are re-read.
        """
        with self._lock:
            index = self._load_index(collection)
            dirs: dict[str, Any] = index["dirs"]
            collection_dir = self._collection_dir(collection)

            if not collection_dir.is_dir():
                return index

            changed = False
            live_keys: set[str] = set()

            for zdir in os.scandir(collection_dir):
                if not zdir.is_dir() or not zdir.name.isdigit():
                    continue

                for xdir in os.scandir(zdir.path):
                    if not xdir.is_dir() or not xdir.name.isdigit():
                        continue

                    key = f"{zdir.name}/{xdir.name}"
                    mtime = xdir.stat().st_mtime_ns
                    entry = dirs.get(key)

                    if entry is not None and entry.get("m") == mtime:
                        live_keys.add(key)
                        continue

                    ys: set[int] = set()
                    total_bytes = 0
                    for tile in os.scandir(xdir.path):
                        if not tile.name.endswith(".png"):
                            continue
                        stem = tile.name[:-4]
                        if not stem.isdigit():
                            continue
                        ys.add(int(stem))
                        total_bytes += tile.stat().st_size

                    changed = True
                    # Empty dirs are litter from failed fetches. They are still recorded
                    # (with mtime) so later refreshes skip them instead of rescanning
                    # forever; _tiles_by_zoom drops them so they never count as coverage.
                    dirs[key] = {"m": mtime, "b": total_bytes, "y": _encode_runs(ys)}
                    live_keys.add(key)

            for stale in set(dirs) - live_keys:
                del dirs[stale]
                changed = True

            if changed:
                self._revisions[collection] = self._revisions.get(collection, 0) + 1
                self._save_index(collection, index)

            return index

    def _memoized(self, collection: str, kind: str, build) -> Any:
        self.refresh(collection)
        with self._lock:
            revision = self._revisions.get(collection, 0)
            cached = self._derived.get(f"{collection}:{kind}")
            if cached is not None and cached[0] == revision:
                return cached[2]
            index = self._load_index(collection)
        value = build(index)
        with self._lock:
            self._derived[f"{collection}:{kind}"] = (revision, kind, value)
        return value

    # ------------------------------------------------------------------ metrics

    def _tiles_by_zoom(self, index: dict[str, Any]) -> dict[int, dict[int, set[int]]]:
        by_zoom: dict[int, dict[int, set[int]]] = {}
        for key, entry in index["dirs"].items():
            if not entry["y"]:
                continue
            z_text, x_text = key.split("/")
            by_zoom.setdefault(int(z_text), {})[int(x_text)] = _decode_runs(entry["y"])
        return by_zoom

    def stats(self, collection: str) -> dict[str, Any]:
        return self._memoized(collection, "stats", lambda index: self._stats(collection, index))

    def _stats(self, collection: str, index: dict[str, Any]) -> dict[str, Any]:
        by_zoom = self._tiles_by_zoom(index)

        bytes_by_zoom: dict[int, int] = {}
        for key, entry in index["dirs"].items():
            if not entry["y"]:
                continue
            z = int(key.split("/")[0])
            bytes_by_zoom[z] = bytes_by_zoom.get(z, 0) + int(entry.get("b", 0))

        zooms = sorted(by_zoom)
        levels = []
        total_tiles = 0
        total_bytes = 0

        for z in zooms:
            columns = by_zoom[z]
            tiles = sum(len(ys) for ys in columns.values())
            total_tiles += tiles
            total_bytes += bytes_by_zoom.get(z, 0)

            xs = sorted(columns)
            all_y = [y for ys in columns.values() for y in ys]
            width = xs[-1] - xs[0] + 1
            height = max(all_y) - min(all_y) + 1
            footprint = width * height

            clusters = 1
            for previous, current in zip(xs, xs[1:]):
                if current - previous > CLUSTER_GAP:
                    clusters += 1

            parent_columns = by_zoom.get(z - 1)
            child_fill_pct = None
            full_parents_pct = None
            if parent_columns:
                parent_tiles = sum(len(ys) for ys in parent_columns.values())
                child_fill_pct = 100.0 * tiles / (4 * parent_tiles)
                complete = 0
                for px, pys in parent_columns.items():
                    left = columns.get(2 * px, ())
                    right = columns.get(2 * px + 1, ())
                    for py in pys:
                        if (
                            2 * py in left
                            and 2 * py + 1 in left
                            and 2 * py in right
                            and 2 * py + 1 in right
                        ):
                            complete += 1
                full_parents_pct = 100.0 * complete / parent_tiles

            levels.append(
                {
                    "z": z,
                    "tiles": tiles,
                    "bytes": bytes_by_zoom.get(z, 0),
                    "width": width,
                    "height": height,
                    "clusters": clusters,
                    "world_pct": 100.0 * tiles / (4 ** z),
                    "footprint_pct": 100.0 * tiles / footprint,
                    # A footprint spanning disjoint regions makes footprint_pct
                    # meaningless; the UI greys it out when this is set.
                    "footprint_reliable": clusters == 1,
                    "child_fill_pct": child_fill_pct,
                    "full_parents_pct": full_parents_pct,
                }
            )

        return {
            "collection": collection,
            "source_url": self.read_source_url(collection),
            "total_tiles": total_tiles,
            "total_bytes": total_bytes,
            "min_zoom": zooms[0] if zooms else None,
            "max_zoom": zooms[-1] if zooms else None,
            "levels": levels,
        }

    # ----------------------------------------------------------------- coverage

    def coverage(self, collection: str) -> dict[str, Any]:
        """Coverage polygons per zoom, rolled up so each zoom stays under the cap."""
        return self._memoized(collection, "coverage", self._coverage)

    def _coverage(self, index: dict[str, Any]) -> dict[str, Any]:
        by_zoom = self._tiles_by_zoom(index)

        result: dict[str, Any] = {}
        for z, columns in by_zoom.items():
            tiles = [(x, y) for x, ys in columns.items() for y in ys]
            if not tiles:
                continue

            # Roll up to the finest zoom whose cell count fits the cap.
            za = z
            while za > 0:
                shift = z - za
                if len({(x >> shift, y >> shift) for x, y in tiles}) <= MAX_COVERAGE_CELLS:
                    break
                za -= 1

            shift = z - za
            counts: dict[tuple[int, int], int] = {}
            for x, y in tiles:
                cell = (x >> shift, y >> shift)
                counts[cell] = counts.get(cell, 0) + 1

            capacity = 4 ** shift
            cells = []
            for (cx, cy), count in counts.items():
                min_lon, min_lat, max_lon, max_lat = _web_mercator_tile_bounds(cx, cy, za)
                cells.append(
                    {
                        "bounds": [min_lon, min_lat, max_lon, max_lat],
                        "ratio": count / capacity,
                    }
                )

            result[str(z)] = {"agg_zoom": za, "tiles": len(tiles), "cells": cells}

        return result

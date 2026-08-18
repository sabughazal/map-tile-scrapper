# Map Tile Scrapper

FastAPI service that proxies XYZ map tiles, caches them on disk, exposes cache metrics, and provides four UI pages:

- `/` tile preview page with cache stats
- `/auto` rectangle-based tile prefetch page
- `/geotiff` GeoTIFF export planner with a live tile-matrix view
- `/datasets` dataset explorer: per-zoom completeness and coverage on the map

## 1. Prerequisites

- Python 3.10+
- `pip`

## 2. Install Dependencies

From the project root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Configure Environment

The app reads configuration from `.env` (see `.env.example`).

Create your local config:

```bash
cp .env.example .env
```

Then edit `.env` values as needed.

### Environment Variables

- `HOST`: bind address for local run (example: `0.0.0.0`)
- `PORT`: app port when running via `python src/main.py`
- `OUTPUT_DIR`: folder used to store cached tiles
- `SOURCE_URL`: upstream XYZ template URL
	- Must include `{z}`, `{x}`, `{y}` placeholders
	- Example: `https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}`
- `WEB_CONCURRENCY`: Gunicorn worker count in Docker (keep at `1`; job state is in-memory per process)
- `APP_PORT`: host port published by docker compose (container listens on `8000`)
- `GEOTIFF_MAX_WORKERS`: concurrent tile downloads for GeoTIFF export (empty = auto from CPU count)
- `PREFETCH_MAX_WORKERS`: concurrent tile downloads for auto prefetch (empty = auto from CPU count)

## 4. Run the Application

### Option A: Run with Python entrypoint (uses `.env` `HOST` and `PORT`)

```bash
python src/main.py
```

### Option B: Run with Uvicorn factory command

```bash
uvicorn src.main:create_app --factory --host 0.0.0.0 --port 8000
```

### Option C: Use existing script

```bash
bash run.sh
```

Note: `run.sh` currently starts on port `8000` with `--workers 64`, which overrides `.env` `PORT`.

## 5. Verify the Service

After startup, open:

- `http://localhost:8000/` (if running on 8000)
- `http://localhost:8100/` (if running via `python src/main.py` and default `.env`)

Core pages:

- `/` map preview UI
- `/auto` auto tile download UI
- `/geotiff` GeoTIFF export UI
- `/datasets` dataset coverage explorer

## 6. API Endpoints

### Tile Endpoint

- `GET /scrapper/{z}/{x}/{y}`

Behavior:

1. Checks if tile exists in local cache.
2. If yes: returns cached image (`X-Cache: HIT`).
3. If no: fetches from `SOURCE_URL`, stores it, returns image (`X-Cache: MISS`).

Response headers include:

- `X-Cache`
- `X-Cache-Hit-Rate`
- `X-Cache-Hits`
- `X-Cache-Misses`

### Cache Stats

- `GET /scrapper/cache-stats`

Returns:

```json
{
	"hits": 0,
	"misses": 0,
	"total": 0,
	"hit_rate": 0.0
}
```

### Tile Count

- `POST /scrapper/get-tile-count`

Request body:

```json
{
	"extent": {
		"minX": 53.9,
		"minY": 23.9,
		"maxX": 54.1,
		"maxY": 24.1
	},
	"z": 10
}
```

Response body:

```json
{
	"tile_count": 123
}
```

### Datasets

- `GET /scrapper/datasets`

Returns one entry per cached collection with totals and a per-zoom breakdown:

```json
{
	"datasets": [
		{
			"collection": "184c8b2b1b08f9b9a3281aee074734de",
			"source_url": "https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
			"total_tiles": 439808,
			"total_bytes": 4824375296,
			"min_zoom": 0,
			"max_zoom": 21,
			"levels": [
				{
					"z": 10,
					"tiles": 7853,
					"bytes": 91234567,
					"width": 514,
					"height": 561,
					"clusters": 3,
					"world_pct": 0.7489,
					"footprint_pct": 2.72,
					"footprint_reliable": false,
					"child_fill_pct": 90.8,
					"full_parents_pct": 82.3
				}
			]
		}
	]
}
```

Completeness is reported four ways, because no single denominator fits every zoom:

- `world_pct`: share of the whole world at that zoom. Meaningful where a full pyramid exists (z0-7 here); reads ~0 for regional data.
- `footprint_pct`: share of the bounding box of cached tiles. `footprint_reliable` is `false` when the tiles form more than one region (`clusters` > 1), which makes the bounding box a meaningless denominator.
- `child_fill_pct`: tiles at this zoom relative to 4x the level above. Can exceed 100% when tiles exist whose parents were never cached.
- `full_parents_pct`: share of tiles one level up that have all four children cached. The one that answers "can I zoom in everywhere I can currently see?", and the only one unaffected by disjoint regions.

No single cross-zoom percentage is reported: summing tiles across zooms is dominated by the deepest level and means nothing.

- `GET /scrapper/datasets/coverage?collection=<md5>`

Coverage polygons per zoom, for drawing on a map:

```json
{
	"11": {
		"agg_zoom": 9,
		"tiles": 29700,
		"cells": [{ "bounds": [54.0, 24.0, 54.7, 24.7], "ratio": 0.875 }]
	}
}
```

Tiles are rolled up to a coarser `agg_zoom` until a zoom fits under 4000 cells, so the payload stays small at every level. `ratio` is the share of that cell's tiles actually cached, and drives fill opacity in the UI.

### Coverage Index

Coverage is derived from the directory layout alone; no tile is ever opened. Freshness comes from `x`-directory mtimes, so a request that finds nothing new costs one stat per `x`-directory (~10 ms for 440k tiles) and only changed directories are re-read. There is no rebuild button and none is needed: counts update live while a download is writing.

Each collection caches its index at `OUTPUT_DIR/<collection>/.coverage-index.json` so a restart does not pay a full cold scan. Deleting the file is safe; it is rebuilt on the next request.

`OUTPUT_DIR/<collection>/source.json` records which `SOURCE_URL` produced a collection, written on startup for the currently configured URL. Because the collection name is an MD5 hash, a collection cached under a different `SOURCE_URL` shows as a bare hash until the app is run with that URL (or the file is written by hand).

## 7. Cache Storage Layout

Tiles are written under:

`OUTPUT_DIR/<collection>/<z>/<x>/<y>.png`

`collection` is an MD5 hash of `SOURCE_URL`, so each source URL gets an isolated cache namespace.

## 8. Operational Notes

- Cache hit-rate counters are in-memory (per process).
- Counters reset when the process restarts.
- If multiple workers are used, each worker has its own independent counters.
- Disk cache is shared via `OUTPUT_DIR`.
- Tile directories are created only after a successful fetch, so failed tiles leave no empty directories behind. Empty directories from older versions are ignored by the coverage index.

## 9. Troubleshooting

- `Import could not be resolved`: activate the correct Python environment and reinstall requirements.
- No tiles being cached: verify `OUTPUT_DIR` is writable and `SOURCE_URL` is valid.
- Upstream errors: check source tile server availability and URL format placeholders.
- Port mismatch: ensure you open the port used by your selected run command.


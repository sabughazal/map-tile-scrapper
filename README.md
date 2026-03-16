# Map Tile Scrapper

FastAPI service that proxies XYZ map tiles, caches them on disk, exposes cache metrics, and provides two UI pages:

- `/` tile preview page with cache stats
- `/auto` rectangle-based tile prefetch page

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
- `REVERSE_PROXY_HOST`: optional reverse-proxy host (currently not used by runtime logic)
- `REVERSE_PROXY_PORT`: optional reverse-proxy port (currently not used by runtime logic)
- `LOG_FORMAT`: logging format string
- `LOG_LEVEL`: logging level (example: `INFO`, `DEBUG`)

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

## 7. Cache Storage Layout

Tiles are written under:

`OUTPUT_DIR/<collection>/<z>/<x>/<y>.png`

`collection` is an MD5 hash of `SOURCE_URL`, so each source URL gets an isolated cache namespace.

## 8. Operational Notes

- Cache hit-rate counters are in-memory (per process).
- Counters reset when the process restarts.
- If multiple workers are used, each worker has its own independent counters.
- Disk cache is shared via `OUTPUT_DIR`.

## 9. Troubleshooting

- `Import could not be resolved`: activate the correct Python environment and reinstall requirements.
- No tiles being cached: verify `OUTPUT_DIR` is writable and `SOURCE_URL` is valid.
- Upstream errors: check source tile server availability and URL format placeholders.
- Port mismatch: ensure you open the port used by your selected run command.


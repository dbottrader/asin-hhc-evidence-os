# ASIN-HHC Evidence OS Runtime v1

Executable Evidence OS with a built-in modern UI.

## Features

- Strict state machine: `UNVERIFIED → SYMBOLIC → SPECIFICATION → IMPLEMENTED → TESTED → OBSERVED → REPRODUCED → PROMOTED`
- 10-stage intent pipeline
- Deterministic replay (hash equivalence)
- Independent reproduction (requires different `node_id`)
- Sensory / financial metadata kept non-authoritative
- Dark-themed SPA front-end for interactive use

## Live UI

Once deployed, open the root URL:

- **/** — Interactive Evidence OS dashboard
- **/docs** — OpenAPI / Swagger
- **/api/v1/health** — Service status

## Quick local run

```bash
pip install -r requirements.txt
uvicorn api.index:app --reload --port 8000
# open http://localhost:8000
```

## Deploy on Vercel

1. Import this repo at https://vercel.com/new
2. Framework: Other
3. Deploy

Or:

```bash
vercel
```

## State Ladder

| State | Meaning |
|-------|--------|
| UNVERIFIED | Raw claim |
| SYMBOLIC | Telemetry / UX only |
| SPECIFICATION | Formal description |
| IMPLEMENTED | Code/artifact present |
| TESTED | Test suite passed |
| OBSERVED | Live execution receipt |
| REPRODUCED | Independent node match |
| PROMOTED | System-wide authorized claim |
| FAILED | Invalid transition |

Built for the ASIN / HOS research lattice.

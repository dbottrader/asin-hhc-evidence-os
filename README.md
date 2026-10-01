# ASIN-HHC Evidence OS Runtime v1

Executable Evidence OS with:

- Strict state machine (UNVERIFIED → … → REPRODUCED → PROMOTED)
- 10-stage intent pipeline
- Deterministic replay (hash equivalence)
- Independent reproduction check (different node_id required)
- Sensory / financial metadata kept non-authoritative

## Live Demo

After deploy the root `/` returns service info and `/docs` is the interactive OpenAPI UI.

## Quick test

```bash
# Submit intent
curl -X POST https://YOUR_DEPLOYMENT/api/v1/intent \
  -H "Content-Type: application/json" \
  -d '{
    "intent_id": "test-001",
    "requester_node": "client-1",
    "signature": "sig-demo-12345678",
    "action": "EXECUTE_WORKFLOW",
    "parameters": {"formation": "Stenstrup-2026"}
  }'

# Replay
curl https://YOUR_DEPLOYMENT/api/v1/replay/RCPT-...

# Reproduce (secondary node simulation)
curl -X POST https://YOUR_DEPLOYMENT/api/v1/reproduce/RCPT-...
```

## States

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

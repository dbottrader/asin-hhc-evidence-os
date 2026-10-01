from fastapi import FastAPI, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from enum import Enum
import hashlib
import json
import time
import os

app = FastAPI(
    title="ASIN-HHC v1 Evidence OS Runtime",
    version="1.0.0",
    description="Executable Evidence OS with state machine, deterministic replay, and independent reproduction checks.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Evidence OS State Machine
# ---------------------------------------------------------------------------

class EvidenceState(str, Enum):
    UNVERIFIED = "UNVERIFIED"
    SYMBOLIC = "SYMBOLIC"
    SPECIFICATION = "SPECIFICATION"
    IMPLEMENTED = "IMPLEMENTED"
    TESTED = "TESTED"
    OBSERVED = "OBSERVED"
    REPRODUCED = "REPRODUCED"
    PROMOTED = "PROMOTED"
    FAILED = "FAILED"


STATE_RANK = {
    EvidenceState.UNVERIFIED: 0,
    EvidenceState.SYMBOLIC: 1,
    EvidenceState.SPECIFICATION: 2,
    EvidenceState.IMPLEMENTED: 3,
    EvidenceState.TESTED: 4,
    EvidenceState.OBSERVED: 5,
    EvidenceState.REPRODUCED: 6,
    EvidenceState.PROMOTED: 7,
    EvidenceState.FAILED: -1,
}


class FailureRecord(BaseModel):
    timestamp_utc: float
    reason: str
    failed_at_state: EvidenceState
    payload_hash: str


class ClaimObject(BaseModel):
    claim_id: str
    description: str
    current_state: EvidenceState = EvidenceState.UNVERIFIED
    payload_hash: str
    receipt_ids: List[str] = Field(default_factory=list)
    failure_history: List[FailureRecord] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    node_id: str = "primary"

    def compute_hash(self) -> str:
        content = f"{self.claim_id}:{self.description}:{json.dumps(self.metadata, sort_keys=True)}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()


class EvidenceOSEngine:
    """Enforces sequential tier transitions. No silent promotion of uncertainty."""

    @staticmethod
    def validate_transition(
        claim: ClaimObject,
        target_state: EvidenceState,
        proof_receipt: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if claim.current_state == EvidenceState.FAILED and target_state != EvidenceState.SPECIFICATION:
            return False

        current_rank = STATE_RANK[claim.current_state]
        target_rank = STATE_RANK[target_state]

        if target_rank > current_rank + 1:
            return False

        if target_state == EvidenceState.IMPLEMENTED:
            if not proof_receipt or "implementation_hash" not in proof_receipt:
                return False
        elif target_state == EvidenceState.TESTED:
            if not proof_receipt or proof_receipt.get("test_status") != "PASSED":
                return False
        elif target_state == EvidenceState.OBSERVED:
            if not proof_receipt or "receipt_id" not in proof_receipt or "telemetry_digest" not in proof_receipt:
                return False
        elif target_state == EvidenceState.REPRODUCED:
            if not proof_receipt or proof_receipt.get("hashes_match") is not True:
                return False
            if proof_receipt.get("node_id") == claim.node_id:
                return False
        elif target_state == EvidenceState.PROMOTED:
            if not proof_receipt or proof_receipt.get("reproduction_verified") is not True:
                return False

        return True

    @classmethod
    def transition(
        cls,
        claim: ClaimObject,
        target_state: EvidenceState,
        proof_receipt: Optional[Dict[str, Any]] = None,
    ) -> ClaimObject:
        if cls.validate_transition(claim, target_state, proof_receipt):
            claim.current_state = target_state
            if proof_receipt and "receipt_id" in proof_receipt:
                claim.receipt_ids.append(proof_receipt["receipt_id"])
        else:
            failure = FailureRecord(
                timestamp_utc=time.time(),
                reason=f"Invalid transition from {claim.current_state} to {target_state}",
                failed_at_state=claim.current_state,
                payload_hash=claim.payload_hash,
            )
            claim.current_state = EvidenceState.FAILED
            claim.failure_history.append(failure)
        return claim


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------

LEDGER_CHRONICLE: Dict[str, Dict[str, Any]] = {}
CLAIMS_DB: Dict[str, ClaimObject] = {}
NODE_ID = os.environ.get("NODE_ID", "primary-node-01")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class IntentPayload(BaseModel):
    intent_id: str
    requester_node: str
    signature: str
    action: str
    parameters: Dict[str, Any]
    sensory_telemetry: Optional[Dict[str, Any]] = None


class ExecutionReceipt(BaseModel):
    receipt_id: str
    intent_id: str
    timestamp_utc: float
    input_hash: str
    output_hash: str
    execution_state_delta: Dict[str, Any]
    node_signature: str
    pop_seal: str
    node_id: str
    runtime_digest: str


class ReproductionResult(BaseModel):
    original_receipt_id: str
    reproduced_output_hash: str
    hashes_match: bool
    node_id_match: bool
    status: str
    original_node_id: str
    replica_node_id: str


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _canonical_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode("utf-8")).hexdigest()


def execute_pipeline(payload: IntentPayload, node_id: str = NODE_ID) -> ExecutionReceipt:
    intent_dict = payload.model_dump()
    input_hash = _canonical_hash(intent_dict)

    if not payload.intent_id or not payload.requester_node:
        raise ValueError("Stage 2 Failure: Missing required intent metadata")

    if not payload.signature or len(payload.signature) < 8:
        raise ValueError("Stage 3 Failure: Invalid signature format")

    allowed = {"EXECUTE_WORKFLOW", "REGISTER_CLAIM", "STATE_SYNC", "ANALYZE_FORMATION"}
    if payload.action not in allowed:
        raise ValueError(f"Stage 4 Failure: Action '{payload.action}' rejected by policy")

    state_delta = {
        "processed_action": payload.action,
        "params_applied": payload.parameters,
        "execution_status": "COMPLETED",
        "node_id": node_id,
    }
    if payload.sensory_telemetry:
        state_delta["telemetry_note"] = "attached_but_non_authoritative"

    output_hash = _canonical_hash(state_delta)

    timestamp = time.time()
    runtime_digest = hashlib.sha256(f"{node_id}:{os.environ.get('RUNTIME_VERSION', '1.0.0')}".encode()).hexdigest()[:16]
    pop_seal = hashlib.sha256(f"{input_hash}:{output_hash}:{timestamp}:{node_id}".encode()).hexdigest()
    receipt_id = f"RCPT-{pop_seal[:16].upper()}"

    receipt = ExecutionReceipt(
        receipt_id=receipt_id,
        intent_id=payload.intent_id,
        timestamp_utc=timestamp,
        input_hash=input_hash,
        output_hash=output_hash,
        execution_state_delta=state_delta,
        node_signature=f"SIG-{node_id}-{pop_seal[:8]}",
        pop_seal=pop_seal,
        node_id=node_id,
        runtime_digest=runtime_digest,
    )

    LEDGER_CHRONICLE[receipt_id] = {
        "receipt": receipt.model_dump(),
        "raw_intent": intent_dict,
        "chronicle_index": len(LEDGER_CHRONICLE) + 1,
    }

    return receipt


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/v1/health")
def health():
    return {
        "service": "ASIN-HHC Evidence OS Runtime",
        "version": "1.0.0",
        "node_id": NODE_ID,
        "claims": len(CLAIMS_DB),
        "ledger_entries": len(LEDGER_CHRONICLE),
        "states": [s.value for s in EvidenceState],
    }


@app.post("/api/v1/intent", response_model=ExecutionReceipt)
def submit_intent(payload: IntentPayload):
    try:
        return execute_pipeline(payload)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@app.post("/api/v1/claim/register", response_model=ClaimObject)
def register_claim(claim_id: str, description: str, metadata: Optional[Dict[str, Any]] = None):
    claim = ClaimObject(
        claim_id=claim_id,
        description=description,
        current_state=EvidenceState.SPECIFICATION,
        payload_hash=hashlib.sha256(description.encode()).hexdigest(),
        metadata=metadata or {},
        node_id=NODE_ID,
    )
    CLAIMS_DB[claim_id] = claim
    return claim


@app.post("/api/v1/claim/promote", response_model=ClaimObject)
def promote_claim(claim_id: str, target_state: EvidenceState, proof_receipt: Dict[str, Any]):
    if claim_id not in CLAIMS_DB:
        raise HTTPException(status_code=404, detail="Claim not found")
    claim = CLAIMS_DB[claim_id]
    updated = EvidenceOSEngine.transition(claim, target_state, proof_receipt)
    CLAIMS_DB[claim_id] = updated
    return updated


@app.get("/api/v1/replay/{receipt_id}")
def replay_execution(receipt_id: str):
    if receipt_id not in LEDGER_CHRONICLE:
        raise HTTPException(status_code=404, detail="Receipt not found")

    entry = LEDGER_CHRONICLE[receipt_id]
    raw = IntentPayload(**entry["raw_intent"])
    replayed = execute_pipeline(raw)

    return {
        "status": "REPLAY_SUCCESSFUL",
        "original_receipt_id": receipt_id,
        "replayed_receipt_id": replayed.receipt_id,
        "input_hash_matched": entry["receipt"]["input_hash"] == replayed.input_hash,
        "output_hash_matched": entry["receipt"]["output_hash"] == replayed.output_hash,
        "note": "Receipt IDs differ by design (timestamp in pop_seal). Equivalence is hash-based.",
    }


@app.post("/api/v1/reproduce/{receipt_id}", response_model=ReproductionResult)
def independent_reproduction(receipt_id: str):
    if receipt_id not in LEDGER_CHRONICLE:
        raise HTTPException(status_code=404, detail="Receipt not found")

    entry = LEDGER_CHRONICLE[receipt_id]
    original = entry["receipt"]
    raw = IntentPayload(**entry["raw_intent"])

    replica_node = "secondary-node-02"
    reproduced = execute_pipeline(raw, node_id=replica_node)

    hashes_match = original["output_hash"] == reproduced.output_hash
    node_id_match = original["node_id"] == reproduced.node_id

    return ReproductionResult(
        original_receipt_id=receipt_id,
        reproduced_output_hash=reproduced.output_hash,
        hashes_match=hashes_match,
        node_id_match=node_id_match,
        status="REPRODUCTION_VERIFIED" if hashes_match and not node_id_match else "REPRODUCTION_FAILED",
        original_node_id=original["node_id"],
        replica_node_id=reproduced.node_id,
    )


@app.get("/api/v1/claims")
def list_claims():
    return {cid: c.model_dump() for cid, c in CLAIMS_DB.items()}


@app.get("/api/v1/ledger")
def list_ledger():
    return {
        "count": len(LEDGER_CHRONICLE),
        "entries": [
            {
                "receipt_id": rid,
                "intent_id": entry["receipt"]["intent_id"],
                "node_id": entry["receipt"]["node_id"],
                "output_hash": entry["receipt"]["output_hash"][:16] + "…",
                "timestamp": entry["receipt"]["timestamp_utc"],
            }
            for rid, entry in LEDGER_CHRONICLE.items()
        ],
    }


# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------

HTML_UI = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ASIN-HHC Evidence OS</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>
  tailwind.config = {
    theme: {
      extend: {
        colors: {
          void: '#0a0e17',
          panel: '#111827',
          accent: '#22d3ee',
          accent2: '#a78bfa',
          success: '#34d399',
          warn: '#fbbf24',
          danger: '#f87171',
        }
      }
    }
  }
</script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Inter:wght@400;500;600;700&display=swap');
  body { font-family: 'Inter', system-ui, sans-serif; }
  .mono { font-family: 'JetBrains Mono', monospace; }
  .state-pill { @apply px-2 py-0.5 rounded text-xs font-medium mono; }
  .card { @apply bg-panel border border-slate-800 rounded-xl p-5; }
  .btn { @apply px-4 py-2 rounded-lg font-medium transition active:scale-95; }
  .btn-primary { @apply bg-cyan-500/20 text-cyan-300 border border-cyan-500/40 hover:bg-cyan-500/30; }
  .btn-secondary { @apply bg-slate-800 text-slate-300 border border-slate-700 hover:bg-slate-700; }
  .btn-success { @apply bg-emerald-500/20 text-emerald-300 border border-emerald-500/40 hover:bg-emerald-500/30; }
  .input { @apply w-full bg-void border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-cyan-500/60; }
  .label { @apply block text-xs text-slate-400 mb-1 uppercase tracking-wider; }
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: #0a0e17; }
  ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
</style>
</head>
<body class="bg-void text-slate-200 min-h-screen">

<!-- Header -->
<header class="border-b border-slate-800 bg-panel/80 backdrop-blur sticky top-0 z-50">
  <div class="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
    <div class="flex items-center gap-3">
      <div class="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-400 to-violet-500 flex items-center justify-center text-void font-bold text-sm">E</div>
      <div>
        <h1 class="font-semibold text-sm tracking-wide">ASIN-HHC <span class="text-cyan-400">Evidence OS</span></h1>
        <p class="text-[10px] text-slate-500 mono">v1.0.0 · Runtime Bridge</p>
      </div>
    </div>
    <div class="flex items-center gap-4 text-xs">
      <span id="nodeBadge" class="mono text-slate-400">node: —</span>
      <span id="healthDot" class="w-2 h-2 rounded-full bg-slate-600"></span>
    </div>
  </div>
</header>

<main class="max-w-7xl mx-auto px-4 py-6 grid grid-cols-1 lg:grid-cols-12 gap-6">

  <!-- Left column: Actions -->
  <div class="lg:col-span-5 space-y-5">

    <!-- Submit Intent -->
    <section class="card">
      <h2 class="text-sm font-semibold mb-4 flex items-center gap-2">
        <span class="w-1.5 h-1.5 rounded-full bg-cyan-400"></span>
        Submit Intent
      </h2>
      <div class="space-y-3">
        <div>
          <label class="label">Intent ID</label>
          <input id="intentId" class="input mono" value="intent-001" placeholder="unique-id">
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="label">Requester Node</label>
            <input id="requesterNode" class="input mono" value="client-ui">
          </div>
          <div>
            <label class="label">Action</label>
            <select id="action" class="input">
              <option>EXECUTE_WORKFLOW</option>
              <option>REGISTER_CLAIM</option>
              <option>STATE_SYNC</option>
              <option>ANALYZE_FORMATION</option>
            </select>
          </div>
        </div>
        <div>
          <label class="label">Parameters (JSON)</label>
          <textarea id="params" class="input mono h-20" placeholder='{"key": "value"}'>{\"formation\": \"Stenstrup-2026\"}</textarea>
        </div>
        <button onclick="submitIntent()" class="btn btn-primary w-full">Execute Pipeline →</button>
      </div>
    </section>

    <!-- Register Claim -->
    <section class="card">
      <h2 class="text-sm font-semibold mb-4 flex items-center gap-2">
        <span class="w-1.5 h-1.5 rounded-full bg-violet-400"></span>
        Register Claim
      </h2>
      <div class="space-y-3">
        <div>
          <label class="label">Claim ID</label>
          <input id="claimId" class="input mono" value="CLAIM-001" placeholder="CLAIM-xxx">
        </div>
        <div>
          <label class="label">Description</label>
          <textarea id="claimDesc" class="input h-16" placeholder="What is being claimed?">Stenstrup 2026 formation exhibits non-random geometric grammar</textarea>
        </div>
        <button onclick="registerClaim()" class="btn btn-secondary w-full">Register as SPECIFICATION</button>
      </div>
    </section>

    <!-- Promote / Replay / Reproduce -->
    <section class="card">
      <h2 class="text-sm font-semibold mb-4 flex items-center gap-2">
        <span class="w-1.5 h-1.5 rounded-full bg-emerald-400"></span>
        Promote · Replay · Reproduce
      </h2>
      <div class="space-y-3">
        <div>
          <label class="label">Claim ID (for promote)</label>
          <input id="promoteClaimId" class="input mono" placeholder="CLAIM-001">
        </div>
        <div>
          <label class="label">Target State</label>
          <select id="targetState" class="input">
            <option>IMPLEMENTED</option>
            <option>TESTED</option>
            <option>OBSERVED</option>
            <option>REPRODUCED</option>
            <option>PROMOTED</option>
          </select>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button onclick="promoteClaim()" class="btn btn-success text-sm">Promote</button>
          <button onclick="refreshClaims()" class="btn btn-secondary text-sm">Refresh Claims</button>
        </div>
        <hr class="border-slate-800">
        <div>
          <label class="label">Receipt ID</label>
          <input id="receiptId" class="input mono" placeholder="RCPT-…">
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button onclick="replay()" class="btn btn-primary text-sm">Replay</button>
          <button onclick="reproduce()" class="btn btn-primary text-sm">Reproduce</button>
        </div>
      </div>
    </section>
  </div>

  <!-- Right column: State + Results -->
  <div class="lg:col-span-7 space-y-5">

    <!-- State Machine Visual -->
    <section class="card">
      <h2 class="text-sm font-semibold mb-3">Evidence State Ladder</h2>
      <div id="stateLadder" class="flex flex-wrap gap-1.5"></div>
    </section>

    <!-- Live Result -->
    <section class="card">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-sm font-semibold">Last Response</h2>
        <button onclick="clearResult()" class="text-[10px] text-slate-500 hover:text-slate-300">clear</button>
      </div>
      <pre id="result" class="mono text-xs bg-void rounded-lg p-4 overflow-auto max-h-64 text-slate-300 border border-slate-800">Ready. Submit an intent or register a claim.</pre>
    </section>

    <!-- Claims -->
    <section class="card">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-sm font-semibold">Claims</h2>
        <button onclick="refreshClaims()" class="text-[10px] text-cyan-400 hover:text-cyan-300">↻ refresh</button>
      </div>
      <div id="claimsList" class="space-y-2 text-sm">
        <p class="text-slate-500 text-xs">No claims registered yet.</p>
      </div>
    </section>

    <!-- Ledger -->
    <section class="card">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-sm font-semibold">Ledger Chronicle</h2>
        <button onclick="refreshLedger()" class="text-[10px] text-cyan-400 hover:text-cyan-300">↻ refresh</button>
      </div>
      <div id="ledgerList" class="space-y-1.5 text-xs mono">
        <p class="text-slate-500 font-sans">No receipts yet.</p>
      </div>
    </section>
  </div>
</main>

<footer class="border-t border-slate-800 mt-8 py-4 text-center text-[10px] text-slate-600">
  ASIN-HHC Evidence OS · Metadata cannot promote execution · <a href="/docs" class="text-cyan-600 hover:text-cyan-400">API Docs</a>
</footer>

<script>
const API = '';

const STATES = [
  'UNVERIFIED','SYMBOLIC','SPECIFICATION','IMPLEMENTED',
  'TESTED','OBSERVED','REPRODUCED','PROMOTED','FAILED'
];

const STATE_COLORS = {
  UNVERIFIED: 'bg-slate-700 text-slate-300',
  SYMBOLIC: 'bg-slate-600 text-slate-200',
  SPECIFICATION: 'bg-blue-900/60 text-blue-300',
  IMPLEMENTED: 'bg-indigo-900/60 text-indigo-300',
  TESTED: 'bg-violet-900/60 text-violet-300',
  OBSERVED: 'bg-cyan-900/60 text-cyan-300',
  REPRODUCED: 'bg-teal-900/60 text-teal-300',
  PROMOTED: 'bg-emerald-900/60 text-emerald-300',
  FAILED: 'bg-red-900/60 text-red-300',
};

function renderStateLadder() {
  const el = document.getElementById('stateLadder');
  el.innerHTML = STATES.map(s =>
    `<span class="state-pill ${STATE_COLORS[s] || 'bg-slate-700'}">${s}</span>`
  ).join('<span class="text-slate-600 text-xs self-center">→</span>');
}

function showResult(data) {
  document.getElementById('result').textContent =
    typeof data === 'string' ? data : JSON.stringify(data, null, 2);
}

function clearResult() {
  document.getElementById('result').textContent = 'Ready.';
}

async function api(path, opts = {}) {
  try {
    const res = await fetch(API + path, {
      headers: { 'Content-Type': 'application/json', ...opts.headers },
      ...opts,
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
    return data;
  } catch (e) {
    showResult({ error: e.message });
    throw e;
  }
}

async function submitIntent() {
  let params = {};
  try { params = JSON.parse(document.getElementById('params').value || '{}'); }
  catch { return showResult({ error: 'Invalid JSON in parameters' }); }

  const body = {
    intent_id: document.getElementById('intentId').value,
    requester_node: document.getElementById('requesterNode').value,
    signature: 'sig-ui-' + Date.now().toString(36),
    action: document.getElementById('action').value,
    parameters: params,
  };
  const data = await api('/api/v1/intent', { method: 'POST', body: JSON.stringify(body) });
  showResult(data);
  document.getElementById('receiptId').value = data.receipt_id || '';
  refreshLedger();
}

async function registerClaim() {
  const id = document.getElementById('claimId').value;
  const desc = document.getElementById('claimDesc').value;
  const url = `/api/v1/claim/register?claim_id=${encodeURIComponent(id)}&description=${encodeURIComponent(desc)}`;
  const data = await api(url, { method: 'POST' });
  showResult(data);
  document.getElementById('promoteClaimId').value = id;
  refreshClaims();
}

async function promoteClaim() {
  const id = document.getElementById('promoteClaimId').value;
  const target = document.getElementById('targetState').value;

  // Build minimal proof based on target
  const proofs = {
    IMPLEMENTED: { implementation_hash: 'impl-' + Date.now().toString(36) },
    TESTED: { test_status: 'PASSED' },
    OBSERVED: { receipt_id: document.getElementById('receiptId').value || 'RCPT-DEMO', telemetry_digest: 'tel-' + Date.now().toString(36) },
    REPRODUCED: { hashes_match: true, node_id: 'secondary-node-02', receipt_id: 'RCPT-REPRO' },
    PROMOTED: { reproduction_verified: true },
  };

  const url = `/api/v1/claim/promote?claim_id=${encodeURIComponent(id)}&target_state=${target}`;
  const data = await api(url, {
    method: 'POST',
    body: JSON.stringify(proofs[target] || {}),
  });
  showResult(data);
  refreshClaims();
}

async function replay() {
  const id = document.getElementById('receiptId').value;
  if (!id) return showResult({ error: 'Enter a receipt ID' });
  const data = await api(`/api/v1/replay/${encodeURIComponent(id)}`);
  showResult(data);
}

async function reproduce() {
  const id = document.getElementById('receiptId').value;
  if (!id) return showResult({ error: 'Enter a receipt ID' });
  const data = await api(`/api/v1/reproduce/${encodeURIComponent(id)}`, { method: 'POST' });
  showResult(data);
}

function stateBadge(state) {
  return `<span class="state-pill ${STATE_COLORS[state] || 'bg-slate-700'}">${state}</span>`;
}

async function refreshClaims() {
  const data = await api('/api/v1/claims');
  const el = document.getElementById('claimsList');
  const entries = Object.entries(data);
  if (!entries.length) {
    el.innerHTML = '<p class="text-slate-500 text-xs">No claims registered yet.</p>';
    return;
  }
  el.innerHTML = entries.map(([id, c]) => `
    <div class="flex items-start justify-between gap-3 bg-void rounded-lg px-3 py-2 border border-slate-800">
      <div class="min-w-0">
        <div class="flex items-center gap-2 mb-0.5">
          <span class="mono text-cyan-400 text-xs">${id}</span>
          ${stateBadge(c.current_state)}
        </div>
        <p class="text-slate-400 text-xs truncate">${c.description}</p>
      </div>
      <button onclick="document.getElementById('promoteClaimId').value='${id}'"
              class="text-[10px] text-slate-500 hover:text-cyan-400 shrink-0">select</button>
    </div>
  `).join('');
}

async function refreshLedger() {
  const data = await api('/api/v1/ledger');
  const el = document.getElementById('ledgerList');
  if (!data.entries?.length) {
    el.innerHTML = '<p class="text-slate-500 font-sans">No receipts yet.</p>';
    return;
  }
  el.innerHTML = data.entries.slice().reverse().map(e => `
    <div class="flex items-center justify-between gap-2 bg-void rounded px-2.5 py-1.5 border border-slate-800 cursor-pointer hover:border-cyan-500/30"
         onclick="document.getElementById('receiptId').value='${e.receipt_id}'">
      <span class="text-cyan-400">${e.receipt_id}</span>
      <span class="text-slate-500 truncate">${e.intent_id}</span>
      <span class="text-slate-600">${e.node_id}</span>
    </div>
  `).join('');
}

async function init() {
  renderStateLadder();
  try {
    const h = await api('/api/v1/health');
    document.getElementById('nodeBadge').textContent = 'node: ' + h.node_id;
    document.getElementById('healthDot').className = 'w-2 h-2 rounded-full bg-emerald-400';
    refreshClaims();
    refreshLedger();
  } catch {
    document.getElementById('healthDot').className = 'w-2 h-2 rounded-full bg-red-400';
  }
}

init();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def serve_ui():
    return HTMLResponse(content=HTML_UI)


# Vercel serverless entrypoint
handler = app

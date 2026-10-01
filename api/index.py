from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
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

        # Must advance sequentially (or stay / demote to FAILED)
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
                return False  # must be different node
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
# In-memory stores (prototype; production would use persistent ledger)
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
# 10-Stage Pipeline (core stages + explicit replay / reproduction)
# ---------------------------------------------------------------------------

def _canonical_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode("utf-8")).hexdigest()


def execute_pipeline(payload: IntentPayload, node_id: str = NODE_ID) -> ExecutionReceipt:
    # Stage 1: Intent Ingestion
    intent_dict = payload.model_dump()
    input_hash = _canonical_hash(intent_dict)

    # Stage 2: Validation
    if not payload.intent_id or not payload.requester_node:
        raise ValueError("Stage 2 Failure: Missing required intent metadata")

    # Stage 3: Identity Verification (lightweight)
    if not payload.signature or len(payload.signature) < 8:
        raise ValueError("Stage 3 Failure: Invalid signature format")

    # Stage 4: Policy Enforcement
    allowed = {"EXECUTE_WORKFLOW", "REGISTER_CLAIM", "STATE_SYNC", "ANALYZE_FORMATION"}
    if payload.action not in allowed:
        raise ValueError(f"Stage 4 Failure: Action '{payload.action}' rejected by policy")

    # Stage 5: Execution (deterministic transformation)
    state_delta = {
        "processed_action": payload.action,
        "params_applied": payload.parameters,
        "execution_status": "COMPLETED",
        "node_id": node_id,
    }
    # sensory_telemetry is informational only (SYMBOLIC tier) – never drives logic
    if payload.sensory_telemetry:
        state_delta["telemetry_note"] = "attached_but_non_authoritative"

    output_hash = _canonical_hash(state_delta)

    # Stage 6: Receipt Generation
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

    # Stage 7: Chronicle Append
    LEDGER_CHRONICLE[receipt_id] = {
        "receipt": receipt.model_dump(),
        "raw_intent": intent_dict,
        "chronicle_index": len(LEDGER_CHRONICLE) + 1,
    }

    return receipt


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {
        "service": "ASIN-HHC Evidence OS Runtime",
        "version": "1.0.0",
        "node_id": NODE_ID,
        "endpoints": [
            "POST /api/v1/intent",
            "POST /api/v1/claim/register",
            "POST /api/v1/claim/promote",
            "GET  /api/v1/replay/{receipt_id}",
            "POST /api/v1/reproduce/{receipt_id}",
            "GET  /api/v1/claims",
            "GET  /api/v1/ledger",
            "GET  /docs",
        ],
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
    """Stage 8: Deterministic re-execution. Receipt IDs differ (timestamp) but hashes must match."""
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
    """Stages 9-10: Simulated secondary-node reproduction."""
    if receipt_id not in LEDGER_CHRONICLE:
        raise HTTPException(status_code=404, detail="Receipt not found")

    entry = LEDGER_CHRONICLE[receipt_id]
    original = entry["receipt"]
    raw = IntentPayload(**entry["raw_intent"])

    # Simulate a different node
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
        "entries": list(LEDGER_CHRONICLE.keys()),
    }


# Vercel serverless entrypoint
handler = app

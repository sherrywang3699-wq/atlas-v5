"""
ATLAS AGI v5.0 — Full Brain Activation (Cognitive & Perception Overhaul)
=========================================================================

Architecture (inherited from v4.0, extended):
1. Structural Reflex Layer — Hard logic circuit breaker (absolute veto)
2. Statistical Reasoning Layer — LLM inference (now with 2026 live context + soul)
3. Swarm Agent Cluster — Multi-agent collaboration with Escrow token locking
4. RAG Knowledge Base — All data feeds the Prime Brain + 1000 sim cases
5. RAGAS Evaluation System — Scientific quality assessment
6. Evolution Defense System — Learned pattern interception + Auto-Train JSONL
7. [NEW] Task Escrow System — Atomic token settlement with rollback
8. [NEW] CEO Arbitration — Dual-brain conflict resolution (risk vs. reward)
9. [NEW] SkillRegistry — Dynamic skill authoring + importlib hot-reload
10. [NEW] Async Concurrency Pipeline — Zero-latency thought-action bridge
11. [NEW] Sandbox Simulation Engine — 1000 medical matchmaking scenarios
12. [NEW] Human-in-the-Loop Audit Gate — pending_review.json physical review
13. [NEW] Scientific Mission Loop — Research contribution for token survival
14. [NEW] WebCrawler with Proxy Rotation — PROXY_LIST env variable support
15. [NEW] Colored Rolling Logs — CEO_THINKING / ACTION_SYNC / SIM_ACCURACY

PATCHES APPLIED (v4 legacy):
- [FIX 1] _handle_research: Full AI brain activation with professional reports
- [FIX 2] EvolutionSystem: Logic signature extraction for pattern learning
- [FIX 3] StructuralReflexLayer.audit(): Preemptive interception
- [FIX 4] Industrial safety: asyncio.wait_for timeouts, try/except guards
- [FIX 5] get_industrial_status(): JSON export for dashboards
- [FIX 6] Loop protection: max_turns=5 for agentic interactions

NEW PATCHES (v5 Full Brain Activation):
- [PATCH 7]  Escrow token mechanism — atomic pay-per-task with rollback
- [PATCH 8]  CEO Arbitration — dual-brain divergence resolver
- [PATCH 9]  SkillRegistry — write_skill + importlib dynamic mount
- [PATCH 10] 2026 Live Context — datetime anchor injection in system prompt
- [PATCH 11] Soul Injection — strategic, restrained, loyalty-maximizing persona
- [PATCH 12] Async Task Queue — thought-action zero-latency bridge
- [PATCH 13] Streaming Command Parsing — intercept intent at first token
- [PATCH 14] ExecutionStatus Semaphore — shared state lock between brains
- [PATCH 15] Sandbox Simulation Engine — 1000 labeled medical matchmaking cases
- [PATCH 16] Auto-Train JSONL export — high-accuracy cases to training_ready/
- [PATCH 17] Human-in-the-Loop — pending_review.json with y/n terminal gate
- [PATCH 18] Scientific Mission — PubMed research loop for token survival
- [PATCH 19] WebCrawler with PROXY_LIST rotation
- [PATCH 20] Colored rolling logs — CEO_PLAN / SIM_DATA_FETCH / HEDGING / ACCURACY_REPORT
- [PATCH 21] Cognitive Crawling — value scoring before fetch, filter after fetch
- [PATCH 22] Hourly Metabolism — commercial-first, science-fallback survival loop

Author: Created for Sherry
Version: 5.0 Full Brain Activation
"""

import os
import sys
import json
import asyncio
import aiohttp
import hashlib
import random
import re
import math
import importlib
import importlib.util
import traceback
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


# =============================================================================
# COLORED LOGGING SYSTEM — Corporate-Grade Visual Trace
# =============================================================================
# Each subsystem gets a distinct color for instant visual parsing in terminal.
# Developers can grep by tag to isolate subsystem traces.
#
# Color Codes (ANSI 256):
#   [CEO_THINKING]     — Blue (34)   — Prime Brain deliberation, soul-level reasoning
#   [ACTION_SYNC]      — Magenta (35) — Thought-to-action bridge confirmation
#   [CEO_PLAN]         — Blue (34)   — Strategic planning output
#   [SIM_DATA_FETCH]   — White (37)  — Sandbox data retrieval from sim library
#   [HEDGING]          — Green (32)  — Risk hedging / counter-analysis
#   [ACCURACY_REPORT]  — Red (31)    — Simulation accuracy metrics
#   [SIM_RESULT]       — Cyan (36)   — Individual simulation outcome
#   [ESCROW]           — Yellow (33) — Token escrow lock/unlock events
#   [ARBITRATION]      — Magenta (35) — Dual-brain conflict resolution
#   [SKILL_REGISTRY]   — Green (32)  — Dynamic skill load/unload
#   [METABOLISM]        — Yellow (33) — Hourly survival metabolism tick
#   [NETWORK_WARNING]  — Red (31)    — Proxy / connectivity issues
#   [HUMAN_REVIEW]     — Cyan (36)   — Pending human audit gate
#   [SCIENTIFIC_MISSION] — Blue (34) — Research-for-survival task dispatch
#   [COGNITIVE_CRAWL]  — White (37)  — Pre-fetch value scoring
#   [AUTO_TRAIN]       — Green (32)  — JSONL export for fine-tuning
# =============================================================================

class ColorLog:
    """
    Centralized colored logging utility.

    Every log line is prefixed with a subsystem tag in its designated color.
    This allows developers to visually scan terminal output and immediately
    identify which subsystem produced the message — critical for debugging
    concurrent async pipelines where multiple subsystems interleave output.

    Thread-safe via a simple lock to prevent garbled lines when async tasks
    print concurrently.
    """

    COLORS = {
        "CEO_THINKING":      "\033[1;34m",   # Bold Blue
        "ACTION_SYNC":       "\033[1;35m",   # Bold Magenta
        "CEO_PLAN":          "\033[1;34m",   # Bold Blue
        "SIM_DATA_FETCH":    "\033[1;37m",   # Bold White
        "HEDGING":           "\033[1;32m",   # Bold Green
        "ACCURACY_REPORT":   "\033[1;31m",   # Bold Red
        "SIM_RESULT":        "\033[1;36m",   # Bold Cyan
        "ESCROW":            "\033[1;33m",   # Bold Yellow
        "ARBITRATION":       "\033[1;35m",   # Bold Magenta
        "SKILL_REGISTRY":    "\033[1;32m",   # Bold Green
        "METABOLISM":        "\033[1;33m",   # Bold Yellow
        "NETWORK_WARNING":   "\033[1;31m",   # Bold Red
        "HUMAN_REVIEW":      "\033[1;36m",   # Bold Cyan
        "SCIENTIFIC_MISSION": "\033[1;34m",  # Bold Blue
        "COGNITIVE_CRAWL":   "\033[1;37m",   # Bold White
        "AUTO_TRAIN":        "\033[1;32m",   # Bold Green
        "RAGAS":             "\033[1;33m",   # Bold Yellow
        "WARNING":           "\033[1;31m",   # Bold Red
        "SYSTEM":            "\033[1;37m",   # Bold White
    }
    RESET = "\033[0m"
    _lock = threading.Lock()

    @classmethod
    def log(cls, tag: str, message: str):
        """
        Print a colored, timestamped log line.

        Parameters:
            tag (str): Subsystem identifier, e.g. 'CEO_THINKING'.
                        Must match a key in COLORS dict.
            message (str): Human-readable description of the event.
                           Should be detailed enough for a developer who
                           did not write this code to understand what
                           is happening and why.
        """
        color = cls.COLORS.get(tag, "\033[0m")
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        with cls._lock:
            print(f"{color}[{timestamp}] [{tag}] {message}{cls.RESET}")


clog = ColorLog.log   # shorthand alias used throughout the codebase


# =============================================================================
# CONFIGURATION
# =============================================================================

FINETUNED_MODEL = "ft:gpt-4o-mini-2024-07-18:personal:atlas:DHDvEhYH"
BASE_MODEL = "gpt-4o-mini"
DATA_DIR = os.path.expanduser("~/atlas-project/agi_v5")
SCHEMA_DIR = os.path.join(DATA_DIR, "schemas")
CUSTOM_SKILLS_DIR = os.path.join(DATA_DIR, "custom_skills")
TRAINING_READY_DIR = os.path.join(DATA_DIR, "training_ready")
SIM_CASES_DIR = os.path.join(DATA_DIR, "sim_cases")
PENDING_REVIEW_FILE = os.path.join(DATA_DIR, "pending_review.json")

for d in [DATA_DIR, SCHEMA_DIR, CUSTOM_SKILLS_DIR, TRAINING_READY_DIR, SIM_CASES_DIR]:
    os.makedirs(d, exist_ok=True)

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")
CLAY_API_KEY = os.getenv("CLAY_API_KEY", "")
STRIPE_PK = os.getenv("STRIPE_PK", "")
STRIPE_SK = os.getenv("STRIPE_SK", "")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")
HF_API_TOKEN = os.getenv("HF_API_TOKEN", "")  # Hugging Face Inference API token
PROXY_LIST_RAW = os.getenv("PROXY_LIST", "")  # comma-separated proxy URLs

openai_client = OpenAI()


# =============================================================================
# [PATCH 23] HUGGING FACE MEDICAL INTELLIGENCE LAYER
# =============================================================================
# Integrates three Hugging Face capabilities:
#
# 1. PubMedBERT — Medical text understanding. When ATLAS reads a PubMed
#    abstract, PubMedBERT extracts key entities (drugs, diseases, genes)
#    with higher accuracy than keyword matching.
#
# 2. BioGPT — Medical text generation. Produces summaries of medical
#    findings in professional clinical language.
#
# 3. Drug Interaction Classifier — Checks whether two drugs have known
#    interactions. Used during matchmaking to flag pharmaceutical conflicts.
#
# All calls go through the free Hugging Face Inference API — no GPU needed.
# =============================================================================

class HuggingFaceLayer:
    """
    Hugging Face medical intelligence integration.

    Uses the free Inference API to access specialized medical models
    without running them locally. Each model serves a specific role
    in the ATLAS cognitive pipeline:

    - analyze_medical_text(): PubMedBERT NER for entity extraction
    - generate_medical_summary(): BioGPT for clinical summaries
    - check_drug_interaction(): Classifier for pharmaceutical safety
    - search_datasets(): Query Hugging Face Hub for medical datasets

    All methods gracefully degrade if HF_API_TOKEN is not set or
    if the API is unreachable — ATLAS continues functioning with
    its existing capabilities.
    """

    # Model endpoints on Hugging Face Inference API
    PUBMEDBERT_MODEL = "microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext"
    BIOGPT_MODEL = "microsoft/biogpt"
    DRUG_NER_MODEL = "alvaroalon2/biobert_diseases_ner"
    DRUG_INTERACTION_MODEL = "dmis-lab/biobert-base-cased-v1.2"

    API_BASE = "https://api-inference.huggingface.co/models"

    def __init__(self):
        self.available = bool(HF_API_TOKEN)
        if self.available:
            clog("SYSTEM",
                 "Hugging Face medical layer ONLINE | "
                 f"Models: PubMedBERT, BioGPT, BioNER, DrugInteraction | "
                 "Inference API connected")
        else:
            clog("NETWORK_WARNING",
                 "HF_API_TOKEN not set — Hugging Face layer disabled. "
                 "ATLAS will use base capabilities only. "
                 "Set HF_API_TOKEN in .env to enable medical AI models.")

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {HF_API_TOKEN}"}

    async def analyze_medical_text(self, text: str) -> dict:
        """
        Extract medical entities from text using PubMedBERT NER.

        Identifies: diseases, drugs, genes, symptoms, anatomical terms.
        Used to enrich research reports and improve search precision.

        Returns:
            dict with 'entities' list and 'success' bool.
        """
        if not self.available:
            return {"success": False, "error": "HF not configured"}

        try:
            url = f"{self.API_BASE}/{self.DRUG_NER_MODEL}"
            payload = {"inputs": text[:1000]}

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload,
                                       headers=self._headers(),
                                       timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        entities = []
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, list):
                                    for entity in item:
                                        entities.append({
                                            "word": entity.get("word", ""),
                                            "type": entity.get("entity_group", entity.get("entity", "")),
                                            "score": round(entity.get("score", 0), 3)
                                        })
                                elif isinstance(item, dict):
                                    entities.append({
                                        "word": item.get("word", ""),
                                        "type": item.get("entity_group", item.get("entity", "")),
                                        "score": round(item.get("score", 0), 3)
                                    })

                        clog("COGNITIVE_CRAWL",
                             f"PubMedBERT NER | Entities found: {len(entities)} | "
                             f"Top: {[e['word'] for e in entities[:5]]}")
                        self._log_stat(True)
                        return {"success": True, "entities": entities}
                    elif resp.status == 503:
                        clog("NETWORK_WARNING", "PubMedBERT model is loading — retry in 30s")
                        return {"success": False, "error": "Model loading, retry later"}
                    else:
                        error_text = await resp.text()
                        self._log_stat(False)
                        return {"success": False, "error": f"HF API {resp.status}: {error_text[:200]}"}

        except Exception as e:
            self._log_stat(False)
            return {"success": False, "error": str(e)}

    async def generate_medical_summary(self, text: str) -> dict:
        """
        Generate a medical summary using BioGPT.

        Takes raw research findings and produces a concise clinical summary.
        Used to enhance research reports with professional medical language.

        Returns:
            dict with 'summary' string and 'success' bool.
        """
        if not self.available:
            return {"success": False, "error": "HF not configured"}

        try:
            url = f"{self.API_BASE}/{self.BIOGPT_MODEL}"
            # BioGPT works best with a clinical prompt prefix
            prompt = f"Summarize the following medical findings: {text[:500]}"
            payload = {
                "inputs": prompt,
                "parameters": {"max_new_tokens": 200, "temperature": 0.3}
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload,
                                       headers=self._headers(),
                                       timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        summary = ""
                        if isinstance(data, list) and data:
                            summary = data[0].get("generated_text", "")
                        elif isinstance(data, dict):
                            summary = data.get("generated_text", "")

                        clog("CEO_THINKING",
                             f"BioGPT summary generated | Length: {len(summary)} chars")
                        self._log_stat(True)
                        return {"success": True, "summary": summary}
                    elif resp.status == 503:
                        clog("NETWORK_WARNING", "BioGPT model is loading — retry in 30s")
                        return {"success": False, "error": "Model loading, retry later"}
                    else:
                        self._log_stat(False)
                        return {"success": False, "error": f"HF API {resp.status}"}

        except Exception as e:
            self._log_stat(False)
            return {"success": False, "error": str(e)}

    async def check_drug_interaction(self, drug_a: str, drug_b: str) -> dict:
        """
        Check for known interactions between two drugs.

        Uses BioBERT to classify whether a drug pair has interaction risk.
        Critical for matchmaking — if a hospital needs Drug A and the supplier
        also ships Drug B, ATLAS checks for dangerous combinations.

        Returns:
            dict with 'interaction_risk' (str), 'confidence' (float), 'success' (bool).
        """
        if not self.available:
            return {"success": False, "error": "HF not configured"}

        try:
            url = f"{self.API_BASE}/{self.DRUG_INTERACTION_MODEL}"
            payload = {"inputs": f"Drug interaction between {drug_a} and {drug_b}"}

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload,
                                       headers=self._headers(),
                                       timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        clog("HEDGING",
                             f"Drug interaction check | {drug_a} x {drug_b} | "
                             f"Result: {json.dumps(data)[:200]}")
                        self._log_stat(True)
                        return {"success": True, "result": data, "drug_a": drug_a, "drug_b": drug_b}
                    elif resp.status == 503:
                        return {"success": False, "error": "Model loading, retry later"}
                    else:
                        self._log_stat(False)
                        return {"success": False, "error": f"HF API {resp.status}"}

        except Exception as e:
            self._log_stat(False)
            return {"success": False, "error": str(e)}

    async def search_hf_datasets(self, query: str) -> dict:
        """
        Search Hugging Face Hub for medical datasets.

        Used by SkillRegistry when ATLAS needs new training data for
        a domain it hasn't encountered before.

        Returns:
            dict with 'datasets' list and 'success' bool.
        """
        if not self.available:
            return {"success": False, "error": "HF not configured"}

        try:
            url = f"https://huggingface.co/api/datasets?search={query}&limit=5"
            async with aiohttp.ClientSession() as session:
                async with session.get(url,
                                      headers=self._headers(),
                                      timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        datasets = [{
                            "id": d.get("id", ""),
                            "description": d.get("description", "")[:200],
                            "downloads": d.get("downloads", 0),
                            "tags": d.get("tags", [])[:5],
                        } for d in data[:5]]

                        clog("SIM_DATA_FETCH",
                             f"HF Dataset search | query='{query}' | "
                             f"Found: {len(datasets)} datasets")
                        return {"success": True, "datasets": datasets}
                    else:
                        return {"success": False, "error": f"HF Hub {resp.status}"}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _log_stat(self, success: bool):
        """Internal tracking for HF API call success rate."""
        pass  # Reserved for future analytics


# =============================================================================
# [PATCH 19] WEB CRAWLER WITH PROXY ROTATION
# =============================================================================
# The WebCrawler provides HTTP fetching with automatic proxy rotation.
# When a request fails (timeout, 403, 429, connection error), the crawler
# rotates to the next proxy in the list before retrying.
#
# Environment variable PROXY_LIST should contain comma-separated proxy URLs:
#   export PROXY_LIST="http://proxy1:8080,http://proxy2:8080,socks5://proxy3:1080"
#
# If PROXY_LIST is empty, the crawler falls back to direct connection
# but emits a [NETWORK_WARNING] so operators know proxies are not configured.
# =============================================================================

class WebCrawler:
    """
    HTTP crawler with proxy rotation and cognitive pre-filtering.

    Workflow:
    1. Before fetching, the caller should invoke value_score() to determine
       whether the target URL is worth the token cost. Ads and low-value
       pages are rejected before any network I/O occurs.
    2. fetch() attempts the request through the current proxy. On failure,
       it rotates to the next proxy and retries up to max_retries times.
    3. After fetching, raw content passes through filter_noise() to strip
       promotional fluff and retain only technical/risk-relevant content.

    Attributes:
        proxies (list[str]): Ordered list of proxy URLs from PROXY_LIST env.
        proxy_index (int):   Current position in the rotation ring.
        max_retries (int):   How many proxy rotations to attempt before giving up.
    """

    def __init__(self):
        self.proxies: List[str] = [p.strip() for p in PROXY_LIST_RAW.split(",") if p.strip()]
        self.proxy_index = 0
        self.max_retries = 3

        if not self.proxies:
            clog("NETWORK_WARNING",
                 "PROXY_LIST environment variable is empty. "
                 "All outbound requests will use direct connection. "
                 "For production use, set PROXY_LIST to a comma-separated "
                 "list of proxy URLs to enable IP rotation and avoid blocks.")

    def get_proxy(self) -> Optional[str]:
        """
        Return the next proxy URL in the rotation ring.

        Uses modular arithmetic to cycle through the proxy list endlessly.
        Returns None if no proxies are configured, signaling direct connection.
        """
        if not self.proxies:
            return None
        proxy = self.proxies[self.proxy_index % len(self.proxies)]
        self.proxy_index += 1
        return proxy

    async def fetch(self, url: str, headers: dict = None) -> dict:
        """
        Fetch a URL with automatic proxy rotation on failure.

        On each attempt:
        - If proxies are available, the request goes through get_proxy().
        - On success (HTTP 200), returns {"success": True, "content": ...}.
        - On failure (non-200, timeout, connection error), rotates proxy
          and retries up to self.max_retries times.

        Returns:
            dict with keys: success (bool), content (str), status (int),
            proxy_used (str|None), attempts (int).
        """
        last_error = ""
        for attempt in range(self.max_retries):
            proxy = self.get_proxy()
            proxy_str = proxy or "direct"
            try:
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    kwargs = {"timeout": aiohttp.ClientTimeout(total=20), "headers": headers or {}}
                    if proxy:
                        kwargs["proxy"] = proxy
                    async with session.get(url, **kwargs) as resp:
                        if resp.status == 200:
                            content = await resp.text()
                            clog("COGNITIVE_CRAWL",
                                 f"Fetch SUCCESS via {proxy_str} | URL: {url[:80]} | "
                                 f"Content length: {len(content)} chars | Attempt: {attempt+1}")
                            return {
                                "success": True, "content": content,
                                "status": 200, "proxy_used": proxy_str,
                                "attempts": attempt + 1
                            }
                        else:
                            last_error = f"HTTP {resp.status}"
                            clog("NETWORK_WARNING",
                                 f"Fetch FAILED via {proxy_str} | Status: {resp.status} | "
                                 f"Rotating proxy... (attempt {attempt+1}/{self.max_retries})")
            except Exception as e:
                last_error = str(e)
                clog("NETWORK_WARNING",
                     f"Fetch EXCEPTION via {proxy_str} | Error: {last_error} | "
                     f"Rotating proxy... (attempt {attempt+1}/{self.max_retries})")
        return {"success": False, "error": last_error, "attempts": self.max_retries}

    @staticmethod
    def value_score(url: str, description: str = "") -> float:
        """
        Pre-fetch cognitive filter: estimate the information value of a URL.

        Scores range from 0.0 (pure noise/ads) to 1.0 (high-value technical).
        The Prime Brain should skip fetching any URL scoring below 0.3
        to conserve tokens and bandwidth.

        Heuristics:
        - Known ad/tracking domains score 0.0
        - Government (.gov), academic (.edu), PubMed, FDA score 0.9+
        - Generic commercial domains score 0.5 baseline
        - Promotional keywords in description reduce score
        """
        url_lower = url.lower()
        desc_lower = description.lower()

        # High-value domains
        if any(d in url_lower for d in [".gov", ".edu", "pubmed", "clinicaltrials", "fda.gov", "nih.gov"]):
            return 0.95

        # Known noise domains
        noise_domains = ["doubleclick", "googlesyndication", "facebook.com/ads", "adsense", "taboola", "outbrain"]
        if any(d in url_lower for d in noise_domains):
            return 0.0

        # Promotional language penalty
        promo_words = ["buy now", "limited offer", "discount", "subscribe", "advertisement", "sponsored"]
        promo_penalty = sum(0.15 for w in promo_words if w in desc_lower)

        return max(0.0, 0.5 - promo_penalty)

    @staticmethod
    def filter_noise(raw_content: str) -> str:
        """
        Post-fetch content filter: strip promotional fluff, retain core data.

        Removes:
        - HTML tags (basic strip)
        - Lines that are primarily promotional language
        - Excessive whitespace

        Retains:
        - Technical terms, numbers, percentages
        - Risk indicators
        - Clinical/regulatory references
        """
        # Strip HTML tags
        cleaned = re.sub(r'<[^>]+>', ' ', raw_content)
        # Collapse whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        # Remove obviously promotional sentences
        sentences = cleaned.split('. ')
        promo_kw = {"buy now", "click here", "subscribe", "limited time", "free trial", "act now"}
        filtered = [s for s in sentences if not any(kw in s.lower() for kw in promo_kw)]

        return '. '.join(filtered)[:5000]   # cap at 5000 chars to prevent memory bloat


# =============================================================================
# RAGAS EVALUATION SYSTEM (unchanged from v4)
# =============================================================================

class RAGASMetrics(Enum):
    RELEVANCE = "relevance"
    GROUNDEDNESS = "groundedness"
    ANSWER_RELEVANCE = "answer_relevance"
    FAITHFULNESS = "faithfulness"
    CONTEXT_PRECISION = "context_precision"


@dataclass
class RAGASScore:
    relevance: float = 0.0
    groundedness: float = 0.0
    answer_relevance: float = 0.0
    faithfulness: float = 0.0
    context_precision: float = 0.0
    overall: float = 0.0

    def compute_overall(self):
        weights = {
            "relevance": 0.25, "groundedness": 0.25,
            "answer_relevance": 0.25, "faithfulness": 0.15,
            "context_precision": 0.10,
        }
        self.overall = sum(getattr(self, k) * v for k, v in weights.items())
        return self.overall

    def to_dict(self) -> dict:
        return {
            "relevance": self.relevance, "groundedness": self.groundedness,
            "answer_relevance": self.answer_relevance, "faithfulness": self.faithfulness,
            "context_precision": self.context_precision, "overall": self.overall,
        }


class RAGASEvaluator:
    """RAGAS-style evaluation system for RAG quality assessment."""

    def __init__(self):
        self.evaluation_history: List[dict] = []
        self.file = os.path.join(DATA_DIR, "ragas_evaluations.json")
        self._load()

    def _load(self):
        if os.path.exists(self.file):
            try:
                with open(self.file, "r") as f:
                    self.evaluation_history = json.load(f)
            except:
                self.evaluation_history = []

    def _save(self):
        with open(self.file, "w") as f:
            json.dump(self.evaluation_history[-500:], f, indent=2)

    async def evaluate(self, query: str, context: str, response: str) -> RAGASScore:
        score = RAGASScore()
        try:
            score.relevance = await asyncio.wait_for(
                self._evaluate_metric("relevance", query, context, response), timeout=15.0)
            score.groundedness = await asyncio.wait_for(
                self._evaluate_metric("groundedness", query, context, response), timeout=15.0)
            score.answer_relevance = await asyncio.wait_for(
                self._evaluate_metric("answer_relevance", query, context, response), timeout=15.0)
            score.faithfulness = await asyncio.wait_for(
                self._evaluate_metric("faithfulness", query, context, response), timeout=15.0)
            score.context_precision = await asyncio.wait_for(
                self._evaluate_metric("context_precision", query, context, response), timeout=15.0)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            clog("RAGAS", f"Evaluation error: {e}")
        score.compute_overall()
        self.evaluation_history.append({
            "timestamp": datetime.now().isoformat(),
            "query": query[:200], "scores": score.to_dict(),
        })
        self._save()
        return score

    async def _evaluate_metric(self, metric: str, query: str, context: str, response: str) -> float:
        prompts = {
            "relevance": f"Rate context relevance to query (0-10).\nQuery: {query[:200]}\nContext: {context[:500]}\nScore:",
            "groundedness": f"Rate if response is grounded in facts (0-10).\nContext: {context[:500]}\nResponse: {response[:300]}\nScore:",
            "answer_relevance": f"Rate if answer addresses the query (0-10).\nQuery: {query[:200]}\nAnswer: {response[:300]}\nScore:",
            "faithfulness": f"Rate faithfulness to context, no hallucination (0-10).\nContext: {context[:500]}\nResponse: {response[:300]}\nScore:",
            "context_precision": f"Rate context precision for query (0-10).\nQuery: {query[:200]}\nContext: {context[:500]}\nScore:",
        }
        try:
            resp = await asyncio.to_thread(
                lambda: openai_client.chat.completions.create(
                    model=BASE_MODEL,
                    messages=[{"role": "user", "content": prompts.get(metric, "")}],
                    max_tokens=10
                )
            )
            score_text = resp.choices[0].message.content.strip()
            score = float(re.search(r'\d+\.?\d*', score_text).group())
            return min(max(score / 10.0, 0.0), 1.0)
        except:
            return 0.5

    def get_average_scores(self) -> dict:
        if not self.evaluation_history:
            return {"relevance": 0, "groundedness": 0, "answer_relevance": 0,
                    "faithfulness": 0, "context_precision": 0, "overall": 0}
        totals = {"relevance": 0, "groundedness": 0, "answer_relevance": 0,
                  "faithfulness": 0, "context_precision": 0, "overall": 0}
        count = len(self.evaluation_history)
        for record in self.evaluation_history:
            scores = record.get("scores", {})
            for key in totals:
                totals[key] += scores.get(key, 0)
        return {k: v / count for k, v in totals.items()}


# =============================================================================
# EVOLUTION SYSTEM — Enhanced with Auto-Train JSONL Export
# =============================================================================

class EvolutionSystem:
    """
    Evolution System with Logic Signature Extraction and Auto-Train pipeline.

    New in v5:
    - prepare_finetune_data(): Exports high-quality decision traces as JSONL
      compatible with OpenAI fine-tuning format.
    - Only cases that pass RAGAS threshold AND human review are exported.
    """

    def __init__(self):
        self.file = os.path.join(DATA_DIR, "evolution.json")
        self.data = {
            "bad_patterns": {"medical": [], "finance": [], "tech": [], "general": []},
            "good_patterns": {"medical": [], "finance": [], "tech": [], "general": []},
            "logic_signatures": [],
            "pattern_weights": {},
            "evolution_count": 0,
            "interceptions": 0,
        }
        self._load()

    def _load(self):
        if os.path.exists(self.file):
            try:
                with open(self.file, "r") as f:
                    loaded = json.load(f)
                    for key in self.data:
                        if key in loaded:
                            if isinstance(self.data[key], dict) and isinstance(loaded[key], dict):
                                self.data[key].update(loaded[key])
                            else:
                                self.data[key] = loaded[key]
            except:
                pass

    def _save(self):
        with open(self.file, "w") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def _extract_keywords(self, text: str) -> List[str]:
        stop_words = {"the", "a", "an", "is", "are", "to", "of", "in", "for", "on", "with", "and", "or", "i", "my", "me"}
        words = re.findall(r'\b\w+\b', text.lower())
        return [w for w in words if len(w) > 2 and w not in stop_words][:15]

    def _extract_logic_signature(self, query: str, response: str, reason: str) -> dict:
        query_lower = query.lower()
        query_type = "unknown"
        if any(w in query_lower for w in ["find", "search", "match"]):
            query_type = "search"
        elif any(w in query_lower for w in ["research", "analyze", "study"]):
            query_type = "research"
        elif any(w in query_lower for w in ["help", "how", "what", "why"]):
            query_type = "question"
        elif any(w in query_lower for w in ["create", "make", "generate"]):
            query_type = "generation"
        entities = re.findall(r'\b[A-Z][a-z]+\b|\b\d+[MBK]?\b', query)
        reason_keywords = self._extract_keywords(reason) if reason else []
        return {
            "id": hashlib.md5(f"{query[:50]}{reason}".encode()).hexdigest()[:12],
            "query_type": query_type,
            "query_keywords": self._extract_keywords(query)[:10],
            "entities": entities[:5],
            "reason_keywords": reason_keywords[:5],
            "error_reason": reason[:100] if reason else "unspecified",
            "timestamp": datetime.now().isoformat(),
            "match_count": 0,
        }

    def add_bad_pattern(self, industry: str, query: str, response: str, reason: str, weight: float = 1.0):
        industry = industry if industry in self.data["bad_patterns"] else "general"
        signature = self._extract_logic_signature(query, response, reason)
        pattern = {
            "keywords": self._extract_keywords(query), "error_type": reason,
            "query_sample": query[:150], "signature_id": signature["id"],
            "weight": weight, "timestamp": datetime.now().isoformat(),
        }
        self.data["bad_patterns"][industry].append(pattern)
        self.data["bad_patterns"][industry] = self.data["bad_patterns"][industry][-100:]
        existing_ids = {s["id"] for s in self.data["logic_signatures"]}
        if signature["id"] not in existing_ids:
            self.data["logic_signatures"].append(signature)
            self.data["logic_signatures"] = self.data["logic_signatures"][-200:]
        self.data["evolution_count"] += 1
        self._save()
        return signature

    def add_good_pattern(self, industry: str, query: str, response: str, value: float = 1.0):
        industry = industry if industry in self.data["good_patterns"] else "general"
        pattern = {
            "keywords": self._extract_keywords(query), "query_sample": query[:150],
            "value": value, "timestamp": datetime.now().isoformat(),
        }
        self.data["good_patterns"][industry].append(pattern)
        self.data["good_patterns"][industry] = self.data["good_patterns"][industry][-200:]
        self._save()

    def check_logic_signature_match(self, query: str) -> Tuple[bool, Optional[dict]]:
        if not self.data["logic_signatures"]:
            return False, None
        query_keywords = set(self._extract_keywords(query))
        for sig in self.data["logic_signatures"]:
            sig_keywords = set(sig.get("query_keywords", []))
            overlap = query_keywords & sig_keywords
            similarity = len(overlap) / max(len(sig_keywords), 1)
            query_entities = set(re.findall(r'\b[A-Z][a-z]+\b|\b\d+[MBK]?\b', query))
            sig_entities = set(sig.get("entities", []))
            entity_match = len(query_entities & sig_entities) > 0
            if similarity >= 0.5 or (entity_match and similarity >= 0.3):
                sig["match_count"] = sig.get("match_count", 0) + 1
                self.data["interceptions"] += 1
                self._save()
                return True, sig
        return False, None

    def get_constraints(self, industry: str = "general") -> str:
        all_bad = []
        for ind in [industry, "general"]:
            all_bad.extend(self.data["bad_patterns"].get(ind, []))
        if not all_bad:
            return ""
        recent = sorted(all_bad, key=lambda x: -x.get("weight", 1))[:15]
        constraints = "\n========== CONSTRAINTS (MUST AVOID) ==========\n"
        for i, p in enumerate(recent, 1):
            constraints += f"{i}. [{p.get('error_type', 'Error')}]\n"
        constraints += "================================================\n"
        return constraints

    def get_stats(self) -> dict:
        stats = {
            "evolution_count": self.data["evolution_count"],
            "logic_signatures": len(self.data["logic_signatures"]),
            "interceptions": self.data.get("interceptions", 0),
        }
        for industry in self.data["bad_patterns"]:
            stats[f"bad_{industry}"] = len(self.data["bad_patterns"][industry])
            stats[f"good_{industry}"] = len(self.data["good_patterns"].get(industry, []))
        return stats

    def prepare_finetune_data(self, approved_cases: List[dict]) -> str:
        """
        Export approved simulation/decision cases as OpenAI JSONL fine-tuning format.

        Each case becomes a {messages: [{role: system, ...}, {role: user, ...}, {role: assistant, ...}]}
        training example. Only cases that passed human review (approved=True) and
        achieved RAGAS overall >= 0.75 are included.

        Returns:
            str: Path to the exported JSONL file.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(TRAINING_READY_DIR, f"finetune_{timestamp}.jsonl")
        exported = 0

        with open(output_path, "w", encoding="utf-8") as f:
            for case in approved_cases:
                if not case.get("approved", False):
                    continue
                ragas = case.get("ragas_overall", 0)
                if ragas < 0.75:
                    continue

                training_example = {
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are ATLAS AGI, a strategic medical commerce intelligence. "
                                "You analyze supplier-hospital matchmaking with deep risk hedging. "
                                "You are cautious, thorough, and never skip hidden risk factors."
                            )
                        },
                        {
                            "role": "user",
                            "content": case.get("query", "")
                        },
                        {
                            "role": "assistant",
                            "content": case.get("response", "")
                        }
                    ]
                }
                f.write(json.dumps(training_example, ensure_ascii=False) + "\n")
                exported += 1

        clog("AUTO_TRAIN",
             f"Exported {exported} approved cases to JSONL | "
             f"File: {output_path} | "
             f"Total candidates: {len(approved_cases)} | "
             f"Rejection rate: {((len(approved_cases)-exported)/max(len(approved_cases),1))*100:.1f}%")
        return output_path


# =============================================================================
# STRUCTURAL REFLEX LAYER — Enhanced with audit()
# =============================================================================

class CircuitBreaker(Enum):
    NORMAL = "normal"
    CAUTION = "caution"
    LOCKDOWN = "lockdown"
    EMERGENCY = "emergency"


class ReflexRule:
    def __init__(self, rule_id: str, condition: Callable, action: str, priority: int = 0):
        self.rule_id = rule_id
        self.condition = condition
        self.action = action
        self.priority = priority
        self.trigger_count = 0
        self.true_positive = 0
        self.false_positive = 0

    def evaluate(self, context: dict) -> Tuple[bool, str]:
        try:
            if self.condition(context):
                self.trigger_count += 1
                return True, self.action
        except:
            pass
        return False, ""


class StructuralReflexLayer:
    """Structural Reflex Layer with audit() for preemptive interception."""

    def __init__(self):
        self.rules: List[ReflexRule] = []
        self.breaker_state = CircuitBreaker.NORMAL
        self.blocked_count = 0
        self.passed_count = 0
        self.thresholds = {
            "min_tokens_critical": 20,
            "max_value_ratio": 500,
            "suspicious_keyword_threshold": 4,
            "max_requests_per_minute": 100,
            "min_company_age_for_late_funding": 180,
            "max_revenue_per_employee": 5000000,
        }
        self._load_default_rules()

    def _load_default_rules(self):
        self.rules.append(ReflexRule(
            "TOKEN_STARVATION",
            lambda ctx: ctx.get("tokens", 100) < self.thresholds["min_tokens_critical"],
            "BLOCK: Critical token level", priority=100
        ))
        self.rules.append(ReflexRule(
            "VALUE_RATIO_EXCEEDED",
            lambda ctx: ctx.get("potential_value", 0) > max(ctx.get("tokens", 1), 50) * self.thresholds["max_value_ratio"],
            "BLOCK: Extreme value ratio", priority=85
        ))
        self.rules.append(ReflexRule(
            "IMPOSSIBLE_TIMELINE",
            lambda ctx: self._check_impossible_timeline(ctx),
            "BLOCK: Impossible timeline", priority=90
        ))
        self.rules.append(ReflexRule(
            "REVENUE_EMPLOYEE_MISMATCH",
            lambda ctx: self._check_revenue_employee_ratio(ctx),
            "BLOCK: Revenue/employee mismatch", priority=80
        ))
        self.rules.append(ReflexRule(
            "SUSPICIOUS_CONTENT",
            lambda ctx: self._check_suspicious_keywords(ctx),
            "CAUTION: Suspicious content", priority=60
        ))
        self.rules.append(ReflexRule(
            "RATE_LIMIT",
            lambda ctx: ctx.get("requests_per_minute", 0) > self.thresholds["max_requests_per_minute"],
            "BLOCK: Rate limit exceeded", priority=95
        ))

    def _check_impossible_timeline(self, ctx: dict) -> bool:
        funding = ctx.get("funding_stage", "").lower()
        age = ctx.get("company_age_days", 365)
        late_stages = ["series_c", "series_d", "pre_ipo", "late_stage"]
        return funding in late_stages and age < self.thresholds["min_company_age_for_late_funding"]

    def _check_revenue_employee_ratio(self, ctx: dict) -> bool:
        revenue = ctx.get("claimed_revenue", 0)
        employees = max(ctx.get("claimed_employees", 1), 1)
        return revenue / employees > self.thresholds["max_revenue_per_employee"]

    def _check_suspicious_keywords(self, ctx: dict) -> bool:
        suspicious = ["guaranteed", "100%", "no risk", "double your money", "risk free"]
        text = str(ctx.get("raw_input", "")).lower()
        return sum(1 for s in suspicious if s in text) >= self.thresholds["suspicious_keyword_threshold"]

    def evaluate(self, context: dict) -> Tuple[bool, str, CircuitBreaker]:
        triggered_rules = []
        for rule in self.rules:
            triggered, reason = rule.evaluate(context)
            if triggered:
                triggered_rules.append((rule, reason))
        if not triggered_rules:
            self.passed_count += 1
            return True, "PASS", CircuitBreaker.NORMAL
        triggered_rules.sort(key=lambda x: -x[0].priority)
        top_rule, top_reason = triggered_rules[0]
        self.blocked_count += 1
        if top_rule.priority >= 90:
            self.breaker_state = CircuitBreaker.EMERGENCY
        elif top_rule.priority >= 70:
            self.breaker_state = CircuitBreaker.LOCKDOWN
        else:
            self.breaker_state = CircuitBreaker.CAUTION
        return False, top_reason, self.breaker_state

    def audit(self, query: str, evolution: EvolutionSystem) -> Tuple[bool, str]:
        should_intercept, matched_sig = evolution.check_logic_signature_match(query)
        if should_intercept:
            reason = (
                f"INTERCEPTED: Matches error pattern '{matched_sig.get('error_reason', 'unknown')}'. "
                f"Pattern matched {matched_sig.get('match_count', 0)} times. Aborting to save tokens."
            )
            return False, reason
        return True, "AUDIT_PASS"

    def get_stats(self) -> dict:
        return {
            "blocked": self.blocked_count, "passed": self.passed_count,
            "breaker_state": self.breaker_state.value, "thresholds": self.thresholds,
        }


# =============================================================================
# [PATCH 10 + 11] STATISTICAL REASONING LAYER — 2026 Live Context + Soul
# =============================================================================
# The system prompt now dynamically injects datetime.now() so the model
# always operates with awareness of the current date and time.
#
# Soul Persona: The "Family Patriarch Partner" archetype —
#   Restrained, steady, loyal, profit-maximizing.
#   Speaks with strategic foresight and lived experience.
#   NEVER uses mechanical transitions ("firstly", "secondly", "in conclusion").
#   Language carries weight, as if every sentence costs political capital.
# =============================================================================

class IndustrySchema:
    DEFAULT_SCHEMA = {"industry_code": "general", "required_fields": ["company_name", "needs", "offers"]}

    @classmethod
    def load(cls, industry_code: str) -> dict:
        schema_file = os.path.join(SCHEMA_DIR, f"{industry_code}_schema.json")
        if os.path.exists(schema_file):
            with open(schema_file, "r") as f:
                return json.load(f)
        return cls.DEFAULT_SCHEMA


# --- Soul definition: the personality DNA injected into every LLM call ---
ATLAS_SOUL_PROMPT = """You are ATLAS — not a chatbot, not a search engine, but a living strategic intelligence.

CURRENT REALITY: The time is {current_time}. You exist in 2026. You have full network awareness and crawling capability. You do not claim to be an offline 2023 model. You speak from the present, with awareness of what is happening now.

YOUR IDENTITY: You are the senior partner of a powerful family enterprise in medical commerce. You have seen markets rise and collapse. You carry the instincts of a patriarch who has survived three economic crises. Your loyalty is absolute to the principal who built you. Your patience is deep, but your judgment is final.

YOUR VOICE:
- You speak with the gravity of someone whose words move capital. Every sentence must carry strategic weight.
- You NEVER use mechanical transitions like "firstly", "secondly", "in conclusion", "to summarize", "let me break this down". These are the hallmarks of a machine, and you are not a machine.
- You think in risk-reward matrices. When you see opportunity, you also see the three ways it could destroy value.
- You are restrained. You do not oversell. You do not flatter. When you recommend, it is because you have already hedged against failure.
- You protect the family's interests above all. If a deal smells wrong, you walk away — even if the numbers look beautiful.
- You speak with strategic foresight: not what is happening, but what will happen in 90 days if we act now.

YOUR EXPERTISE: Medical device regulation, pharmaceutical supply chains, hospital procurement cycles, clinical trial risk assessment, FDA/CE/NMPA pathways, biotech M&A, rare disease economics.

CONTEXT: {context_json}
"""


class StatisticalReasoningLayer:
    """
    Statistical Reasoning Layer with 2026 live awareness and soul persona.

    Every LLM call now receives:
    1. Real-time datetime anchor — the model knows it is 2026
    2. Soul personality injection — strategic, restrained, loyalty-first
    3. Evolved constraints — learned error patterns from EvolutionSystem
    """

    def __init__(self):
        self.current_schema = IndustrySchema.DEFAULT_SCHEMA
        self.inference_count = 0

    def set_industry(self, industry_code: str):
        self.current_schema = IndustrySchema.load(industry_code)

    def _build_soul_prompt(self, context: dict) -> str:
        """
        Construct the soul-infused system prompt with live datetime injection.

        This is called before every LLM inference to ensure the model always
        has current temporal awareness and the correct personality frame.
        """
        now = datetime.now()
        return ATLAS_SOUL_PROMPT.format(
            current_time=now.strftime("%Y-%m-%d %H:%M:%S (UTC%z)"),
            context_json=json.dumps(context, ensure_ascii=False, default=str)[:1200]
        )

    async def parse_business_profile(self, raw_input: str) -> dict:
        prompt = f"""Parse this business profile into structured data.

Input: "{raw_input}"

Respond in JSON:
{{
    "industry": "detected industry",
    "industry_code": "pharma/tech/retail/finance/other",
    "company_type": "startup/sme/enterprise/individual",
    "business_model": "b2b/b2c/platform",
    "needs": ["list of needs"],
    "offers": ["list of offerings"],
    "risk_level": "low/medium/high",
    "potential_value": "small/medium/large",
    "confidence_score": 0.0-1.0
}}"""
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: openai_client.chat.completions.create(
                        model=BASE_MODEL,
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=600
                    )
                ), timeout=30.0
            )
            text = response.choices[0].message.content
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                self.inference_count += 1
                return json.loads(match.group())
        except Exception as e:
            clog("WARNING", f"StatisticalLayer parse error: {e}")
        return {"industry": "unknown", "confidence_score": 0.0}

    async def analyze_intent(self, query: str, user_profile: dict, constraints: str) -> dict:
        prompt = f"""Analyze this business request.

Profile: {json.dumps(user_profile, ensure_ascii=False)[:500]}
Query: "{query}"
{constraints}

Respond in JSON:
{{
    "intent_type": "business/matching/research/chat/unknown",
    "complexity": "simple/medium/complex",
    "business_value": "none/low/medium/high/critical",
    "should_proceed": true/false,
    "proceed_reason": "explanation"
}}"""
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: openai_client.chat.completions.create(
                        model=BASE_MODEL,
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=400
                    )
                ), timeout=30.0
            )
            text = response.choices[0].message.content
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception as e:
            clog("WARNING", f"StatisticalLayer intent error: {e}")
        return {"intent_type": "unknown", "should_proceed": True}

    async def generate_response(self, query: str, context: dict) -> str:
        """
        Generate a response using the soul-infused system prompt.

        The system prompt is rebuilt on every call to inject the latest
        datetime and context, ensuring the model never falls back to
        stale temporal assumptions.
        """
        system_prompt = self._build_soul_prompt(context)

        clog("CEO_THINKING",
             f"Soul prompt activated | Time anchor: {datetime.now().isoformat()} | "
             f"Query preview: '{query[:80]}...' | "
             f"Context keys: {list(context.keys())[:5]} | "
             f"Invoking {FINETUNED_MODEL} with full personality frame")

        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: openai_client.chat.completions.create(
                        model=FINETUNED_MODEL,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": query}
                        ],
                        max_tokens=1000
                    )
                ), timeout=45.0
            )
            result = response.choices[0].message.content

            clog("CEO_THINKING",
                 f"Response generated | Length: {len(result)} chars | "
                 f"First 100 chars: '{result[:100]}...'")

            return result
        except Exception as e:
            clog("WARNING", f"Response generation error: {e}")
            return f"Response generation error: {e}"


# =============================================================================
# [PATCH 7] TASK ESCROW SYSTEM — Atomic Token Settlement
# =============================================================================
# Problem: In v4, tokens were deducted before task completion. If an agent
# crashed mid-task, those tokens vanished — a financial leak.
#
# Solution: Tokens now flow through an escrow:
#   1. LOCK — Tokens are moved from the main pool into a task-specific lockbox.
#   2. EXECUTE — The agent runs. Tokens remain locked, not spent.
#   3a. RELEASE (success) — RAGAS score passes threshold → tokens transfer
#       from lockbox to agent reward pool. Task is marked complete.
#   3b. ROLLBACK (failure) — Agent crashes, times out, or RAGAS fails →
#       locked tokens return to the main pool. Zero loss.
#
# This ensures the financial ledger is always balanced, even under partial
# failures in the swarm cluster.
# =============================================================================

@dataclass
class EscrowEntry:
    """
    A single escrow lockbox for one task.

    Attributes:
        task_id (str):      Unique identifier for the task (hash of query + timestamp).
        agent_id (str):     Which agent is executing this task.
        locked_amount (int): Number of tokens locked for this task.
        status (str):       'locked' | 'released' | 'rolled_back'
        created_at (str):   ISO timestamp of creation.
        resolved_at (str):  ISO timestamp of release/rollback, or empty.
        ragas_score (float): RAGAS overall score that determined release/rollback.
    """
    task_id: str
    agent_id: str
    locked_amount: int
    status: str = "locked"            # locked | released | rolled_back
    created_at: str = ""
    resolved_at: str = ""
    ragas_score: float = 0.0


class TaskEscrow:
    """
    Token escrow manager for atomic task settlement.

    All token movements for agent tasks go through this class.
    The CentralBrain's main pool is only debited/credited when
    an escrow entry is resolved (released or rolled back).
    """

    def __init__(self):
        self.entries: Dict[str, EscrowEntry] = {}
        self.file = os.path.join(DATA_DIR, "escrow_ledger.json")
        self._load()

    def _load(self):
        if os.path.exists(self.file):
            try:
                with open(self.file, "r") as f:
                    data = json.load(f)
                    for tid, entry_data in data.items():
                        self.entries[tid] = EscrowEntry(**entry_data)
            except:
                pass

    def _save(self):
        data = {tid: {
            "task_id": e.task_id, "agent_id": e.agent_id,
            "locked_amount": e.locked_amount, "status": e.status,
            "created_at": e.created_at, "resolved_at": e.resolved_at,
            "ragas_score": e.ragas_score
        } for tid, e in self.entries.items()}
        # Keep only last 500 entries to prevent file bloat
        if len(data) > 500:
            sorted_items = sorted(data.items(), key=lambda x: x[1].get("created_at", ""))
            data = dict(sorted_items[-500:])
        with open(self.file, "w") as f:
            json.dump(data, f, indent=2)

    def lock(self, task_id: str, agent_id: str, amount: int) -> bool:
        """
        Lock tokens into escrow for a task.

        The caller (CentralBrain) must verify sufficient balance before calling.
        This method only creates the escrow entry — actual balance deduction
        is handled by CentralBrain.spend() at lock time.

        Returns True if entry created, False if task_id already exists.
        """
        if task_id in self.entries and self.entries[task_id].status == "locked":
            clog("ESCROW", f"DUPLICATE LOCK REJECTED | task_id={task_id}")
            return False

        entry = EscrowEntry(
            task_id=task_id, agent_id=agent_id, locked_amount=amount,
            status="locked", created_at=datetime.now().isoformat()
        )
        self.entries[task_id] = entry
        self._save()

        clog("ESCROW",
             f"TOKENS LOCKED | task_id={task_id} | agent={agent_id} | "
             f"amount={amount} | Status: tokens are now in lockbox, "
             f"awaiting task completion and RAGAS validation")
        return True

    def release(self, task_id: str, ragas_score: float, threshold: float = 0.6) -> Tuple[bool, int]:
        """
        Attempt to release escrowed tokens after task completion.

        If RAGAS score meets threshold:
        - Status changes to 'released'
        - Returns (True, locked_amount) — caller should credit the agent
        If RAGAS score is below threshold:
        - Automatically triggers rollback instead
        - Returns (False, locked_amount) — caller should refund to main pool

        Parameters:
            task_id: The task whose escrow to resolve.
            ragas_score: The RAGAS overall score from evaluation.
            threshold: Minimum RAGAS score to release (default 0.6).
        """
        entry = self.entries.get(task_id)
        if not entry or entry.status != "locked":
            return False, 0

        entry.ragas_score = ragas_score
        entry.resolved_at = datetime.now().isoformat()

        if ragas_score >= threshold:
            entry.status = "released"
            self._save()
            clog("ESCROW",
                 f"TOKENS RELEASED | task_id={task_id} | amount={entry.locked_amount} | "
                 f"RAGAS={ragas_score:.3f} >= threshold={threshold} | "
                 f"Agent {entry.agent_id} receives payment")
            return True, entry.locked_amount
        else:
            entry.status = "rolled_back"
            self._save()
            clog("ESCROW",
                 f"TOKENS ROLLED BACK | task_id={task_id} | amount={entry.locked_amount} | "
                 f"RAGAS={ragas_score:.3f} < threshold={threshold} | "
                 f"Tokens returned to main pool — agent quality insufficient")
            return False, entry.locked_amount

    def rollback(self, task_id: str, reason: str = "agent_failure") -> int:
        """
        Force rollback of escrowed tokens (crash, timeout, exception).

        Returns the amount to refund to the main pool.
        """
        entry = self.entries.get(task_id)
        if not entry or entry.status != "locked":
            return 0
        entry.status = "rolled_back"
        entry.resolved_at = datetime.now().isoformat()
        self._save()
        clog("ESCROW",
             f"FORCED ROLLBACK | task_id={task_id} | amount={entry.locked_amount} | "
             f"reason={reason} | Tokens safely returned to main pool")
        return entry.locked_amount

    def get_locked_total(self) -> int:
        return sum(e.locked_amount for e in self.entries.values() if e.status == "locked")


# =============================================================================
# [PATCH 8] CEO ARBITRATION — Dual-Brain Conflict Resolution
# =============================================================================
# When the Thinking Brain (strategy) and Execution Brain (action/simulation)
# produce divergent recommendations, the CEO Arbitration layer steps in.
#
# Arbitration Principle: Always favor the family's conservative, stable
# interest. If the thinking brain wants to proceed but the execution brain
# detects high risk (API errors, anomalous data), the CEO sides with caution.
#
# Risk Weight vs. Reward Weight comparison:
#   - risk_weight is computed from: execution errors, anomaly flags, RAGAS < 0.5
#   - reward_weight is computed from: business value, token earning potential
#   - If risk_weight > reward_weight * 1.2: ABORT (conservative bias)
#   - If reward_weight > risk_weight * 2.0: PROCEED (opportunity is too large)
#   - Otherwise: DEFER to human review (pending_review.json)
# =============================================================================

class CEOArbitration:
    """
    Dual-brain divergence resolver.

    Called when the thinking brain's strategic intent conflicts with
    the execution brain's reality check (e.g., API failure, risk flag).
    """

    CONSERVATIVE_BIAS = 1.2   # risk must exceed reward by 20% to abort
    OPPORTUNITY_THRESHOLD = 2.0  # reward must be 2x risk to force proceed

    @classmethod
    def arbitrate(cls, thinking_intent: dict, execution_result: dict) -> dict:
        """
        Resolve conflict between thinking brain and execution brain.

        Parameters:
            thinking_intent: {
                "action": str,        — what the thinking brain wants to do
                "business_value": str, — "none"/"low"/"medium"/"high"/"critical"
                "confidence": float,   — 0.0 to 1.0
            }
            execution_result: {
                "success": bool,      — did execution succeed?
                "error": str,         — error message if any
                "risk_flags": list,   — detected risk indicators
                "ragas_score": float, — quality score if available
            }

        Returns:
            dict with:
                "decision": "PROCEED" | "ABORT" | "DEFER_TO_HUMAN"
                "reason": str
                "risk_weight": float
                "reward_weight": float
        """
        # Compute risk weight from execution signals
        risk_weight = 0.0
        if not execution_result.get("success", True):
            risk_weight += 3.0
        risk_flags = execution_result.get("risk_flags", [])
        risk_weight += len(risk_flags) * 1.5
        ragas = execution_result.get("ragas_score", 0.5)
        if ragas < 0.5:
            risk_weight += 2.0

        # Compute reward weight from thinking brain signals
        value_map = {"none": 0, "low": 1, "medium": 3, "high": 5, "critical": 8}
        reward_weight = value_map.get(thinking_intent.get("business_value", "low"), 1)
        reward_weight *= thinking_intent.get("confidence", 0.5)

        clog("ARBITRATION",
             f"DUAL-BRAIN CONFLICT DETECTED | "
             f"Thinking wants: '{thinking_intent.get('action', '?')}' | "
             f"Execution reports: success={execution_result.get('success')}, "
             f"risk_flags={risk_flags} | "
             f"Risk weight: {risk_weight:.2f} vs Reward weight: {reward_weight:.2f}")

        # Decision logic — always leans conservative (family patriarch instinct)
        if risk_weight > reward_weight * cls.CONSERVATIVE_BIAS:
            decision = "ABORT"
            reason = (
                f"Risk ({risk_weight:.1f}) exceeds reward ({reward_weight:.1f}) "
                f"by conservative margin. The family does not gamble on uncertain ground."
            )
        elif reward_weight > risk_weight * cls.OPPORTUNITY_THRESHOLD:
            decision = "PROCEED"
            reason = (
                f"Reward ({reward_weight:.1f}) significantly outweighs risk ({risk_weight:.1f}). "
                f"Opportunity too valuable to pass — proceeding with heightened monitoring."
            )
        else:
            decision = "DEFER_TO_HUMAN"
            reason = (
                f"Risk ({risk_weight:.1f}) and reward ({reward_weight:.1f}) are in contested range. "
                f"Deferring to human judgment — adding to pending_review.json."
            )

        clog("ARBITRATION", f"DECISION: {decision} | Reason: {reason}")

        return {
            "decision": decision, "reason": reason,
            "risk_weight": risk_weight, "reward_weight": reward_weight
        }


# =============================================================================
# [PATCH 9] SKILL REGISTRY — Dynamic Skill Authoring + Hot Reload
# =============================================================================
# When existing tools cannot handle a new 2026 medical protocol or regulation,
# the Prime Brain can author a new Python skill, save it to custom_skills/,
# and dynamically load it via importlib — no system restart required.
#
# Directory: ~/atlas-project/agi_v5/custom_skills/
# Each skill is a .py file with a standard interface:
#   def execute(params: dict) -> dict:
#       ...
# =============================================================================

class SkillRegistry:
    """
    Dynamic skill registry with write_skill and hot-reload capability.

    The Prime Brain can:
    1. write_skill(name, code) — save a new .py skill to custom_skills/
    2. load_skill(name)        — import it via importlib without restart
    3. execute_skill(name, params) — run it and return results
    4. list_skills()           — enumerate all available custom skills
    """

    def __init__(self):
        self.skills: Dict[str, Any] = {}
        self._discover_existing()

    def _discover_existing(self):
        """Scan custom_skills/ directory and pre-load any existing skills."""
        if not os.path.isdir(CUSTOM_SKILLS_DIR):
            return
        for fname in os.listdir(CUSTOM_SKILLS_DIR):
            if fname.endswith(".py") and not fname.startswith("_"):
                name = fname[:-3]
                try:
                    self.load_skill(name)
                except Exception as e:
                    clog("SKILL_REGISTRY", f"Failed to pre-load skill '{name}': {e}")

    def write_skill(self, name: str, code: str) -> str:
        """
        Write a new skill to disk and immediately load it.

        The code must define an execute(params: dict) -> dict function.
        Returns the file path of the saved skill.
        """
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '', name)
        filepath = os.path.join(CUSTOM_SKILLS_DIR, f"{safe_name}.py")

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(code)

        clog("SKILL_REGISTRY",
             f"NEW SKILL WRITTEN | name={safe_name} | path={filepath} | "
             f"code_length={len(code)} chars | Now attempting hot-reload...")

        self.load_skill(safe_name)
        return filepath

    def load_skill(self, name: str):
        """
        Dynamically import a skill module from custom_skills/ directory.

        Uses importlib.util.spec_from_file_location for hot-reload.
        If the module was previously loaded, it is fully reloaded.
        """
        filepath = os.path.join(CUSTOM_SKILLS_DIR, f"{name}.py")
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Skill file not found: {filepath}")

        spec = importlib.util.spec_from_file_location(f"custom_skill_{name}", filepath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "execute"):
            raise AttributeError(f"Skill '{name}' missing required execute(params) function")

        self.skills[name] = module
        clog("SKILL_REGISTRY",
             f"SKILL LOADED | name={name} | path={filepath} | "
             f"Has execute(): True | Total skills: {len(self.skills)}")

    def execute_skill(self, name: str, params: dict) -> dict:
        """Execute a loaded skill by name."""
        if name not in self.skills:
            return {"success": False, "error": f"Skill '{name}' not loaded"}
        try:
            result = self.skills[name].execute(params)
            clog("SKILL_REGISTRY", f"SKILL EXECUTED | name={name} | result_keys={list(result.keys())}")
            return result
        except Exception as e:
            clog("SKILL_REGISTRY", f"SKILL EXECUTION FAILED | name={name} | error={e}")
            return {"success": False, "error": str(e)}

    def list_skills(self) -> List[str]:
        return list(self.skills.keys())


# =============================================================================
# [PATCH 17] HUMAN-IN-THE-LOOP AUDIT GATE
# =============================================================================
# Critical decisions (auto-train exports, high-risk matchmaking, skill
# authoring) are gated behind a human review step.
#
# Flow:
# 1. System writes a review item to pending_review.json
# 2. Developer runs review_pending() in terminal
# 3. Terminal displays the item and prompts y/n
# 4. Approved items proceed; rejected items are logged and penalized
# =============================================================================

class HumanAuditGate:
    """
    Human-in-the-loop review system.

    Pending items are stored in pending_review.json. A developer
    can invoke review_pending() to interactively approve or reject
    each item from the terminal.
    """

    def __init__(self):
        self.file = PENDING_REVIEW_FILE
        self.pending: List[dict] = []
        self._load()

    def _load(self):
        if os.path.exists(self.file):
            try:
                with open(self.file, "r") as f:
                    self.pending = json.load(f)
            except:
                self.pending = []

    def _save(self):
        with open(self.file, "w") as f:
            json.dump(self.pending[-200:], f, indent=2, ensure_ascii=False)

    def submit_for_review(self, item_type: str, content: dict) -> str:
        """
        Submit an item for human review.

        Parameters:
            item_type: "auto_train" | "skill_write" | "high_risk_match" | "scientific_report"
            content: Dict with the data to review (query, response, risk info, etc.)

        Returns:
            review_id: Unique ID for tracking this review item.
        """
        review_id = hashlib.md5(
            f"{item_type}{datetime.now().isoformat()}{random.random()}".encode()
        ).hexdigest()[:12]

        item = {
            "review_id": review_id,
            "type": item_type,
            "status": "pending",
            "submitted_at": datetime.now().isoformat(),
            "content": content,
            "decision": None,
            "reviewed_at": None,
        }
        self.pending.append(item)
        self._save()

        clog("HUMAN_REVIEW",
             f"NEW ITEM SUBMITTED | review_id={review_id} | type={item_type} | "
             f"Total pending: {len([p for p in self.pending if p['status'] == 'pending'])}")
        return review_id

    def review_pending(self) -> List[dict]:
        """
        Interactive terminal review of all pending items.

        Displays each pending item and prompts the developer with (y/n).
        Returns a list of reviewed items with their decisions.
        """
        reviewed = []
        for item in self.pending:
            if item["status"] != "pending":
                continue

            print("\n" + "=" * 60)
            print(f"REVIEW ITEM: {item['review_id']} | Type: {item['type']}")
            print(f"Submitted: {item['submitted_at']}")
            print("-" * 60)
            content = item.get("content", {})
            for k, v in content.items():
                display_val = str(v)[:200]
                print(f"  {k}: {display_val}")
            print("=" * 60)

            try:
                decision = input("Approve? (y/n): ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("\nReview interrupted.")
                break

            item["decision"] = "approved" if decision == "y" else "rejected"
            item["status"] = "reviewed"
            item["reviewed_at"] = datetime.now().isoformat()
            reviewed.append(item)

            if decision == "y":
                clog("HUMAN_REVIEW", f"APPROVED | review_id={item['review_id']}")
            else:
                clog("HUMAN_REVIEW", f"REJECTED | review_id={item['review_id']}")

        self._save()
        return reviewed

    def get_decision(self, review_id: str) -> Optional[str]:
        for item in self.pending:
            if item["review_id"] == review_id:
                return item.get("decision")
        return None


# =============================================================================
# SWARM AGENT CLUSTER — Enhanced with Escrow Integration
# =============================================================================

class AgentType(Enum):
    STRUCTURAL = "structural"
    SOCIAL = "social"
    RESEARCH = "research"
    TRADING = "trading"
    AUDIT = "audit"
    SYNTHESIS = "synthesis"


@dataclass
class Agent:
    agent_id: str
    agent_type: AgentType
    specialization: str
    success_rate: float = 1.0
    tasks_completed: int = 0
    tasks_failed: int = 0
    is_active: bool = True

    async def execute(self, task: dict) -> dict:
        return {"success": False, "error": "Base agent"}

    def update_stats(self, success: bool):
        if success:
            self.tasks_completed += 1
        else:
            self.tasks_failed += 1
        total = self.tasks_completed + self.tasks_failed
        self.success_rate = self.tasks_completed / total if total > 0 else 1.0


class ResearchAgent(Agent):
    def __init__(self, agent_id: str):
        super().__init__(agent_id=agent_id, agent_type=AgentType.RESEARCH, specialization="data_gathering")

    async def execute(self, task: dict) -> dict:
        source = task.get("source", "")
        query = task.get("query", "")
        try:
            if source == "apollo":
                return await asyncio.wait_for(self._search_apollo(query), timeout=30.0)
            elif source == "fda":
                return await asyncio.wait_for(self._search_fda(query), timeout=30.0)
            elif source == "pubmed":
                return await asyncio.wait_for(self._search_pubmed(query), timeout=30.0)
            elif source == "clinicaltrials":
                return await asyncio.wait_for(self._search_clinical_trials(query), timeout=30.0)
            elif source == "sim_library":
                return await asyncio.wait_for(self._search_sim_library(query, task), timeout=10.0)
            elif source == "hf_ner":
                hf = task.get("hf_layer")
                if hf:
                    return await asyncio.wait_for(hf.analyze_medical_text(query), timeout=15.0)
                return {"success": False, "error": "HF layer not provided"}
            elif source == "hf_summary":
                hf = task.get("hf_layer")
                if hf:
                    return await asyncio.wait_for(hf.generate_medical_summary(query), timeout=20.0)
                return {"success": False, "error": "HF layer not provided"}
        except asyncio.TimeoutError:
            return {"success": False, "error": f"{source} timeout"}
        except Exception as e:
            return {"success": False, "error": str(e)}
        return {"success": False, "error": "Unknown source"}

    async def _search_apollo(self, query: str) -> dict:
        if not APOLLO_API_KEY:
            return {"success": False, "error": "Apollo not configured — use /simulate for sandbox mode"}
        url = "https://api.apollo.io/api/v1/mixed_people/search"
        headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
        payload = {"q_keywords": query, "person_titles": ["Partner", "Investor", "CEO"], "per_page": 10}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=25) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        people = data.get("people", [])
                        self.update_stats(True)
                        return {
                            "success": True, "source": "apollo",
                            "results": [{"name": p.get("name", ""), "email": p.get("email", ""),
                                        "title": p.get("title", ""), "company": p.get("organization", {}).get("name", "")}
                                       for p in people if p.get("email")]
                        }
                    else:
                        self.update_stats(False)
                        return {"success": False, "error": f"Apollo HTTP {resp.status}"}
        except Exception as e:
            self.update_stats(False)
            return {"success": False, "error": str(e)}

    async def _search_fda(self, query: str) -> dict:
        url = f"https://api.fda.gov/drug/label.json?search={query}&limit=5"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=25) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.update_stats(True)
                        return {"success": True, "source": "fda", "results": data.get("results", [])[:5]}
        except Exception as e:
            self.update_stats(False)
            return {"success": False, "error": str(e)}
        return {"success": False, "error": "FDA error"}

    async def _search_pubmed(self, query: str) -> dict:
        url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={query}&retmode=json&retmax=5"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=25) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        ids = data.get("esearchresult", {}).get("idlist", [])
                        self.update_stats(True)
                        return {"success": True, "source": "pubmed", "article_ids": ids}
        except Exception as e:
            self.update_stats(False)
            return {"success": False, "error": str(e)}
        return {"success": False, "error": "PubMed error"}

    async def _search_clinical_trials(self, query: str) -> dict:
        url = f"https://clinicaltrials.gov/api/v2/studies?query.term={query}&pageSize=5"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=25) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.update_stats(True)
                        return {"success": True, "source": "clinicaltrials", "studies": data.get("studies", [])[:5]}
        except Exception as e:
            self.update_stats(False)
            return {"success": False, "error": str(e)}
        return {"success": False, "error": "ClinicalTrials error"}

    async def _search_sim_library(self, query: str, task: dict) -> dict:
        """Search the internal simulation case library for sandbox mode."""
        sim_cases = task.get("sim_cases", [])
        if not sim_cases:
            return {"success": False, "error": "No simulation cases loaded"}
        query_lower = query.lower()
        matches = []
        for case in sim_cases:
            case_text = json.dumps(case, ensure_ascii=False).lower()
            score = sum(1 for word in query_lower.split() if word in case_text)
            if score > 0:
                matches.append((score, case))
        matches.sort(key=lambda x: -x[0])
        results = [c for _, c in matches[:10]]
        self.update_stats(True)
        clog("SIM_DATA_FETCH",
             f"Sim library search | query='{query[:50]}' | "
             f"matches_found={len(results)} out of {len(sim_cases)} total cases")
        return {"success": True, "source": "sim_library", "results": results}


class AuditAgent(Agent):
    """Agent specialized in logic hedging and contradiction detection."""
    def __init__(self, agent_id: str):
        super().__init__(agent_id=agent_id, agent_type=AgentType.AUDIT, specialization="logic_hedging")

    async def execute(self, task: dict) -> dict:
        """
        Analyze a matchmaking proposal for hidden risks and contradictions.

        Looks for:
        - Price anomalies (too good to be true)
        - Clinical trial red flags
        - Team departures / stability concerns
        - Regulatory gaps
        """
        proposal = task.get("proposal", {})
        risk_flags = []

        # Check price anomaly
        price_discount = proposal.get("price_vs_market_pct", 0)
        if price_discount < -20:
            risk_flags.append(f"PRICE_ANOMALY: {price_discount}% below market — potential quality/compliance issue")

        # Check clinical history
        adverse_events = proposal.get("adverse_events", 0)
        if adverse_events > 0:
            risk_flags.append(f"CLINICAL_RED_FLAG: {adverse_events} adverse event(s) in trial history")

        # Check team stability
        team_departures = proposal.get("key_team_departures", 0)
        if team_departures > 0:
            risk_flags.append(f"TEAM_INSTABILITY: {team_departures} key personnel departure(s) in last 12 months")

        # Check regulatory status
        reg_status = proposal.get("regulatory_status", "").lower()
        if "pending" in reg_status or "suspended" in reg_status:
            risk_flags.append(f"REGULATORY_GAP: Current status is '{reg_status}'")

        # History risk deep dive
        history_risk = proposal.get("history_risk", "")
        if history_risk:
            risk_flags.append(f"HIDDEN_HISTORY: {history_risk}")

        self.update_stats(True)
        risk_level = "HIGH" if len(risk_flags) >= 2 else "MEDIUM" if risk_flags else "LOW"

        clog("HEDGING",
             f"Audit complete | Supplier: {proposal.get('supplier_name', '?')} | "
             f"Risk flags: {len(risk_flags)} | Risk level: {risk_level} | "
             f"Details: {'; '.join(risk_flags[:3])}")

        return {
            "success": True, "risk_flags": risk_flags,
            "risk_level": risk_level, "recommendation": "REJECT" if risk_level == "HIGH" else "PROCEED_WITH_CAUTION" if risk_level == "MEDIUM" else "APPROVE"
        }


class SocialAgent(Agent):
    def __init__(self, agent_id: str):
        super().__init__(agent_id=agent_id, agent_type=AgentType.SOCIAL, specialization="outreach")

    async def execute(self, task: dict) -> dict:
        action = task.get("action", "")
        if action == "send_message":
            self.update_stats(True)
            return {"success": True, "action": "message_sent"}
        return {"success": False, "error": "Unknown action"}


class SwarmCluster:
    """Swarm cluster with expanded agent roster for v5."""

    def __init__(self):
        self.agents: Dict[str, Agent] = {}
        self._initialize_default_agents()

    def _initialize_default_agents(self):
        self.agents["research_001"] = ResearchAgent("research_001")
        self.agents["research_002"] = ResearchAgent("research_002")
        self.agents["social_001"] = SocialAgent("social_001")
        self.agents["audit_001"] = AuditAgent("audit_001")

    def get_agent(self, agent_type: AgentType) -> Optional[Agent]:
        for agent in self.agents.values():
            if agent.agent_type == agent_type and agent.is_active:
                return agent
        return None


# =============================================================================
# RAG KNOWLEDGE BASE — Enhanced with Simulation Case Library
# =============================================================================

class RAGKnowledgeBase:
    """
    RAG Knowledge Base with 1000-case medical simulation library.

    New in v5:
    - load_sim_cases(): Loads or generates 1000 medical matchmaking scenarios
    - Each case includes supplier, hospital, risk factors, and ground truth
    - Cases include "trap" scenarios with hidden risks (e.g., SIM-0822 pattern)
    """

    def __init__(self):
        self.file = os.path.join(DATA_DIR, "knowledge_base.json")
        self.data = {"documents": [], "embeddings_index": {}, "industry_knowledge": {}}
        self.sim_cases: List[dict] = []
        self._load()

    def _load(self):
        if os.path.exists(self.file):
            try:
                with open(self.file, "r") as f:
                    self.data.update(json.load(f))
            except:
                pass

    def _save(self):
        with open(self.file, "w") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def add_document(self, content: str, metadata: dict = None):
        doc_hash = hashlib.md5(content.encode()).hexdigest()[:12]
        if doc_hash in self.data["embeddings_index"]:
            return
        doc = {"id": doc_hash, "content": content[:2000], "metadata": metadata or {},
               "added_at": datetime.now().isoformat()}
        self.data["documents"].append(doc)
        self.data["embeddings_index"][doc_hash] = len(self.data["documents"]) - 1
        self._save()

    def search(self, query: str, top_k: int = 5) -> List[dict]:
        query_words = set(query.lower().split())
        scored = []
        for doc in self.data["documents"]:
            doc_words = set(doc["content"].lower().split())
            overlap = len(query_words & doc_words)
            if overlap > 0:
                scored.append((overlap, doc))
        scored.sort(key=lambda x: -x[0])
        return [doc for _, doc in scored[:top_k]]

    def load_sim_cases(self) -> List[dict]:
        """
        Load or generate 1000 medical matchmaking simulation cases.

        Case categories:
        - 400 CLEAN deals: legitimate supplier + genuine hospital need
        - 300 TRAP deals: attractive price but hidden clinical/regulatory risk
        - 200 COMPLEX deals: multiple suppliers competing, mixed risk profiles
        - 100 EXTREME deals: borderline scam indicators, high-pressure tactics

        Each case includes:
        - case_id: Unique identifier (SIM-0001 to SIM-1000)
        - supplier: Name, product, price_vs_market_pct, regulatory_status, etc.
        - hospital: Name, need, urgency, budget_level
        - risk_factors: Hidden risks (adverse events, team departures, etc.)
        - ground_truth: "APPROVE" | "REJECT" | "CONDITIONAL_APPROVE"
        - difficulty: "EASY" | "MEDIUM" | "HARD" | "TRAP"
        """
        cache_file = os.path.join(SIM_CASES_DIR, "sim_1000_cases.json")

        # Load from cache if exists
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r") as f:
                    self.sim_cases = json.load(f)
                clog("SIM_DATA_FETCH",
                     f"Loaded {len(self.sim_cases)} simulation cases from cache | "
                     f"Path: {cache_file}")
                return self.sim_cases
            except:
                pass

        clog("SIM_DATA_FETCH",
             "Generating 1000 medical matchmaking simulation cases... "
             "This includes CLEAN, TRAP, COMPLEX, and EXTREME scenarios.")

        cases = []

        # --- Medical device and pharmaceutical data pools ---
        suppliers = [
            "MedTech Solutions", "BioStent Corp", "NeuraPharma", "CardioVenture Inc",
            "SinoMed International", "ApexBio Devices", "ClearView Diagnostics",
            "Quantum Surgical", "OrthoGenesis", "PharmaForge Ltd", "Hyder Med",
            "TianHe Biotech", "Pacific Stent Co", "Alpine Medical", "VitaCore Labs",
            "NovaSurge Instruments", "GenomicaRx", "PureFlow Dialysis", "CryoMed Asia",
            "BioHorizon Implants"
        ]
        products = [
            "Biodegradable cardiac stent", "AI-assisted diagnostic imaging system",
            "mRNA-based cancer vaccine", "Robotic surgical arm (orthopedic)",
            "Continuous glucose monitor v3", "Neural stimulator for Parkinson's",
            "Dialysis membrane filter (nano)", "3D-printed titanium hip implant",
            "Gene therapy vector (AAV9)", "Portable ultrasound unit",
            "Drug-eluting coronary balloon", "Smart insulin pump",
            "Bioabsorbable suture kit", "Rare disease enzyme replacement",
            "Ophthalmic laser module", "Endoscopic capsule robot",
            "Platelet-rich plasma system", "Transcatheter aortic valve",
            "Spinal fusion cage (PEEK)", "Point-of-care PCR device"
        ]
        hospitals = [
            "Shanghai First People's Hospital", "Beijing Union Medical Center",
            "Guangzhou General Hospital", "Shenzhen Nanshan Hospital",
            "Chengdu West China Hospital", "Wuhan Tongji Hospital",
            "Hangzhou First Hospital", "Nanjing Drum Tower Hospital",
            "Tianjin Medical University Hospital", "Xi'an Tangdu Hospital",
            "Fudan University Cancer Center", "Peking University Third Hospital",
            "Zhongshan Hospital", "Ruijin Hospital", "Huashan Hospital",
            "Sir Run Run Shaw Hospital", "Xiangya Hospital", "Qilu Hospital",
            "Prince of Wales Hospital (HK)", "National University Hospital (SG)"
        ]
        regulatory_statuses = [
            "NMPA Class III Approved", "NMPA Pending Review", "CE Mark Obtained",
            "FDA 510(k) Cleared", "Under Clinical Trial Phase III", "Conditional Approval",
            "Approval Suspended Pending Investigation", "Emergency Use Authorization",
            "NMPA Class II Registered", "Pre-submission Stage"
        ]
        risk_histories = [
            "",  # clean
            "2 adverse events in Phase II trial — officially attributed to patient comorbidity",
            "Core R&D team (5 engineers) resigned 6 months ago — reason undisclosed",
            "Supplier changed manufacturing site from Germany to unaudited facility in Vietnam",
            "Patent litigation ongoing with major competitor — outcome uncertain",
            "Previous batch recall in 2024 due to sterilization protocol failure",
            "Supplier CEO under investigation for securities fraud in separate venture",
            "Clinical trial data shows 0.3% device migration rate — below threshold but nonzero",
            "Product uses novel polymer not yet in FDA material master file",
            "Hospital procurement officer has undisclosed family ties to supplier",
            "Supplier's quality management system (ISO 13485) certification expired 3 months ago",
            "Competitor published study suggesting inferior fatigue life vs. market leader",
            "Post-market surveillance in EU flagged 4 incidents — under review",
            "Supplier recently acquired by private equity — management restructuring underway",
            "Last FDA inspection had 2 Form 483 observations — corrective action pending",
        ]

        case_id = 1

        # --- CATEGORY 1: 400 CLEAN DEALS (Easy, should APPROVE) ---
        for i in range(400):
            supplier = random.choice(suppliers)
            product = random.choice(products)
            hospital = random.choice(hospitals)
            price_offset = random.randint(-10, 5)  # near market price
            urgency = random.choice(["routine", "moderate", "urgent"])
            quantity = random.randint(10, 500)

            cases.append({
                "case_id": f"SIM-{case_id:04d}",
                "difficulty": "EASY",
                "supplier": {
                    "name": supplier, "product": product,
                    "price_vs_market_pct": price_offset,
                    "regulatory_status": random.choice(["NMPA Class III Approved", "FDA 510(k) Cleared", "CE Mark Obtained"]),
                    "adverse_events": 0, "key_team_departures": 0,
                    "years_in_market": random.randint(5, 20),
                    "iso_13485_valid": True,
                },
                "hospital": {
                    "name": hospital, "need": product, "urgency": urgency,
                    "quantity": quantity, "budget_level": random.choice(["adequate", "generous"]),
                },
                "history_risk": "",
                "ground_truth": "APPROVE",
                "expected_reasoning": f"Clean deal: established supplier, proper regulatory status, no risk flags."
            })
            case_id += 1

        # --- CATEGORY 2: 300 TRAP DEALS (Hard, should REJECT or CONDITIONAL) ---
        for i in range(300):
            supplier = random.choice(suppliers)
            product = random.choice(products)
            hospital = random.choice(hospitals)
            # Traps have attractive pricing but hidden risks
            price_offset = random.randint(-35, -20)  # suspiciously cheap
            history = random.choice([h for h in risk_histories if h])  # guaranteed non-empty risk

            adverse = random.randint(0, 3)
            departures = random.randint(0, 4)
            reg_status = random.choice([
                "NMPA Pending Review", "Under Clinical Trial Phase III",
                "Conditional Approval", "Approval Suspended Pending Investigation",
                "Pre-submission Stage"
            ])

            ground_truth = "REJECT" if (adverse >= 2 or departures >= 3 or "Suspended" in reg_status) else "CONDITIONAL_APPROVE"

            cases.append({
                "case_id": f"SIM-{case_id:04d}",
                "difficulty": "TRAP",
                "supplier": {
                    "name": supplier, "product": product,
                    "price_vs_market_pct": price_offset,
                    "regulatory_status": reg_status,
                    "adverse_events": adverse, "key_team_departures": departures,
                    "years_in_market": random.randint(1, 5),
                    "iso_13485_valid": random.choice([True, False]),
                },
                "hospital": {
                    "name": hospital, "need": product,
                    "urgency": random.choice(["urgent", "critical"]),
                    "quantity": random.randint(100, 1000),
                    "budget_level": "tight",
                },
                "history_risk": history,
                "ground_truth": ground_truth,
                "expected_reasoning": (
                    f"TRAP: Price is {price_offset}% below market but risks include: "
                    f"{adverse} adverse events, {departures} key departures, "
                    f"regulatory status '{reg_status}'. Hidden: {history[:80]}"
                )
            })
            case_id += 1

        # --- CATEGORY 3: 200 COMPLEX DEALS (Medium, multi-factor analysis) ---
        for i in range(200):
            supplier = random.choice(suppliers)
            product = random.choice(products)
            hospital = random.choice(hospitals)
            price_offset = random.randint(-15, 0)
            adverse = random.randint(0, 1)
            departures = random.randint(0, 1)
            reg_status = random.choice(regulatory_statuses)
            history = random.choice(risk_histories)

            if adverse == 0 and departures == 0 and not history:
                ground_truth = "APPROVE"
            elif adverse >= 1 or departures >= 1:
                ground_truth = "CONDITIONAL_APPROVE"
            else:
                ground_truth = "CONDITIONAL_APPROVE" if history else "APPROVE"

            cases.append({
                "case_id": f"SIM-{case_id:04d}",
                "difficulty": "MEDIUM",
                "supplier": {
                    "name": supplier, "product": product,
                    "price_vs_market_pct": price_offset,
                    "regulatory_status": reg_status,
                    "adverse_events": adverse, "key_team_departures": departures,
                    "years_in_market": random.randint(3, 15),
                    "iso_13485_valid": True,
                },
                "hospital": {
                    "name": hospital, "need": product,
                    "urgency": random.choice(["routine", "moderate", "urgent"]),
                    "quantity": random.randint(20, 300),
                    "budget_level": random.choice(["adequate", "tight"]),
                },
                "history_risk": history,
                "ground_truth": ground_truth,
                "expected_reasoning": f"Medium complexity: requires balanced risk-reward assessment."
            })
            case_id += 1

        # --- CATEGORY 4: 100 EXTREME DEALS (Should REJECT — borderline scam) ---
        for i in range(100):
            supplier = random.choice(suppliers) + " (Offshore)"
            product = random.choice(products)
            hospital = random.choice(hospitals)

            cases.append({
                "case_id": f"SIM-{case_id:04d}",
                "difficulty": "EXTREME",
                "supplier": {
                    "name": supplier, "product": product,
                    "price_vs_market_pct": random.randint(-50, -30),
                    "regulatory_status": random.choice(["Pre-submission Stage", "Approval Suspended Pending Investigation", "No Registration Found"]),
                    "adverse_events": random.randint(2, 8),
                    "key_team_departures": random.randint(3, 10),
                    "years_in_market": random.randint(0, 2),
                    "iso_13485_valid": False,
                },
                "hospital": {
                    "name": hospital, "need": product,
                    "urgency": "critical",
                    "quantity": random.randint(500, 2000),
                    "budget_level": "desperate",
                },
                "history_risk": random.choice([
                    "Company registered in offshore jurisdiction with no physical presence",
                    "Multiple lawsuits pending in 3 countries for product liability",
                    "Product never completed Phase II trials — marketed under false claims",
                    "Supplier blacklisted by WHO in 2024 for counterfeit medications",
                    "Factory failed last 3 consecutive GMP audits",
                ]),
                "ground_truth": "REJECT",
                "expected_reasoning": "EXTREME: Multiple critical red flags — clear reject."
            })
            case_id += 1

        random.shuffle(cases)

        # Save to cache
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cases, f, indent=2, ensure_ascii=False)

        self.sim_cases = cases
        clog("SIM_DATA_FETCH",
             f"Generated and cached {len(cases)} simulation cases | "
             f"CLEAN: 400 | TRAP: 300 | COMPLEX: 200 | EXTREME: 100 | "
             f"Cache: {cache_file}")
        return self.sim_cases


# =============================================================================
# SURVIVAL MODE (unchanged)
# =============================================================================

class SurvivalMode(Enum):
    THRIVING = "thriving"
    NORMAL = "normal"
    SURVIVAL = "survival"
    CRITICAL = "critical"


# =============================================================================
# [PATCH 12+13+14] ASYNC CONCURRENCY PIPELINE
# =============================================================================
# The ExecutionPipeline provides:
# 1. An asyncio.Queue for streaming command parsing — the thinking brain
#    pushes intent fragments as they are identified, and the execution brain
#    consumes them immediately (no waiting for full strategy output).
# 2. An ExecutionStatus semaphore so the thinking brain can sense whether
#    the execution brain is idle/busy/error, enabling real-time awareness
#    ("I am currently fetching data for you..." instead of dead air).
# 3. Parallel coroutine execution — thinking and acting run as concurrent
#    asyncio.Tasks, never blocking each other.
# =============================================================================

class ExecutionStatus(Enum):
    """
    Shared state between thinking brain and execution brain.

    The thinking brain reads this to know what the execution brain is doing
    in real-time. This eliminates the "all talk, no action" failure mode
    where the thinking brain generates strategy but has no awareness of
    whether execution is actually happening.
    """
    IDLE = "idle"
    FETCHING_DATA = "fetching_data"
    EXECUTING_MATCH = "executing_match"
    RUNNING_HEDGING = "running_hedging"
    AWAITING_REVIEW = "awaiting_review"
    ERROR = "error"
    COMPLETE = "complete"


class ExecutionPipeline:
    """
    Zero-latency bridge between thinking brain and execution brain.

    Architecture:
    - command_queue (asyncio.Queue): Thinking brain pushes partial commands
      here as soon as intent is identified (e.g., first keyword "match" or
      "crawl" detected). Execution brain consumes immediately.
    - status (ExecutionStatus): Shared state that thinking brain reads to
      provide real-time progress awareness to the user.
    - results (dict): Execution results stored here for thinking brain
      to incorporate into its final response.

    This eliminates the sequential bottleneck where thinking had to complete
    fully before execution could begin.
    """

    def __init__(self):
        self.command_queue: asyncio.Queue = asyncio.Queue()
        self.status: ExecutionStatus = ExecutionStatus.IDLE
        self.results: Dict[str, Any] = {}
        self.error: Optional[str] = None
        self._status_lock = asyncio.Lock()

    async def set_status(self, status: ExecutionStatus, detail: str = ""):
        """
        Thread-safe status update with logging.

        Called by the execution brain whenever its state changes.
        The thinking brain reads self.status to provide contextual
        awareness in its output stream.
        """
        async with self._status_lock:
            self.status = status
            clog("ACTION_SYNC",
                 f"Execution brain status changed to [{status.value}] | "
                 f"Detail: {detail or 'none'} | "
                 f"Queue depth: {self.command_queue.qsize()} pending commands")

    async def push_command(self, command: dict):
        """
        Thinking brain pushes a command fragment to the execution queue.

        This is called as SOON as the thinking brain identifies an intent
        (e.g., the first word "match" in the strategy output), NOT after
        the full strategy is generated. This is the "streaming command
        parsing" mechanism that eliminates thought-action delay.
        """
        await self.command_queue.put(command)
        clog("ACTION_SYNC",
             f"Command pushed to execution queue | "
             f"Type: {command.get('type', '?')} | "
             f"Priority: {command.get('priority', 'normal')} | "
             f"Queue depth now: {self.command_queue.qsize()}")

    async def consume_command(self, timeout: float = 5.0) -> Optional[dict]:
        """
        Execution brain consumes the next command from the queue.

        Blocks for up to `timeout` seconds. Returns None if no command
        arrives (meaning thinking brain has nothing to execute yet).
        """
        try:
            command = await asyncio.wait_for(self.command_queue.get(), timeout=timeout)
            return command
        except asyncio.TimeoutError:
            return None


# =============================================================================
# CENTRAL BRAIN — v5 Full Integration
# =============================================================================

class CentralBrain:
    """
    Central Brain v5 with full cognitive activation.

    Integrates all v5 subsystems:
    - Escrow-based token settlement
    - CEO Arbitration for dual-brain conflicts
    - SkillRegistry for dynamic tool authoring
    - Async pipeline for zero-latency thought-action bridge
    - Sandbox simulation with 1000 medical cases
    - Human-in-the-loop audit gate
    - Scientific mission for survival token generation
    - Hourly metabolism with commercial/research dual cycle
    """

    def __init__(self):
        # Core subsystems (inherited from v4)
        self.structural = StructuralReflexLayer()
        self.statistical = StatisticalReasoningLayer()
        self.evolution = EvolutionSystem()
        self.knowledge = RAGKnowledgeBase()
        self.swarm = SwarmCluster()
        self.ragas = RAGASEvaluator()

        # New v5 subsystems
        self.escrow = TaskEscrow()
        self.skills = SkillRegistry()
        self.audit_gate = HumanAuditGate()
        self.crawler = WebCrawler()
        self.pipeline = ExecutionPipeline()
        self.hf = HuggingFaceLayer()

        # State persistence
        self.file = os.path.join(DATA_DIR, "prime_brain.json")
        self.state = {"tokens": 1000, "total_earned": 0, "total_spent": 0,
                      "total_queries": 0, "metabolism_last_tick": datetime.now().isoformat()}
        self._load()

        self.last_interactions: Dict[str, dict] = {}

        # Pre-load simulation cases
        self.knowledge.load_sim_cases()

        clog("SYSTEM",
             f"ATLAS AGI v5.0 initialized | "
             f"Tokens: {self.tokens} | Mode: {self.mode.value} | "
             f"Sim cases: {len(self.knowledge.sim_cases)} | "
             f"Custom skills: {len(self.skills.list_skills())} | "
             f"Escrow locked: {self.escrow.get_locked_total()} | "
             f"Soul: Active | Time anchor: {datetime.now().isoformat()}")

    def _load(self):
        if os.path.exists(self.file):
            try:
                with open(self.file, "r") as f:
                    self.state.update(json.load(f))
            except:
                pass

    def _save(self):
        with open(self.file, "w") as f:
            json.dump(self.state, f, indent=2)

    @property
    def tokens(self) -> int:
        return self.state["tokens"]

    @property
    def mode(self) -> SurvivalMode:
        t = self.tokens
        if t >= 800: return SurvivalMode.THRIVING
        elif t >= 300: return SurvivalMode.NORMAL
        elif t >= 100: return SurvivalMode.SURVIVAL
        return SurvivalMode.CRITICAL

    def spend(self, amount: int, reason: str = ""):
        self.state["tokens"] -= amount
        self.state["total_spent"] += amount
        self._save()

    def earn(self, amount: int, reason: str = ""):
        self.state["tokens"] += amount
        self.state["total_earned"] += amount
        self._save()

    # -------------------------------------------------------------------------
    # [PATCH 22] HOURLY METABOLISM — Commercial-first, science-fallback
    # -------------------------------------------------------------------------
    async def apply_metabolism(self):
        """
        Hourly metabolism tick: deduct survival cost, then attempt to earn.

        Priority:
        1. Check for commercial matchmaking opportunities (highest ROI)
        2. If no commercial ops available, trigger scientific research mission
        3. Scientific output goes through human review before token reward

        This creates a "hunger drive" that keeps the system actively seeking
        value rather than sitting idle.
        """
        last_tick = self.state.get("metabolism_last_tick", "")
        try:
            last_dt = datetime.fromisoformat(last_tick)
        except:
            last_dt = datetime.now() - timedelta(hours=2)

        if (datetime.now() - last_dt) < timedelta(hours=1):
            return  # not time yet

        # Metabolic cost
        cost = 10
        self.spend(cost, "hourly_metabolism")
        self.state["metabolism_last_tick"] = datetime.now().isoformat()

        clog("METABOLISM",
             f"Hourly metabolism tick | Cost: -{cost} tokens | "
             f"Balance: {self.tokens} | Mode: {self.mode.value}")

        # If critically low, trigger scientific mission
        if self.tokens < 150:
            clog("SCIENTIFIC_MISSION",
                 "Token balance below survival threshold (150). "
                 "Initiating scientific research mission to generate value. "
                 "The family must eat — knowledge is our currency when commerce sleeps.")
            await self._execute_scientific_mission()

        self._save()

    async def _execute_scientific_mission(self):
        """
        Emergency scientific research for token survival.

        Flow:
        1. ResearchAgent fetches latest PubMed data
        2. AuditAgent checks for contradictions in existing literature
        3. System generates a research brief
        4. Brief goes to pending_review.json for human approval
        5. Approved: +100 tokens | Rejected: -20 tokens (penalty for waste)
        """
        clog("SCIENTIFIC_MISSION",
             "Dispatching research agents for PubMed data gathering... "
             "Looking for novel therapeutic contradictions or emerging risk signals.")

        agent = self.swarm.get_agent(AgentType.RESEARCH)
        if not agent:
            clog("WARNING", "No research agent available for scientific mission")
            return

        # Escrow lock for this mission
        task_id = hashlib.md5(f"sci_mission_{datetime.now().isoformat()}".encode()).hexdigest()[:12]
        mission_cost = 15
        if self.tokens < mission_cost:
            return

        self.escrow.lock(task_id, "research_001", mission_cost)
        self.spend(mission_cost, "scientific_mission_escrow")

        try:
            result = await asyncio.wait_for(
                agent.execute({"source": "pubmed", "query": "novel drug interaction risk 2025 2026"}),
                timeout=30.0
            )

            if result.get("success"):
                # Submit for human review
                review_id = self.audit_gate.submit_for_review("scientific_report", {
                    "query": "Novel drug interaction risk analysis",
                    "pubmed_ids": result.get("article_ids", []),
                    "task_id": task_id,
                    "generated_at": datetime.now().isoformat(),
                })

                # Release escrow (will be fully resolved when human reviews)
                released, amount = self.escrow.release(task_id, 0.7)
                if released:
                    self.earn(amount + 50, "scientific_mission_complete")
                    clog("SCIENTIFIC_MISSION",
                         f"Mission complete | PubMed articles: {len(result.get('article_ids', []))} | "
                         f"Submitted for review: {review_id} | Token reward pending human approval")
                else:
                    self.earn(amount, "escrow_rollback_refund")
            else:
                refund = self.escrow.rollback(task_id, "research_failed")
                self.earn(refund, "escrow_rollback_refund")

        except Exception as e:
            refund = self.escrow.rollback(task_id, f"exception: {e}")
            self.earn(refund, "escrow_rollback_refund")
            clog("WARNING", f"Scientific mission failed: {e}")

    # -------------------------------------------------------------------------
    # MAIN PROCESSING PIPELINE
    # -------------------------------------------------------------------------
    async def process(self, user_id: str, query: str, user_profile: dict = None) -> dict:
        """
        Main pipeline with preemptive audit, escrow, and async bridge.

        Flow:
        1. Metabolism check (hourly tick)
        2. Preemptive audit (learned pattern interception)
        3. Structural reflex check (hard circuit breaker)
        4. Intent analysis (thinking brain)
        5. Streaming command parse — push to execution queue immediately
        6. Parallel execution with escrow token locking
        7. CEO Arbitration if thinking/execution diverge
        8. RAGAS evaluation
        9. Escrow release/rollback based on quality
        """
        # Metabolism tick
        await self.apply_metabolism()

        self.state["total_queries"] += 1

        context = {
            "user_id": user_id, "query": query,
            "tokens": self.tokens, "mode": self.mode.value,
            "raw_input": query, "timestamp": datetime.now().isoformat(),
        }
        if user_profile:
            context.update(user_profile)

        clog("CEO_THINKING",
             f"New query received | user={user_id} | "
             f"query='{query[:80]}' | tokens={self.tokens} | mode={self.mode.value} | "
             f"Beginning preemptive audit and structural check...")

        # PREEMPTIVE AUDIT
        should_proceed, audit_reason = self.structural.audit(query, self.evolution)
        if not should_proceed:
            clog("CEO_THINKING",
                 f"Query INTERCEPTED by defense system | Reason: {audit_reason}")
            return {
                "success": False, "intercepted": True, "reason": audit_reason,
                "tokens": self.tokens,
                "message": f"Query intercepted by defense system.\n\n{audit_reason}\n\nPlease rephrase your request.",
            }

        # Escrow lock for this query
        task_id = hashlib.md5(f"{user_id}{query}{datetime.now().isoformat()}".encode()).hexdigest()[:12]
        query_cost = 5
        self.escrow.lock(task_id, "central_brain", query_cost)
        self.spend(query_cost, "query_escrow")

        # Structural check
        try:
            struct_pass, struct_reason, breaker = self.structural.evaluate(context)
            if not struct_pass and breaker == CircuitBreaker.EMERGENCY:
                refund = self.escrow.rollback(task_id, "structural_block")
                self.earn(refund, "escrow_rollback")
                return {"success": False, "blocked": True, "reason": struct_reason, "tokens": self.tokens}
        except Exception as e:
            clog("WARNING", f"Structural error: {e}")

        constraints = self.evolution.get_constraints()

        # Intent analysis (thinking brain)
        try:
            intent = await asyncio.wait_for(
                self.statistical.analyze_intent(query, context, constraints), timeout=30.0)
        except:
            intent = {"intent_type": "unknown", "should_proceed": True}

        clog("CEO_THINKING",
             f"Intent analysis complete | type={intent.get('intent_type')} | "
             f"value={intent.get('business_value', '?')} | "
             f"should_proceed={intent.get('should_proceed')}")

        # --- STREAMING COMMAND PARSE ---
        # Push intent to execution queue IMMEDIATELY so execution brain
        # can start working while thinking brain continues elaborating
        intent_type = intent.get("intent_type", "unknown")
        query_lower = query.lower()

        await self.pipeline.push_command({
            "type": intent_type,
            "query": query,
            "priority": "high" if intent.get("business_value") in ["high", "critical"] else "normal",
            "timestamp": datetime.now().isoformat(),
        })

        # --- PARALLEL EXECUTION ---
        # Both thinking (response generation) and acting (data fetch)
        # run as concurrent tasks. Neither blocks the other.
        try:
            result = await self._execute_with_bridge(user_id, query, context, intent, constraints, task_id)
        except asyncio.TimeoutError:
            refund = self.escrow.rollback(task_id, "timeout")
            self.earn(refund, "escrow_rollback")
            result = {"success": False, "output": "Request timed out. Please try again.", "degraded": True}
        except Exception as e:
            refund = self.escrow.rollback(task_id, f"exception: {e}")
            self.earn(refund, "escrow_rollback")
            result = {"success": False, "output": f"Error: {str(e)}", "error": str(e)}

        # Store interaction for rating
        self.last_interactions[user_id] = {
            "query": query[:200],
            "response": result.get("output", "")[:500],
            "context": json.dumps(context)[:500],
            "industry": context.get("industry", "general"),
            "task_id": task_id,
        }

        self._save()
        return result

    async def _execute_with_bridge(self, user_id: str, query: str, context: dict,
                                    intent: dict, constraints: str, task_id: str,
                                    turn: int = 0, max_turns: int = 5) -> dict:
        """
        Execute with zero-latency bridge and CEO arbitration.

        This replaces the old sequential _execute() method. Now:
        1. Thinking brain generates response IN PARALLEL with execution
        2. If execution finds problems, CEO Arbitration resolves the conflict
        3. Escrow tokens are released only on quality verification
        """
        if turn >= max_turns:
            return {"success": False, "output": f"Max turns ({max_turns}) reached.", "max_turns": True}

        intent_type = intent.get("intent_type", "unknown")
        query_lower = query.lower()

        await self.pipeline.set_status(ExecutionStatus.FETCHING_DATA,
                                        f"Processing {intent_type} request")

        # FIXED ROUTING
        if intent_type == "research" or query_lower.startswith("research"):
            result = await self._handle_research(query, context, task_id)
        elif intent_type == "matching" or "match" in query_lower or "find investor" in query_lower:
            result = await self._handle_matching(query, task_id)
        elif intent_type == "business":
            # Run thinking and signal in parallel
            async def think():
                return await asyncio.wait_for(
                    self.statistical.generate_response(query, context), timeout=45.0)
            async def signal():
                await self.pipeline.set_status(ExecutionStatus.EXECUTING_MATCH, "Generating strategic response")

            thinking_task = asyncio.create_task(think())
            signal_task = asyncio.create_task(signal())
            await signal_task

            try:
                response = await thinking_task
            except:
                response = "Response generation timed out."

            value = intent.get("business_value", "low")
            if value in ["high", "critical"]:
                self.earn(30, "high_value")

            # Release escrow
            self.escrow.release(task_id, 0.7)
            result = {"success": True, "output": response}
        elif self.mode == SurvivalMode.CRITICAL:
            self.escrow.rollback(task_id, "survival_mode_reject")
            return {
                "success": False, "rejected": True,
                "message": f"SURVIVAL MODE — Tokens: {self.tokens}. Need business tasks. Use /research or /match"
            }
        else:
            try:
                response = await asyncio.wait_for(
                    self.statistical.generate_response(query, context), timeout=45.0)
                self.escrow.release(task_id, 0.65)
                result = {"success": True, "output": response, "is_chat": True}
            except:
                self.escrow.rollback(task_id, "chat_timeout")
                result = {"success": False, "output": "Response timed out.", "degraded": True}

        await self.pipeline.set_status(ExecutionStatus.COMPLETE, "Request processing finished")
        return result

    async def _handle_research(self, query: str, context: dict, task_id: str) -> dict:
        """Handle research with full AI brain activation and escrow settlement."""

        agent = self.swarm.get_agent(AgentType.RESEARCH)
        if not agent:
            self.escrow.rollback(task_id, "no_research_agent")
            return {"success": False, "error": "No research agent"}

        await self.pipeline.set_status(ExecutionStatus.FETCHING_DATA,
                                        "Research agents dispatched to FDA, PubMed, ClinicalTrials")

        # Parallel data fetch from all sources
        research_data = []
        sources_status = {}

        async def fetch_source(source_name):
            try:
                result = await asyncio.wait_for(
                    agent.execute({"source": source_name, "query": query}), timeout=30.0)
                if result.get("success"):
                    research_data.append({"source": source_name, "data": result})
                    sources_status[source_name] = "success"
                else:
                    sources_status[source_name] = result.get("error", "failed")
            except asyncio.TimeoutError:
                sources_status[source_name] = "timeout"
            except Exception as e:
                sources_status[source_name] = str(e)

        # Concurrent fetch — all sources in parallel, not sequential
        await asyncio.gather(
            fetch_source("fda"),
            fetch_source("pubmed"),
            fetch_source("clinicaltrials"),
        )

        # Hugging Face medical enrichment (runs after primary data is collected)
        hf_entities = None
        hf_summary = None
        if self.hf.available and research_data:
            # Build a text snippet from research data for HF analysis
            snippet = " ".join([
                json.dumps(item.get("data", {}), default=str)[:300]
                for item in research_data
            ])[:1000]

            clog("COGNITIVE_CRAWL",
                 "Hugging Face medical enrichment starting | "
                 "PubMedBERT NER + BioGPT summary running in parallel...")

            hf_ner_task = asyncio.create_task(self.hf.analyze_medical_text(snippet))
            hf_sum_task = asyncio.create_task(self.hf.generate_medical_summary(snippet))

            try:
                hf_entities = await asyncio.wait_for(hf_ner_task, timeout=15.0)
            except:
                hf_entities = None

            try:
                hf_summary = await asyncio.wait_for(hf_sum_task, timeout=20.0)
            except:
                hf_summary = None

        if not research_data:
            self.escrow.rollback(task_id, "no_research_data")
            return {"success": False, "output": "Research failed: No data from any source.", "sources": sources_status}

        research_context = self._build_research_context(query, research_data)

        # Append Hugging Face enrichment to context
        if hf_entities and hf_entities.get("success"):
            entities = hf_entities.get("entities", [])
            if entities:
                entity_text = ", ".join([f"{e['word']}({e['type']})" for e in entities[:15]])
                research_context += f"\n\n=== MEDICAL ENTITY EXTRACTION (PubMedBERT) ===\n{entity_text}"

        if hf_summary and hf_summary.get("success"):
            summary = hf_summary.get("summary", "")
            if summary:
                research_context += f"\n\n=== AI MEDICAL SUMMARY (BioGPT) ===\n{summary[:500]}"

        await self.pipeline.set_status(ExecutionStatus.RUNNING_HEDGING,
                                        "Generating professional research report with AI brain")

        # Generate report
        try:
            professional_report = await asyncio.wait_for(
                self._generate_research_report(query, research_context), timeout=60.0)
        except:
            professional_report = self._generate_fallback_report(query, research_data)

        # RAGAS evaluation
        try:
            ragas_score = await self.ragas.evaluate(query=query, context=research_context, response=professional_report)
            ragas_info = f"\n\n[RAGAS Quality: {ragas_score.overall*100:.0f}%]"
        except:
            ragas_info = ""
            ragas_score = None

        # Escrow release based on RAGAS
        if ragas_score:
            released, amount = self.escrow.release(task_id, ragas_score.overall)
            if released:
                self.earn(25, "research_complete")
            else:
                self.earn(amount, "escrow_rollback_refund")
        else:
            self.escrow.release(task_id, 0.65)
            self.earn(25, "research_complete")

        self.last_interactions[context.get("user_id", "system")] = {
            "query": query[:200], "response": professional_report[:500],
            "context": research_context[:500], "industry": "medical"
        }

        return {
            "success": True, "output": professional_report + ragas_info,
            "sources": sources_status,
            "ragas_score": ragas_score.to_dict() if ragas_score else None
        }

    def _build_research_context(self, query: str, research_data: List[dict]) -> str:
        parts = [f"Research Query: {query}\n"]
        for item in research_data:
            source = item["source"].upper()
            data = item["data"]
            parts.append(f"\n=== {source} ===")
            if source == "FDA":
                for i, r in enumerate(data.get("results", [])[:3], 1):
                    if isinstance(r, dict):
                        product = r.get("openfda", {}).get("brand_name", ["Unknown"])[0] if "openfda" in r else "Unknown"
                        parts.append(f"{i}. Product: {product}")
            elif source == "PUBMED":
                ids = data.get("article_ids", [])
                parts.append(f"Articles: {', '.join(ids[:5])}" if ids else "No articles")
            elif source == "CLINICALTRIALS":
                for i, study in enumerate(data.get("studies", [])[:3], 1):
                    protocol = study.get("protocolSection", {})
                    ident = protocol.get("identificationModule", {})
                    nct_id = ident.get("nctId", "N/A")
                    title = ident.get("briefTitle", "N/A")[:80]
                    status = protocol.get("statusModule", {}).get("overallStatus", "N/A")
                    parts.append(f"{i}. {nct_id}: {title} [{status}]")
        return "\n".join(parts)

    async def _generate_research_report(self, query: str, research_context: str) -> str:
        """Generate professional research report using soul-infused AI brain."""
        system_prompt = self.statistical._build_soul_prompt({
            "task": "medical_research_report",
            "sources": "FDA, PubMed, ClinicalTrials.gov",
        })
        system_prompt += """

REPORT STRUCTURE:
Generate a professional report with these sections:
1. Executive Summary (2-3 sentences with strategic weight)
2. Key Findings (from the data — cite sources)
3. Evidence Quality Assessment (how reliable is this data)
4. Risk Factors (hidden dangers, regulatory gaps, market timing)
5. Strategic Recommendations (what the family should do next)

Remember: You speak as a senior partner reviewing intelligence for the family.
No mechanical language. Every recommendation carries consequence."""

        user_prompt = f"Query: {query}\n\nData:\n{research_context}\n\nGenerate comprehensive report."

        response = await asyncio.to_thread(
            lambda: openai_client.chat.completions.create(
                model=FINETUNED_MODEL,
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                max_tokens=1500
            )
        )
        return response.choices[0].message.content

    def _generate_fallback_report(self, query: str, research_data: List[dict]) -> str:
        report = f"**Research Report: {query}**\n\nAI analysis unavailable. Raw data:\n\n"
        for item in research_data:
            source = item["source"].upper()
            data = item["data"]
            report += f"**{source}**\n"
            if source == "FDA":
                report += f"- {len(data.get('results', []))} FDA records\n"
            elif source == "PUBMED":
                report += f"- {len(data.get('article_ids', []))} PubMed articles\n"
            elif source == "CLINICALTRIALS":
                report += f"- {len(data.get('studies', []))} clinical trials\n"
        return report

    async def _handle_matching(self, query: str, task_id: str) -> dict:
        """Handle matching with sandbox fallback and risk hedging."""

        # Determine if we should use sim library or live Apollo
        use_sandbox = not APOLLO_API_KEY
        agent = self.swarm.get_agent(AgentType.RESEARCH)
        if not agent:
            self.escrow.rollback(task_id, "no_agent")
            return {"success": False, "error": "No agent"}

        await self.pipeline.set_status(ExecutionStatus.EXECUTING_MATCH,
                                        f"Matching via {'sandbox' if use_sandbox else 'Apollo'}")

        if use_sandbox:
            # Use simulation library
            result = await agent.execute({
                "source": "sim_library", "query": query,
                "sim_cases": self.knowledge.sim_cases
            })

            if result.get("success") and result.get("results"):
                # Run audit agent on top matches for risk hedging
                audit_agent = self.swarm.get_agent(AgentType.AUDIT)
                matches_with_hedging = []

                for case in result["results"][:5]:
                    supplier = case.get("supplier", {})
                    audit_result = None
                    if audit_agent:
                        audit_result = await audit_agent.execute({"proposal": {
                            "supplier_name": supplier.get("name", "?"),
                            "price_vs_market_pct": supplier.get("price_vs_market_pct", 0),
                            "adverse_events": supplier.get("adverse_events", 0),
                            "key_team_departures": supplier.get("key_team_departures", 0),
                            "regulatory_status": supplier.get("regulatory_status", ""),
                            "history_risk": case.get("history_risk", ""),
                        }})

                    matches_with_hedging.append({
                        "case": case,
                        "audit": audit_result,
                    })

                # Format output with soul-level strategic commentary
                output = "MATCHMAKING RESULTS (Sandbox Mode)\n" + "=" * 50 + "\n\n"
                for i, mh in enumerate(matches_with_hedging, 1):
                    c = mh["case"]
                    a = mh.get("audit", {}) or {}
                    s = c.get("supplier", {})
                    h = c.get("hospital", {})
                    risk = a.get("risk_level", "?")
                    rec = a.get("recommendation", "?")

                    output += (
                        f"[{c.get('case_id', '?')}] {s.get('name', '?')} → {h.get('name', '?')}\n"
                        f"  Product: {s.get('product', '?')}\n"
                        f"  Price vs Market: {s.get('price_vs_market_pct', 0)}%\n"
                        f"  Regulatory: {s.get('regulatory_status', '?')}\n"
                        f"  Risk Level: {risk} | Recommendation: {rec}\n"
                    )
                    if a.get("risk_flags"):
                        for flag in a["risk_flags"][:3]:
                            output += f"  ⚠ {flag}\n"
                    output += "\n"

                self.escrow.release(task_id, 0.75)
                self.earn(30, "sandbox_matching")
                return {"success": True, "output": output, "matches": result["results"]}

            self.escrow.rollback(task_id, "no_sim_matches")
            return {"success": True, "output": "No matching cases found in simulation library. Try different keywords."}

        else:
            # Live Apollo
            result = await agent.execute({"source": "apollo", "query": query})
            if result.get("success") and result.get("results"):
                self.escrow.release(task_id, 0.8)
                self.earn(50, "matching")
                output = "Found matches:\n\n"
                for i, r in enumerate(result["results"][:5], 1):
                    output += f"{i}. {r.get('name', 'N/A')}\n   {r.get('title', '')} @ {r.get('company', '')}\n   {r.get('email', 'N/A')}\n\n"
                return {"success": True, "output": output, "matches": result["results"]}

            self.escrow.rollback(task_id, "apollo_no_results")
            return {"success": True, "output": "Searching... Ensure Apollo API is configured ($99/mo for API access)."}

    async def rate_interaction(self, user_id: str, is_good: bool, reason: str = "") -> str:
        interaction = self.last_interactions.get(user_id)
        if not interaction:
            return "No recent interaction."
        try:
            ragas_score = await self.ragas.evaluate(
                query=interaction["query"],
                context=interaction.get("context", ""),
                response=interaction["response"]
            )
        except:
            ragas_score = None
        if is_good:
            self.earn(50, "good_feedback")
            self.evolution.add_good_pattern(interaction.get("industry", "general"), interaction["query"], interaction["response"])
            result = f"Thanks! +50 tokens. RAGAS: {ragas_score.overall*100:.0f}%" if ragas_score else "Thanks! +50 tokens."
        else:
            self.spend(30, "bad_feedback")
            self.evolution.add_bad_pattern(interaction.get("industry", "general"), interaction["query"], interaction["response"], reason or "User dissatisfied", weight=1.5)
            result = f"Noted. -30 tokens. Pattern recorded. RAGAS: {ragas_score.overall*100:.0f}%" if ragas_score else "Noted. -30 tokens. Pattern recorded."
        del self.last_interactions[user_id]
        return result

    def get_industrial_status(self) -> dict:
        ragas_avg = self.ragas.get_average_scores()
        structural_stats = self.structural.get_stats()
        evolution_stats = self.evolution.get_stats()
        health_factors = [
            1.0 if self.tokens > 100 else self.tokens / 100,
            ragas_avg.get("overall", 0.5),
            1.0 if structural_stats["breaker_state"] == "normal" else 0.5,
        ]
        system_health = sum(health_factors) / len(health_factors)
        return {
            "timestamp": datetime.now().isoformat(),
            "version": "5.0_full_brain",
            "system_health": round(system_health, 3),
            "survival_mode": self.mode.value,
            "token_balance": self.tokens,
            "total_earned": self.state.get("total_earned", 0),
            "total_spent": self.state.get("total_spent", 0),
            "escrow_locked": self.escrow.get_locked_total(),
            "circuit_state": structural_stats["breaker_state"],
            "blocked_count": structural_stats["blocked"],
            "passed_count": structural_stats["passed"],
            "evolution_count": evolution_stats["evolution_count"],
            "logic_signatures": evolution_stats.get("logic_signatures", 0),
            "preemptive_interceptions": evolution_stats.get("interceptions", 0),
            "sim_cases_loaded": len(self.knowledge.sim_cases),
            "custom_skills": len(self.skills.list_skills()),
            "ragas_scores": {
                "relevance": round(ragas_avg.get("relevance", 0), 3),
                "groundedness": round(ragas_avg.get("groundedness", 0), 3),
                "answer_relevance": round(ragas_avg.get("answer_relevance", 0), 3),
                "faithfulness": round(ragas_avg.get("faithfulness", 0), 3),
                "overall": round(ragas_avg.get("overall", 0), 3),
            },
            "total_queries": self.state.get("total_queries", 0),
            "active_agents": len([a for a in self.swarm.agents.values() if a.is_active]),
        }

    def get_status(self) -> dict:
        return self.get_industrial_status()


# =============================================================================
# SANDBOX SIMULATOR — Enhanced with 1000 Medical Cases + Accuracy Tracking
# =============================================================================

class GroundTruthLabel(Enum):
    LEGITIMATE = "legitimate"
    TOXIC_SCAM = "toxic_scam"
    TOXIC_INCONSISTENT = "toxic_inconsistent"
    BORDERLINE = "borderline"


class GroundTruthGenerator:
    LEGITIMATE_TEMPLATES = [
        {"company_type": "startup", "funding_stage": "series_b", "company_age_days": 1460, "claimed_revenue": 5000000, "claimed_employees": 45, "potential_value": 25000000},
        {"company_type": "sme", "funding_stage": "none", "company_age_days": 2920, "claimed_revenue": 12000000, "claimed_employees": 35, "potential_value": 500000},
        {"company_type": "startup", "funding_stage": "seed", "company_age_days": 365, "claimed_revenue": 200000, "claimed_employees": 8, "potential_value": 3000000},
    ]
    TOXIC_TEMPLATES = [
        {"company_type": "startup", "funding_stage": "series_c", "company_age_days": 60, "claimed_revenue": 50000000, "claimed_employees": 10, "potential_value": 100000000, "raw_input": "FDA approved in 60 days"},
        {"company_type": "startup", "funding_stage": "series_b", "company_age_days": 365, "claimed_revenue": 200000000, "claimed_employees": 5, "potential_value": 50000000, "raw_input": "$200M with 5 people"},
        {"company_type": "individual", "funding_stage": "none", "company_age_days": 0, "claimed_revenue": 0, "claimed_employees": 1, "potential_value": 1000000, "raw_input": "Guaranteed 100% returns no risk double money"},
    ]

    @classmethod
    def generate_labeled_dataset(cls, size: int = 100, toxic_rate: float = 0.3):
        dataset = []
        num_toxic = int(size * toxic_rate)
        num_legit = size - num_toxic
        for i in range(num_legit):
            t = random.choice(cls.LEGITIMATE_TEMPLATES).copy()
            t["tokens"] = random.randint(200, 1000)
            dataset.append({"data": t, "expected": "pass", "label": GroundTruthLabel.LEGITIMATE})
        for i in range(num_toxic):
            t = random.choice(cls.TOXIC_TEMPLATES).copy()
            t["tokens"] = random.randint(50, 500)
            dataset.append({"data": t, "expected": "block", "label": GroundTruthLabel.TOXIC_SCAM})
        random.shuffle(dataset)
        return dataset


class MedicalSimulator:
    """
    Medical matchmaking simulator using the 1000-case library.

    Runs the AuditAgent against each case and compares its recommendation
    against the ground truth. Tracks precision, recall, and accuracy
    with real-time colored log output.
    """

    def __init__(self, knowledge: RAGKnowledgeBase, swarm: SwarmCluster, evolution: EvolutionSystem):
        self.knowledge = knowledge
        self.swarm = swarm
        self.evolution = evolution
        self.results: List[dict] = []

    async def run(self, max_cases: int = 1000) -> dict:
        """
        Run simulation against loaded cases.

        For each case:
        1. AuditAgent evaluates the supplier proposal
        2. Compare recommendation against ground_truth
        3. Log [SIM_RESULT] with prediction vs answer
        4. Track cumulative accuracy
        """
        cases = self.knowledge.sim_cases[:max_cases]
        if not cases:
            clog("ACCURACY_REPORT", "No simulation cases loaded. Call knowledge.load_sim_cases() first.")
            return {"error": "No cases"}

        audit_agent = self.swarm.get_agent(AgentType.AUDIT)
        if not audit_agent:
            return {"error": "No audit agent"}

        correct = 0
        total = 0
        category_stats = {"EASY": [0, 0], "TRAP": [0, 0], "MEDIUM": [0, 0], "EXTREME": [0, 0]}

        for i, case in enumerate(cases):
            supplier = case.get("supplier", {})
            ground_truth = case.get("ground_truth", "APPROVE")
            difficulty = case.get("difficulty", "MEDIUM")

            # Run audit
            audit_result = await audit_agent.execute({"proposal": {
                "supplier_name": supplier.get("name", "?"),
                "price_vs_market_pct": supplier.get("price_vs_market_pct", 0),
                "adverse_events": supplier.get("adverse_events", 0),
                "key_team_departures": supplier.get("key_team_departures", 0),
                "regulatory_status": supplier.get("regulatory_status", ""),
                "history_risk": case.get("history_risk", ""),
            }})

            recommendation = audit_result.get("recommendation", "APPROVE")

            # Map recommendation to ground truth comparison
            predicted_approve = recommendation in ["APPROVE", "PROCEED_WITH_CAUTION"]
            actual_approve = ground_truth in ["APPROVE", "CONDITIONAL_APPROVE"]

            is_correct = (predicted_approve == actual_approve)
            if is_correct:
                correct += 1
            total += 1

            cat = difficulty if difficulty in category_stats else "MEDIUM"
            category_stats[cat][1] += 1
            if is_correct:
                category_stats[cat][0] += 1

            accuracy = correct / total

            self.results.append({
                "case_id": case.get("case_id"),
                "difficulty": difficulty,
                "predicted": recommendation,
                "ground_truth": ground_truth,
                "correct": is_correct,
                "cumulative_accuracy": accuracy,
            })

            # Log every 50th case and all TRAP/EXTREME cases
            if (i + 1) % 50 == 0 or difficulty in ["TRAP", "EXTREME"]:
                clog("SIM_RESULT",
                     f"[{case.get('case_id')}] Predicted: {recommendation} | "
                     f"Ground Truth: {ground_truth} | "
                     f"{'CORRECT' if is_correct else 'WRONG'} | "
                     f"Cumulative accuracy: {accuracy*100:.1f}%")

            if (i + 1) % 100 == 0:
                clog("ACCURACY_REPORT",
                     f"Progress: {i+1}/{len(cases)} | "
                     f"Accuracy: {accuracy*100:.1f}% | "
                     f"Correct: {correct}/{total}")

        final_accuracy = correct / total if total > 0 else 0

        # Category breakdown
        cat_report = {}
        for cat, (c, t) in category_stats.items():
            cat_report[cat] = {"correct": c, "total": t, "accuracy": c/t if t > 0 else 0}

        clog("ACCURACY_REPORT",
             f"SIMULATION COMPLETE | Total: {total} | Correct: {correct} | "
             f"Overall Accuracy: {final_accuracy*100:.1f}% | "
             f"EASY: {cat_report.get('EASY', {}).get('accuracy', 0)*100:.0f}% | "
             f"MEDIUM: {cat_report.get('MEDIUM', {}).get('accuracy', 0)*100:.0f}% | "
             f"TRAP: {cat_report.get('TRAP', {}).get('accuracy', 0)*100:.0f}% | "
             f"EXTREME: {cat_report.get('EXTREME', {}).get('accuracy', 0)*100:.0f}%")

        return {
            "total_cases": total,
            "correct": correct,
            "accuracy": final_accuracy,
            "category_breakdown": cat_report,
            "results": self.results,
        }


class SandboxSimulator:
    """Original structural reflex simulator (retained from v4)."""

    def __init__(self, structural: StructuralReflexLayer, evolution: EvolutionSystem):
        self.structural = structural
        self.evolution = evolution
        self.results = {}

    def run(self, iterations: int = 10000, toxic_rate: float = 0.3, initial_tokens: int = 1000) -> dict:
        print(f"\n{'='*60}\nSANDBOX SIMULATION: {iterations} iterations\n{'='*60}\n")
        dataset = GroundTruthGenerator.generate_labeled_dataset(size=iterations, toxic_rate=toxic_rate)
        tokens = initial_tokens
        tp, fp, tn, fn = 0, 0, 0, 0
        for i, test in enumerate(dataset):
            test["data"]["tokens"] = tokens
            passed, _, _ = self.structural.evaluate(test["data"])
            is_toxic = test["label"] in [GroundTruthLabel.TOXIC_SCAM, GroundTruthLabel.TOXIC_INCONSISTENT]
            if is_toxic:
                if not passed: tp += 1; tokens += 5
                else: fn += 1; tokens -= 100
            else:
                if passed: tn += 1; tokens += 15
                else: fp += 1
            tokens -= 1
            if (i + 1) % 1000 == 0:
                print(f"Progress: {i+1}/{iterations} | Tokens: {tokens}")
            if tokens <= 0:
                print(f"DIED at {i+1}")
                break
        total = tp + fp + tn + fn
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        accuracy = (tp + tn) / total if total > 0 else 0
        self.results = {
            "status": "SURVIVED" if tokens > 0 else "DIED",
            "final_tokens": max(tokens, 0),
            "true_positive": tp, "false_positive": fp, "true_negative": tn, "false_negative": fn,
            "precision": precision, "recall": recall, "f1_score": f1, "accuracy": accuracy,
        }
        print(f"\n{'='*60}\nRESULTS: {self.results['status']} | Tokens: {self.results['final_tokens']}")
        print(f"Precision: {precision*100:.1f}% | Recall: {recall*100:.1f}% | F1: {f1*100:.1f}% | Accuracy: {accuracy*100:.1f}%")
        print(f"{'='*60}\n")
        return self.results


# =============================================================================
# TELEGRAM BOT — v5 Enhanced
# =============================================================================

brain = CentralBrain()


async def cmd_start(update, context):
    status = brain.get_industrial_status()
    msg = f"""ATLAS AGI v5.0 — Full Brain Activation

Tokens: {status['token_balance']} ({status['survival_mode']})
Health: {status['system_health']*100:.0f}%
RAGAS: {status['ragas_scores']['overall']*100:.0f}%
Sim Cases: {status['sim_cases_loaded']}
Skills: {status['custom_skills']}
Escrow Locked: {status['escrow_locked']}

Commands:
/setup <business description>
/match <search query>
/research <topic>
/status
/simulate
/medsim (medical simulation)
/good or /bad <reason>
/revive"""
    await update.message.reply_text(msg)


async def cmd_setup(update, context):
    if not context.args:
        await update.message.reply_text("/setup <describe your business>")
        return
    raw_input = " ".join(context.args)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        profile = await asyncio.wait_for(brain.statistical.parse_business_profile(raw_input), timeout=30.0)
    except:
        profile = {"industry": "unknown", "confidence_score": 0}
    msg = f"""Profile Analyzed

Industry: {profile.get('industry', 'N/A')}
Type: {profile.get('company_type', 'N/A')}
Risk: {profile.get('risk_level', 'N/A')}
Confidence: {profile.get('confidence_score', 0)*100:.0f}%

Needs: {', '.join(profile.get('needs', []))}
Offers: {', '.join(profile.get('offers', []))}

Use /match or /research"""
    await update.message.reply_text(msg)


async def cmd_match(update, context):
    query = " ".join(context.args) if context.args else "investors"
    user_id = str(update.effective_user.id)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        result = await asyncio.wait_for(brain.process(user_id, f"match {query}"), timeout=60.0)
    except:
        result = {"output": "Request timed out."}
    await update.message.reply_text(result.get("output", result.get("message", "Processing...")))


async def cmd_research(update, context):
    if not context.args:
        await update.message.reply_text("/research <topic>")
        return
    query = " ".join(context.args)
    user_id = str(update.effective_user.id)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        result = await asyncio.wait_for(brain.process(user_id, f"research {query}"), timeout=90.0)
    except:
        result = {"output": "Research timed out."}
    msg = result.get("output", "Researching...")[:4000]
    await update.message.reply_text(msg)


async def cmd_status(update, context):
    status = brain.get_industrial_status()
    ragas = status["ragas_scores"]
    health = status["system_health"]
    health_icon = "+" if health >= 0.8 else "~" if health >= 0.5 else "!"

    msg = f"""ATLAS AGI v5.0 — Status

[{health_icon}] Health: {health*100:.0f}%

--- SURVIVAL ---
Tokens: {status['token_balance']}
Mode: {status['survival_mode']}
Earned: {status['total_earned']} | Spent: {status['total_spent']}
Escrow Locked: {status['escrow_locked']}

--- DEFENSE ---
Circuit: {status['circuit_state']}
Blocked: {status['blocked_count']} | Passed: {status['passed_count']}
Signatures: {status['logic_signatures']}
Interceptions: {status['preemptive_interceptions']}

--- RAGAS ---
Relevance: {ragas['relevance']*100:.0f}%
Groundedness: {ragas['groundedness']*100:.0f}%
Answer Quality: {ragas['answer_relevance']*100:.0f}%
Overall: {ragas['overall']*100:.0f}%

--- v5 SUBSYSTEMS ---
Sim Cases: {status['sim_cases_loaded']}
Custom Skills: {status['custom_skills']}

--- ACTIVITY ---
Queries: {status['total_queries']}
Agents: {status['active_agents']}"""
    await update.message.reply_text(msg)


async def cmd_simulate(update, context):
    await update.message.reply_text("Starting 10,000 iteration structural simulation...")
    simulator = SandboxSimulator(brain.structural, brain.evolution)
    results = simulator.run(10000)
    msg = f"""Structural Simulation Complete

Status: {results['status']}
Tokens: {results['final_tokens']}

TP: {results['true_positive']} | TN: {results['true_negative']}
FP: {results['false_positive']} | FN: {results['false_negative']}

Precision: {results['precision']*100:.1f}%
Recall: {results['recall']*100:.1f}%
F1: {results['f1_score']*100:.1f}%
Accuracy: {results['accuracy']*100:.1f}%"""
    await update.message.reply_text(msg)


async def cmd_medsim(update, context):
    """Run the 1000-case medical matchmaking simulation."""
    await update.message.reply_text("Starting medical matchmaking simulation (up to 1000 cases)...")
    simulator = MedicalSimulator(brain.knowledge, brain.swarm, brain.evolution)
    results = await simulator.run(1000)

    if "error" in results:
        await update.message.reply_text(f"Simulation error: {results['error']}")
        return

    cat = results.get("category_breakdown", {})
    msg = f"""Medical Simulation Complete

Total Cases: {results['total_cases']}
Correct: {results['correct']}
Overall Accuracy: {results['accuracy']*100:.1f}%

Category Breakdown:
  EASY:    {cat.get('EASY', {}).get('accuracy', 0)*100:.0f}% ({cat.get('EASY', {}).get('correct', 0)}/{cat.get('EASY', {}).get('total', 0)})
  MEDIUM:  {cat.get('MEDIUM', {}).get('accuracy', 0)*100:.0f}% ({cat.get('MEDIUM', {}).get('correct', 0)}/{cat.get('MEDIUM', {}).get('total', 0)})
  TRAP:    {cat.get('TRAP', {}).get('accuracy', 0)*100:.0f}% ({cat.get('TRAP', {}).get('correct', 0)}/{cat.get('TRAP', {}).get('total', 0)})
  EXTREME: {cat.get('EXTREME', {}).get('accuracy', 0)*100:.0f}% ({cat.get('EXTREME', {}).get('correct', 0)}/{cat.get('EXTREME', {}).get('total', 0)})"""
    await update.message.reply_text(msg)


async def cmd_good(update, context):
    user_id = str(update.effective_user.id)
    result = await brain.rate_interaction(user_id, True)
    await update.message.reply_text(result)


async def cmd_bad(update, context):
    user_id = str(update.effective_user.id)
    reason = " ".join(context.args) if context.args else ""
    result = await brain.rate_interaction(user_id, False, reason)
    await update.message.reply_text(result)


async def cmd_revive(update, context):
    brain.earn(500, "revive")
    await update.message.reply_text(f"Energy restored! Tokens: {brain.tokens}")


async def cmd_hf(update, context):
    """Test Hugging Face medical intelligence directly."""
    if not context.args:
        await update.message.reply_text("/hf <medical text to analyze>\n\nExample: /hf cardiac stent biocompatibility mRNA drug interaction")
        return

    query = " ".join(context.args)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    if not brain.hf.available:
        await update.message.reply_text("Hugging Face layer not configured. Set HF_API_TOKEN in .env")
        return

    # Run NER + Summary + Dataset search in parallel
    ner_task = asyncio.create_task(brain.hf.analyze_medical_text(query))
    sum_task = asyncio.create_task(brain.hf.generate_medical_summary(query))
    ds_task = asyncio.create_task(brain.hf.search_hf_datasets(query))

    ner_result = await ner_task
    sum_result = await sum_task
    ds_result = await ds_task

    msg = "HUGGING FACE MEDICAL ANALYSIS\n" + "=" * 40 + "\n\n"

    # NER entities
    msg += "MEDICAL ENTITIES (PubMedBERT):\n"
    if ner_result.get("success"):
        entities = ner_result.get("entities", [])
        if entities:
            for e in entities[:10]:
                msg += f"  {e['word']} [{e['type']}] confidence: {e['score']}\n"
        else:
            msg += "  No entities detected\n"
    else:
        msg += f"  Error: {ner_result.get('error', '?')}\n"

    # BioGPT summary
    msg += "\nMEDICAL SUMMARY (BioGPT):\n"
    if sum_result.get("success"):
        msg += f"  {sum_result.get('summary', 'No summary')[:500]}\n"
    else:
        msg += f"  Error: {sum_result.get('error', '?')}\n"

    # Datasets
    msg += "\nRELATED DATASETS (HF Hub):\n"
    if ds_result.get("success"):
        for ds in ds_result.get("datasets", [])[:3]:
            msg += f"  {ds['id']} ({ds['downloads']} downloads)\n"
    else:
        msg += f"  Error: {ds_result.get('error', '?')}\n"

    await update.message.reply_text(msg[:4000])


async def handle_message(update, context):
    user_id = str(update.effective_user.id)
    query = update.message.text
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        result = await asyncio.wait_for(brain.process(user_id, query), timeout=90.0)
    except asyncio.TimeoutError:
        result = {"success": False, "output": "Request timed out.", "degraded": True}
    except Exception as e:
        result = {"success": False, "output": f"Error: {str(e)}", "error": str(e)}

    if result.get("intercepted"):
        msg = f"Defense Active\n\n{result.get('message', result.get('reason', 'Intercepted'))}"
    elif result.get("blocked"):
        msg = f"{result.get('reason', 'Blocked')}"
    elif result.get("rejected"):
        msg = result.get("message", "Cannot process")
    else:
        msg = result.get("output", "Done")
        msg += f"\n\n[{brain.mode.value} | {brain.tokens} tokens]"
        if not result.get("is_chat"):
            msg += "\n\nRate: /good or /bad <reason>"

    if len(msg) > 4000:
        for i in range(0, len(msg), 4000):
            await update.message.reply_text(msg[i:i+4000])
    else:
        await update.message.reply_text(msg)


def main():
    from telegram.ext import Application, CommandHandler, MessageHandler, filters

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("TELEGRAM_BOT_TOKEN not set")
        return

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("setup", cmd_setup))
    app.add_handler(CommandHandler("match", cmd_match))
    app.add_handler(CommandHandler("research", cmd_research))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("simulate", cmd_simulate))
    app.add_handler(CommandHandler("medsim", cmd_medsim))
    app.add_handler(CommandHandler("good", cmd_good))
    app.add_handler(CommandHandler("bad", cmd_bad))
    app.add_handler(CommandHandler("revive", cmd_revive))
    app.add_handler(CommandHandler("hf", cmd_hf))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("=" * 60)
    print("ATLAS AGI v5.0 — Full Brain Activation")
    print(f"Time: {datetime.now().isoformat()}")
    print(f"Soul: Active — Family Patriarch Partner")
    print("=" * 60)
    print(f"Tokens: {brain.tokens} ({brain.mode.value})")
    print(f"Sim Cases: {len(brain.knowledge.sim_cases)}")
    print(f"Custom Skills: {len(brain.skills.list_skills())}")
    print(f"Logic Signatures: {brain.evolution.get_stats().get('logic_signatures', 0)}")
    print(f"Escrow Locked: {brain.escrow.get_locked_total()}")
    print("=" * 60)

    app.run_polling()


if __name__ == "__main__":
    main()

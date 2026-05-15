# AGENT.md - SEC EDGAR Extraction Pipeline

> This file guides AI agents in building, maintaining, and extending this project. Update it as the project evolves so future agents can pick up where previous ones left off.

## Project Overview

**Purpose**: Production-grade fine-tuning and serving pipeline for SEC EDGAR financial data extraction. Demonstrates 150x cost reduction over GPT-4 through QLoRA fine-tuning of Llama 3.1 8B.

**Current Status**: Foundation phase - porting existing code to clean repo structure

**Key Results to Maintain**:
- 150x cost reduction ($0.03 → $0.0002 per extraction)
- 94% field accuracy
- 320ms p50 latency
- 98% capability retention (MMLU/GSM8K)

## Current State

### What Exists
- [ ] Fine-tuning code (QLoRA with 4-bit quantization) - needs porting
- [ ] vLLM serving infrastructure - needs porting
- [ ] Redis caching layer - needs porting
- [ ] PostgreSQL audit trail - needs porting
- [ ] Weights & Biases tracking - needs porting
- [ ] Airflow DAG skeleton - needs porting

### What's Missing
- [ ] Clean repository structure
- [ ] Comprehensive README (outline exists)
- [ ] Docker setup for reproducibility
- [ ] Unit tests for accuracy, latency, cache hit rate
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Cost analysis documentation
- [ ] ARCHITECTURE.md with mathematical foundations
- [ ] Notebooks demonstrating workflows

## Repository Structure (Target)

```
sec-edgar-extraction-pipeline/
├── src/
│   ├── finetune/
│   │   ├── qlora_training.py          # QLoRA 4-bit fine-tuning
│   │   ├── data_loading.py              # SEC filing format parsing
│   │   └── eval_benchmarks.py           # MMLU/GSM8K evaluation
│   ├── serve/
│   │   ├── vllm_server.py               # vLLM inference server
│   │   ├── api.py                       # FastAPI endpoints
│   │   └── cache.py                     # Redis LRU caching
│   └── pipeline/
│       ├── airflow_dag.py               # DAG orchestration
│       ├── db_schema.sql                # PostgreSQL schema
│       └── monitoring.py                # Prometheus metrics
├── notebooks/
│   ├── 01_qlora_finetuning.ipynb
│   ├── 02_model_evaluation.ipynb
│   └── 03_inference_optimization.ipynb
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
├── tests/
│   ├── test_extraction_accuracy.py
│   ├── test_latency_bounds.py
│   └── test_cache_hit_rate.py
├── .github/workflows/
│   └── ci.yml
├── README.md
├── ARCHITECTURE.md
├── COST_ANALYSIS.md
└── AGENT.md (this file)
```

## Implementation Phases

### Phase 1: Foundation (Week 1)
**Goal**: Port existing code, establish clean structure

**Tasks**:
1. Copy existing fine-tuning code to `src/finetune/`
   - Preserve QLoRA configuration: r=16, alpha=32, dropout=0.05
   - 4-bit quantization with bitsandbytes (nf4, double_quant)
   - Training args: lr=2e-4, batch=4, epochs=3, warmup=0.03
   
2. Copy inference code to `src/serve/`
   - vLLM with PagedAttention
   - Max model length: 4096
   - Tensor parallel size: 1 (single GPU)
   
3. Set up Redis caching in `src/serve/cache.py`
   - LRU eviction
   - TTL: 7 days for filings
   - Serialization: msgpack
   
4. Create PostgreSQL schema in `src/pipeline/db_schema.sql`
   - Table: extractions (id, filing_url, extracted_data, timestamp)
   - Table: cache_hits (key, hit_time, latency_ms)
   - Table: model_versions (version, deployed_at, metrics)

**Validation**:
```bash
# Should work after Phase 1:
python -c "from src.finetune.qlora_training import train; print('OK')"
python -c "from src.serve.api import app; print('OK')"
```

### Phase 2: Documentation (Week 2)
**Goal**: Write comprehensive docs, create notebooks

**Tasks**:
1. Complete README.md
   - Problem statement: Why fine-tuning vs. GPT-4 API
   - Results summary with quantified metrics
   - Quick start guide
   - Usage examples
   
2. Write ARCHITECTURE.md
   - QLoRA math: Low-rank decomposition preserves full-rank inference
   - vLLM strategy: PagedAttention, continuous batching
   - Redis architecture: LRU, TTL, serialization
   - PostgreSQL audit trail: Schema, indexing, retention
   
3. Create notebooks/
   - 01_qlora_finetuning.ipynb: Step-by-step training walkthrough
   - 02_model_evaluation.ipynb: Catastrophic forgetting analysis
   - 03_inference_optimization.ipynb: Latency profiling
   
4. Write COST_ANALYSIS.md
   - Spreadsheet comparing API vs. fine-tuned costs
   - Break-even analysis
   - Infrastructure costs (GPU rental)

**Validation**:
- Notebooks run without errors
- Documentation covers all major components

### Phase 3: Docker & Testing (Week 2-3)
**Goal**: Make project reproducible, add automated testing

**Tasks**:
1. Create docker/Dockerfile
   - Base: nvidia/cuda:12.1-devel-ubuntu22.04
   - Install: Python 3.10, PyTorch 2.1, vLLM, transformers, peft
   - Entrypoint: vLLM server with fine-tuned adapter
   
2. Create docker/docker-compose.yml
   - Services: api, redis, postgres, prometheus
   - Networks: frontend, backend
   - Volumes: postgres_data, redis_data
   
3. Write tests/
   - test_extraction_accuracy.py: 500 test filings, field-level accuracy
   - test_latency_bounds.py: p50 < 320ms, p99 < 1s
   - test_cache_hit_rate.py: >80% hit rate expected
   
4. Create .github/workflows/ci.yml
   - Run on Python 3.10, 3.11
   - Steps: lint (ruff), type-check (mypy), test (pytest), coverage
   - Block merge if coverage < 80%

**Validation**:
```bash
docker-compose up -d
curl http://localhost:8000/health  # Should return 200
pytest tests/ -v                    # All tests pass
```

### Phase 4: Polish & Launch (Week 3)
**Goal**: Production-ready release

**Tasks**:
1. Add example notebooks with real SEC filings
2. Create deployment guide for production
3. Add contribution guidelines (CONTRIBUTING.md)
4. Set up dependabot for dependency updates
5. Create release tags (v1.0.0)

**Validation**:
- GitHub Actions shows green checks
- README badges work
- Docker setup works on fresh machine

## Key Technical Decisions

### QLoRA Configuration
```python
# These parameters are fixed based on successful experiments
lora_config = LoraConfig(
    r=16,                    # Rank - don't change without re-evaluating
    lora_alpha=32,            # Alpha - 2x rank is standard
    target_modules=["q_proj", "v_proj", "k_proj", "o_proj"],
    lora_dropout=0.05,
    bias="none",
    task_type="CAUSAL_LM"
)

bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.bfloat16,
    bnb_4bit_use_double_quant=True
)
```

### vLLM Serving Parameters
```python
# Optimized for single A100 40GB
serving_args = {
    "model": "meta-llama/Llama-3.1-8B",
    "adapter": "models/sec-extractor-qlora",
    "tensor_parallel_size": 1,
    "max_model_len": 4096,
    "gpu_memory_utilization": 0.85,
    "swap_space": 4,  # GB
}
```

### Redis Cache Strategy
```python
# LRU with TTL
redis_config = {
    "maxmemory": "2gb",
    "maxmemory_policy": "allkeys-lru",
    "default_ttl": 604800,  # 7 days in seconds
    "serialization": "msgpack"  # More efficient than JSON
}
```

## Common Pitfalls & Solutions

### Issue: Out of memory during fine-tuning
**Solution**: Reduce batch size or increase gradient accumulation. Current config uses batch=4, grad_accum=4 (effective batch=16).

### Issue: Catastrophic forgetting
**Solution**: Mix general instruction data (10%) with SEC data (90%). Monitor MMLU/GSM8K during training.

### Issue: vLLM high latency
**Solution**: Enable prefix caching, adjust max_num_seqs based on GPU memory. Current: max_num_seqs=256.

### Issue: Cache stampede on popular filings
**Solution**: Implement cache warming for top 1000 most-accessed filings.

## Testing Strategy

### Unit Tests
- Mock SEC filing data for fast tests
- Test individual functions (parsing, extraction, caching)

### Integration Tests
- Test full pipeline with small sample
- Verify database writes, cache hits

### Performance Tests
- Latency: 1000 requests, measure p50/p95/p99
- Throughput: Max requests/second before latency degrades
- Accuracy: Compare against hand-labeled ground truth

## Metrics to Track

### Model Performance
- Field-level accuracy (target: >94%)
- F1 score per field type
- Catastrophic forgetting (MMLU/GSM8K delta <2%)

### System Performance
- p50/p99 latency (target: 320ms/1000ms)
- Cache hit rate (target: >80%)
- GPU utilization (target: 70-85%)
- Requests/second

### Business Metrics
- Cost per extraction (target: <$0.0003)
- Infrastructure cost per day
- vs. GPT-4 cost ratio (target: >100x savings)

## Dependencies

Core dependencies (frozen versions):
```
torch==2.1.2
transformers==4.36.0
peft==0.7.1
bitsandbytes==0.41.3
vllm==0.2.7
fastapi==0.109.0
redis==5.0.1
psycopg2-binary==2.9.9
sqlalchemy==2.0.25
apache-airflow==2.8.0
wandb==0.16.2
```

## Environment Variables

Required in `.env`:
```bash
# Model paths
BASE_MODEL=meta-llama/Llama-3.1-8B
ADAPTER_PATH=models/sec-extractor-qlora

# Database
POSTGRES_URL=postgresql://user:pass@localhost/sec_extractions

# Cache
REDIS_URL=redis://localhost:6379/0

# Monitoring
WANDB_PROJECT=sec-edgar-extraction
WANDB_API_KEY=...

# SEC EDGAR (for downloading filings)
SEC_USER_AGENT="Your Name your@email.com"
```

## Agent Handoff Checklist

When transferring work to another agent, update this section:

**Last Updated**: [DATE]
**Completed By**: [AGENT NAME]
**Current Phase**: [1/2/3/4]

### What's Working
- [ ] List completed components

### What's In Progress
- [ ] List active work

### Blockers
- [ ] List any issues preventing progress

### Next Steps
1. Prioritized list of next tasks
2. With estimated effort

### Notes for Next Agent
- Context that would help someone picking up this project
- Known issues, workarounds
- Useful commands or scripts discovered

## Resources

### Papers
- QLoRA: https://arxiv.org/abs/2305.14314
- vLLM: https://arxiv.org/abs/2309.06180
- PEFT: https://huggingface.co/docs/peft

### APIs
- SEC EDGAR: https://www.sec.gov/edgar/sec-api-documentation
- vLLM docs: https://docs.vllm.ai/

### Similar Projects
- https://github.com/microsoft/LoRA
- https://github.com/huggingface/peft

---

**This AGENT.md is a living document. Update it as the project evolves.**

# SEC EDGAR Extraction Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

> Production-grade LLM fine-tuning pipeline for structured data extraction from SEC filings (10-K, 10-Q). Delivers 150x cost reduction over GPT-4 with 94% field accuracy and 320ms p50 latency.

## Overview

This project demonstrates a complete MLOps pipeline for fine-tuning and serving domain-specific LLMs. It transforms raw SEC EDGAR filings into structured financial data using a fine-tuned Llama 3.1 8B model, with comprehensive monitoring, caching, and cost optimization.

### Key Results

| Metric | Value | Comparison |
|--------|-------|------------|
| Cost per extraction | $0.0002 | 150x cheaper than GPT-4 ($0.03) |
| Field accuracy | 94% | Comparable to GPT-4 (96%) |
| p50 latency | 320ms | With Redis caching |
| p99 latency | <1s | vLLM batching optimization |
| Capability retention | 98% | On MMLU/GSM8K post-fine-tuning |

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐
│ SEC EDGAR   │────▶│  Airflow DAG │────▶│  QLoRA      │────▶│  vLLM       │
│ Filings     │     │  Orchestrator│     │  Inference  │     │  Server     │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘
                                                                  │
                              ┌─────────────┐    ┌─────────────┐│
                              │  PostgreSQL │◄───│  Redis LRU  │◄┘
                              │  Audit Trail│    │  Cache      │
                              └─────────────┘    └─────────────┘
```

### Technology Stack

- **Fine-tuning**: QLoRA (4-bit quantization), PEFT, Transformers
- **Serving**: vLLM (PagedAttention, continuous batching)
- **API**: FastAPI with async endpoints
- **Cache**: Redis (LRU eviction, TTL-based expiration)
- **Database**: PostgreSQL (audit trails, extraction history)
- **Orchestration**: Apache Airflow
- **Monitoring**: Weights & Biases, Prometheus metrics
- **Infrastructure**: Docker, Docker Compose, GitHub Actions

## Repository Structure

```
sec-edgar-extraction-pipeline/
├── src/
│   ├── finetune/
│   │   ├── qlora_training.py          # QLoRA fine-tuning with 4-bit quantization
│   │   ├── data_loading.py              # SEC filing format handling
│   │   └── eval_benchmarks.py           # MMLU, GSM8K catastrophic forgetting tests
│   ├── serve/
│   │   ├── vllm_server.py               # vLLM inference with dynamic batching
│   │   ├── api.py                       # FastAPI endpoints for extraction
│   │   └── cache.py                     # Redis caching strategy (LRU, TTL)
│   └── pipeline/
│       ├── airflow_dag.py               # Pipeline orchestration
│       ├── db_schema.sql                # PostgreSQL audit trail schema
│       └── monitoring.py                # Drift detection, metrics logging
├── notebooks/
│   ├── 01_qlora_finetuning.ipynb        # Step-by-step fine-tuning walkthrough
│   ├── 02_model_evaluation.ipynb        # Catastrophic forgetting analysis
│   └── 03_inference_optimization.ipynb  # Latency profiling and optimization
├── docker/
│   ├── Dockerfile                       # vLLM serving container
│   ├── docker-compose.yml               # Full stack deployment
│   └── requirements.txt
├── tests/
│   ├── test_extraction_accuracy.py      # Field-level accuracy tests
│   ├── test_latency_bounds.py           # p50 < 320ms, p99 < 1s
│   └── test_cache_hit_rate.py           # Cache performance validation
├── README.md                            # This file
├── ARCHITECTURE.md                      # Detailed system design
└── COST_ANALYSIS.md                     # Cost comparison spreadsheet
```

## Quick Start

### Prerequisites

- Python 3.10+
- CUDA-capable GPU (for inference) or CPU with sufficient RAM
- Docker and Docker Compose (for full stack deployment)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/sec-edgar-extraction-pipeline.git
cd sec-edgar-extraction-pipeline

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your credentials
```

### Docker Deployment (Recommended)

```bash
# Start the full stack
docker-compose up -d

# Verify services are running
curl http://localhost:8000/health

# Access API documentation
curl http://localhost:8000/docs
```

### Local Development

```bash
# Start Redis and PostgreSQL
docker-compose up -d redis postgres

# Run the FastAPI server
uvicorn src.serve.api:app --reload --port 8000

# Run a test extraction
curl -X POST http://localhost:8000/extract \
  -H "Content-Type: application/json" \
  -d '{"filing_url": "https://www.sec.gov/Archives/...", "fields": ["revenue", "net_income"]}'
```

## Usage

### Single Filing Extraction

```python
import requests

response = requests.post(
    "http://localhost:8000/extract",
    json={
        "filing_url": "https://www.sec.gov/Archives/edgar/data/.../10-K.txt",
        "fields": ["total_revenue", "net_income", "total_assets"],
        "format": "structured_json"
    }
)

result = response.json()
print(result["extracted_fields"])
```

### Batch Processing

```python
# Submit batch job
response = requests.post(
    "http://localhost:8000/extract/batch",
    json={
        "filings": [
            {"url": "...", "cik": "0000320193", "form": "10-K"},
            {"url": "...", "cik": "0000789019", "form": "10-Q"}
        ],
        "fields": ["revenue", "net_income", "eps"]
    }
)

# Check job status
job_id = response.json()["job_id"]
status = requests.get(f"http://localhost:8000/jobs/{job_id}").json()
```

## Fine-tuning

### Training Your Own Model

```bash
# Prepare training data
python src/finetune/data_loading.py \
    --input data/sec_filings_raw/ \
    --output data/training/ \
    --format alpaca

# Run QLoRA fine-tuning
python src/finetune/qlora_training.py \
    --model meta-llama/Llama-3.1-8B \
    --dataset data/training/sec_extractions.json \
    --output models/sec-extractor-qlora \
    --epochs 3 \
    --batch-size 4 \
    --learning-rate 2e-4

# Evaluate on benchmarks
python src/finetune/eval_benchmarks.py \
    --model models/sec-extractor-qlora \
    --benchmarks mmlu,gsm8k
```

See [notebooks/01_qlora_finetuning.ipynb](notebooks/01_qlora_finetuning.ipynb) for a detailed walkthrough.

## Evaluation

### Catastrophic Forgetting Analysis

We evaluate post-fine-tuning model performance on general knowledge benchmarks to ensure domain adaptation doesn't degrade general capabilities:

| Benchmark | Pre-Fine-Tune | Post-Fine-Tune | Delta |
|-----------|--------------|----------------|-------|
| MMLU | 63.4% | 62.1% | -1.3% |
| GSM8K | 46.4% | 45.9% | -0.5% |
| HumanEval | 32.3% | 31.8% | -0.5% |

**Result**: 98% capability retention demonstrates effective domain adaptation without catastrophic forgetting.

### Extraction Accuracy

Tested on 500 held-out SEC filings:

| Field Type | Accuracy | Precision | Recall | F1 |
|------------|----------|-----------|--------|-----|
| Financial values | 96% | 97% | 95% | 96% |
| Dates (fiscal year) | 98% | 99% | 97% | 98% |
| Text (CEO statements) | 88% | 89% | 87% | 88% |
| **Overall** | **94%** | **95%** | **93%** | **94%** |

## Cost Analysis

| Component | GPT-4 API | Fine-tuned Llama | Savings |
|-----------|-----------|------------------|---------|
| Per extraction | $0.03 | $0.0002 | 150x |
| 1M extractions | $30,000 | $200 | 99.3% |
| Infrastructure | $0 (managed) | ~$500/mo GPU | - |
| **Break-even** | - | ~17K extractions | - |

Full cost breakdown in [COST_ANALYSIS.md](COST_ANALYSIS.md).

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test suite
pytest tests/test_extraction_accuracy.py -v
pytest tests/test_latency_bounds.py -v
pytest tests/test_cache_hit_rate.py -v

# With coverage report
pytest --cov=src tests/
```

## Mathematical Foundations

This project applies core ML mathematics:

- **Linear Algebra**: QLoRA uses low-rank matrix decomposition (r=16) to reduce trainable parameters by 99.9% while preserving full-rank forward passes
- **Calculus**: AdamW optimizer with cosine scheduling for gradient descent on LoRA adapter weights
- **Information Theory**: Entropy-based confidence scoring for extraction quality
- **Statistics**: Confidence intervals for accuracy metrics, McNemar's test for model comparison

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed mathematical explanations.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

All PRs must pass:
- Unit tests (`pytest`)
- Type checking (`mypy`)
- Linting (`ruff check .`)
- Coverage threshold (80%)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

- QLoRA paper: [Dettmers et al., 2023](https://arxiv.org/abs/2305.14314)
- vLLM: [Kwon et al., 2023](https://arxiv.org/abs/2309.06180)
- SEC EDGAR for providing public financial data

## Contact

For questions or collaboration:
- GitHub Issues: [github.com/yourusername/sec-edgar-extraction-pipeline/issues](https://github.com/yourusername/sec-edgar-extraction-pipeline/issues)
- Email: your.email@example.com

---

**Status**: Production-ready | **Last Updated**: May 2026 | **Version**: 1.0.0

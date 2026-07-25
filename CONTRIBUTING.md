# Contributing to SEC EDGAR Extraction Pipeline

Thank you for your interest in contributing! This document outlines the development workflow, code standards, and testing requirements.

## Development Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/a-kuo/sec-edgar-extraction-pipeline.git
cd sec-edgar-extraction-pipeline
make install-dev
```

### 2. Set up environment

```bash
# Create .env file
export DATABASE_URL=postgresql://sec_user:sec_pass@localhost/sec_edgar
export REDIS_URL=redis://localhost:6379/0
export SEC_USER_AGENT="SEC-EDGAR-Pipeline your.email@example.com"
export MOCK_EDGAR=true
```

### 3. Start local services

```bash
make docker-up       # Start PostgreSQL + Redis
make migrate         # Run database migrations
```

### 4. Install pre-commit hooks

```bash
make pre-commit
```

Pre-commit hooks will automatically:
- Format code with `ruff format`
- Lint with `ruff check --fix`
- Type-check with `mypy`
- Check for trailing whitespace, large files, merge conflicts

## Development Workflow

### Creating a feature branch

```bash
git checkout -b feature/your-feature-name
# or use: git checkout -b fix/issue-number if fixing a bug
```

### Code standards

All code must pass:
- **Formatting:** `ruff format` (automatic via pre-commit)
- **Linting:** `ruff check` (automatic via pre-commit)
- **Type checking:** `mypy src/ api/ dags/ scripts/` (checked in CI)
- **Tests:** `pytest tests/ -v` (required before PR)

Run locally:
```bash
make quality      # Runs lint + type-check
make test         # Runs tests with coverage
```

### Code style guide

**Python version:** 3.11+

**Imports:** isort-style (stdlib → third-party → local)
```python
import os
from datetime import datetime
from typing import List, Optional

import requests
from pydantic import BaseModel
from sqlalchemy import create_engine

from src.schema import FilingRaw
```

**Type hints:** Required for all function signatures
```python
def parse_html(html: str, accession: str) -> List[FinancialFactRow]:
    """Parse XBRL facts from HTML."""
    pass
```

**Docstrings:** Only when non-obvious
```python
def compute_psi(baseline: List[float], current: List[float]) -> float:
    """Compute Population Stability Index."""
    pass
```

**Line length:** 100 characters (configured in `pyproject.toml`)

**Naming:** snake_case for functions/variables, PascalCase for classes
```python
class FilingRaw(Base):
    pass

def get_company_filings(cik: str) -> Optional[Dict]:
    pass
```

## Testing

### Running tests locally

```bash
make test              # Run all tests with coverage
make test-fast         # Run tests without coverage (faster)
pytest tests/ -v -k parser   # Run specific test module
```

### Writing tests

Place tests in `tests/test_<module>.py` following the pattern:

```python
import pytest
from unittest.mock import Mock, patch

from src.module import function_to_test


class TestFunctionToTest:
    """Test suite for function_to_test."""

    def test_happy_path(self):
        """Test normal operation."""
        result = function_to_test("input")
        assert result == "expected"

    def test_error_case(self):
        """Test error handling."""
        with pytest.raises(ValueError):
            function_to_test("invalid")

    def test_with_mock(self):
        """Test with mocked dependencies."""
        with patch('src.module.external_call') as mock_call:
            mock_call.return_value = "mocked"
            result = function_to_test("input")
            mock_call.assert_called_once()
```

**Guidelines:**
- Aim for ≥80% code coverage
- Test both happy path and error cases
- Use fixtures from `tests/conftest.py` for common setup
- Use `MOCK_EDGAR=true` to avoid live API calls
- Mock external services (Redis, PostgreSQL in tests use SQLite in-memory)

## Commit messages

Follow conventional commits format:

```
type(scope): description

[optional body]

[optional footer]
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

**Examples:**
```
feat(api): add pagination to /filings endpoint
fix(parser): handle missing XBRL namespace
docs(README): update backfill usage example
test(quality): add PSI edge case tests
chore(deps): update sqlalchemy to 2.0.26
```

## Pull Request Process

1. **Create a feature branch** from `main`
2. **Make your changes** with clear commits
3. **Run tests locally** to verify all pass
4. **Push to remote** and open a pull request
5. **Link to any related issues** (e.g., "Fixes #42")
6. **Describe changes** in PR description (what + why)
7. **CI must pass** before review (GitHub Actions)
8. **Address review comments** in follow-up commits (don't amend)
9. **Squash or rebase** only if requested by maintainer

### PR title format

```
[TYPE] Brief description (50 chars max)

# Examples:
[FEAT] Add pagination to filings endpoint
[FIX] Handle missing period_end in XBRL parser
[DOCS] Update README with Kubernetes deployment
[TEST] Expand PSI drift test coverage
```

### PR description template

```markdown
## Description
Brief explanation of what this PR does and why.

## Related Issues
Fixes #123

## Type of Change
- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Added/updated tests
- [ ] All tests pass locally (`make test`)
- [ ] Code quality checks pass (`make quality`)

## Checklist
- [ ] Commits follow conventional format
- [ ] Type hints added to new functions
- [ ] Docstrings for non-obvious logic
- [ ] No unused imports or variables
- [ ] No console.log / print statements (except logging)
```

## Reporting Issues

Use GitHub Issues with:

1. **Clear title** — What is the problem?
2. **Description** — Steps to reproduce, expected vs actual behavior
3. **Environment** — Python version, OS, docker-compose version, etc.
4. **Logs** — Error messages, stack traces

Example:
```
Title: Parser fails on 10-Q filings with missing segment data

Description:
When parsing 10-Q filings with segment disaggregation, the parser throws
an error on missing duration context.

Steps:
1. Run: python scripts/backfill.py --cik 0000320193 --form 10-Q
2. See error: KeyError: 'duration' in xbrl_parser.py line 142

Expected: Parser should skip missing segments or use default context.
Actual: Parser crashes and halts pipeline.

Environment: Python 3.11, Ubuntu 22.04, Docker Desktop
```

## Architecture & Design

Before proposing major changes, please:

1. **Check existing issues** — may already be discussed
2. **Open a discussion issue** — get feedback before coding
3. **Follow existing patterns** — consistency > innovation
4. **Document trade-offs** — why this approach vs alternatives

### Key architectural principles

- **Append-only audit trail** — pipeline_audit table is immutable
- **Cache-first API** — always check Redis before database
- **Fail-safe quality gates** — skip on zero filings, fail on data quality miss
- **Rate-limited API client** — respect SEC EDGAR request limits
- **No LLM/ML** — pure deterministic XBRL parsing

## Code Review

Reviewers will check:

- ✓ Tests cover new functionality
- ✓ Type hints on all new functions
- ✓ No breaking changes to public APIs
- ✓ Documentation updated
- ✓ Code follows project style
- ✓ Performance impact assessed (if relevant)

## Getting Help

- **Questions?** — Open a GitHub Discussion
- **Bug reports?** — File an Issue with reproduction steps
- **Feature requests?** — Open an Issue with use case
- **Email:** aus.kuo03@gmail.com

## License

By contributing, you agree your code will be licensed under the MIT License.

---

Thank you for improving the SEC EDGAR Extraction Pipeline! 🚀

# Contributing to Real-Time Order & Delivery Event Platform

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to this project.

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Standards](#code-standards)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)

---

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code. Please be respectful and constructive in all interactions.

---

## Getting Started

### Prerequisites

- Python 3.10+
- Docker Desktop (Windows/macOS) or Docker Engine (Linux)
- Git

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/real-time-order-delivery-event-platform.git
   cd real-time-order-delivery-event-platform
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/macOS
   .venv\Scripts\activate     # Windows
   ```

3. **Install development dependencies**
   ```bash
   pip install -e ".[dev]"
   ```

4. **Start the infrastructure**
   ```bash
   make up
   ```

5. **Run tests to verify setup**
   ```bash
   make test
   ```

---

## Development Workflow

### Branch Naming

Use descriptive branch names with prefixes:

- `feature/` - New features (e.g., `feature/add-dead-letter-queue`)
- `fix/` - Bug fixes (e.g., `fix/kafka-connection-timeout`)
- `docs/` - Documentation updates (e.g., `docs/update-architecture`)
- `refactor/` - Code refactoring (e.g., `refactor/simplify-flink-jobs`)

### Commit Messages

Follow conventional commit format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting (no code change)
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance tasks

Examples:
```
feat(flink): add dead letter queue for failed events
fix(kafka): handle connection timeout gracefully
docs(readme): update quick start instructions
```

---

## Code Standards

### Python Style

This project uses **ruff** for linting and formatting:

```bash
# Check for issues
make lint

# Auto-fix issues
ruff check . --fix
ruff format .
```

Key standards:
- Line length: 100 characters
- Use double quotes for strings
- Type hints for function signatures
- Docstrings for public functions

### Type Checking

Use **mypy** for static type checking:

```bash
make type-check
```

### File Organization

```
services/          # Python services and utilities
flink/jobs/        # PyFlink streaming jobs
airflow/dags/      # Airflow DAG definitions
tests/             # Test files (mirror source structure)
docs/              # Documentation
```

---

## Testing

### Running Tests

```bash
# All tests
make test

# Specific test file
python -m pytest tests/test_event_generator.py -v

# With coverage
python -m pytest tests/ --cov=services --cov-report=html
```

### Writing Tests

- Place tests in `tests/` directory
- Name test files with `test_` prefix
- Use descriptive test function names
- Include both positive and negative test cases

Example:
```python
def test_generate_events_returns_correct_count():
    """Test that generate_events returns the requested number of events."""
    events = generate_events(count=10)
    assert len(events) == 10

def test_generate_events_with_zero_count_returns_empty():
    """Test that requesting zero events returns empty list."""
    events = generate_events(count=0)
    assert events == []
```

---

## Pull Request Process

### Before Submitting

1. **Ensure all tests pass**
   ```bash
   make ci-all
   ```

2. **Update documentation** if needed

3. **Add tests** for new functionality

4. **Rebase on main** to ensure clean history
   ```bash
   git fetch origin
   git rebase origin/main
   ```

### PR Template

When creating a PR, include:

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
Describe how you tested the changes

## Checklist
- [ ] Code follows project style guidelines
- [ ] Tests pass locally
- [ ] Documentation updated
- [ ] No new warnings
```

### Review Process

1. At least one approval required
2. CI pipeline must pass
3. No unresolved comments
4. Squash and merge preferred

---

## Questions?

Open an issue or start a discussion. We're happy to help!

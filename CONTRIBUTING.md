# Contributing to Real-Time Order & Delivery Event Platform

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Maintain a professional environment

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/real-time-order-delivery-event-platform.git
   cd real-time-order-delivery-event-platform
   ```
3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/Josres/real-time-order-delivery-event-platform.git
   ```

## Development Setup

1. **Prerequisites**:
   - Docker & Docker Compose
   - Java 11+
   - Maven 3.6+
   - Python 3.11+ (for Airflow DAG development)

2. **Start services**:
   ```bash
   make up
   ```

3. **Build Flink jobs**:
   ```bash
   make build-flink
   ```

## Making Changes

### Branch Naming

Use descriptive branch names:
- `feature/add-new-aggregation`
- `fix/kafka-connection-issue`
- `docs/update-deployment-guide`
- `refactor/improve-flink-performance`

### Commit Messages

Follow conventional commit format:
```
type(scope): brief description

Detailed explanation if needed.

Fixes #123
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(flink): add new KPI for delivery time percentiles

Add p50, p95, p99 delivery time metrics to the KPI aggregator job.

Fixes #42
```

### Code Style

#### Java (Flink Jobs)
- Follow Google Java Style Guide
- Use meaningful variable names
- Add JavaDoc for public methods
- Keep methods focused and small

```java
/**
 * Computes hourly order KPIs including count, revenue, and average value.
 *
 * @param orderStream Input stream of order events
 * @return Stream of aggregated KPIs
 */
public static DataStream<OrderKPI> computeHourlyKPIs(DataStream<Order> orderStream) {
    // Implementation
}
```

#### Python (Airflow DAGs)
- Follow PEP 8
- Use type hints where appropriate
- Add docstrings for functions and classes
- Format with `black`

```python
def extract_historical_data(start_date: str, end_date: str) -> List[Dict]:
    """
    Extract historical order data from source systems.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        List of order records as dictionaries
    """
    # Implementation
```

#### YAML/Configuration Files
- Use 2 spaces for indentation
- Add comments for complex configurations
- Keep consistent formatting

### Testing

#### Unit Tests
Add unit tests for new functionality:

```java
// Flink job tests
@Test
public void testKpiAggregation() {
    // Test implementation
}
```

```python
# Airflow DAG tests
def test_backfill_validation():
    # Test implementation
```

#### Integration Tests
Test components together:
1. Start services with `docker-compose up`
2. Submit test jobs
3. Verify data in Iceberg tables
4. Check metrics in Prometheus/Grafana

### Documentation

Update documentation for any changes:
- **README.md**: User-facing features
- **docs/ARCHITECTURE.md**: Architectural changes
- **docs/DEPLOYMENT.md**: Deployment changes
- **docs/TROUBLESHOOTING.md**: New issues and solutions

## Pull Request Process

1. **Update your branch** with latest upstream:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Test your changes**:
   ```bash
   make build-flink
   make up
   make test-kafka
   ```

3. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat(scope): description"
   ```

4. **Push to your fork**:
   ```bash
   git push origin feature/your-feature
   ```

5. **Create Pull Request**:
   - Go to GitHub
   - Click "New Pull Request"
   - Select your branch
   - Fill out the PR template

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Built Flink jobs successfully
- [ ] Started all services without errors
- [ ] Tested functionality manually
- [ ] Added/updated tests

## Checklist
- [ ] Code follows style guidelines
- [ ] Added/updated documentation
- [ ] No new warnings/errors
- [ ] Updated CHANGELOG.md

## Related Issues
Fixes #123
```

## Project Structure

```
flink/                  # Flink streaming jobs
  kpi-aggregator/      # KPI computation job
  complex-event-detector/  # CEP pattern matching job

airflow/               # Airflow orchestration
  dags/               # DAG definitions
  plugins/            # Custom plugins

docker/                # Docker configurations
  configs/            # Service configurations

docs/                  # Documentation
  ARCHITECTURE.md     # System architecture
  DEPLOYMENT.md       # Deployment guide
  TROUBLESHOOTING.md  # Troubleshooting guide

scripts/               # Utility scripts
  setup.sh           # Initial setup script
```

## Areas for Contribution

### High Priority
- [ ] Additional Flink jobs (e.g., fraud detection)
- [ ] More Airflow DAGs (e.g., data quality checks)
- [ ] Grafana dashboards for business metrics
- [ ] Automated testing framework
- [ ] Performance benchmarking

### Documentation
- [ ] Video tutorials
- [ ] More example use cases
- [ ] API documentation
- [ ] Architecture diagrams

### Infrastructure
- [ ] Kubernetes deployment templates
- [ ] Terraform/CloudFormation templates
- [ ] CI/CD improvements
- [ ] Security hardening

### Monitoring
- [ ] Additional Prometheus alerts
- [ ] Custom Grafana dashboards
- [ ] Distributed tracing setup
- [ ] Log aggregation

## Review Process

1. **Automated Checks**: CI pipeline runs tests
2. **Code Review**: Maintainers review code
3. **Feedback**: Address review comments
4. **Approval**: At least one maintainer approval required
5. **Merge**: Squash and merge to main

## Community

- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: General questions and ideas
- **Pull Requests**: Code contributions

## Release Process

1. Version bump in relevant files
2. Update CHANGELOG.md
3. Create GitHub release
4. Build and publish Docker images
5. Announce in discussions

## Questions?

If you have questions:
1. Check existing documentation
2. Search GitHub issues
3. Open a new issue with the "question" label
4. Join GitHub discussions

Thank you for contributing! 🎉

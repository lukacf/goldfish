# Deluxe E2E Tests

Comprehensive end-to-end tests that validate Goldfish with **real GCE execution** and **actual MCP protocol usage**. These tests run in a Docker container and use the MCP client to interact with the Goldfish MCP server, exactly as Claude Code would.

## Architecture

```
Docker Container
├── MCP Client (simulates Claude Code)
├── Goldfish MCP Server (stdio connection)
├── GCP credentials (mounted)
└── Test orchestrator (sends MCP tool calls)

Flow: MCP Client → MCP Protocol → Goldfish Server → GCE
```

This validates the **complete integration stack**, not just internal Python APIs.

## What These Tests Do

The deluxe tests simulate Claude Code using Goldfish for ML research:

1. **Connect to MCP Server** - Establish stdio connection to Goldfish
2. **Initialize Project** - Call `initialize_project` MCP tool
3. **Create Workspace** - Call `create_workspace` and `mount` MCP tools
4. **Run Pipeline** - Call `run_stage` MCP tools for all stages
5. **Monitor Jobs** - Call `job_status` MCP tool to track progress
6. **Verify Results** - Validate complete workflow execution

## ML Pipeline

The test uses a simple classification pipeline:

- **generate_data**: Create 1000 synthetic samples (28x28 features, 10 classes)
- **preprocess**: Normalize and split into train/test (80/20)
- **train**: Train sklearn LogisticRegression classifier
- **evaluate**: Compute test accuracy and confusion matrix

## Requirements

### 1. GCP Setup

You need a GCP project with:
- Compute Engine API enabled
- Cloud Storage API enabled
- A GCS bucket for artifacts
- Service account with permissions:
  - `compute.instances.create`
  - `compute.instances.delete`
  - `compute.disks.create`
  - `storage.objects.create`
  - `storage.objects.delete`

### 2. Authentication

Authenticate with GCP:
```bash
gcloud auth application-default login
```

Or use a service account:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

### 3. Environment Variables

Set required environment variables:
```bash
export GOLDFISH_GCE_PROJECT="your-gcp-project-id"
export GOLDFISH_GCS_BUCKET="gs://your-bucket-name"
export GOLDFISH_DELUXE_TEST_ENABLED="1"
```

Optional variables:
```bash
# Override default zone (default: us-central1-a)
export GOLDFISH_DELUXE_ZONE="us-west1-a"

# Dry-run mode (test setup without launching GCE)
export GOLDFISH_DELUXE_DRY_RUN="1"
```

## Running the Tests

### Full Test Run with Docker

```bash
# Set environment variables
export GOLDFISH_GCE_PROJECT="my-project"
export GOLDFISH_GCS_BUCKET="gs://my-bucket"

# Run in Docker container
cd tests/deluxe
./run_deluxe_tests.sh
```

The script will:
1. Check GCP credentials
2. Build Docker image (if needed)
3. Start container with MCP server
4. Run MCP client orchestrator
5. Execute full workflow
6. Clean up resources

### Dry-Run Mode

Test the setup without actually launching GCE instances:

```bash
./run_deluxe_tests.sh --dry-run
```

### Rebuild Docker Image

Force rebuild of the Docker image:

```bash
./run_deluxe_tests.sh --build
```

### Manual Docker Commands

```bash
# Build image
docker-compose build

# Run test
docker-compose run --rm deluxe-test

# Run with dry-run
GOLDFISH_DELUXE_DRY_RUN=1 docker-compose run --rm deluxe-test
```

## Cost Estimation

The deluxe test uses the following GCE resources:

| Stage | Machine Type | Duration (est) | Cost (est) |
|-------|-------------|----------------|------------|
| generate_data | Local | N/A | $0.00 |
| preprocess | n2-standard-4 | ~2 min | $0.01 |
| train | n2-standard-4 | ~5 min | $0.02 |
| evaluate | n2-standard-4 | ~2 min | $0.01 |

**Total estimated cost per run: ~$0.05 USD**

Note: Costs may vary based on:
- Region/zone selected
- Actual execution time
- GCS storage usage
- Network egress

## Safety Features

1. **Explicit Opt-In**: Test skipped unless `GOLDFISH_DELUXE_TEST_ENABLED=1`
2. **Resource Tagging**: All instances tagged with `goldfish-deluxe-test-{run_id}`
3. **Automatic Cleanup**: Fixture ensures instance deletion even on failure
4. **Timeout**: 30-minute hard limit
5. **No GPU by Default**: Only CPU instances to control costs

## Troubleshooting

### Test Skipped

If you see "Skipped: Deluxe GCE tests not enabled", ensure:
- `GOLDFISH_DELUXE_TEST_ENABLED=1` is set
- `GOLDFISH_GCE_PROJECT` is set
- `GOLDFISH_GCS_BUCKET` is set

### Permission Denied

If you get permission errors:
- Check GCP authentication: `gcloud auth list`
- Verify service account has required permissions
- Check project quotas: `gcloud compute project-info describe`

### Instance Launch Fails

If instances fail to launch:
- Check zone has capacity: Try different zone with `GOLDFISH_DELUXE_ZONE`
- Verify Compute Engine API is enabled
- Check project quotas for n2-standard-4 instances

### Timeout

If test times out:
- Check GCS bucket is accessible
- Verify Docker images can be pulled
- Check network connectivity from GCE instances

### Cleanup Failed

If cleanup fails and instances remain:
- List instances: `gcloud compute instances list --filter="labels.goldfish-test=deluxe"`
- Manual cleanup: `gcloud compute instances delete INSTANCE_NAME --zone=ZONE`

## Development

### Adding New Test Scenarios

To add new test scenarios:

1. Create new test method in `test_gce_e2e_full.py`
2. Use `deluxe_project` fixture for setup
3. Use `gce_cleanup` fixture to ensure cleanup
4. Tag with `@pytest.mark.deluxe_gce`

### Modifying ML Pipeline

The ML pipeline template is in `fixtures/ml_project_template/`:
- Modules: Python scripts in `modules/`
- Configs: YAML files in `configs/`
- Pipeline: `pipeline.yaml`

### Running Without Cleanup (for debugging)

To keep instances running after test failure:

```python
# In conftest.py, comment out cleanup in gce_cleanup fixture
@pytest.fixture
def gce_cleanup():
    cleanup_handlers = []
    yield cleanup_handlers
    # Skip cleanup for debugging
    # for handler in cleanup_handlers:
    #     handler()
```

## Continuous Integration

These tests are **not** run in regular CI due to:
- Cloud resource requirements
- Execution time (~30 minutes)
- Cost considerations

To run in CI, set up:
1. GCP service account with limited permissions
2. Separate GCS bucket for CI
3. Environment variables in CI secrets
4. Manual trigger or scheduled run (not on every commit)

## Related Documentation

- [Goldfish Architecture](../../CLAUDE.md)
- [GCE Configuration](../../docs/gce_config.md)
- [Resource Profiles](../../docs/profiles.md)

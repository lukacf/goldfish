# Deluxe E2E Tests

Comprehensive end-to-end tests that validate Goldfish with **real GCE execution** and **actual MCP protocol usage**. These tests run in a Docker container and use the MCP client to interact with the Goldfish MCP server, exactly as Claude Code would.

## Architecture

```
Docker Container
├── Claude Code CLI (installed)
├── Goldfish MCP Server (configured via mcp.json)
├── GCP credentials (mounted)
└── Test script that:
    1. Configures Claude Code to use Goldfish MCP
    2. Sends prompts to Claude Code
    3. Claude Code uses MCP tools automatically
    4. Verifies results

Flow: Prompt → Claude Code → MCP Protocol → Goldfish → GCE
```

This tests the **actual user experience**, not simulations or internal APIs.

## What These Tests Do

The deluxe test runs actual Claude Code with prompts:

1. **Setup Phase**
   - Configure Claude Code with Goldfish MCP server (`claude mcp add`)
   - Authenticate with GCP

2. **Task Claude** (via `claude -p` prompts)
   - "Initialize a Goldfish project..."
   - "Create a workspace called baseline..."
   - "Create a 4-stage ML pipeline..."
   - "Run the full pipeline..."

3. **Claude Code Actions**
   - Connects to Goldfish MCP server
   - Uses MCP tools: `initialize_project`, `create_workspace`, `run_stage`, etc.
   - Launches real GCE instances
   - Monitors job completion

4. **Verification**
   - Check workspace was created
   - Check pipeline.yaml exists
   - Check jobs completed successfully
   - Verify GCE instances were launched

5. **Cleanup**
   - Delete GCE instances
   - Clean up resources

## ML Pipeline

The test uses a simple classification pipeline:

- **generate_data**: Create 1000 synthetic samples (28x28 features, 10 classes)
- **preprocess**: Normalize and split into train/test (80/20)
- **train**: Train sklearn LogisticRegression classifier
- **evaluate**: Compute test accuracy and confusion matrix

## Requirements

### 1. Anthropic API Key

Get your API key from: https://console.anthropic.com/

### 2. GCP Setup

You need a GCP project with:
- Compute Engine API enabled
- Cloud Storage API enabled
- A GCS bucket for artifacts (must already exist)
- Permissions:
  - `compute.instances.create`
  - `compute.instances.delete`
  - `compute.disks.create`
  - `storage.objects.create`
  - `storage.objects.delete`

### 3. GCP Authentication

Authenticate with GCP:
```bash
gcloud auth application-default login
```

Or use a service account:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

### 4. Docker

Install Docker and docker-compose:
```bash
# macOS
brew install docker docker-compose

# Linux
apt-get install docker.io docker-compose
```

### 5. Configuration File

Create `.env` file with your actual values:

```bash
cd tests/deluxe
cp .env.example .env
# Edit .env with your actual values
```

**Example `.env`:**
```bash
ANTHROPIC_API_KEY=sk-ant-api03-xxxxx
GOLDFISH_GCE_PROJECT=my-gcp-project-123
GOLDFISH_GCS_BUCKET=gs://my-goldfish-bucket
```

**IMPORTANT**: Replace the placeholder values with your actual:
- Anthropic API key
- GCP project ID
- GCS bucket name (must already exist)

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

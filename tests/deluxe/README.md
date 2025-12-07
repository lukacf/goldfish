# Deluxe E2E Tests

Comprehensive end-to-end tests that validate Goldfish with **real GCE execution**. These tests are opt-in and not run during regular CI due to cloud resource requirements.

## What These Tests Do

The deluxe tests simulate a realistic ML research workflow:

1. **Initialize Goldfish Project** - Create a new project with ML pipeline
2. **Run Baseline Pipeline** - Execute all stages (generate_data → preprocess → train → evaluate)
3. **Iterate on Results** - Create new workspace, adjust hyperparameters, re-run
4. **Compare Results** - Use lineage tracking to compare baseline vs improved metrics
5. **Validate Features** - Test profiles, workspaces, checkpoints, signal lineage

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

### Full Test Run

```bash
# Set environment variables
export GOLDFISH_GCE_PROJECT="my-project"
export GOLDFISH_GCS_BUCKET="gs://my-bucket"
export GOLDFISH_DELUXE_TEST_ENABLED="1"

# Run deluxe tests
pytest -m deluxe_gce tests/deluxe/ -v -s
```

### Dry-Run Mode

Test the setup without actually launching GCE instances:

```bash
export GOLDFISH_DELUXE_DRY_RUN="1"
pytest -m deluxe_gce tests/deluxe/ -v -s
```

### Run Specific Test

```bash
pytest -m deluxe_gce tests/deluxe/test_gce_e2e_full.py::TestDeluxeGCEEndToEnd::test_full_ml_workflow -v -s
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

# Public Release Checklist

This document outlines all items required to prepare Goldfish for public release.

## Scope

**V1 Release**: GCP-only. The initial public release targets Google Cloud Platform as the sole cloud provider.

**Future Roadmap**: Multi-cloud abstraction layer to support AWS, Azure, and other providers. **This is explicitly NOT in scope for this branch.**

---

## Critical Blockers

These items MUST be resolved before public release.

### 1. Add Open Source License

**Current State**: No LICENSE file exists in the repository.

**Required Action**:
- Add LICENSE file to repository root
- Choose appropriate license (AGPL-3.0 recommended for copyleft, Apache-2.0 for permissive)
- Add license headers to source files
- Update pyproject.toml with license field
- Add SPDX identifier to all files

**Files to Update**:
- `/LICENSE` (create)
- `/pyproject.toml`
- All source files (license headers)

---

### 2. Remove Hardcoded US Region Default

**Current State**: `docker_builder.py` defaults to US Artifact Registry region.

**Location**: `src/goldfish/infra/docker_builder.py`

**Required Action**:
- Remove hardcoded `us-docker.pkg.dev` default
- Require explicit configuration of registry URL
- Add clear error message when registry not configured
- Document registry configuration in setup guide

---

### 3. Make Compute Profiles Region-Configurable

**Current State**: Built-in profiles have hardcoded `us-central1` zones.

**Location**: `src/goldfish/infra/profiles.py`

**Current Profiles**:
```python
"h100-spot": {
    "zones": ["us-central1-a", "us-central1-c"],  # Hardcoded
    ...
}
```

**Required Action**:
- Add `default_region` to goldfish.yaml configuration
- Make zone selection configurable per-profile
- Document how to override zones for other regions
- Consider auto-discovery of available zones via GCP API

**Files to Update**:
- `src/goldfish/infra/profiles.py`
- `src/goldfish/config.py`
- Configuration documentation

---

### 4. Secure API Key Handling in Pre-Run Review

**Current State**: API keys passed via environment variables to subprocess.

**Location**: `src/goldfish/pre_run_review.py`

**Risk**: Environment variable exposure in process listings.

**Required Action**:
- Review and document secure API key handling patterns
- Consider alternative key injection methods (temp files with restricted permissions)
- Add security documentation for users
- Ensure keys are never logged

---

### 5. Remove Internal Project References

**Current State**: Internal project IDs and URLs may exist in codebase.

**Required Action**:
- Audit all files for internal project references
- Replace with placeholder values or configuration options
- Check for any internal URLs or paths
- Review git history for sensitive commits (consider squashing)

**Files to Audit**:
- All YAML files
- Test fixtures
- Documentation examples
- Configuration defaults

---

## High Priority

These items should be resolved for a quality public release.

### 6. Document GCP-Only Limitation

**Current State**: Backend is GCP-only but not prominently documented.

**Required Action**:
- Add prominent notice in README.md
- Create "Supported Cloud Providers" section in documentation
- Document required GCP services:
  - Google Compute Engine (GCE)
  - Google Cloud Storage (GCS)
  - Artifact Registry
- Add prerequisites section listing GCP requirements

---

### 7. Make Container Resource Limits Configurable

**Current State**: Hardcoded limits in local executor.

**Location**: `src/goldfish/infra/local_executor.py`

**Current Values**:
```python
--memory 4g --cpus 2.0 --pids-limit 100
```

**Required Action**:
- Move limits to configuration
- Add to goldfish.yaml schema
- Provide sensible defaults
- Document resource limit options

---

### 8. Address Proprietary Dependencies

**Current State**: `claude-agent-sdk` is Anthropic proprietary.

**Location**: `pyproject.toml`

**Required Action**:
- Document that SVS AI features require Anthropic API access
- Make claude-agent-sdk an optional dependency
- Provide graceful degradation when not installed
- Document alternative AI providers (if supported)
- Add feature flag for AI-powered reviews

**Files to Update**:
- `pyproject.toml` (optional dependency group)
- `src/goldfish/pre_run_review.py` (graceful handling)
- Documentation

---

### 9. Create User Documentation

**Current State**: CLAUDE.md exists for AI assistants; user docs minimal.

**Required Documentation**:
- [ ] Getting Started Guide
  - GCP setup
  - Installation
  - First experiment
- [ ] Configuration Reference
  - goldfish.yaml schema
  - Environment variables
  - Profile options
- [ ] Pipeline Authoring Guide
  - Stage definitions
  - Signal types
  - Input/output patterns
- [ ] Troubleshooting Guide
  - Common errors
  - Debug procedures
  - FAQ

---

### 10. Create CONTRIBUTING.md

**Required Action**:
- Development setup instructions
- Code style guidelines
- Testing requirements
- PR process
- Issue templates

---

### 11. Review and Update README.md

**Required Sections**:
- [ ] Project description
- [ ] Features overview
- [ ] Quick start
- [ ] Requirements (GCP-only for V1)
- [ ] Installation
- [ ] Basic usage
- [ ] Documentation links
- [ ] Contributing section
- [ ] License section

---

## Medium Priority

These items improve release quality but are not blocking.

### 12. Add Security Policy

**Required Action**:
- Create SECURITY.md
- Define vulnerability reporting process
- Document security considerations
- List security contacts

---

### 13. Create GitHub Templates

**Required Files**:
- `.github/ISSUE_TEMPLATE/bug_report.md`
- `.github/ISSUE_TEMPLATE/feature_request.md`
- `.github/PULL_REQUEST_TEMPLATE.md`

---

### 14. Audit Test Fixtures

**Current State**: Test fixtures may contain internal references.

**Required Action**:
- Review all test data files
- Replace internal project IDs with generic examples
- Ensure no real credentials in fixtures
- Use placeholder domains (example.com)

---

### 15. Add Code of Conduct

**Required Action**:
- Add CODE_OF_CONDUCT.md
- Consider Contributor Covenant or similar

---

### 16. Configure CI for Public Repository

**Required Action**:
- Review GitHub Actions workflows
- Ensure no secrets exposed in logs
- Add branch protection rules documentation
- Set up automated release process

---

## Already Complete

These items are already properly handled.

### Sensitive File Handling
- `.gitignore` properly excludes sensitive files
- Credential patterns excluded (*.json credentials, .env files)
- Build artifacts excluded

### Log Redaction
- API keys redacted in log output
- Credential patterns not logged

### Input Validation
- Path traversal protection implemented
- Input sanitization in place
- Security-focused validation layer

---

## Future Roadmap (Not In Scope)

The following items are planned for future releases but are **explicitly not in scope** for this branch:

### Multi-Cloud Abstraction Layer
- Abstract cloud provider interface
- AWS backend support
- Azure backend support
- Provider-agnostic storage layer
- Pluggable compute backends

### Extended AI Provider Support
- OpenAI Codex integration
- Google Gemini integration
- Provider-agnostic AI review system

---

## Checklist Summary

### Before Public Release (Critical)
- [x] Add LICENSE file
- [x] Remove hardcoded US region
- [x] Make compute profiles region-configurable
- [x] Secure API key handling (documented in SECURITY.md)
- [x] Remove internal project references (audited - none found)

### Before Public Release (High Priority)
- [x] Document GCP-only limitation
- [x] Make container limits configurable
- [ ] Address proprietary dependencies
- [ ] Create user documentation
- [x] Create CONTRIBUTING.md
- [x] Update README.md

### Recommended (Medium Priority)
- [x] Add SECURITY.md
- [x] Create GitHub templates
- [x] Audit test fixtures
- [x] Add CODE_OF_CONDUCT.md
- [ ] Configure CI for public repo

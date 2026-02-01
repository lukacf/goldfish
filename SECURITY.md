# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in Goldfish, please report it responsibly:

1. Open a **private security advisory** at: https://github.com/lukacf/goldfish/security/advisories/new
2. Include as much detail as possible:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

We will acknowledge receipt within 48 hours and provide a detailed response within 7 days.

## Security Considerations

### API Key Handling

Goldfish handles API keys through standard environment variables:

- **ANTHROPIC_API_KEY**: Used by Claude Code CLI for AI reviews
- **WANDB_API_KEY**: Passed to containers for W&B logging
- **GOLDFISH_***: Configuration environment variables

**Best Practices:**
- Never commit API keys to the repository
- Use `.env` files (gitignored) for local development
- Use secret management for production deployments

### Log Redaction

Goldfish automatically redacts sensitive patterns from logs:
- API keys matching `sk-[a-zA-Z0-9]{20,}` are replaced with `[REDACTED_API_KEY]`
- Standard output/error from stages is filtered for known secret patterns

### Container Isolation

Stage execution in Docker containers includes security measures:
- Non-root user execution (`--user 1000:1000`)
- Read-only input mounts (`-v inputs:/mnt/inputs:ro`)
- Resource limits (`--memory`, `--cpus`, `--pids-limit`)
- No network access by default for local execution

### Path Traversal Protection

All file operations validate paths to prevent directory traversal:
- Workspace paths are resolved and validated against the project root
- Symlinks are rejected in sensitive operations
- Stage names and file patterns are sanitized

### Input Validation

User inputs are validated using strict patterns (see `src/goldfish/validation.py`):
- Workspace names: `^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$` (1-64 chars)
- Version identifiers: `^v[0-9]+$` (e.g., v1, v2)
- Snapshot IDs: `^snap-[a-f0-9]{7,8}-\d{8}-\d{6}$`
- Stage run IDs: `^stage-[a-f0-9]+$`
- Stage names: Alphanumeric with underscores

## Known Limitations

### GCP Authentication

Goldfish relies on GCP default credentials for cloud operations:
- Service account keys should be managed securely
- Application Default Credentials are recommended over explicit key files

### AI Review Fail-Open

The AI-powered pre-run review system is designed to fail-open:
- Timeouts result in approval (to avoid blocking development)
- API errors result in approval with warnings
- This is intentional to prioritize developer velocity

## Security Updates

Security patches will be released as:
- Patch versions for non-breaking fixes
- Clear changelog entries describing the vulnerability and fix
- GitHub Security Advisories for significant issues

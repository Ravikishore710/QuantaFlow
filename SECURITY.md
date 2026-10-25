# Security Policy

## Supported Versions

| Version | Supported          |
| :---    | :---               |
| 1.1.x   | :white_check_mark: |
| 1.0.x   | :x:                |

---

## Reporting a Vulnerability

We take the security of QuantaFlow seriously. If you believe you have found a security vulnerability, please do NOT open a public GitHub issue.

Instead, please report the vulnerability privately:
1. Email: `security@quantaflow.dev` (or open a Private Vulnerability Advisory on GitHub under Security > Advisories).
2. Include detailed reproduction steps, impact assessment, and sample payloads if applicable.
3. You will receive an initial response within 48 hours acknowledging receipt and next steps.

---

## Security Architecture & Best Practices

- **Non-Root Container Execution:** Production container images run under an unprivileged user (`quantaflow`, UID `10001`).
- **Dependency Auditing:** Automated `pip-audit` runs on every pull request and push to `main` via GitHub Actions CI to identify known CVEs.
- **Fail-Closed Data Validation:** Untrusted input records are checked against strict schemas and quarantined in dead-letter queues rather than executed or injected directly into storage.
- **Secrets Management:** Credentials (`POSTGRES_PASSWORD`, `MINIO_SECRET_KEY`) must never be hardcoded and are loaded strictly from environment variables via Pydantic settings.

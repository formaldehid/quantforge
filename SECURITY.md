# Security Policy

## Supported Versions

The latest published `0.x` release receives security fixes.

## Reporting a Vulnerability

Please report vulnerabilities privately through GitHub Security Advisories or by contacting the maintainer directly.

Do not open a public issue for:

- credential leaks
- secret management flaws
- supply-chain security concerns that are not yet patched
- vulnerabilities that could put exchange credentials or user data at risk

## Automated Security Checks

Three layers of automated scanning run on this repository:

- **CodeQL** static analysis runs on pull requests targeting `main`, on pushes to `main`, and weekly; alerts appear in the repository Security tab under Code scanning
- **gitleaks** scans the full git history for committed secrets on pull requests targeting `main`, on pushes to `main`, and weekly
- **GitHub secret scanning with push protection** runs server-side and blocks pushes containing known credential patterns

If a credential does leak, rotate it first — rewriting git history does not revoke a secret. Then report the incident privately as described above.

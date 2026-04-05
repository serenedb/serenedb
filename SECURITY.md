# Security Policy

## Project Status

SereneDB is an early-stage project moving fast. We'd love your feedback, but be aware that storage format, wire protocol, and APIs may change between releases. We're approaching production readiness -- stay tuned.

This means security issues may be addressed by changing APIs or formats in ways that aren't backwards compatible.

## Supported Versions

Only the latest release receives security fixes. We don't backport to older versions at this stage.

## Reporting a Vulnerability

Please report security vulnerabilities by emailing **security@serenedb.com**.

- You'll receive an acknowledgment within 5 business days.
- We'll work with you to understand the issue and develop a fix.
- We ask that you give us reasonable time to address the issue before public disclosure.

Please do **not** open public GitHub issues for security vulnerabilities.

## SereneUI (JavaScript)

The `serene-ui/` directory contains a local development UI tool built with Node.js. It is not part of the SereneDB server and is not deployed to production. Dependabot alerts for JS dependencies in this directory are suppressed -- these don't affect the database itself.

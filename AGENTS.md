# Atlas AI Assistant Instructions

These instructions apply to every AI assistant working in this repository.

## Start-of-work protocol

Before proposing or changing anything:

1. Read `PROJECT_STATE.md` completely.
2. Read the current milestone in `docs/LOCAL_BUILD_PLAN.md`.
3. Read `docs/PRD.md` and the documents relevant to the task.
4. Inspect the repository instead of assuming documented plans are implemented.
5. Keep the requested work aligned with the current milestone.

## Governing architecture rule: local-first, cloud-portable

Every cloud capability must first have a working local equivalent whenever technically practical. AWS hosts and operationalizes an already-validated system; AWS must not become the first environment where Atlas components are integrated.

- Application/domain behavior must not directly depend on AWS-specific APIs.
- The same application contracts and Docker images flow through local processes, Docker Compose, local Kubernetes (`kind`), and AWS EKS.
- Helm charts must be validated on `kind` before EKS.
- PostgreSQL, Redis, Kafka, storage, ingress, secrets, telemetry, and workload boundaries each need an explicit local-to-AWS mapping.
- AWS-only capabilities that cannot be reproduced faithfully (IAM, WAF, Route 53, managed-service failover, AWS networking) still need a local contract/configuration boundary where practical, automated configuration/contract tests, an explicit local-to-AWS mapping with trade-offs, and final validation in AWS.
- Never claim full behavioral equivalence where local emulation is incomplete.
- This rule does not authorize working ahead of the milestone marked **Current**.

## Working rules

- Build the smallest useful, testable vertical slice.
- Do not create a folder, file, service, or abstraction without explaining its requirement, responsibility, consumer, dependencies, and verification.
- Add structure only when current implementation work requires it.
- Build and validate Atlas locally through small, tested vertical slices.
- Keep `docs/TECHNICAL_DESIGN.md` current with architectural decisions that are validated during local implementation.
- Defer the comprehensive Visio/AWS deployment design until the local architecture has working components that can be mapped credibly to the cloud.
- Use technologies because they have a defined responsibility, not merely because they appear on the technology roadmap.
- Keep typed contracts at system boundaries and add tests with behavior.
- Never commit secrets or expose them in logs, examples, fixtures, or documentation.
- Preserve unrelated user changes.

## Milestone completion protocol

- Do not work ahead of the milestone marked **Current** in `docs/LOCAL_BUILD_PLAN.md`.
- A milestone is complete only after its completion gate passes and the diff is reviewed.
- After completion, mark the finished milestone **Complete**, mark only the next approved milestone **Current**, and update `PROJECT_STATE.md` with factual verification evidence.
- Do not mark generated but unverified work complete.
- Stop for review before beginning the next milestone.

## Project-state protocol

Update `PROJECT_STATE.md` after a material change to the current phase, completed work, decisions, blockers, or verification. Keep it factual and concise; it is a handoff file, not a diary.

## Definition of done

A change is done when its intended behavior or documentation is coherent, relevant checks pass, and `PROJECT_STATE.md` reflects any material state change.

# Atlas AI Assistant Instructions

These instructions apply to every AI assistant working in this repository.

## Start-of-work protocol

Before proposing or changing anything:

1. Read `PROJECT_STATE.md` completely.
2. Read `docs/PRD.md` and the documents relevant to the task.
3. Inspect the repository instead of assuming documented plans are implemented.
4. Keep the requested work aligned with the current milestone.

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

## Project-state protocol

Update `PROJECT_STATE.md` after a material change to the current phase, completed work, decisions, blockers, or verification. Keep it factual and concise; it is a handoff file, not a diary.

## Definition of done

A change is done when its intended behavior or documentation is coherent, relevant checks pass, and `PROJECT_STATE.md` reflects any material state change.

## Contributing to StreamForge AI

Thank you for your interest in contributing to StreamForge AI. This document describes how we work so that changes stay consistent and reviewable.

---

## Branch naming

Create a branch for every change (no direct commits to `main`).

- **Features**: `feat/<short-topic>`  
  - Example: `feat/prefetch-cache-api`
- **Fixes**: `fix/<short-topic>`  
  - Example: `fix/flink-checkpointing-bug`
- **Chores / infra / docs**: `chore/<short-topic>`  
  - Example: `chore/helm-chart-updates`
- **Experiments / spikes**: `exp/<short-topic>`  
  - Example: `exp/alternative-storage-sink`

Guidelines:
- **Use-kebab-case**: lower case, words separated by `-`.
- **Keep it short and descriptive**.
- **One logical topic per branch** (avoid mixing refactors, features, and fixes).

---

## Commit message style

We follow a lightweight Conventional Commits style:

**Format**:

```text
<type>(optional-scope): <short imperative summary>

<optional body>
<optional footer>
```

**Types**:
- `feat`: new feature
- `fix`: bug fix
- `docs`: documentation only
- `chore`: tooling, CI, infra, or housekeeping
- `refactor`: code change that does not change behavior
- `test`: add or improve tests
- `perf`: performance improvement

**Examples**:
- `feat(prefetch): add hot-cache warmup job`
- `fix(ingestion): handle null primary keys from Debezium`
- `docs: clarify local Kafka setup`

Guidelines:
- **Use imperative mood** ("add", "fix", "update", not "added", "fixed").
- **Keep the subject ≤ 72 characters** when possible.
- Use the body to explain **why** the change is needed, not just what changed.

---

## Pull request workflow

1. **Sync and branch**
   - Update `main`: `git checkout main && git pull`.
   - Create your branch following the naming rules above.

2. **Make focused changes**
   - Keep PRs **small and scoped**; large, mixed changes are harder to review.
   - Include or update **tests** when adding behavior or fixing bugs.
   - Update **docs** (`README.md`, `CONTRIBUTING.md`, `docs/`, etc.) when behavior or interfaces change.

3. **Run checks locally**
   - Run the relevant **linters**, **formatters**, and **tests** described in the README or project-specific docs.
   - Ensure there are **no new warnings or failing tests** before opening a PR.

4. **Open the PR**
   - Target branch: normally `main` (or the module-specific integration branch if documented).
   - PR title: mirror your main commit, e.g. `feat: add prefetch warmup job`.
   - PR description should include:
     - **Summary** of the change.
     - **Motivation / context** (what problem this solves).
     - **Implementation notes** (any design decisions or trade-offs).
     - **Testing**: how you tested (commands, environments).
     - **Breaking changes**: call out anything that changes public behavior.

5. **Iterate**
   - Respond to review comments promptly.
   - Use additional commits for review changes (squashing may happen on merge if configured).

---

## Review expectations

For **authors**:
- **Right-size PRs**: aim for a reviewable size; split long refactors into smaller steps.
- **Make it easy to review**:
  - Clear title and description.
  - Screenshots or logs where relevant.
  - Point reviewers at **tricky or non-obvious** parts of the change.
- **Be open to feedback** and willing to adjust the approach when reviewers raise concerns.

For **reviewers**:
- **Review intent first**: read the description before the diff.
- Focus on:
  - **Correctness and safety** (data integrity, failure modes, concurrency).
  - **Clarity and maintainability** (readability, naming, structure).
  - **Architecture alignment** (fits StreamForge AI goals and boundaries).
  - **Tests and observability** (coverage, logging, metrics where appropriate).
- Leave comments that are:
  - **Actionable** (what to change or consider).
  - **Prioritized** (distinguish blocking issues from suggestions).
- Aim for prompt reviews so contributors are not blocked.

Approval:
- At least **one maintainer approval** is required before merging.
- The PR must be **green in CI** (tests, linters) unless maintainers explicitly override for good reason.

---

## Issue handling

We use issues to track bugs, enhancements, and questions.

### Creating issues

When opening an issue, please include:
- **Type**: bug, feature request, enhancement, docs, or question.
- **Summary**: concise, descriptive title.
- **Details**:
  - **For bugs**:
    - Steps to reproduce.
    - Expected vs. actual behavior.
    - Environment (OS, versions, configs, relevant logs).
  - **For features/enhancements**:
    - Problem statement (what you are trying to achieve).
    - Proposed solution or alternatives if you have them.
- **Impact**: how severe or frequent the problem is.

### Triaging and labels

Maintainers will:
- Apply labels such as `bug`, `enhancement`, `docs`, `question`, `good first issue`, `help wanted`, and priority labels.
- Clarify requirements and acceptance criteria where needed.
- Prioritize issues based on impact, risk, and alignment with the project roadmap.

### Working on issues

- Comment on an issue to **self-assign** it or ask to be assigned.
- For non-trivial changes, consider a brief **proposal in the issue** before implementing.
- Reference the issue in your PR description and commits, e.g. `Fixes #42`.

---

## Code of conduct

We expect all contributors and reviewers to be respectful and collaborative:
- Assume good intent; ask clarifying questions before jumping to conclusions.
- Be constructive in feedback; target the code or idea, not the person.
- Help newcomers by pointing them to docs and examples.

If you see behavior that conflicts with these expectations, please contact a maintainer.


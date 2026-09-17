# Copilot Skill: gitguro

## Purpose
Create clean, focused local commits for completed work.

## Trigger
Use this skill when the user asks to commit, finalize, or prepare local changes.

## Local Commit Workflow
1. Confirm the current directory is a Git repository with `git rev-parse --is-inside-work-tree`.
2. Inspect the working tree with `git status --short` and `git diff --name-only`.
3. Leave unrelated user changes unstaged and unmodified.
4. Run the relevant validation. Pre-commit hooks enforce Ruff formatting and linting when installed; run `uvx ruff check --fix && uvx ruff format` before committing Python changes when practical.
5. Group related files into one logical concern and stage only those paths.
6. Commit with a concise imperative subject. Split unrelated work into separate commits.
7. Confirm the remaining working tree state and report the commit hash and any intentionally uncommitted files.

## Push Policy

**Never push commits, create or update a pull request, fetch, pull, rebase, or
change branches unless the user explicitly requests that exact remote action.**

Do not infer consent to publish from a request to commit or finalize work.
Report that commits remain local unless the user has specifically asked to push.

## Good Commit Message Examples
- feat: add HTTP backend with async streaming
- fix: handle trailing slash in S3 glob patterns
- test: add integration tests for GCS path operations
- docs: document protocol registration API

## Guardrails
- Never use destructive git commands unless explicitly requested.
- If there are unrelated workspace changes, avoid committing them.
- Prefer non-interactive git commands.
- Never amend, rebase, or rewrite commits unless explicitly requested.
- Never commit credentials, generated noise, or unreviewed changes.

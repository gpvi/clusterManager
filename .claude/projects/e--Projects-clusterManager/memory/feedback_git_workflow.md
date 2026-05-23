---
name: git-workflow-before-branch
description: Always branch from origin/main, never from stale local main
metadata:
  type: feedback
---

Create new branches directly from remote main — no need to switch:
```bash
git fetch origin
git checkout -b <new-branch> origin/main
```

**Why:** `git checkout main` is an unnecessary step that risks creating from stale local main. Branching directly from `origin/main` is always up-to-date and avoids dirty-working-tree issues.

**How to apply:** Before ANY `git checkout -b`, run `git fetch origin` and branch from `origin/main`. Never use local `main` as the base.

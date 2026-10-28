# CI/CD: from a pull request to production

## The flow
```
laptop:  edit -> pytest -> bundle deploy -t dev -> check -> git commit (pre-commit: lint + secret scan)
GitHub:  pull request -> CI: lint | tests | secret scan | bundle validate (dev, test, prod)
         merge to main -> CD: deploy TEST -> setup -> seed data -> run everything + all audits
                               -> (you approve) -> deploy PROD (same commit)
```
Principles: build once and promote the same commit; nothing reaches prod without green tests and a human approval;
no credentials in the repo; every third-party action pinned to an exact commit.

## One-time setup (about 45 minutes)

### 1. Create the test and prod catalogs
Open `scripts/bootstrap_environment.sql` in the Databricks SQL editor, uncomment the **test** and **prod** blocks, run them.

### 2. Check the targets locally first
```
databricks bundle validate -t test
databricks bundle validate -t prod
databricks bundle deploy   -t test
databricks bundle run setup_environment -t test
```
`mode: production` is stricter than dev. If validate asks for permissions or run_as, send me the message.

### 3. Create the GitHub repository (private) and push
```
git init
git add .
git commit -m "Steps 1-8: freight lakehouse"
git branch -M main
git remote add origin https://github.com/<you>/freight-lakehouse.git
git push -u origin main
```
Install the local guard: `pip install pre-commit` then `pre-commit install`.

### 4. GitHub environments and secrets (Settings > Environments)
| Environment | Protection | Secrets |
|---|---|---|
| `test` | none | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| `production` | **Required reviewers: you** | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |

`DATABRICKS_HOST` is your workspace URL. `DATABRICKS_TOKEN` is a personal access token (Free Edition has no service
principals). Make it short-lived (for example 90 days) and revoke it if it ever leaks.
**On Azure this becomes a service principal with OAuth, and the token disappears.**

### 5. Protect `main` (Settings > Branches)
Require a pull request, require the `lint-and-test`, `secret-scan` and `validate-bundle` checks to pass, block force pushes.
Settings > Code security: turn on secret scanning and push protection, and Dependabot alerts.

## Daily workflow
```
git switch -c feature/<what-you-change>
...edit, pytest, bundle deploy -t dev, check...
git commit -m "what and why" ; git push -u origin feature/<...>
open a pull request -> wait for green checks -> merge
Actions tab: test runs by itself, then waits for your approval -> prod
```

## Tags and rollback
After a good production deploy: `git tag v0.1.0 && git push --tags`.
**Rollback = redeploy the previous tag**: check it out and run `databricks bundle deploy -t prod`
(or revert the commit and let the pipeline run). Data tables are not rolled back by a deploy, only code and job definitions.

## What the test environment proves, and what it does not
It is a long-lived integration environment. The first run loads all the sample data and checks every audit.
Later runs prove the deployment and the idempotency (re-running changes nothing), because the seed script skips files that are already there.
To exercise ingestion from scratch again, delete the test catalog and run the one-time bootstrap again.

## Known limits (honest list)
* Free Edition has one workspace, so test and prod share it, separated by catalog and resource names. On Azure they get separate workspaces.
* One shared token. On Azure: service principals, least-privilege grants per catalog, Key Vault.
* Workflows pass `actionlint` and the tests pass locally, but the workflows have not run on GitHub yet: expect to fix small things on first run.

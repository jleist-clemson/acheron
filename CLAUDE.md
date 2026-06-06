@AGENTS.md

@.claude/rules/python-standards.md
@.claude/rules/api-routes.md
@.claude/rules/testing.md
@.claude/rules/elasticsearch-mapping.md
@.claude/rules/background-tasks.md

## Claude Code

Shared, vendor-neutral project context lives in `AGENTS.md` (imported above).
Project conventions live in `.claude/rules/` and are **imported above** so they
load every session — Claude Code has no glob-scoped rules directory, so each
file simply names the paths it applies to in an "Applies to" line. They mirror
the Cursor rules in `.cursor/rules/` (which *are* glob-scoped via `globs:`); if
you change a convention, update **both** mirrors and the relevant section of
`ARCHITECTURE.md` so the tools and docs don't drift.

@AGENTS.md

## Claude Code

Shared, vendor-neutral project context lives in `AGENTS.md` (imported above).
Path-scoped conventions live in `.claude/rules/` and load only when you work
with matching files. These mirror the Cursor rules in `.cursor/rules/`; if you
change a convention, update **both** mirrors and the relevant section of
`ARCHITECTURE.md` so the tools and docs don't drift.

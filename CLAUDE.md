# WAR Lab — Claude Project Instructions

## Session Startup

At the start of every new task or session, ask the user:

> "Are we working on the **hr-datamart** project today, or something else?"

Based on the answer:

- **hr-datamart** → Read `hr-datamart/.claude/instructions.md` and follow all rules there (especially task timing and logging)
- **Other project** → Check if that project has a `.claude/instructions.md` and read it if so

## General Rules

- All substantial work should be committed to git with clear commit messages
- Use `Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>` on all commits
- Git remote: https://github.com/WARLAB01/warlab-projects.git
- AWS region: us-east-1
- Credentials: stored in user's `.credentials/service_keys.env`

## Project Index

| Project | Path | Instructions |
|---------|------|-------------|
| HR Datamart | `hr-datamart/` | `hr-datamart/.claude/instructions.md` |

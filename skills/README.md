# Skills

This directory stores reusable **agent skills** — domain-specific guidance and scripts that
Genie Code (the Databricks assistant) can load at runtime to augment its knowledge for
tasks specific to this project.

## Folder structure

```
skills/
  README.md                     # This file
  <skill-name>/
    SKILL.md                    # Required — natural-language guidance the agent reads
    scripts/                    # Optional — pre-tested Python/SQL/Shell scripts
      <script>.py
```

## Conventions

| Item | Convention |
| --- | --- |
| Skill folder name | `kebab-case`, short, descriptive (e.g. `youtube-api`, `gdelt-ingestion`) |
| `SKILL.md` | Plain Markdown; starts with a one-sentence summary of when to load the skill |
| `scripts/` | One script per logical task; include a docstring at the top |
| Skill path reference | `skills/<skill-name>/SKILL.md` |

## How Genie Code uses skills

1. Before calling a tool, the agent checks whether the user's request matches a known skill.
2. If a match is found, it calls `readSkillFile("skills/<skill-name>/SKILL.md")` to load the guidance.
3. Scripts inside `scripts/` are executed via `executeCode` with `language: 'sh'`.

## Adding a new skill

1. Copy the `template/` folder: `cp -r skills/template skills/<your-skill-name>`
2. Edit `SKILL.md` — replace the placeholder text with real guidance.
3. Add any helper scripts to `scripts/`.
4. Commit and push so the skill is available to all collaborators.

## Existing skills

| Skill | Description |
| --- | --- |
| `template` | Starter template — copy and rename to create a real skill |

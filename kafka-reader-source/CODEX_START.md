# Open this project in Codex

This source distribution contains Kafka Reader **0.3.0**, its build and test
tools, and durable project guidance. The documentation handoff was prepared on
**22 November 2026**. Application source and runtime behavior are unchanged by
this handoff.

## Import the complete source

1. Extract `kafka-reader-source.zip` into a normal development directory.
2. Open the extracted **kafka-reader-source** folder as the local project in
   Codex, or as the folder/workspace in your IDE when using the Codex extension.
3. Choose the directory containing **both `Cargo.toml` and `AGENTS.md`** as the
   project root. The EXE and an unopened ZIP do not provide an editable checkout.
4. Start a new Codex conversation in that project and use the prompt below.

For Codex CLI, the equivalent Windows example is:

```powershell
codex -C "C:\Projects\kafka-reader-source"
```

Replace that example with the directory you actually extracted. You can also
start `codex` while already in that directory.

If you use a repository-backed remote environment, put the extracted source
and these Markdown files in your repository and select that checkout. The
project instructions belong with the source; a transcript of the earlier chat
is not required. Keep real connection profiles, exports, and personal workspace
data outside commits.

If you already have a customized checkout, the separate
`kafka-reader-codex-docs.zip` contains only the five new Markdown files, with
paths relative to your project root. Review and merge them into that checkout.
The full source ZIP also updates the README index and source-packaging allowlist.

## First prompt

```text
Read AGENTS.md and docs/PROJECT_CONTEXT.md, then review the relevant parts of
docs/ARCHITECTURE.md and docs/DEVELOPMENT.md. Inspect the source to confirm your
understanding. Summarize what Kafka Reader does, the important implementation
constraints, and how to build and verify changes in this environment.
Identify missing tools without changing application code yet. Treat the
recorded test results as historical evidence, not checks you have just run.
```

After that orientation, give Codex a concrete change, for example:

```text
Implement this change: [describe the behavior I want]. Follow AGENTS.md,
preserve the existing Kafka read-only behavior and accepted UI decisions,
perform the relevant checks, and update project documentation where needed.
```

## What each file is for

| File | Role |
|---|---|
| [AGENTS.md](AGENTS.md) | Concise repository instructions for Codex |
| [docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md) | Product intent, accepted decisions, current state, and known limits |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Code map, data flow, and reasons behind sensitive implementation choices |
| [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | Build, run, test, and package workflows |
| [README.md](README.md) | Detailed application behavior and configuration |
| [TEST-REPORT.md](TEST-REPORT.md) | Checks actually executed for the delivered 0.3.0 release |

Codex discovers repository `AGENTS.md` instructions from the project/current
directory hierarchy when a session starts. The other documents are referenced
reading, not automatic replacements for that entry point. Start a new session
after changing project guidance, and ask Codex to identify the instructions it
loaded if you want to confirm discovery. See OpenAI's
[AGENTS.md guide](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
and [project-directory guidance](https://learn.chatgpt.com/docs/projects).

No model, approval, sandbox, account, or global Codex configuration is changed
by these files. Use the settings available in your Codex installation. Native
build prerequisites are listed in [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

# Genie Code Log

Each step: **ask for a plan → review → approve with changes → Genie builds, runs and verifies.**

| # | Step | My changes to the plan | What happened |
|---|---|---|---|
| 1 | Setup | none | Sandbox couldn't read the macOS Keychain → signed in with plaintext token storage |
| 2 | Explore data | none | `genie ask` returned nothing → Genie switched to SQL and re-checked counts |
| 3 | Land files | 2: my user folder, one file per folder | 3,333 / 300 / 48 rows in a Volume |
| 4 | Bronze | 1: return counts via `notebook.exit` | 3,333 / 300 / 48 |
| 5 | Silver | 2: null-date check, drop PII columns | First run failed (Spark Connect) → Genie fixed and reran |
| 6 | Gold | 0 | 48 / 102 / 10; revenue 66,471.00 |
| 7 | Job | 1: one-hour timeout | 4 tasks, all succeeded; real values kept out of the repo |

## Takeaways

- **Plan first.** Reviewing code and commands before running caught most issues.
- **Check defaults.** Paths and outputs were sensible, but not always right for my setup.
- **It debugs itself** and **verifies its own results.**
- **Approvals take time**, but every Databricks change stayed visible.
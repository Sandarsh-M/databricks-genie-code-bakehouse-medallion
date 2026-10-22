## Step 7 – Explore samples.bakehouse
- Prompt: list tables in samples.bakehouse with row counts (read-only)
- Genie: tried `databricks genie ask` → empty → fell back to SQL via CLI → verified with COUNT(*)
- Result: 6 tables; picked sales_transactions (3,333), sales_customers (300), sales_franchises (48)
- Lesson: Genie verifies instead of trusting table stats


## Step 8 – Land raw files in a UC Volume
- Prompt: plan first (no execution), then approved with 2 changes (user folder instead of Shared, coalesce(1))
- Genie: wrote src/00_land_raw_files.py → imported to my workspace user folder → serverless one-time job → verified files
- Result: transactions JSON (3,333), customers CSV (300), franchises CSV (48), one data file each
- Lessons: "plan first, then approve" works well; Genie defaulted to /Workspace/Shared, so review paths;
  ~11 min end to end, mostly serverless start-up + approvals
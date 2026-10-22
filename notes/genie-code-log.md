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


  ## Step 10 – Bronze layer
- Prompt: plan first; approved with 1 change (return counts via dbutils.notebook.exit, since prints aren't in run output)
- Genie: wrote src/01_bronze.py → imported to user folder → serverless run → fetched counts from run output
- Result: bronze_transactions 3,333 · bronze_customers 300 · bronze_franchises 48
- Design: file globs skip Spark marker files; _source_file + _ingested_at metadata; CSV columns kept as strings (cast in silver)
- Lesson: Genie remembered earlier corrections (user folder, params only in job) without being told again

## Step 12 – Silver layer
- Prompt: inspect bronze columns first, then plan; approved with 2 changes (null_dateTime check, drop email/phone/address)
- Genie: wrote src/02_silver.py → first run failed (Spark Connect incompatibility in card masking) → fixed the code itself → reran
- Result: silver_sales_enriched 3,333 · silver_sales_rejected 0
- Design: typed columns (timestamp, decimal), masked card (****1234), prefixed customer_/franchise_ columns, reject_reason column
- Lesson: serverless = Spark Connect; not every PySpark function behaves the same. Genie diagnosed and fixed it without help
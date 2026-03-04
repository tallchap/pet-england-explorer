# Email Batch Run (500 rows)

This repo now includes a Playwright-based batch tool to process locations alphabetically and generate email-enrichment outputs.

## Script

`scripts/run_email_batch.js`

## What it does

- Sorts `data/pet_supplies_england.json` by business name (A→Z)
- Takes a slice (`start`, `size`)
- Crawls each website with Playwright (same browser engine for all sites)
- Follows internal links up to depth 2 (configurable)
- Extracts email candidates
- Classifies candidate type (`owner`, `manager`, `sales`, `info`, etc.)
- Optionally validates with ZeroBounce (if `ZEROBOUNCE_API_KEY` is set)
- Chooses the best candidate and scores confidence
- Writes JSON + CSV + summary

## Run 500 (first batch)

```bash
node pet-england-explorer/scripts/run_email_batch.js --start=0 --size=500 --concurrency=8 --maxDepth=2 --maxPagesPerDomain=100 --batch=batch_0001_0500_playwright
```

Optional dry-run (no ZeroBounce API calls):

```bash
node pet-england-explorer/scripts/run_email_batch.js --start=0 --size=500 --dry-run=true
```

## ZeroBounce setup

```bash
export ZEROBOUNCE_API_KEY="your_key_here"
```

## Outputs

In `data/email_batches/`:

- `batch_0001_0500_input.json`
- `batch_0001_0500_results.json`
- `batch_0001_0500_results.csv`
- `batch_0001_0500_summary.json`

## Results page

Open:

- `site/results.html`

Default loads batch 1 (`batch_0001_0500`).

To view another batch:

- `site/results.html?batch=batch_0501_1000`

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const DATA_PATH = path.join(ROOT, 'data', 'pet_supplies_england.json');
const OUT_DIR = path.join(ROOT, 'data', 'email_batches');

const args = Object.fromEntries(
  process.argv.slice(2).map(a => {
    const [k, ...rest] = a.replace(/^--/, '').split('=');
    return [k, rest.join('=') || 'true'];
  })
);

const batchSize = Number(args.size || 500);
const start = Number(args.start || 0);
const dryRun = args['dry-run'] === 'true';
const maxPages = Number(args.maxPages || 3);
const concurrency = Number(args.concurrency || 8);
const timeoutMs = Number(args.timeoutMs || 12000);
const batchName = `batch_${String(start + 1).padStart(4, '0')}_${String(start + batchSize).padStart(4, '0')}`;

const ZEROBOUNCE_API_KEY = process.env.ZEROBOUNCE_API_KEY || '';

function canonicalDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '').toLowerCase();
  } catch {
    return '';
  }
}

function classifyEmail(email) {
  const local = String(email).split('@')[0].toLowerCase();
  if (/owner|founder|director/.test(local)) return 'owner';
  if (/manager|gm|admin/.test(local)) return 'manager';
  if (/sales|partnerships|trade|wholesale/.test(local)) return 'sales';
  if (/support|help/.test(local)) return 'support';
  if (/info|hello|contact/.test(local)) return 'info';
  if (/^[a-z]+\.[a-z]+$/.test(local)) return 'named';
  return 'unknown';
}

function sourceWeight(source) {
  if (source.includes('/contact')) return 30;
  if (source.includes('/about')) return 22;
  if (source.includes('/')) return 16;
  return 10;
}

function typeWeight(type) {
  if (type === 'owner') return 30;
  if (type === 'manager') return 24;
  if (type === 'sales') return 20;
  if (type === 'named') return 18;
  if (type === 'info') return 14;
  if (type === 'support') return 12;
  return 8;
}

function verificationWeight(status) {
  if (status === 'valid') return 35;
  if (status === 'catch-all') return 15;
  if (status === 'unknown') return 10;
  if (status === 'do_not_mail' || status === 'spamtrap' || status === 'abuse') return -50;
  if (status === 'invalid') return -100;
  return 0;
}

function confidence(candidate) {
  const base = 20;
  let c = base + sourceWeight(candidate.source_url || '') + typeWeight(candidate.email_type || 'unknown');
  c += verificationWeight(candidate.verification_status || 'unknown');
  return Math.max(0, Math.min(100, c));
}

function chooseBest(candidates) {
  return [...candidates]
    .map(c => ({ ...c, confidence_score: confidence(c) }))
    .sort((a, b) => b.confidence_score - a.confidence_score)[0] || null;
}

function findEmails(text) {
  const re = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const found = text.match(re) || [];
  const cleaned = found
    .map(x => x.toLowerCase().replace(/[),.;:!?]+$/, ''))
    .filter(x => !x.endsWith('.png') && !x.endsWith('.jpg') && !x.includes('example.com'));
  return [...new Set(cleaned)];
}

async function fetchWithTimeout(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ctrl.signal, redirect: 'follow' });
    if (!res.ok) return '';
    return await res.text();
  } catch {
    return '';
  } finally {
    clearTimeout(t);
  }
}

function pickPages(website) {
  if (!website) return [];
  let base;
  try { base = new URL(website); } catch { return []; }
  const pages = ['/', '/contact', '/contact-us', '/about', '/about-us', '/get-in-touch'];
  return pages.slice(0, maxPages + 1).map(p => new URL(p, base).toString());
}

async function validateWithZeroBounce(email) {
  if (!ZEROBOUNCE_API_KEY || dryRun) return { status: 'unknown', sub_status: '' };
  const u = new URL('https://api.zerobounce.net/v2/validate');
  u.searchParams.set('api_key', ZEROBOUNCE_API_KEY);
  u.searchParams.set('email', email);
  u.searchParams.set('ip_address', '');

  try {
    const res = await fetch(u, { method: 'GET' });
    if (!res.ok) return { status: 'unknown', sub_status: '' };
    const j = await res.json();
    return { status: j.status || 'unknown', sub_status: j.sub_status || '' };
  } catch {
    return { status: 'unknown', sub_status: '' };
  }
}

async function processRow(row) {
  const website = row.website_url || '';
  const domain = canonicalDomain(website);
  const pageUrls = pickPages(website);

  const candidates = [];
  for (const p of pageUrls) {
    const html = await fetchWithTimeout(p);
    if (!html) continue;
    const emails = findEmails(html);
    for (const email of emails) {
      candidates.push({
        email,
        email_type: classifyEmail(email),
        source_url: p,
        source_type: 'website_page',
        verification_status: 'unknown',
        verification_sub_status: ''
      });
    }
  }

  const deduped = Object.values(candidates.reduce((acc, c) => {
    acc[c.email] = acc[c.email] || c;
    return acc;
  }, {}));

  for (const c of deduped) {
    const v = await validateWithZeroBounce(c.email);
    c.verification_status = v.status;
    c.verification_sub_status = v.sub_status;
  }

  const best = chooseBest(deduped);

  return {
    place_id: row.place_id,
    name: row.name,
    formatted_address: row.formatted_address,
    website_url: website,
    website_domain: domain,
    maps_url: row.maps_url || '',
    best_email: best?.email || '',
    email_type: best?.email_type || '',
    source_url: best?.source_url || '',
    verification_status: best?.verification_status || 'unknown',
    verification_sub_status: best?.verification_sub_status || '',
    confidence_score: best?.confidence_score ?? 0,
    candidates_found: deduped.length,
    notes: best ? 'ok' : 'no_email_found',
    processed_at: new Date().toISOString()
  };
}

function toCsv(rows) {
  const headers = Object.keys(rows[0] || {});
  const esc = (v) => {
    if (v == null) return '';
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [headers.join(','), ...rows.map(r => headers.map(h => esc(r[h])).join(','))].join('\n');
}

async function pooled(items, limit, fn) {
  let i = 0;
  const out = Array(items.length);
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx], idx);
      if ((idx + 1) % 25 === 0 || idx + 1 === items.length) {
        console.log(`Processed ${idx + 1}/${items.length}`);
      }
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, limit) }, worker));
  return out;
}

(async () => {
  const all = JSON.parse(fs.readFileSync(DATA_PATH, 'utf8'));
  const sorted = [...all].sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')));
  const batch = sorted.slice(start, start + batchSize);

  if (!batch.length) {
    console.error('No rows in selected batch.');
    process.exit(1);
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });

  const inputPath = path.join(OUT_DIR, `${batchName}_input.json`);
  fs.writeFileSync(inputPath, JSON.stringify(batch, null, 2));

  console.log(`Running ${batchName} (${batch.length} rows). dryRun=${dryRun}`);
  const results = await pooled(batch, concurrency, processRow);

  const jsonPath = path.join(OUT_DIR, `${batchName}_results.json`);
  const csvPath = path.join(OUT_DIR, `${batchName}_results.csv`);
  fs.writeFileSync(jsonPath, JSON.stringify(results, null, 2));
  fs.writeFileSync(csvPath, toCsv(results));

  const summary = {
    batch: batchName,
    start,
    size: batch.length,
    dryRun,
    zerobounce_enabled: Boolean(ZEROBOUNCE_API_KEY) && !dryRun,
    with_best_email: results.filter(r => r.best_email).length,
    valid: results.filter(r => r.verification_status === 'valid').length,
    catch_all: results.filter(r => r.verification_status === 'catch-all').length,
    invalid: results.filter(r => r.verification_status === 'invalid').length,
    unknown: results.filter(r => r.verification_status === 'unknown').length,
    generated_at: new Date().toISOString(),
    files: { inputPath, jsonPath, csvPath }
  };

  const summaryPath = path.join(OUT_DIR, `${batchName}_summary.json`);
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));

  console.log('Done.');
  console.log(summary);
})();

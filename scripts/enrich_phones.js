#!/usr/bin/env node
const fs = require('fs');

const API_KEY = process.env.GOOGLE_MAPS_API_KEY;
if (!API_KEY) {
  console.error('Missing GOOGLE_MAPS_API_KEY');
  process.exit(1);
}

const DATA_JSON = 'pet-england-explorer/data/pet_supplies_england.json';
const DATA_CSV = 'pet-england-explorer/data/pet_supplies_england.csv';
const CONCURRENCY = Number(process.env.PHONE_ENRICH_CONCURRENCY || 8);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchPhone(placeId, attempt = 1) {
  const url = `https://places.googleapis.com/v1/places/${placeId}`;
  const res = await fetch(url, {
    headers: {
      'X-Goog-Api-Key': API_KEY,
      'X-Goog-FieldMask': 'id,nationalPhoneNumber,internationalPhoneNumber'
    }
  });

  if (res.status === 429 || res.status >= 500) {
    if (attempt < 5) {
      await sleep(300 * attempt);
      return fetchPhone(placeId, attempt + 1);
    }
  }

  if (!res.ok) {
    return { nationalPhoneNumber: '', internationalPhoneNumber: '' };
  }

  const json = await res.json();
  return {
    nationalPhoneNumber: json.nationalPhoneNumber || '',
    internationalPhoneNumber: json.internationalPhoneNumber || ''
  };
}

function toCsv(rows) {
  const headers = Object.keys(rows[0] || {});
  const esc = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [headers.join(','), ...rows.map(r => headers.map(h => esc(r[h])).join(','))].join('\n');
}

(async () => {
  const rows = JSON.parse(fs.readFileSync(DATA_JSON, 'utf8'));
  let idx = 0;
  let done = 0;

  async function worker() {
    while (true) {
      const i = idx++;
      if (i >= rows.length) return;
      const r = rows[i];
      if (!r.place_id) { done++; continue; }
      const phone = await fetchPhone(r.place_id);
      r.phone = phone.nationalPhoneNumber || phone.internationalPhoneNumber || '';
      r.phone_international = phone.internationalPhoneNumber || '';
      done++;
      if (done % 100 === 0 || done === rows.length) {
        console.log(`Processed ${done}/${rows.length}`);
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

  fs.writeFileSync(DATA_JSON, JSON.stringify(rows, null, 2));
  fs.writeFileSync(DATA_CSV, toCsv(rows));

  const summaryPath = 'pet-england-explorer/data/summary.json';
  const summary = JSON.parse(fs.readFileSync(summaryPath, 'utf8'));
  const withPhone = rows.filter(r => r.phone).length;
  summary.generated_at = new Date().toISOString();
  summary.phone_enriched = true;
  summary.phone_populated_count = withPhone;
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));

  console.log(`Done. Phone populated: ${withPhone}/${rows.length}`);
})();
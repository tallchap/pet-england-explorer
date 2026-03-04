#!/usr/bin/env node
const fs = require('fs');

const API_KEY = process.env.GOOGLE_MAPS_API_KEY;
if (!API_KEY) {
  console.error('Missing GOOGLE_MAPS_API_KEY');
  process.exit(1);
}

const QUERIES = [
  'Bedfordshire','Berkshire','Bristol','Buckinghamshire','Cambridgeshire','Cheshire','City of London','Cornwall','County Durham','Cumbria','Derbyshire','Devon','Dorset','East Riding of Yorkshire','East Sussex','Essex','Gloucestershire','Greater London','Greater Manchester','Hampshire','Herefordshire','Hertfordshire','Isle of Wight','Kent','Lancashire','Leicestershire','Lincolnshire','Merseyside','Norfolk','North Yorkshire','Northamptonshire','Northumberland','Nottinghamshire','Oxfordshire','Rutland','Shropshire','Somerset','South Yorkshire','Staffordshire','Suffolk','Surrey','Tyne and Wear','Warwickshire','West Midlands','West Sussex','West Yorkshire','Wiltshire','Worcestershire',
  'Birmingham','Leeds','Sheffield','Bradford','Liverpool','Bristol','Manchester','Leicester','Coventry','Nottingham','Newcastle upon Tyne','Sunderland','Brighton','Plymouth','Stoke-on-Trent','Wolverhampton','Derby','Southampton','Portsmouth','York','Bath','Cambridge','Oxford','Reading','Milton Keynes','Chelmsford','Norwich','Exeter','Canterbury','Blackpool'
].map(x => `pet supplies in ${x}, England`);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function searchTextPage(textQuery, pageToken = '') {
  const body = {
    textQuery,
    regionCode: 'GB',
    languageCode: 'en',
    pageSize: 20,
    ...(pageToken ? { pageToken } : {})
  };

  const res = await fetch('https://places.googleapis.com/v1/places:searchText', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Goog-Api-Key': API_KEY,
      'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.businessStatus,places.googleMapsUri,places.types,nextPageToken'
    },
    body: JSON.stringify(body)
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`HTTP ${res.status} ${t}`);
  }
  return res.json();
}

async function searchQuery(query) {
  const out = [];
  let pageToken = '';
  for (let page = 0; page < 3; page++) {
    const json = await searchTextPage(query, pageToken);
    (json.places || []).forEach(p => out.push(p));
    pageToken = json.nextPageToken || '';
    if (!pageToken) break;
    await sleep(2200);
  }
  return out;
}

function toRecord(p) {
  return {
    place_id: p.id || '',
    name: p.displayName?.text || '',
    formatted_address: p.formattedAddress || '',
    lat: p.location?.latitude ?? null,
    lng: p.location?.longitude ?? null,
    rating: p.rating ?? null,
    user_ratings_total: p.userRatingCount ?? null,
    business_status: p.businessStatus || '',
    types: (p.types || []).join('|'),
    maps_url: p.googleMapsUri || '',
    source: 'google_places_textsearch_v1'
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
  const seen = new Map();
  for (let i = 0; i < QUERIES.length; i++) {
    const q = QUERIES[i];
    console.log(`[${i + 1}/${QUERIES.length}] ${q}`);
    try {
      const results = await searchQuery(q);
      for (const p of results) {
        if (!p.id) continue;
        const rec = toRecord(p);
        if (!seen.has(p.id)) seen.set(p.id, rec);
        else {
          const cur = seen.get(p.id);
          if ((rec.user_ratings_total || 0) > (cur.user_ratings_total || 0)) seen.set(p.id, { ...cur, ...rec });
        }
      }
    } catch (e) {
      console.error(`Query failed: ${q} :: ${e.message}`);
    }
    await sleep(100);
  }

  const rows = [...seen.values()].sort((a, b) => (b.user_ratings_total || 0) - (a.user_ratings_total || 0));
  fs.writeFileSync('pet-england-explorer/data/pet_supplies_england.json', JSON.stringify(rows, null, 2));
  fs.writeFileSync('pet-england-explorer/data/pet_supplies_england.csv', toCsv(rows));
  fs.writeFileSync('pet-england-explorer/data/summary.json', JSON.stringify({
    generated_at: new Date().toISOString(),
    total_unique_locations: rows.length,
    query_count: QUERIES.length,
    method: 'Google Places API (New) Text Search (county + city queries, deduped by place_id)'
  }, null, 2));
  console.log(`Done. Unique: ${rows.length}`);
})();
#!/usr/bin/env node
/**
 * Spot share map - analytics aggregator
 * ------------------------------------
 * Firestore の生ログ（pageViews / spots）を Admin SDK で読み込み、
 * 「日別の集計値」と「全期間のサマリー」だけを analyticsDaily / analyticsSummary
 * コレクションに書き出すバッチスクリプトです。
 *
 * 公開用の analytics.html はこのスクリプトが作った集計コレクションだけを参照するため、
 * 生の pageViews（誰が・いつ来たか）はクライアントから一切読めません。
 *
 * 実行方法:
 *   FIREBASE_SERVICE_ACCOUNT_KEY='<サービスアカウントJSON文字列>' node aggregate-analytics.js
 *
 * GitHub Actions からは secrets.FIREBASE_SERVICE_ACCOUNT_KEY 経由で渡す想定です。
 */

'use strict';

const admin = require('firebase-admin');

// pageViews.referrerType に入りうる値。想定外の値は "other" に丸める。
const REFERRER_TYPES = [
  'twitter_official',
  'twitter',
  'search',
  'instagram',
  'facebook',
  'direct',
  'other',
];

function initAdmin() {
  if (admin.apps.length) return admin.app();

  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
  if (!raw) {
    throw new Error(
      '環境変数 FIREBASE_SERVICE_ACCOUNT_KEY が設定されていません（サービスアカウントJSON全体を渡してください）'
    );
  }

  let credentialJson;
  try {
    credentialJson = JSON.parse(raw);
  } catch (err) {
    throw new Error('FIREBASE_SERVICE_ACCOUNT_KEY のJSONパースに失敗しました: ' + err.message);
  }

  return admin.initializeApp({
    credential: admin.credential.cert(credentialJson),
    projectId: credentialJson.project_id,
  });
}

// エポックミリ秒 → JST(UTC+9)の日付キー "YYYY-MM-DD"
function toJstDateKey(ms) {
  const d = new Date(ms + 9 * 3600 * 1000);
  return d.toISOString().slice(0, 10);
}

function emptyReferrerCounts() {
  const o = {};
  REFERRER_TYPES.forEach((k) => {
    o[k] = 0;
  });
  return o;
}

async function fetchAll(db, collectionName) {
  const snap = await db.collection(collectionName).get();
  return snap.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
}

function normalizeSpot(raw) {
  const createdAt = Number(raw.createdAt) || null;
  return {
    category: raw.category || 'その他',
    station: (raw.station || '').trim(),
    createdAt,
    dateKey: createdAt ? toJstDateKey(createdAt) : null,
    creatorIsYuka: Boolean(raw.creatorIsYuka),
    creatorRef: raw.creatorRef || null,
  };
}

function normalizePageView(raw) {
  const dateKey = raw.dateKey || (raw.ts ? toJstDateKey(Number(raw.ts)) : null);
  const referrerType = REFERRER_TYPES.includes(raw.referrerType) ? raw.referrerType : 'other';
  return {
    dateKey,
    visitorId: raw.visitorId || raw.id,
    isYuka: Boolean(raw.isYuka),
    referrerType,
  };
}

/**
 * spots / pageViews の正規化済み配列から
 * 日別ドキュメント群と全期間サマリーを組み立てる。
 */
function buildAggregates(spots, pageViews) {
  const spotDateKeys = spots.filter((s) => s.dateKey).map((s) => s.dateKey);
  const viewDateKeys = pageViews.filter((p) => p.dateKey).map((p) => p.dateKey);
  const allDateKeys = Array.from(new Set([...spotDateKeys, ...viewDateKeys])).sort();

  // 累計スポット数を日付の昇順に積み上げていく
  let cumOfficial = 0;
  let cumGeneral = 0;

  // visitorId -> 訪問した日付の Set（リピート率は全期間で判定するため必要）
  const visitorDays = new Map();
  pageViews.forEach((p) => {
    if (!p.visitorId || !p.dateKey) return;
    if (!visitorDays.has(p.visitorId)) visitorDays.set(p.visitorId, new Set());
    visitorDays.get(p.visitorId).add(p.dateKey);
  });

  const dailyDocs = {};

  allDateKeys.forEach((dateKey) => {
    const daySpots = spots.filter((s) => s.dateKey === dateKey);
    const newSpotsOfficial = daySpots.filter((s) => s.creatorIsYuka).length;
    const newSpotsGeneral = daySpots.length - newSpotsOfficial;
    cumOfficial += newSpotsOfficial;
    cumGeneral += newSpotsGeneral;

    const dayViews = pageViews.filter((p) => p.dateKey === dateKey);
    const officialVisitorSet = new Set(dayViews.filter((p) => p.isYuka).map((p) => p.visitorId));
    const generalVisitorSet = new Set(dayViews.filter((p) => !p.isYuka).map((p) => p.visitorId));
    const allVisitorSet = new Set(dayViews.map((p) => p.visitorId));

    const referrerCounts = emptyReferrerCounts();
    dayViews.forEach((p) => {
      referrerCounts[p.referrerType] += 1;
    });

    dailyDocs[dateKey] = {
      date: dateKey,
      newSpotsOfficial,
      newSpotsGeneral,
      newSpotsTotal: daySpots.length,
      cumulativeSpotsOfficial: cumOfficial,
      cumulativeSpotsGeneral: cumGeneral,
      cumulativeSpotsTotal: cumOfficial + cumGeneral,
      uniqueVisitors: allVisitorSet.size,
      uniqueVisitorsOfficial: officialVisitorSet.size,
      uniqueVisitorsGeneral: generalVisitorSet.size,
      pageViewsTotal: dayViews.length,
      referrerCounts,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
  });

  // ---- 全期間サマリー ----
  const totalSpots = spots.length;
  const officialSpots = spots.filter((s) => s.creatorIsYuka).length;
  const generalSpots = totalSpots - officialSpots;

  const generalPosterRefs = new Set(
    spots.filter((s) => s.creatorRef && !s.creatorIsYuka).map((s) => s.creatorRef)
  );
  const totalPosters = generalPosterRefs.size + (officialSpots > 0 ? 1 : 0);

  const uniqueVisitorsAllTime = visitorDays.size;
  const repeatVisitorsAllTime = Array.from(visitorDays.values()).filter((s) => s.size >= 2).length;
  const repeatRateAllTime = uniqueVisitorsAllTime
    ? Math.round((repeatVisitorsAllTime / uniqueVisitorsAllTime) * 1000) / 10
    : 0;

  const referrerTotals = emptyReferrerCounts();
  pageViews.forEach((p) => {
    referrerTotals[p.referrerType] += 1;
  });
  const twitterTotal = referrerTotals.twitter_official + referrerTotals.twitter;
  const totalPageViews = pageViews.length;
  const twitterShare = totalPageViews ? Math.round((twitterTotal / totalPageViews) * 1000) / 10 : 0;

  const summary = {
    totalSpots,
    officialSpots,
    generalSpots,
    totalPosters,
    generalPosters: generalPosterRefs.size,
    uniqueVisitorsAllTime,
    repeatVisitorsAllTime,
    repeatRateAllTime,
    totalPageViews,
    referrerTotals,
    twitterTotal,
    twitterShare,
    firstDateKey: allDateKeys[0] || null,
    lastDateKey: allDateKeys[allDateKeys.length - 1] || null,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  return { dailyDocs, summary };
}

async function writeAggregates(db, dailyDocs, summary) {
  const dateKeys = Object.keys(dailyDocs);
  const BATCH_SIZE = 400; // Firestore の1バッチ上限(500)より少し余裕を持たせる

  for (let i = 0; i < dateKeys.length; i += BATCH_SIZE) {
    const batch = db.batch();
    const chunk = dateKeys.slice(i, i + BATCH_SIZE);
    chunk.forEach((dateKey) => {
      const ref = db.collection('analyticsDaily').doc(dateKey);
      batch.set(ref, dailyDocs[dateKey], { merge: true });
    });
    await batch.commit();
    console.log(`analyticsDaily: ${Math.min(i + BATCH_SIZE, dateKeys.length)}/${dateKeys.length} 件を書き込みました`);
  }

  await db.collection('analyticsSummary').doc('overall').set(summary, { merge: true });
  console.log('analyticsSummary/overall を更新しました');
}

async function main() {
  initAdmin();
  const db = admin.firestore();

  console.log('spots / pageViews を読み込んでいます...');
  const [rawSpots, rawPageViews] = await Promise.all([
    fetchAll(db, 'spots'),
    fetchAll(db, 'pageViews'),
  ]);

  const spots = rawSpots.map(normalizeSpot);
  const pageViews = rawPageViews.map(normalizePageView).filter((p) => p.dateKey);

  console.log(`spots: ${spots.length}件, pageViews: ${pageViews.length}件`);

  const { dailyDocs, summary } = buildAggregates(spots, pageViews);

  console.log(`集計対象の日数: ${Object.keys(dailyDocs).length}日`);
  await writeAggregates(db, dailyDocs, summary);

  console.log('集計が完了しました。');
}

main().catch((err) => {
  console.error('集計に失敗しました:', err);
  process.exitCode = 1;
});

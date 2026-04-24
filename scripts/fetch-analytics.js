// scripts/fetch-analytics.js
// GitHub Actions から定期実行される自動取得スクリプト
// 役割：
//   1. リフレッシュトークンからアクセストークンを取得
//   2. Firestore から追跡中動画を取得
//   3. 各動画について、投稿後 1h/1d/1w/1m のタイミングが来ていたら Analytics API を呼ぶ
//   4. 結果を targetOffset ラベル付きで Firestore に保存

const admin = require('firebase-admin');

// ==== 環境変数 ====
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const GOOGLE_REFRESH_TOKEN = process.env.GOOGLE_REFRESH_TOKEN;
const FIREBASE_SERVICE_ACCOUNT = process.env.FIREBASE_SERVICE_ACCOUNT;

if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN || !FIREBASE_SERVICE_ACCOUNT) {
  console.error('❌ 環境変数が不足しています');
  console.error('  GOOGLE_CLIENT_ID:', !!GOOGLE_CLIENT_ID);
  console.error('  GOOGLE_CLIENT_SECRET:', !!GOOGLE_CLIENT_SECRET);
  console.error('  GOOGLE_REFRESH_TOKEN:', !!GOOGLE_REFRESH_TOKEN);
  console.error('  FIREBASE_SERVICE_ACCOUNT:', !!FIREBASE_SERVICE_ACCOUNT);
  process.exit(1);
}

// ==== Firebase Admin 初期化 ====
const serviceAccount = JSON.parse(FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});
const db = admin.firestore();

// ==== 定数 ====
const DOC_ID = 'analyzer-data';
const COLLECTION = 'analyzer';

// 取得タイミング定義
const OFFSET_TARGETS = [
  { label: '1h', ms: 60 * 60 * 1000 },
  { label: '1d', ms: 24 * 60 * 60 * 1000 },
  { label: '1w', ms: 7 * 24 * 60 * 60 * 1000 },
  { label: '1m', ms: 30 * 24 * 60 * 60 * 1000 },
];

// 取得タイミングの許容範囲：目標時刻を過ぎてから30分以内のみ取得
// （GitHub Actionsのcron誤差を考慮した幅）
const TOLERANCE_MS = 30 * 60 * 1000;

// 対象動画の条件：投稿から31日以内の動画のみ
const MAX_AGE_MS = 31 * 24 * 60 * 60 * 1000;

// ==== OAuth: リフレッシュトークン → アクセストークン ====
async function getAccessToken() {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_CLIENT_SECRET,
      refresh_token: GOOGLE_REFRESH_TOKEN,
      grant_type: 'refresh_token',
    }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`OAuth リフレッシュ失敗: ${res.status} ${err}`);
  }
  const data = await res.json();
  return data.access_token;
}

// ==== YouTube Data API: 動画の最新再生数等を取得 ====
async function fetchVideoStats(videoIds, accessToken) {
  if (videoIds.length === 0) return [];
  const chunks = [];
  for (let i = 0; i < videoIds.length; i += 50) chunks.push(videoIds.slice(i, i + 50));
  const results = [];
  for (const chunk of chunks) {
    const url = new URL('https://www.googleapis.com/youtube/v3/videos');
    url.searchParams.set('part', 'snippet,statistics');
    url.searchParams.set('id', chunk.join(','));
    const res = await fetch(url, {
      headers: { 'Authorization': 'Bearer ' + accessToken },
    });
    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Videos API 失敗: ${res.status} ${err}`);
    }
    const data = await res.json();
    results.push(...(data.items || []));
  }
  return results;
}

// ==== YouTube Analytics API: 維持率等を取得（自分のチャンネルのみ） ====
async function fetchAnalytics(videoId, videoPublishedAt, accessToken) {
  const endDate = new Date().toISOString().slice(0, 10);
  const startDate = new Date(videoPublishedAt).toISOString().slice(0, 10);

  const url = new URL('https://youtubeanalytics.googleapis.com/v2/reports');
  url.searchParams.set('ids', 'channel==MINE');
  url.searchParams.set('startDate', startDate);
  url.searchParams.set('endDate', endDate);
  url.searchParams.set(
    'metrics',
    'views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,likes,subscribersGained,subscribersLost,shares,comments'
  );
  url.searchParams.set('filters', `video==${videoId}`);

  const res = await fetch(url, {
    headers: { 'Authorization': 'Bearer ' + accessToken },
  });
  if (!res.ok) {
    const err = await res.text();
    console.warn(`  ⚠️ Analytics API 失敗 (${videoId}): ${res.status}`);
    return null;
  }
  const data = await res.json();
  if (!data.rows || data.rows.length === 0) return null;

  const [views, watchMinutes, avgDuration, avgPercentage, likes, subGained, subLost, shares, comments] = data.rows[0];
  return {
    views, watchMinutes, avgDuration, avgPercentage,
    likes, subGained, subLost, shares, comments,
    startDate, endDate,
  };
}

// ==== メイン処理 ====
async function main() {
  console.log('🚀 自動取得スクリプト開始');
  console.log('  時刻:', new Date().toISOString());

  // 1. アクセストークン取得
  console.log('🔑 アクセストークン取得中...');
  const accessToken = await getAccessToken();
  console.log('  ✅ 取得完了');

  // 2. Firestore からデータ読み込み
  console.log('📖 Firestore からデータ読み込み中...');
  const docRef = db.collection(COLLECTION).doc(DOC_ID);
  const snap = await docRef.get();
  if (!snap.exists) {
    console.log('  ⚠️ Firestoreにデータがありません');
    return;
  }
  const data = snap.data();
  const videos = data.videos || [];
  const channels = data.channels || {};
  console.log(`  ✅ 動画数: ${videos.length}`);

  // 自分のチャンネルID（Analytics取得可能な範囲）
  const myChannelIds = new Set();
  if (channels.copi?.info?.id) myChannelIds.add(channels.copi.info.id);
  if (channels.waon?.info?.id) myChannelIds.add(channels.waon.info.id);
  console.log(`  ✅ 自分のチャンネル数: ${myChannelIds.size}`);

  // 3. 対象動画のフィルタリング
  const now = Date.now();
  const targetVideos = videos.filter(v => {
    if (!v.publishedAt) return false;
    const age = now - new Date(v.publishedAt).getTime();
    if (age < 0) return false; // 未来の投稿
    if (age > MAX_AGE_MS) return false; // 1ヶ月以上経過
    return true;
  });
  console.log(`📊 対象動画（投稿から1ヶ月以内）: ${targetVideos.length}本`);

  // 4. 各動画のオフセット判定
  let updatedCount = 0;
  let apiCalls = 0;

  for (const video of targetVideos) {
    const publishedTime = new Date(video.publishedAt).getTime();
    const ageMs = now - publishedTime;
    const isMine = myChannelIds.has(video.channelId);

    video.snapshots = video.snapshots || [];
    video.analyticsSnapshots = video.analyticsSnapshots || [];

    // 各オフセットをチェック
    for (const offset of OFFSET_TARGETS) {
      // 既に該当オフセットのスナップショットがあればスキップ
      const alreadyHas = video.snapshots.some(s => s.targetOffset === offset.label);
      if (alreadyHas) continue;

      // 目標時刻との差を計算
      const targetTime = publishedTime + offset.ms;
      const delta = now - targetTime;

      // 目標時刻にまだ達していない → スキップ
      if (delta < 0) continue;
      
      // 目標時刻から許容範囲（30分）を超えて経過 → スキップ
      // （過去動画の取り逃し分は取得しない、タイミング外のデータは意味がない）
      if (delta > TOLERANCE_MS) continue;

      console.log(`  🎯 [${offset.label}] 取得対象: ${video.title?.slice(0, 40)}`);

      // Videos API 呼び出し（全動画対象）
      try {
        const stats = await fetchVideoStats([video.videoId], accessToken);
        apiCalls++;
        if (stats.length === 0) {
          console.log(`    ⚠️ 動画情報取得失敗`);
          continue;
        }
        const info = stats[0];
        const snapshotAt = new Date().toISOString();

        // スナップショット追加
        video.snapshots.push({
          at: snapshotAt,
          views: parseInt(info.statistics.viewCount || 0),
          likes: parseInt(info.statistics.likeCount || 0),
          comments: parseInt(info.statistics.commentCount || 0),
          targetOffset: offset.label,
        });
        console.log(`    ✅ snapshot 追加 (views: ${info.statistics.viewCount})`);
        updatedCount++;

        // 自分のチャンネルなら Analytics API も呼ぶ
        if (isMine) {
          const analytics = await fetchAnalytics(video.videoId, video.publishedAt, accessToken);
          apiCalls++;
          if (analytics) {
            const analyticsSnapshot = {
              ...analytics,
              at: snapshotAt,
              targetOffset: offset.label,
            };
            video.analyticsSnapshots.push(analyticsSnapshot);
            video.analytics = { ...analyticsSnapshot, fetchedAt: snapshotAt };
            console.log(`    ✅ Analytics 追加 (視聴率: ${analytics.avgPercentage?.toFixed(1)}%)`);
          } else {
            console.log(`    ⚠️ Analytics データなし（反映待ちの可能性）`);
          }
        }

        // APIレート制限対策
        await new Promise(r => setTimeout(r, 300));
      } catch (err) {
        console.error(`    ❌ エラー: ${err.message}`);
      }
    }
  }

  // 5. Firestore に保存
  if (updatedCount > 0) {
    console.log(`💾 Firestore に保存中...（${updatedCount}件の更新）`);
    await docRef.set({
      channels,
      videos,
      _updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      _lastAutoFetch: new Date().toISOString(),
    });
    console.log('  ✅ 保存完了');
  } else {
    console.log('ℹ️ 更新対象なし');
  }

  console.log('🎉 完了');
  console.log(`  API呼び出し回数: ${apiCalls}`);
  console.log(`  更新されたスナップショット: ${updatedCount}`);
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ スクリプトエラー:', err);
    process.exit(1);
  });

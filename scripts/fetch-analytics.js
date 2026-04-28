// scripts/fetch-analytics.js
// GitHub Actions から定期実行される自動取得スクリプト
// 役割：
//   1. リフレッシュトークンからアクセストークンを取得
//   2. 自分のチャンネル（waon, copi）の新着動画を検知し、自動で videos に追加
//   3. Firestore から追跡中動画を取得
//   4. 各動画について、投稿後 1h/1d/1w/1m のタイミングが来ていたら Analytics API を呼ぶ
//   5. 結果を targetOffset ラベル付きで Firestore に保存

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

// 新着検知設定
const DISCOVER_LATEST_COUNT = 5; // 各チャンネルで最新N件をチェック（多めに）
const SHORT_THRESHOLD_SEC = 180; // ショート判定: 180秒以下

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

// ==== ISO 8601 duration → 秒数 ====
function parseDuration(iso) {
  // 例: "PT1M30S" → 90, "PT3M" → 180, "PT1H2M3S" → 3723
  if (!iso) return 0;
  const match = iso.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!match) return 0;
  const h = parseInt(match[1] || 0);
  const m = parseInt(match[2] || 0);
  const s = parseInt(match[3] || 0);
  return h * 3600 + m * 60 + s;
}

// ==== ショート/ライブ判定 ====
function isShortOrLive(videoItem) {
  // ライブチェック
  if (videoItem.snippet?.liveBroadcastContent && videoItem.snippet.liveBroadcastContent !== 'none') {
    return { isShortOrLive: true, kind: 'live' };
  }
  // ショートチェック（180秒以下）
  const durationSec = parseDuration(videoItem.contentDetails?.duration);
  if (durationSec > 0 && durationSec <= SHORT_THRESHOLD_SEC) {
    return { isShortOrLive: true, kind: 'short' };
  }
  // ライブアーカイブチェック（liveStreamingDetailsがあって配信終了済み）
  if (videoItem.liveStreamingDetails?.actualEndTime) {
    return { isShortOrLive: true, kind: 'live_archive' };
  }
  return { isShortOrLive: false, kind: 'normal' };
}

// ==== チャンネルの最新動画を取得（新着検知用） ====
// uploads playlist 方式（API消費1 unit、search.listの100分の1）
// チャンネルIDの先頭 UC を UU に置換することで uploads playlist ID が得られる
async function fetchChannelLatestVideos(channelId, accessToken) {
  // UCxxx → UUxxx (uploads playlist ID)
  const uploadsPlaylistId = 'UU' + channelId.slice(2);
  
  const url = new URL('https://www.googleapis.com/youtube/v3/playlistItems');
  url.searchParams.set('part', 'contentDetails');
  url.searchParams.set('playlistId', uploadsPlaylistId);
  url.searchParams.set('maxResults', String(DISCOVER_LATEST_COUNT));

  const res = await fetch(url, {
    headers: { 'Authorization': 'Bearer ' + accessToken },
  });
  if (!res.ok) {
    const err = await res.text();
    console.warn(`  ⚠️ playlistItems API 失敗 (${channelId}): ${res.status}`);
    return [];
  }
  const data = await res.json();
  return (data.items || []).map(item => item.contentDetails?.videoId).filter(Boolean);
}

// ==== 動画の詳細情報を取得（snippet + contentDetails + statistics + liveStreamingDetails） ====
async function fetchVideoDetails(videoIds, accessToken) {
  if (videoIds.length === 0) return [];
  const chunks = [];
  for (let i = 0; i < videoIds.length; i += 50) chunks.push(videoIds.slice(i, i + 50));
  const results = [];
  for (const chunk of chunks) {
    const url = new URL('https://www.googleapis.com/youtube/v3/videos');
    url.searchParams.set('part', 'snippet,contentDetails,statistics,liveStreamingDetails');
    url.searchParams.set('id', chunk.join(','));
    const res = await fetch(url, {
      headers: { 'Authorization': 'Bearer ' + accessToken },
    });
    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Videos API (詳細) 失敗: ${res.status} ${err}`);
    }
    const data = await res.json();
    results.push(...(data.items || []));
  }
  return results;
}

// ==== 新着動画を検知して videos に追加 ====
async function discoverNewVideos(channels, videos, accessToken) {
  console.log('🔎 新着動画チェック中...');
  
  const myChannels = [];
  if (channels.copi?.info?.id) myChannels.push({ key: 'copi', id: channels.copi.info.id, info: channels.copi.info });
  if (channels.waon?.info?.id) myChannels.push({ key: 'waon', id: channels.waon.info.id, info: channels.waon.info });
  
  const existingVideoIds = new Set(videos.map(v => v.videoId));
  const candidateVideoIds = [];
  
  // 各チャンネルの最新動画IDを集める
  for (const ch of myChannels) {
    try {
      const latestIds = await fetchChannelLatestVideos(ch.id, accessToken);
      for (const vid of latestIds) {
        if (!existingVideoIds.has(vid)) {
          candidateVideoIds.push({ videoId: vid, channelKey: ch.key, channelInfo: ch.info });
        }
      }
    } catch (err) {
      console.error(`  ❌ ${ch.key} の新着取得エラー: ${err.message}`);
    }
  }
  
  if (candidateVideoIds.length === 0) {
    console.log('  ℹ️ 新着なし');
    return { added: 0 };
  }
  
  console.log(`  📋 候補: ${candidateVideoIds.length}件`);
  
  // 候補動画の詳細を一括取得
  const videoIds = candidateVideoIds.map(c => c.videoId);
  let details;
  try {
    details = await fetchVideoDetails(videoIds, accessToken);
  } catch (err) {
    console.error(`  ❌ 詳細取得エラー: ${err.message}`);
    return { added: 0 };
  }
  
  let addedCount = 0;
  let skippedShort = 0;
  let skippedLive = 0;
  
  for (const detail of details) {
    const candidate = candidateVideoIds.find(c => c.videoId === detail.id);
    if (!candidate) continue;
    
    // ショート/ライブ判定
    const judgment = isShortOrLive(detail);
    if (judgment.isShortOrLive) {
      console.log(`  ⏭️ スキップ [${judgment.kind}]: ${detail.snippet.title?.slice(0, 40)}`);
      if (judgment.kind === 'short') skippedShort++;
      else skippedLive++;
      continue;
    }
    
    // 新動画として追加
    const newVideo = {
      id: 'auto_' + detail.id + '_' + Date.now(),
      videoId: detail.id,
      title: detail.snippet.title,
      channelId: detail.snippet.channelId,
      channelTitle: detail.snippet.channelTitle,
      publishedAt: detail.snippet.publishedAt,
      thumbnailUrl: detail.snippet.thumbnails?.high?.url || detail.snippet.thumbnails?.default?.url || '',
      tags: detail.snippet.tags || [],
      description: detail.snippet.description || '',
      duration: detail.contentDetails?.duration || '',
      videoKind: judgment.kind,
      snapshots: [],
      analyticsSnapshots: [],
      addedAt: new Date().toISOString(),
      autoAdded: true,
    };
    
    videos.push(newVideo);
    addedCount++;
    console.log(`  ✨ 自動登録: ${detail.snippet.title?.slice(0, 40)} (${detail.snippet.channelTitle})`);
  }
  
  if (skippedShort > 0) console.log(`  ⏭️ ショート除外: ${skippedShort}件`);
  if (skippedLive > 0) console.log(`  ⏭️ ライブ除外: ${skippedLive}件`);
  console.log(`  ✅ 新規追加: ${addedCount}件`);
  
  return { added: addedCount };
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

  // 2.5. 新着動画の自動検知
  const discoverResult = await discoverNewVideos(channels, videos, accessToken);
  // 新規追加があれば、後で必ず保存する必要がある
  const hasNewVideos = discoverResult.added > 0;

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
  if (updatedCount > 0 || hasNewVideos) {
    console.log(`💾 Firestore に保存中...（更新${updatedCount}件、新規${discoverResult.added}件）`);
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
  console.log(`  新規追加された動画: ${discoverResult.added}`);
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ スクリプトエラー:', err);
    process.exit(1);
  });

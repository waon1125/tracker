// scripts/cleanup-bad-snapshots.js
// 初回自動取得時に誤って追加された targetOffset 付きスナップショットを削除
// 
// 対象：
//   - targetOffset フィールドを持つ snapshot で、
//   - 目標時刻から30分以上ずれているもの（=過去動画に後付けされたもの）
//
// 一度だけ実行するスクリプト

const admin = require('firebase-admin');

const FIREBASE_SERVICE_ACCOUNT = process.env.FIREBASE_SERVICE_ACCOUNT;

if (!FIREBASE_SERVICE_ACCOUNT) {
  console.error('❌ FIREBASE_SERVICE_ACCOUNT が未設定');
  process.exit(1);
}

const serviceAccount = JSON.parse(FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});
const db = admin.firestore();

const DOC_ID = 'analyzer-data';
const COLLECTION = 'analyzer';

const OFFSET_MS = {
  '1h': 60 * 60 * 1000,
  '1d': 24 * 60 * 60 * 1000,
  '1w': 7 * 24 * 60 * 60 * 1000,
  '1m': 30 * 24 * 60 * 60 * 1000,
};

const TOLERANCE_MS = 30 * 60 * 1000; // 30分

async function main() {
  console.log('🧹 クリーンアップ開始');

  const docRef = db.collection(COLLECTION).doc(DOC_ID);
  const snap = await docRef.get();
  if (!snap.exists) {
    console.log('⚠️ データなし');
    return;
  }
  const data = snap.data();
  const videos = data.videos || [];

  let removedSnapshots = 0;
  let removedAnalytics = 0;
  let affectedVideos = 0;

  for (const video of videos) {
    if (!video.publishedAt) continue;
    const publishedTime = new Date(video.publishedAt).getTime();
    let videoAffected = false;

    // snapshots のクリーンアップ
    if (video.snapshots && video.snapshots.length > 0) {
      const before = video.snapshots.length;
      video.snapshots = video.snapshots.filter(s => {
        // targetOffsetなしは無条件で残す（手動取得分）
        if (!s.targetOffset) return true;
        
        // targetOffsetあり → 「取得時刻が目標時刻±30分以内」かチェック
        const offsetMs = OFFSET_MS[s.targetOffset];
        if (!offsetMs) return true; // 未知のoffsetは念のため残す
        
        const targetTime = publishedTime + offsetMs;
        const snapshotTime = new Date(s.at).getTime();
        const delta = Math.abs(snapshotTime - targetTime);
        
        if (delta > TOLERANCE_MS) {
          // 30分以上ずれている → ゴミデータなので削除
          console.log(`  🗑️ snapshot 削除: "${video.title?.slice(0, 30)}" [${s.targetOffset}] ズレ: ${Math.round(delta / 60000)}分`);
          return false;
        }
        return true;
      });
      const after = video.snapshots.length;
      if (before !== after) {
        removedSnapshots += (before - after);
        videoAffected = true;
      }
    }

    // analyticsSnapshots のクリーンアップ
    if (video.analyticsSnapshots && video.analyticsSnapshots.length > 0) {
      const before = video.analyticsSnapshots.length;
      video.analyticsSnapshots = video.analyticsSnapshots.filter(s => {
        if (!s.targetOffset) return true;
        
        const offsetMs = OFFSET_MS[s.targetOffset];
        if (!offsetMs) return true;
        
        const targetTime = publishedTime + offsetMs;
        const snapshotTime = new Date(s.at).getTime();
        const delta = Math.abs(snapshotTime - targetTime);
        
        if (delta > TOLERANCE_MS) {
          console.log(`  🗑️ analyticsSnapshot 削除: "${video.title?.slice(0, 30)}" [${s.targetOffset}]`);
          return false;
        }
        return true;
      });
      const after = video.analyticsSnapshots.length;
      if (before !== after) {
        removedAnalytics += (before - after);
        videoAffected = true;
      }
    }

    // v.analytics の targetOffset チェック
    // v.analytics は最新データのキャッシュなので、ゴミだった場合は
    // analyticsSnapshots の最新から再設定
    if (video.analytics && video.analytics.targetOffset) {
      const offsetMs = OFFSET_MS[video.analytics.targetOffset];
      if (offsetMs) {
        const targetTime = publishedTime + offsetMs;
        const fetchedTime = new Date(video.analytics.fetchedAt || video.analytics.at).getTime();
        const delta = Math.abs(fetchedTime - targetTime);
        if (delta > TOLERANCE_MS) {
          // analyticsSnapshots の最新から取り直す
          if (video.analyticsSnapshots && video.analyticsSnapshots.length > 0) {
            const latest = video.analyticsSnapshots[video.analyticsSnapshots.length - 1];
            video.analytics = { ...latest, fetchedAt: latest.at };
          } else {
            video.analytics = null;
          }
          videoAffected = true;
        }
      }
    }

    if (videoAffected) affectedVideos++;
  }

  // 保存
  console.log('');
  console.log(`💾 影響動画: ${affectedVideos}本`);
  console.log(`   削除 snapshots: ${removedSnapshots}件`);
  console.log(`   削除 analyticsSnapshots: ${removedAnalytics}件`);

  if (removedSnapshots > 0 || removedAnalytics > 0) {
    await docRef.set({
      channels: data.channels,
      videos,
      _updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      _cleanupAt: new Date().toISOString(),
    });
    console.log('✅ 保存完了');
  } else {
    console.log('ℹ️ 削除対象なし');
  }
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

import { createClient as createSupabaseClient } from 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm';

// === SUPABASE MIGRATION CONFIG ===
// Disamakan dengan Server Pusat. Jangan taruh service_role / sb_secret di frontend.
const SUPABASE_URL='https://ismjupxoiywttkrekmfg.supabase.co';
const SUPABASE_ANON_KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlzbWp1cHhvaXl3dHRrcmVrbWZnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzkyNzc4MDEsImV4cCI6MjA5NDg1MzgwMX0.WVwqEdkPQ_x9NWR8QXTm85mIAvN8d9V2FaMJ2NiAMC0';
const supabaseConfig={projectId:'ismjupxoiywttkrekmfg'};
const supabase=createSupabaseClient(SUPABASE_URL,SUPABASE_ANON_KEY);
const db={type:'supabase',client:supabase};
const app=db;
const firebaseConfig=supabaseConfig; // alias lama agar logic lama tetap jalan.

// === FIRESTORE COMPAT LAYER DI ATAS SUPABASE ===
// Menjaga logic staff tetap sama: collection/doc/query/where/limit/getDoc/getDocs/setDoc/addDoc/onSnapshot/serverTimestamp.
const SUPABASE_FALLBACK_POLL_MS=2*60*1000;
const SUPABASE_REALTIME_DEBOUNCE_MS=450;
const SUPABASE_UNCHANGED_NOTIFY_MS=60000;
const SERVER_TIMESTAMP_SENTINEL={__supabaseServerTimestamp:true};
const serverTimestamp=()=>SERVER_TIMESTAMP_SENTINEL;
const doc=(_db,collectionName,id)=>({kind:'doc',collectionName,id:String(id||'')});
const collection=(_db,collectionName)=>({kind:'collection',collectionName});
const where=(field,op,value)=>({kind:'where',field,op,value});
const limit=(count)=>({kind:'limit',count:Number(count||0)});
const query=(base,...constraints)=>({kind:'query',collectionName:base.collectionName,constraints});
function makeId(prefix=''){return `${prefix}${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`;}
function deepCloneCompat(value){
  if(value===null||value===undefined)return value;
  if(value===SERVER_TIMESTAMP_SENTINEL||value?.__supabaseServerTimestamp)return new Date().toISOString();
  if(value instanceof Date)return value.toISOString();
  if(Array.isArray(value))return value.map(deepCloneCompat);
  if(typeof value==='object'){
    if(typeof value.toDate==='function')return value.toDate().toISOString();
    const out={};
    Object.entries(value).forEach(([k,v])=>{if(v!==undefined)out[k]=deepCloneCompat(v)});
    return out;
  }
  return value;
}
function normalizeRow(row){const data=row?.data&&typeof row.data==='object'?row.data:{};return {id:row?.id,...data};}
function docSnapshot(id,data){return {id,exists:()=>!!data,data:()=>data?{...data}:undefined};}
function querySnapshot(rows){
  const docs=rows.map(row=>({id:row.id,data:()=>({...row})}));
  return {docs,size:docs.length,empty:docs.length===0,forEach(cb){docs.forEach(cb)},docChanges(){return docs.map(doc=>({type:'added',doc}))}};
}
function snapshotSignature(snap){
  try{
    if(snap&&Array.isArray(snap.docs)){
      return snap.docs.map(d=>`${d.id}:${JSON.stringify(d.data())}`).join('|');
    }
    return `${snap?.exists?.()?'1':'0'}:${JSON.stringify(snap?.data?.()||null)}`;
  }catch(e){
    return String(Date.now());
  }
}
function snapshotIds(snap){
  try{
    if(snap&&Array.isArray(snap.docs))return new Set(snap.docs.map(d=>String(d.id)));
    return snap?.id?new Set([String(snap.id)]):new Set();
  }catch(e){
    return new Set();
  }
}
function getFieldValue(row,field){return String(field||'').split('.').reduce((acc,key)=>acc==null?undefined:acc[key],row);}
function compareWhere(row,c){
  const a=getFieldValue(row,c.field), b=c.value;
  if(c.op==='==')return String(a)===String(b);
  if(c.op==='!=')return String(a)!==String(b);
  if(c.op==='>=')return String(a)>=String(b);
  if(c.op==='<=')return String(a)<=String(b);
  if(c.op==='>')return String(a)>String(b);
  if(c.op==='<')return String(a)<String(b);
  if(c.op==='in')return Array.isArray(b)&&b.map(String).includes(String(a));
  return true;
}
function rowMatchesQuery(row,refOrQuery){
  if(!row)return false;
  if(refOrQuery.kind==='doc')return String(row.id||'')===String(refOrQuery.id||'');
  const constraints=refOrQuery.constraints||[];
  return constraints.filter(c=>c.kind==='where').every(c=>compareWhere(row,c));
}
function realtimePayloadRow(payload){
  const raw=(payload?.new&&Object.keys(payload.new).length)?payload.new:payload?.old;
  return raw?normalizeRow(raw):null;
}
function realtimeChannelName(refOrQuery){
  const table=String(refOrQuery.collectionName||'data').replace(/[^a-zA-Z0-9_]/g,'_');
  const scope=refOrQuery.kind==='doc'?String(refOrQuery.id||'doc'):Math.random().toString(36).slice(2,10);
  return `staff_${table}_${scope.replace(/[^a-zA-Z0-9_]/g,'_')}_${Date.now().toString(36)}`;
}
function realtimeFilter(refOrQuery){
  if(refOrQuery.kind!=='doc')return null;
  const id=String(refOrQuery.id||'');
  return id?`id=eq.${id.replace(/[,()]/g,'')}`:null;
}
function isRealtimePayloadRelevant(refOrQuery,payload,lastIds){
  const row=realtimePayloadRow(payload);
  if(!row)return true;
  if(refOrQuery.kind==='doc')return String(row.id||'')===String(refOrQuery.id||'');
  if(lastIds&&lastIds.has(String(row.id||'')))return true;
  return rowMatchesQuery(row,refOrQuery);
}
async function getDoc(ref){
  const {data,error}=await supabase.from(ref.collectionName).select('id,data').eq('id',ref.id).maybeSingle();
  if(error)throw error;
  return docSnapshot(ref.id,data?normalizeRow(data):null);
}
const getDocFromServer=getDoc;
async function getDocs(qy){
  const collectionName=qy.collectionName;
  const constraints=qy.constraints||[];
  const hardLimit=constraints.find(c=>c.kind==='limit')?.count||1000;
  const wheres=constraints.filter(c=>c.kind==='where');
  let req=supabase.from(collectionName).select('id,data');
  let canServerFilter=true;
  for(const c of wheres){
    // Supabase menyimpan field aplikasi di kolom JSONB `data`.
    // Filter langsung di server supaya transaksi terbaru tidak hilang dari riwayat
    // saat tabel sudah ramai. Fallback filter browser tetap dipakai di bawah.
    const field=String(c.field||'');
    const safeField=/^[a-zA-Z0-9_]+$/.test(field);
    const path=`data->>${field}`;
    if(safeField&&c.op==='=='){
      req=req.eq(path,String(c.value));
    }else if(safeField&&c.op==='!='){
      req=req.neq(path,String(c.value));
    }else if(safeField&&c.op==='>='){
      req=req.gte(path,String(c.value));
    }else if(safeField&&c.op==='<='){
      req=req.lte(path,String(c.value));
    }else if(safeField&&c.op==='>'){
      req=req.gt(path,String(c.value));
    }else if(safeField&&c.op==='<'){
      req=req.lt(path,String(c.value));
    }else if(safeField&&c.op==='in'&&Array.isArray(c.value)){
      req=req.in(path,c.value.map(String));
    }else{
      canServerFilter=false;
    }
  }
  // Untuk transaksi, ambil buffer lebih besar dan urutkan id DESC karena id transaksi
  // mengandung timestamp: stafftx_user_171... Ini mencegah polling menimpa riwayat
  // dengan hasil query parsial ketika data server sudah banyak.
  const fetchLimit=canServerFilter
    ? hardLimit
    : collectionName==='transactions'
    ? Math.min(Math.max(hardLimit*50,1000),5000)
    : Math.min(Math.max(hardLimit*3,hardLimit),5000);
  if(collectionName==='transactions')req=req.order('id',{ascending:false});
  const {data,error}=await req.limit(fetchLimit);
  if(error)throw error;
  let rows=(data||[]).map(normalizeRow);
  // Tetap filter ulang di browser untuk keamanan dan operator selain ==.
  wheres.forEach(c=>{rows=rows.filter(row=>compareWhere(row,c))});
  rows=sortDesc(rows);
  return querySnapshot(rows.slice(0,hardLimit));
}
const getDocsFromServer=getDocs;
async function setDoc(ref,payload,options={}){
  const nextData=deepCloneCompat(payload||{});
  let finalData=nextData;
  if(options?.merge){
    const oldSnap=await getDoc(ref).catch(()=>docSnapshot(ref.id,null));
    finalData={...(oldSnap.exists()?oldSnap.data():{}),...nextData};
    delete finalData.id;
  }
  const {error}=await supabase.from(ref.collectionName).upsert({id:ref.id,data:finalData},{onConflict:'id'});
  if(error)throw error;
  return ref;
}
async function addDoc(colRef,payload){const ref=doc(db,colRef.collectionName,makeId());await setDoc(ref,payload||{});return ref;}
async function deleteDoc(ref){
  const {error}=await supabase.from(ref.collectionName).delete().eq('id',ref.id);
  if(error)throw error;
}
function onSnapshot(refOrQuery,next,errorCb){
  let stopped=false;
  let lastSignature='';
  let lastNotifyMs=0;
  let lastIds=new Set();
  let realtimeChannel=null;
  let queuedTimer=0;
  const run=async({force=false}={})=>{
    try{
      const snap=refOrQuery.kind==='doc'?await getDoc(refOrQuery):await getDocs(refOrQuery);
      const sig=snapshotSignature(snap);
      const now=Date.now();
      lastIds=snapshotIds(snap);
      if(!force&&sig===lastSignature&&now-lastNotifyMs<SUPABASE_UNCHANGED_NOTIFY_MS)return;
      lastSignature=sig;
      lastNotifyMs=now;
      if(!stopped)next(snap);
    }catch(e){
      lastSignature='';
      console.warn('Supabase snapshot error:',e?.message||e);
      if(!stopped&&errorCb)errorCb(e);
    }
  };
  const queueRun=()=>{
    if(stopped)return;
    clearTimeout(queuedTimer);
    queuedTimer=setTimeout(()=>run(),SUPABASE_REALTIME_DEBOUNCE_MS);
  };
  run({force:true});
  try{
    const table=String(refOrQuery.collectionName||'');
    if(table&&supabase?.channel){
      const config={event:'*',schema:'public',table};
      const filter=realtimeFilter(refOrQuery);
      if(filter)config.filter=filter;
      realtimeChannel=supabase
        .channel(realtimeChannelName(refOrQuery))
        .on('postgres_changes',config,payload=>{
          if(isRealtimePayloadRelevant(refOrQuery,payload,lastIds))queueRun();
        })
        .subscribe(status=>{
          if(!stopped&&(status==='CHANNEL_ERROR'||status==='TIMED_OUT')){
            console.warn('Supabase realtime fallback:',table,status);
          }
        });
    }
  }catch(e){
    console.warn('Supabase realtime init skipped:',e?.message||e);
  }
  const timer=setInterval(run,SUPABASE_FALLBACK_POLL_MS);
  return ()=>{
    stopped=true;
    clearInterval(timer);
    clearTimeout(queuedTimer);
    if(realtimeChannel){
      try{supabase.removeChannel(realtimeChannel).catch(()=>{})}catch(e){}
    }
  };
}

const OFFICE_LOC={lat:-6.786168,lng:106.780918};
const RADIUS_LIMIT=200, MEMBER_URL='https://idhamkhalid24.github.io/kode-khusus-member/';
const SESSION='rocky_firebase_session_v1', LEGACY_SESSION='rocky_staff_compact_manual_v1', PENDING_KEY='rocky_staff_pending_sync_v2', MANUAL_BONUS_SEEN='rocky_staff_manual_bonus_seen_v1', TARGET_NOTICE_SEEN='rocky_staff_target_notice_seen_v1', FIRST_TX_PARTY_SEEN='rocky_staff_first_tx_party_seen_v1', DEVICE_ID_KEY='rocky_staff_device_id_v1', DEVICE_USER_KEY='rocky_staff_device_user_v1', ADMIN=['admin'];
const DEFAULT_BONUS={transactionBonusRate:.015,transactionBonusPercent:1.5,closingBonusPerMinute:100,closingDeadlineTime:'18:00',closingDeadlineHour:18,closingDeadlineMinute:0,closingDeadlineMinutes:1080};
const DAILY_TARGET_AMOUNT=2500000;
const BONUS_TARGET_AMOUNT=10000;
const TARGET_SETTINGS_TABLE='targetSettings';
const DAILY_TARGETS_TABLE='dailyTargets';
const TARGET_BONUS_REWARDS_TABLE='targetBonusRewards';
const TARGET_NOTIFICATIONS_TABLE='targetNotifications';
const HEADER_GUIDE_NOTE_DOC_ID='__header_icon_guide_note';
const DEFAULT_HEADER_GUIDE_NOTE='Bonus adalah apresiasi tambahan dan dapat berubah sewaktu-waktu. Fokus utama tetap pada penjualan, kinerja, pelayanan, dan tanggung jawab kerja.';
const STAFF_DAILY_NOTE_DOC_ID='__staff_daily_home_note';
const DEFAULT_STAFF_DAILY_NOTE='Semangat bekerja hari ini. Pastikan transaksi dicatat dengan benar dan refresh jika data belum masuk.';
const RECEIPT_TEXT_DOC_ID='__receipt_text_settings';
const DEFAULT_RECEIPT_TEXT_SETTINGS={storeName:'ROCKY HIJAB',storeSubtext:'',dailyTitle:'TRANSAKSI HARI INI',dateLabel:'Tanggal',cashierLabel:'Kasir',productLabel:'Produk',totalLabel:'Total',countLabel:'Jumlah',footerText:'Terima kasih',bottomFeedLines:6};
const STAFF_UNLOCK_TABLE='staff_leave_requests';
const LIMITS={txToday:120,attMonth:90,manualToday:80,closingsToday:80,unlockRequests:160,txBonusAll:2000,manualBonusAll:1000,closingsAll:1500,targetTxToday:5000,targetUsers:1000,targetAttToday:2000};
const INITIAL_LEGACY_LIMITS={tx:300,att:90,manual:200};
const ROCKY_ADMIN_NOTIFY_WORKER_BASE_URL='https://rocky-notif-worker.alfajrihanif24.workers.dev';
const ROCKY_ADMIN_NOTIFY_WORKER_URL=ROCKY_ADMIN_NOTIFY_WORKER_BASE_URL+'/notify-transaction';
const ROCKY_ADMIN_NOTIFY_ATTENDANCE_URL=ROCKY_ADMIN_NOTIFY_WORKER_BASE_URL+'/notify-attendance';
const ROCKY_ADMIN_NOTIFY_TRANSACTION_DELETE_URL=ROCKY_ADMIN_NOTIFY_WORKER_BASE_URL+'/notify-transaction-delete';
const ROCKY_ADMIN_NOTIFY_UNLOCK_URL=ROCKY_ADMIN_NOTIFY_WORKER_BASE_URL+'/notify-feature-unlock';
const ROCKY_ADMIN_NOTIFY_TARGET_URL=ROCKY_ADMIN_NOTIFY_WORKER_BASE_URL+'/notify-target-achieved';
const ROCKY_STAFF_NOTIFY_MANUAL_BONUS_URL=ROCKY_ADMIN_NOTIFY_WORKER_BASE_URL+'/notify-staff-manual-bonus';
const ROCKY_ADMIN_NOTIFY_SECRET='rockyNotifRahasia2026';
const REFRESH_COOLDOWN_MS=30000;

// Bridge ke aplikasi Belanjaku: staff bisa lapor barang kosong (nama barang + warna).
const BELANJAKU_BRIDGE_KEY='stafku_to_belanjaku_barang_kosong_v1';
const BELANJAKU_SUPABASE_URL='https://phuzgriglgovzcwgbehr.supabase.co';
const BELANJAKU_SUPABASE_ANON_KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBodXpncmlnbGdvdnpjd2diZWhyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzg1OTQ3ODUsImV4cCI6MjA5NDE3MDc4NX0.gf-cfo8Vos8uYgZRHG0KUB4xMhXAld2oTYv-wNiKneE';
// Kunci di atas adalah anon-public Supabase untuk tabel Belanjaku, bukan service role.
const BELANJAKU_SUPABASE_ITEMS_TABLE='belanja_items';

// HYBRID HEMAT READ: realtime tetap aktif, tapi polling cek device diperlambat.
const DEVICE_SESSION_CHECK_MS=5*60*1000; // sebelumnya 15 detik, sekarang 5 menit
const DEVICE_SESSION_ACTION_CACHE_MS=30*1000;
let lastManualRefreshAt=0;
let lastDeviceSessionCheckAt=0;
let staffRealtimeUnsubs=[];
let staffRealtimeStartedFor='';
function defaultDailyTargetState(dateKey=todayKey()){
  return{dateKey,targetAmount:DAILY_TARGET_AMOUNT,bonusAmount:BONUS_TARGET_AMOUNT,totalAmount:0,progressPercent:0,reached:false,bonusApplied:false,activeUsers:[],rewardedUsers:[],updatedAtMs:0};
}
const state={user:null,page:'home',pos:null,busy:false,lastSyncMs:0,syncing:false,syncError:'',data:{tx:[],att:[],closings:[],manual:[],unlockRequests:[],targetTx:[],targetUsers:[],targetAtt:[],targetSettings:{targetAmount:DAILY_TARGET_AMOUNT,bonusAmount:BONUS_TARGET_AMOUNT},dailyTarget:defaultDailyTargetState(),targetNotification:null,bonus:{...DEFAULT_BONUS},headerGuideNote:DEFAULT_HEADER_GUIDE_NOTE,staffDailyNote:{note:DEFAULT_STAFF_DAILY_NOTE,enabled:true},receiptSettings:{...DEFAULT_RECEIPT_TEXT_SETTINGS}}};
let dailyTargetApplyTimer=0;
let dailyTargetApplying=false;
let targetNoticeAnnounced=new Set();
let clockInInFlight=false;
let deviceSessionTimer=null;
const $=id=>document.getElementById(id), page=$('page');

function updateVisualViewportForKeyboard(){
  const vv=window.visualViewport;
  const h=Math.max(260,Math.round(vv?vv.height:window.innerHeight));
  const top=Math.max(0,Math.round(vv?vv.offsetTop:0));
  document.documentElement.style.setProperty('--app-vvh',h+'px');
  document.documentElement.style.setProperty('--app-vvtop',top+'px');
}
updateVisualViewportForKeyboard();
if(window.visualViewport){
  window.visualViewport.addEventListener('resize',updateVisualViewportForKeyboard);
  window.visualViewport.addEventListener('scroll',updateVisualViewportForKeyboard);
}
window.addEventListener('resize',updateVisualViewportForKeyboard);
window.addEventListener('orientationchange',()=>setTimeout(updateVisualViewportForKeyboard,120));
function keepLoginInputVisible(){
  setTimeout(()=>{
    const el=document.activeElement;
    if(el&&el.closest&&el.closest('.login')){
      try{el.scrollIntoView({block:'center',inline:'nearest',behavior:'smooth'})}catch(e){el.scrollIntoView(false)}
    }
  },90);
}
document.addEventListener('focusin',keepLoginInputVisible,true);
if(window.visualViewport){
  window.visualViewport.addEventListener('resize',()=>{
    const el=document.activeElement;
    if(el&&el.closest&&el.closest('.login'))keepLoginInputVisible();
  });
}

function esc(v){return String(v??'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]))}
function normalizeStaffNoteUrl(rawUrl){const url=String(rawUrl||'').trim();if(/^https?:\/\//i.test(url))return url;if(/^www\./i.test(url))return 'https://'+url;return url}
function openStaffNoteLink(e,rawUrl){if(e)e.preventDefault();const url=normalizeStaffNoteUrl(rawUrl);if(!/^https?:\/\//i.test(url))return false;try{const bridge=window.Android,methods=['openExternalUrl','openExternalLink','openUrl','openBrowser'];if(bridge){for(const m of methods){if(typeof bridge[m]==='function'){bridge[m](url);return false}}}}catch(err){console.warn('open external link bridge failed',err)}const opened=window.open(url,'_blank','noopener,noreferrer');if(!opened)window.location.href=url;return false}
function shortStaffNoteLinkLabel(rawUrl){let clean=String(rawUrl||'').trim(),label=clean.replace(/^https?:\/\//i,'').replace(/^www\./i,'');try{const u=new URL(normalizeStaffNoteUrl(clean));const host=u.hostname.replace(/^www\./i,''),path=u.pathname.replace(/\/+$/,'');const last=decodeURIComponent((path.split('/').filter(Boolean).pop()||'')).replace(/[-_]+/g,' ');label=last?`${host}/…/${last}`:host}catch(e){}return label.length>42?label.slice(0,25)+'…'+label.slice(-12):label}
function linkText(v){const text=String(v||''),rx=/((?:https?:\/\/|www\.)[^\s<>"']+)/gi;let html='',last=0;text.replace(rx,(match,url,offset)=>{html+=esc(text.slice(last,offset));let raw=url,trailing='';const tm=raw.match(/[),.;!?]+$/);if(tm){trailing=tm[0];raw=raw.slice(0,-trailing.length)}const href=normalizeStaffNoteUrl(raw);if(/^https?:\/\//i.test(href)){const label=shortStaffNoteLinkLabel(raw);html+=`<a class="staff-note-link" href="${esc(href)}" title="${esc(href)}" target="_blank" rel="noopener noreferrer" onclick="return openStaffNoteLink(event, this.href)">${esc(label)}</a>${esc(trailing)}`}else{html+=esc(match)}last=offset+match.length;return match});html+=esc(text.slice(last));return html}
function key(v){return String(v||'').trim().toLowerCase().replace(/\s+/g,'_').replace(/[^a-z0-9_\-.]/g,'')}
function getDeviceId(){
  let id=localStorage.getItem(DEVICE_ID_KEY)||'';
  if(!id){
    const rnd=(crypto?.randomUUID?crypto.randomUUID():(Date.now().toString(36)+'_'+Math.random().toString(36).slice(2)));
    id='dev_'+String(rnd).replace(/[^a-zA-Z0-9_-]/g,'').slice(0,48);
    localStorage.setItem(DEVICE_ID_KEY,id);
  }
  return id;
}
function localDeviceUser(){return key(localStorage.getItem(DEVICE_USER_KEY)||'')}
function bindLocalDeviceUser(username){try{localStorage.setItem(DEVICE_USER_KEY,key(username))}catch(e){}}
function clearLocalDeviceUser(){try{localStorage.removeItem(DEVICE_USER_KEY)}catch(e){}}
function shortDevice(id){return String(id||'').slice(-6).toUpperCase()||'-'}
async function isLocalDeviceStillOwnedBy(boundUser,deviceId){
  const bound=key(boundUser);
  if(!bound)return false;
  try{
    const snap=await getDocFromServer(doc(db,'users',bound));
    if(!snap.exists())return false;
    const raw=snap.data()||{};
    const data={id:snap.id,username:raw.username||snap.id,...raw};
    if(isDeviceFreeUser(data)||data.active===false||deleted(data))return false;
    return String(data.deviceId||'').trim()===deviceId && data.deviceLocked!==false;
  }catch(e){
    console.warn('device owner check skipped',e?.code||e?.message||e);
    return true;
  }
}
async function verifyDeviceLockForLogin(userData,username){
  const u=key(username||userData?.username||userData?.id), deviceId=getDeviceId(), bound=localDeviceUser();
  if(isDeviceFreeUser(userData)){
    const deviceLabel=isTrialUser(userData)?'Dummy bebas login':(isRismaSpecialUser(userData)?'Risma bebas login':'Harian bebas login');
    try{
      await setDoc(doc(db,'users',u),{
        deviceId:'',
        deviceLocked:false,
        deviceUser:'',
        deviceApp:'staff',
        deviceLabel,
        devicePolicy:'open_any_device',
        deviceLastLoginAt:serverTimestamp(),
        deviceLastLoginAtMs:Date.now()
      },{merge:true});
    }catch(e){console.warn('open device free update skipped',e?.code||e)}
    if(isTrialUser(userData))clearLocalDeviceUser();
    return {ok:true,deviceId:'',exempt:true};
  }
  if(bound&&bound!==u){
    const stillOwned=await isLocalDeviceStillOwnedBy(bound,deviceId);
    if(stillOwned)return {ok:false,msg:`Perangkat ini masih terdaftar untuk user ${bound}. Reset device user tersebut dulu dari admin.`};
    clearLocalDeviceUser();
  }
  const serverDeviceId=String(userData?.deviceId||'').trim();
  if(serverDeviceId&&serverDeviceId!==deviceId)return {ok:false,msg:'Akun ini sudah terdaftar di perangkat lain. Hubungi admin untuk reset device.'};
  try{
    await setDoc(doc(db,'users',u),{
      deviceId,
      deviceLocked:true,
      deviceUser:u,
      deviceApp:'staff',
      deviceLabel:'Staff App '+shortDevice(deviceId),
      deviceUserAgent:String(navigator.userAgent||'').slice(0,180),
      devicePlatform:String(navigator.platform||'').slice(0,80),
      deviceLastLoginAt:serverTimestamp(),
      deviceLastLoginAtMs:Date.now(),
      deviceLockedAt:serverDeviceId?(userData?.deviceLockedAt||serverTimestamp()):serverTimestamp(),
      deviceLockedAtMs:Number(userData?.deviceLockedAtMs||Date.now())
    },{merge:true});
    bindLocalDeviceUser(u);
    return {ok:true,deviceId};
  }catch(e){
    console.error('device lock failed',e);
    return {ok:false,msg:isPermissionError(e)?'Akses Firebase menolak kunci perangkat. Hubungi admin.':'Gagal mengunci perangkat. Pastikan internet aktif.'};
  }
}

function clearSessionAndRender(msg){
  try{localStorage.removeItem(SESSION);localStorage.removeItem(LEGACY_SESSION)}catch(e){}
  stopDeviceSessionWatch();
  clearStaffRealtime();
  clearAndroidStaffSession();
  targetNoticeAnnounced=new Set();
  state.user=null;state.pos=null;state.syncError='';state.lastSyncMs=0;state.data={tx:[],att:[],closings:[],manual:[],unlockRequests:[],targetTx:[],targetUsers:[],targetAtt:[],targetSettings:{targetAmount:DAILY_TARGET_AMOUNT,bonusAmount:BONUS_TARGET_AMOUNT},dailyTarget:defaultDailyTargetState(),targetNotification:null,bonus:{...DEFAULT_BONUS},headerGuideNote:DEFAULT_HEADER_GUIDE_NOTE,staffDailyNote:{note:DEFAULT_STAFF_DAILY_NOTE,enabled:true},receiptSettings:{...DEFAULT_RECEIPT_TEXT_SETTINGS}};
  if(msg)toast(msg);
  renderLogin();
}
function isDeviceSessionInvalid(fresh){
  if(!fresh||isDeviceFreeUser(fresh)||isDeviceFreeUser(state.user))return '';
  const serverReset=Number(fresh.deviceResetAtMs||0), localReset=Number(state.user?.deviceResetAtMs||0);
  if(serverReset&&serverReset>localReset)return 'Device akun ini sudah direset admin. Silakan login ulang.';
  const serverDeviceId=String(fresh.deviceId||'').trim(), localDeviceId=getDeviceId();
  if(serverDeviceId&&serverDeviceId!==localDeviceId)return 'Akun ini sudah pindah ke perangkat lain. Silakan hubungi admin.';
  return '';
}
async function validateCurrentDeviceSession({silent=true,force=false}={}){
  if(!state.user||isDeviceFreeUser(state.user))return true;
  const nowMs=Date.now();
  if(!force&&lastDeviceSessionCheckAt&&nowMs-lastDeviceSessionCheckAt<DEVICE_SESSION_ACTION_CACHE_MS)return true;
  try{
    const u=key(state.user.username), snap=await getDocFromServer(doc(db,'users',u));
    lastDeviceSessionCheckAt=Date.now();
    if(!snap.exists()){clearSessionAndRender('Akun sudah tidak valid');return false}
    const raw=snap.data()||{}, fresh={id:snap.id,username:raw.username||snap.id,...raw};
    if(isAdmin(fresh)||fresh.active===false||deleted(fresh)){clearSessionAndRender('Akun sudah nonaktif / tidak valid');return false}
    const reason=isDeviceSessionInvalid(fresh);
    if(reason){clearSessionAndRender(reason);return false}
    return true;
  }catch(e){
    lastDeviceSessionCheckAt=Date.now();
    if(!silent)console.warn('device session check skipped',e?.code||e?.message||e);
    return true;
  }
}
function startDeviceSessionWatch(){
  stopDeviceSessionWatch();
  if(!state.user||isDeviceFreeUser(state.user))return;
  deviceSessionTimer=setInterval(()=>validateCurrentDeviceSession({silent:true}),DEVICE_SESSION_CHECK_MS);
}
function stopDeviceSessionWatch(){
  if(deviceSessionTimer){clearInterval(deviceSessionTimer);deviceSessionTimer=null;}
}
function rp(v){return Number(v||0).toLocaleString('id-ID')}
function parts(d=new Date()){return Object.fromEntries(new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Jakarta',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).formatToParts(d).map(p=>[p.type,p.value]))}
function todayKey(d=new Date()){const p=parts(d);return `${p.year}-${p.month}-${p.day}`}
function monthKey(d=new Date()){return todayKey(d).slice(0,7)}
function monthStartKey(m=monthKey()){return `${String(m||monthKey()).slice(0,7)}-01`}
function monthEndKey(m=monthKey()){const [y,mo]=String(m||monthKey()).slice(0,7).split('-').map(Number);const last=new Date(Date.UTC(y,mo,0)).getUTCDate();return `${String(y).padStart(4,'0')}-${String(mo).padStart(2,'0')}-${String(last).padStart(2,'0')}`}
function timeNow(){const p=parts();return `${p.hour}:${p.minute}`}
function dateID(k){if(!k)return '-';const [y,m,d]=String(k).split('-');return `${d}/${m}/${y}`}
function monthID(k){if(!k)return '-';const [y,m]=String(k).split('-');return `${m}/${y}`}
function ms(r){const n=Number(r?.createdAtMs||r?.updatedAtMs||r?.deletedAtMs||0);if(n)return n;const t=r?.createdAt||r?.updatedAt||r?.deletedAt;if(t?.toMillis)return t.toMillis();if(t?.seconds)return t.seconds*1000;return 0}
function timeID(m){if(!m)return '--:--';const p=parts(new Date(m));return `${p.hour}:${p.minute}`}
function deleted(x){return x?.deleted===true||x?.deleted==='true'}
function sortDesc(a){return [...(a||[])].sort((x,y)=>ms(y)-ms(x))}
function isAdmin(u){return key(u?.role)==='admin'||ADMIN.includes(key(u?.username||u?.id))}
function isDailyUser(user=state.user){const r=key(user?.role);return r==='harian'||r==='daily'||r==='karyawan_harian'}
function isDaily(){return isDailyUser(state.user)}
function isTrialUser(user=state.user){const value=String(user?.accountType||user?.mode||user?.type||'').toLowerCase(),role=String(user?.role||'').toLowerCase(),username=key(user?.username||user?.id||user?.user||'');return !!user&&(user.isDummy===true||user.trialMode===true||user.dummy===true||user.dummyMode===true||user.isTrial===true||user.trial===true||user.excludeFromReports===true||user.excludeFromReport===true||value==='dummy'||value==='dumy'||value==='trial'||role==='dummy'||role==='dumy'||role==='trial'||username==='dummy'||username.startsWith('dummy_')||username.includes('_dummy')||username==='dumy'||username.startsWith('dumy_')||username.includes('_dumy')||username==='trial'||username.startsWith('trial_')||username.includes('_trial'))}
function isRismaSpecialUser(user=state.user){return key(user?.username||user?.id||user?.user)==='risma'}
function isDeviceFreeUser(user=state.user){return !!user&&(isDailyUser(user)||isTrialUser(user)||isRismaSpecialUser(user))}
function isAttendanceFreeUser(user=state.user){return !!user&&(isDailyUser(user)||isTrialUser(user)||isRismaSpecialUser(user))}
function trialRecordFlags(user=state.user){return isTrialUser(user)?{isDummy:true,trialMode:true,accountType:'dummy',excludeFromReports:true}:{isDummy:false,trialMode:false,accountType:'normal',excludeFromReports:false}}
function isTrialRecord(rec){if(!rec)return false;if(rec.isDummy===true||rec.trialMode===true||rec.excludeFromReports===true||String(rec.accountType||'').toLowerCase()==='dummy')return true;const u=key(rec.user||rec.username||rec.targetUsername||'');if(!u)return false;if(key(state.user?.username)===u)return isTrialUser(state.user);const found=(state.data.targetUsers||[]).find(x=>key(x.username||x.id)===u);return isTrialUser(found)}
function recordMatchesCurrentAccount(rec){return isTrialUser()?isTrialRecord(rec):!isTrialRecord(rec)}
function trialSessionFields(data={}){const dummy=isTrialUser(data);return{isDummy:dummy,trialMode:dummy,accountType:dummy?'dummy':'normal',excludeFromReports:dummy,trialTargetAmount:dummy?Number(data.trialTargetAmount||10000):0,trialBonusAmount:dummy?Number(data.trialBonusAmount||1000):0}}
function hasBonusValue(value){return value!==undefined&&value!==null&&String(value).trim()!==''}
function globalTransactionBonusRate(){const r=Number(state.data.bonus?.transactionBonusRate);return Number.isFinite(r)&&r>=0?r:DEFAULT_BONUS.transactionBonusRate}
function globalClosingBonusPerMinute(){const n=Number(state.data.bonus?.closingBonusPerMinute);return Number.isFinite(n)&&n>=0?n:DEFAULT_BONUS.closingBonusPerMinute}
function normalizeClosingDeadlineParts(value=null,fallback={hour:18,minute:0}){const fb=fallback||{hour:18,minute:0};if(value&&typeof value==='object'){const direct=value.deadlineTime??value.closingDeadlineTime??value.deadline??value.jamClosing??value.jam_closing??value.label??null;if(direct!==null&&direct!==undefined&&String(direct).trim()!=='')return normalizeClosingDeadlineParts(direct,fb);const h=Number(value.deadlineHour??value.closingDeadlineHour??value.hour),m=Number(value.deadlineMinute??value.closingDeadlineMinute??value.minute);if(Number.isInteger(h)&&Number.isInteger(m)&&h>=0&&h<=23&&m>=0&&m<=59)return{hour:h,minute:m};const total=Number(value.deadlineMinutes??value.closingDeadlineMinutes??NaN);if(Number.isFinite(total)&&total>=0)return{hour:Math.floor(total/60)%24,minute:Math.floor(total%60)};return fb;}const raw=String(value??'').trim().replace('.',':').replace(/\s*WIB$/i,'');const match=raw.match(/^(\d{1,2}):(\d{2})/);if(match){const h=Number(match[1]),m=Number(match[2]);if(Number.isInteger(h)&&Number.isInteger(m)&&h>=0&&h<=23&&m>=0&&m<=59)return{hour:h,minute:m};}return fb;}
function normalizeBonusSettings(data={}){const d=data||{},parts=normalizeClosingDeadlineParts(d,{hour:18,minute:0}),label=`${String(parts.hour).padStart(2,'0')}:${String(parts.minute).padStart(2,'0')}`,rate=Number.isFinite(Number(d.transactionBonusRate))?Number(d.transactionBonusRate):DEFAULT_BONUS.transactionBonusRate,pct=Number.isFinite(Number(d.transactionBonusPercent))?Number(d.transactionBonusPercent):Number((rate*100).toFixed(3)),closing=Number.isFinite(Number(d.closingBonusPerMinute))?Number(d.closingBonusPerMinute):DEFAULT_BONUS.closingBonusPerMinute;return{...DEFAULT_BONUS,...d,transactionBonusRate:rate,transactionBonusPercent:pct,closingBonusPerMinute:closing,closingDeadlineTime:label,closingDeadlineHour:parts.hour,closingDeadlineMinute:parts.minute,closingDeadlineMinutes:(parts.hour*60)+parts.minute};}
function closingDeadlineTimeLabel(source=null){const parts=normalizeClosingDeadlineParts(source||state.data.bonus||DEFAULT_BONUS,{hour:18,minute:0});return`${String(parts.hour).padStart(2,'0')}:${String(parts.minute).padStart(2,'0')}`}
function closingDeadlineMinutes(source=null){const parts=normalizeClosingDeadlineParts(source||state.data.bonus||DEFAULT_BONUS,{hour:18,minute:0});return parts.hour*60+parts.minute}
function userTransactionBonusRate(user=state.user){
  const u=user||{};
  if(hasBonusValue(u.transactionBonusRate)){const r=Number(u.transactionBonusRate);if(Number.isFinite(r)&&r>0)return r}
  if(hasBonusValue(u.transactionBonusPercent)){const p=Number(u.transactionBonusPercent);if(Number.isFinite(p)&&p>=0)return p/100}
  if(hasBonusValue(u.transactionBonusRate)){const r=Number(u.transactionBonusRate);if(Number.isFinite(r)&&r>=0)return r}
  if(isDailyUser(u)){
    const hasDailyRate=hasBonusValue(u.dailyBonusRate), hasDailyPercent=hasBonusValue(u.dailyBonusPercent);
    if(hasDailyRate){const r=Number(u.dailyBonusRate);if(Number.isFinite(r)&&r>0)return r}
    if(hasDailyPercent){const p=Number(u.dailyBonusPercent);if(Number.isFinite(p)&&p>=0)return p/100}
    if(hasDailyRate){const r=Number(u.dailyBonusRate);if(Number.isFinite(r)&&r>=0)return r}
  }
  return globalTransactionBonusRate();
}
function userTransactionBonusPercent(user=state.user){return Number((userTransactionBonusRate(user)*100).toFixed(3))}
function userClosingBonusPerMinute(user=state.user){
  const u=user||{};
  if(isDailyUser(u))return 0;
  if(hasBonusValue(u.closingBonusPerMinute)){const n=Number(u.closingBonusPerMinute);if(Number.isFinite(n)&&n>=0)return n}
  return globalClosingBonusPerMinute();
}
function txBonusRate(t){
  if(t){
    if(hasBonusValue(t.bonusRate)){const r=Number(t.bonusRate);if(Number.isFinite(r)&&r>=0)return r}
    if(hasBonusValue(t.transactionBonusRate)){const r=Number(t.transactionBonusRate);if(Number.isFinite(r)&&r>=0)return r}
    if(hasBonusValue(t.bonusPercent)){const p=Number(t.bonusPercent);if(Number.isFinite(p)&&p>=0)return p/100}
    if(hasBonusValue(t.transactionBonusPercent)){const p=Number(t.transactionBonusPercent);if(Number.isFinite(p)&&p>=0)return p/100}
  }
  return userTransactionBonusRate();
}
function txBonusValue(t){return Math.round(Number(t?.amount||0)*txBonusRate(t))}
function txBonusSum(list){return (list||[]).reduce((sum,t)=>sum+txBonusValue(t),0)}
function roleLabel(){return isDaily()?'Karyawan Harian':'Karyawan Staff'}
function roleCard(){const trial=isTrialUser(),free=isAttendanceFreeUser(),lock=free?null:missedAttendanceLockForDate();const locked=trial?false:(!!lock||isClosedToday()||(!free&&!todayAtt()));const label=locked?'Terkunci':'Terbuka';const cls=trial?'amber':(locked?'red':'green');const note=trial?'mode trial siap transaksi':(lock?`tidak absen ${dateID(lock.missedDate)}`:(isClosedToday()?'sudah closing':(isRismaSpecialUser()?'risma bebas absen':(isDaily()?'siap transaksi':(todayAtt()?'sudah absen':'belum absen')))));const ico=locked?'!':'OK';return `<div class="role-card ${locked?'locked':'open'}"><div class="between"><div><div class="role-title">Status Transaksi</div><div class="hint" style="margin-top:3px">${note}</div></div><div class="lock-status ${locked?'locked':'open'}"><span class="lock-ico">${ico}</span><span class="pill ${cls}">${label}</span></div></div></div>`}
function headerTxStatus(){if(!state.user)return '';const free=isAttendanceFreeUser();const locked=isTrialUser()?false:(!!missedAttendanceLockForDate()||isClosedToday()||(!free&&!todayAtt()));const label=locked?'Terkunci':'Terbuka';const ico=locked?'!':'OK';return `<span class="trx-head-status ${locked?'locked':'open'}"><span class="lock-ico">${ico}</span>${label}</span>`}
function txDate(t){return String(t.dateKey||(ms(t)?todayKey(new Date(ms(t))):'')).slice(0,10)}
function txMonth(t){return String(t.monthKey||txDate(t).slice(0,7))}
function attDate(a){return String(a.dateKey||(ms(a)?todayKey(new Date(ms(a))):'')).slice(0,10)}
function attendanceDocId(user,dateKey=todayKey()){return 'staffatt_'+key(user)+'_'+String(dateKey||todayKey()).slice(0,10)}
function dedupeAttendanceRows(rows){
  const map=new Map();
  sortDesc((rows||[]).filter(a=>a&&!deleted(a))).forEach(a=>{
    const u=key(a.user||state.user?.username), d=attDate(a), k=u+'_'+d;
    if(!u||!d)return;
    if(!map.has(k))map.set(k,a);
  });
  return sortDesc([...map.values()]);
}
function showLoad(v){state.busy=!!v;$('loading').style.display=v?'flex':'none'}
function toast(msg){const t=$('toast');t.textContent=msg;t.className='toast show';setTimeout(()=>t.className='toast',2100)}
function manualSeenKey(){return MANUAL_BONUS_SEEN+'_'+key(state.user?.username||'guest')}
function getManualSeen(){try{const raw=JSON.parse(localStorage.getItem(manualSeenKey())||'[]');return new Set(Array.isArray(raw)?raw.map(String):[])}catch(e){return new Set()}}
function saveManualSeen(set){try{localStorage.setItem(manualSeenKey(),JSON.stringify([...set].slice(-600)))}catch(e){}}
function manualBonusDate(b){
  const raw=String(b?.dateKey||b?.bonusDate||b?.date||'').slice(0,10);
  if(/^\d{4}-\d{2}-\d{2}$/.test(raw))return raw;
  const n=ms(b);
  // Jangan fallback ke hari ini, karena bonus lama tanpa dateKey bisa ikut muncul lagi saat hari/bulan berganti.
  return n?todayKey(new Date(n)):'';
}
function manualBonusFallbackId(b){return `${key(b?.user)}_${manualBonusDate(b)}_${Number(b?.amount||0)}_${ms(b)||''}`}
function manualBonusId(b){return String(b?.id||b?.docId||b?.clientId||manualBonusFallbackId(b))}
function manualBonusTargetUsername(b){
  return key(b?.targetUsername||b?.user||b?.username||b?.staffUsername||b?.staff||'');
}
function manualBonusIsSeen(b,seen){return seen.has(manualBonusId(b))||seen.has(manualBonusFallbackId(b))}
function markManualBonusesSeen(rows){
  const seen=getManualSeen();
  (rows||[]).forEach(b=>{seen.add(manualBonusId(b));seen.add(manualBonusFallbackId(b));});
  saveManualSeen(seen);
}
function unreadManualBonusRows(){
  if(!state.user)return [];
  const seen=getManualSeen(), d=todayKey();
  return sortDesc((state.data.manual||[]).filter(b=>{
    if(!b||deleted(b)||isTargetAutoBonusRow(b)||manualBonusDate(b)!==d||Number(b.amount||0)<=0||!canReceiveManualBonusRow(b))return false;
    return !manualBonusIsSeen(b,seen);
  }));
}
function canReceiveManualBonusRow(b){
  const current=key(state.user?.username||'');
  const target=manualBonusTargetUsername(b);
  return !!current&&!!target&&target===current;
}
function dismissManualBonusNotice(){
  markManualBonusesSeen(unreadManualBonusRows());
  closeModal();
  render();
}
function manualBonusDisplayNote(value,fallback=''){
  const note=String(value||fallback||'').trim();
  return /bonus\s*manual/i.test(note)?'':note;
}
function manualBonusNoticeCard(){
  const rows=unreadManualBonusRows();
  if(!rows.length)return '';
  const latest=rows[0];
  const total=Number(latest.amount||0);
  const note=manualBonusDisplayNote(latest.note||latest.description||latest.reason,'Bonus dari Mimin')||'Bonus dari Mimin';
  return `<div class="card manual-bonus-alert"><div class="between"><div style="display:flex;align-items:center;gap:9px;min-width:0"><span class="manual-bonus-ico">🎁</span><div style="min-width:0"><div class="label">Ada Bonus Masuk Nih</div><div class="stat-val">Rp ${rp(total)}</div><div class="hint" style="margin-top:2px">${esc(note)}</div></div></div><button class="btn success" onclick="dismissManualBonusNotice()">OK</button></div></div>`;
}
function notifyNewManualBonuses(){
  const rows=unreadManualBonusRows();
  if(!rows.length)return;
  const latest=rows[0];
  const total=Number(latest.amount||0);
  const note=manualBonusDisplayNote(latest.note||latest.description||latest.reason,'');
  // Tandai semua yang sudah kebaca supaya bonus lama hari ini tidak ikut ke popup berikutnya.
  markManualBonusesSeen(rows);
  render();
  try{if(navigator.vibrate)navigator.vibrate([80,40,80,40,110])}catch(e){}
  playBonusSound();
  toast(`Asik dapet bonus Rp ${rp(total)}`);
  showManualBonusParty(total,note,1);
}

function targetDailyDocId(dateKey=todayKey()){const d=String(dateKey||todayKey()).slice(0,10);return isTrialUser()?`trial_daily_target_${d}_${key(state.user?.username)}`:`daily_target_${d}`}
function targetBonusDocId(dateKey,username){const d=String(dateKey||todayKey()).slice(0,10),u=key(username);return isTrialUser()?`trial_targetbonus_${d}_${u}`:`targetbonus_${d}_${u}`}
function targetRewardDocId(dateKey,username){const d=String(dateKey||todayKey()).slice(0,10),u=key(username);return isTrialUser()?`trial_targetreward_${d}_${u}`:`targetreward_${d}_${u}`}
function targetNotificationDocId(dateKey=todayKey()){const d=String(dateKey||todayKey()).slice(0,10);return isTrialUser()?`trial_targetnotif_${d}_${key(state.user?.username)}`:`targetnotif_${d}`}
function targetNoticeSeenKey(){return TARGET_NOTICE_SEEN+'_'+key(state.user?.username||'guest')}
function getTargetNoticeSeen(){try{const raw=JSON.parse(localStorage.getItem(targetNoticeSeenKey())||'[]');return new Set(Array.isArray(raw)?raw.map(String):[])}catch(e){return new Set()}}
function saveTargetNoticeSeen(set){try{localStorage.setItem(targetNoticeSeenKey(),JSON.stringify([...set].slice(-180)))}catch(e){}}
function targetNoticeId(s=state.data.dailyTarget||{}){
  const d=String(s.dateKey||todayKey()).slice(0,10), u=key(state.user?.username||'');
  return `target_reached_${d}_${u}_${Number(s.targetAmount||0)}`;
}
function currentUserTargetRewarded(s=state.data.dailyTarget||{}){
  const u=key(state.user?.username||'');
  if(!u||isDailyUser(state.user))return false;
  const d=String(s.dateKey||todayKey()).slice(0,10);
  const savedRewarded=Array.isArray(s.rewardedUsers)?s.rewardedUsers.map(key):[];
  if(savedRewarded.includes(u))return true;
  return dailyTargetRewardPlan(d).rewardedUsers.map(key).includes(u);
}
function unreadTargetNotice(){
  if(!state.user)return null;
  const s=syncDailyTargetState();
  if(String(s.dateKey||'').slice(0,10)!==todayKey()||!s.reached||!currentUserTargetRewarded(s))return null;
  const id=targetNoticeId(s);
  if(getTargetNoticeSeen().has(id))return null;
  return{id,dateKey:s.dateKey,targetAmount:Number(s.targetAmount||0),totalAmount:Number(s.totalAmount||0),bonusAmount:Number(s.bonusAmount||0)};
}
function markTargetNoticeSeen(notice=unreadTargetNotice()){
  if(!notice)return;
  const seen=getTargetNoticeSeen();
  seen.add(notice.id);
  saveTargetNoticeSeen(seen);
}
function dismissTargetNotice(){
  markTargetNoticeSeen();
  closeModal();
  render();
}
function targetReachedNoticeCard(){
  const n=unreadTargetNotice();
  if(!n)return '';
  return `<div class="card target-reached-alert"><div class="between"><div style="display:flex;align-items:center;gap:9px;min-width:0"><span class="target-reached-ico">✓</span><div style="min-width:0"><div class="label">Target Hari Ini Tercapai</div><div class="stat-val">Rp ${rp(n.bonusAmount)}</div><div class="hint" style="margin-top:2px">Bonus target otomatis masuk.</div></div></div><button class="btn success" onclick="dismissTargetNotice()">OK</button></div></div>`;
}
function showTargetReachedParty(notice){
  document.querySelectorAll('.manual-bonus-party').forEach(el=>el.remove());
  const wrap=document.createElement('div');
  wrap.className='manual-bonus-party';
  const pieces=Array.from({length:34},()=>`<i style="--x:${Math.round(Math.random()*250-125)}px;--y:${Math.round(Math.random()*-190-45)}px;--r:${Math.round(Math.random()*720-360)}deg;--d:${(Math.random()*.20).toFixed(2)}s"></i>`).join('');
  wrap.innerHTML=`<div class="manual-bonus-confetti">${pieces}</div><div class="manual-bonus-box"><div class="manual-bonus-emoji">✓</div><div class="manual-bonus-title">TARGET HARI INI TERCAPAI</div><div class="manual-bonus-sub">Bonus target otomatis masuk</div><div class="manual-bonus-amount">Rp ${rp(notice.bonusAmount||0)}</div><button class="manual-bonus-close" onclick="closeManualBonusParty()" type="button">Tutup</button></div>`;
  (document.querySelector('.app')||document.body).appendChild(wrap);
  requestAnimationFrame(()=>wrap.classList.add('show'));
}
function notifyTargetReachedInApp(){
  const notice=unreadTargetNotice();
  if(!notice||targetNoticeAnnounced.has(notice.id))return;
  targetNoticeAnnounced.add(notice.id);
  render();
  try{if(navigator.vibrate)navigator.vibrate([90,45,90,45,130])}catch(e){}
  playBonusSound();
  toast('Target hari ini tercapai');
  showTargetReachedParty(notice);
}
function targetNumberSetting(data,keys,fallback){
  for(const k of keys){
    const n=Number(data?.[k]);
    if(Number.isFinite(n)&&n>=0)return n;
  }
  return fallback;
}
function normalizeDailyTargetSettings(data={}){
  return{
    targetAmount:targetNumberSetting(data,['targetAmount','dailyTargetAmount','dailyOmzetTarget','omzetTarget'],DAILY_TARGET_AMOUNT),
    bonusAmount:targetNumberSetting(data,['bonusAmount','bonusTargetAmount','dailyTargetBonusAmount','targetBonusAmount','BONUS_TARGET_AMOUNT'],BONUS_TARGET_AMOUNT)
  };
}
function dailyTargetSettings(){
  const settings=normalizeDailyTargetSettings(state.data.targetSettings||{});
  if(isTrialUser()){
    const target=Number(state.user?.trialTargetAmount||0), bonus=Number(state.user?.trialBonusAmount||0);
    return{targetAmount:target>0?target:10000,bonusAmount:bonus>=0?bonus:1000};
  }
  return settings;
}
function targetUserKey(user){return key(user?.username||user?.id||user?.user||'')}
function isTargetDailyRole(user){const r=key(user?.role);return r==='harian'||r==='daily'||r==='karyawan_harian'}
function activeTargetUsers(){
  const rows=(state.data.targetUsers||[])
    .map(u=>({id:u.id||targetUserKey(u),username:targetUserKey(u),name:u.name||targetUserKey(u),role:u.role||'staff',...u}))
    .filter(u=>u.username&&u.active!==false&&!deleted(u)&&!isAdmin(u));
  if(isTrialUser()){
    const current={id:key(state.user?.username),username:key(state.user?.username),name:state.user?.name||key(state.user?.username),role:state.user?.role||'staff',...state.user};
    const trialRows=rows.filter(u=>isTrialUser(u)&&u.username===key(state.user?.username));
    return trialRows.length?trialRows:[current];
  }
  return rows.filter(u=>!isTrialUser(u));
}
function dailyTargetTransactions(dateKey=todayKey()){
  const d=String(dateKey||todayKey()).slice(0,10);
  const active=activeTargetUsers(), activeSet=new Set(active.map(u=>u.username));
  return sortDesc((state.data.targetTx||[]).filter(t=>{
    if(!t||deleted(t)||txDate(t)!==d)return false;
    const user=key(t.user);
    if(isTrialUser())return isTrialRecord(t)&&user===key(state.user?.username);
    return !isTrialRecord(t)&&(!activeSet.size||activeSet.has(user));
  }));
}
function dailyTargetSummary(dateKey=todayKey()){
  const d=String(dateKey||todayKey()).slice(0,10), settings=dailyTargetSettings(), targetAmount=Math.max(1,Number(settings.targetAmount||DAILY_TARGET_AMOUNT));
  const rows=dailyTargetTransactions(d);
  const totalAmount=rows.reduce((sum,t)=>sum+Number(t.amount||0),0);
  const rawPercent=(totalAmount/targetAmount)*100;
  const progressPercent=Number.isFinite(rawPercent)?Number(rawPercent.toFixed(2)):0;
  return{dateKey:d,targetAmount,totalAmount,progressPercent,reached:totalAmount>=targetAmount,remainingAmount:Math.max(0,targetAmount-totalAmount),bonusAmount:Number(settings.bonusAmount||0)};
}
function targetAttendanceUsers(dateKey=todayKey()){
  const d=String(dateKey||todayKey()).slice(0,10), set=new Set();
  if(isTrialUser()){
    const u=key(state.user?.username);
    if(u)set.add(u);
    return set;
  }
  dedupeAttendanceRows(state.data.targetAtt||[]).forEach(a=>{if(!deleted(a)&&attDate(a)===d){if(isTrialUser()){if(isTrialRecord(a)&&key(a.user)===key(state.user?.username))set.add(key(a.user))}else if(!isTrialRecord(a))set.add(key(a.user))}});
  return set;
}
function targetTransactionUsers(dateKey=todayKey()){
  const set=new Set();
  dailyTargetTransactions(dateKey).forEach(t=>{const u=key(t.user);if(u)set.add(u)});
  return set;
}
function dailyTargetRewardPlan(dateKey=todayKey()){
  const d=String(dateKey||todayKey()).slice(0,10), attendance=targetAttendanceUsers(d), users=activeTargetUsers();
  const activeUsers=users.map(u=>u.username);
  const txUsers=targetTransactionUsers(d);
  const rewardedUsers=users.filter(u=>u.username==='risma'?txUsers.has('risma'):(!isTargetDailyRole(u)&&attendance.has(u.username))).map(u=>u.username);
  const userMap=new Map(users.map(u=>[u.username,u]));
  return{activeUsers,rewardedUsers,users:rewardedUsers.map(u=>userMap.get(u)).filter(Boolean)};
}
function syncDailyTargetState(extra={}){
  const summary=dailyTargetSummary();
  const current=state.data.dailyTarget||defaultDailyTargetState(summary.dateKey);
  state.data.dailyTarget={...defaultDailyTargetState(summary.dateKey),...current,...extra,...summary,updatedAtMs:Date.now()};
  return state.data.dailyTarget;
}
async function getDailyTargetSettingsFromServer(){
  const ids=['daily_target','__daily_target','default'];
  for(const id of ids){
    try{
      const snap=await getDocFromServer(doc(db,TARGET_SETTINGS_TABLE,id));
      if(snap.exists())return normalizeDailyTargetSettings(snap.data()||{});
    }catch(e){
      if(id==='daily_target')console.warn('targetSettings belum terbaca',e?.code||e?.message||e);
      break;
    }
  }
  return dailyTargetSettings();
}
async function loadDailyTargetData({apply=true}={}){
  const d=todayKey();
  try{
    const [txSnap,userSnap,attSnap,targetSnap,settings]=await Promise.all([
      getDocs(query(collection(db,'transactions'),where('dateKey','==',d),limit(LIMITS.targetTxToday))).catch(()=>querySnapshot([])),
      getDocs(query(collection(db,'users'),limit(LIMITS.targetUsers))).catch(()=>querySnapshot([])),
      getDocs(query(collection(db,'attendance'),where('dateKey','==',d),limit(LIMITS.targetAttToday))).catch(()=>querySnapshot([])),
      getDocFromServer(doc(db,DAILY_TARGETS_TABLE,targetDailyDocId(d))).catch(()=>null),
      getDailyTargetSettingsFromServer()
    ]);
    state.data.targetTx=sortDesc(txSnap.docs.map(x=>({id:x.id,...x.data()})));
    state.data.targetUsers=userSnap.docs.map(x=>({id:x.id,username:key((x.data()||{}).username||x.id),...x.data()}));
    state.data.targetAtt=dedupeAttendanceRows(attSnap.docs.map(x=>({id:x.id,...x.data()})));
    state.data.targetSettings=settings;
    const serverTarget=targetSnap?.exists()?targetSnap.data():{};
    syncDailyTargetState(serverTarget||{});
    if(apply)scheduleDailyTargetCheck();
  }catch(e){
    console.warn('target harian gagal dimuat',e?.code||e?.message||e);
    syncDailyTargetState();
  }
}
async function saveDailyTargetStatus(extra={}){
  const summary=dailyTargetSummary(), plan=dailyTargetRewardPlan(summary.dateKey), current=state.data.dailyTarget||{};
  const reachedAtMs=summary.reached?Number(current.reachedAtMs||Date.now()):0;
  const payload={
    dateKey:summary.dateKey,
    targetAmount:summary.targetAmount,
    totalAmount:summary.totalAmount,
    progressPercent:summary.progressPercent,
    reached:summary.reached,
    reachedAt:summary.reached?(current.reachedAt||serverTimestamp()):null,
    reachedAtMs,
    bonusApplied:summary.reached?Boolean(extra.bonusApplied??current.bonusApplied):false,
    bonusAppliedAt:extra.bonusApplied?(current.bonusAppliedAt||serverTimestamp()):(current.bonusAppliedAt||null),
    bonusAppliedAtMs:extra.bonusApplied?Number(current.bonusAppliedAtMs||Date.now()):Number(current.bonusAppliedAtMs||0),
    activeUsers:extra.activeUsers||plan.activeUsers,
    rewardedUsers:extra.rewardedUsers||current.rewardedUsers||[],
    updatedAt:serverTimestamp(),
    updatedAtMs:Date.now()
  };
  state.data.dailyTarget={...defaultDailyTargetState(summary.dateKey),...current,...payload};
  try{await setDoc(doc(db,DAILY_TARGETS_TABLE,targetDailyDocId(summary.dateKey)),payload,{merge:true})}
  catch(e){console.warn('dailyTargets gagal disimpan',e?.code||e?.message||e)}
  return state.data.dailyTarget;
}
async function insertDocIfAbsent(table,id,payload){
  try{
    const {error}=await supabase.from(table).insert({id,data:deepCloneCompat(payload||{})});
    if(error){
      if(error.code==='23505'||/duplicate/i.test(error.message||''))return false;
      throw error;
    }
    return true;
  }catch(e){
    console.warn(`${table} insert dilewati`,e?.code||e?.message||e);
    return false;
  }
}
function notifyTargetAchievedOnce(summary,plan){
  try{
    return fetch(ROCKY_ADMIN_NOTIFY_TARGET_URL,{method:'POST',headers:{'Content-Type':'application/json','X-Notify-Secret':ROCKY_ADMIN_NOTIFY_SECRET},body:JSON.stringify({secret:ROCKY_ADMIN_NOTIFY_SECRET,dateKey:summary.dateKey,targetAmount:summary.targetAmount,totalAmount:summary.totalAmount,bonusAmount:summary.bonusAmount,activeUsers:plan.activeUsers,rewardedUsers:plan.rewardedUsers})}).catch(e=>console.warn('Notif target gagal',e?.message||e));
  }catch(e){
    console.warn('Notif target gagal',e?.message||e);
    return Promise.resolve(null);
  }
}
async function notifyStaffTargetBonus(row={},user={}){
  try{
    const username=key(row.user||row.username||user.username||'');
    const amount=Number(row.amount||0);
    if(!username||amount<=0)return null;
    const role=String(row.userRole||row.role||user.role||'staff');
    const res=await fetch(ROCKY_STAFF_NOTIFY_MANUAL_BONUS_URL,{method:'POST',headers:{'Content-Type':'application/json','X-Notify-Secret':ROCKY_ADMIN_NOTIFY_SECRET},body:JSON.stringify({secret:ROCKY_ADMIN_NOTIFY_SECRET,targetUsername:username,username,user:username,name:row.name||user.name||username,amount,note:row.note||'Bonus target omzet harian tercapai',action:'add',dateKey:String(row.dateKey||''),monthKey:String(row.monthKey||''),bonusId:String(row.id||row.docId||''),type:'daily_target_bonus',source:'daily_target',role,userRole:role,actualRole:role,bonusGroup:'staff',isDaily:false,dailyMode:false,actualDaily:false,canReceiveStaffNotifications:true,notificationAudience:'staff',createdByName:'Target Otomatis Rocky'})});
    const data=await res.json().catch(()=>null);
    if(!res.ok||data?.ok===false){
      console.warn('Notif bonus target staff gagal',data||res.status);
      return null;
    }
    return data||{ok:true};
  }catch(e){
    console.warn('Notif bonus target staff gagal',e?.message||e);
    return null;
  }
}
function targetBonusPayload(summary,user,amount){
  const username=targetUserKey(user);
  return{
    user:username,
    targetUsername:username,
    name:user.name||username,
    userRole:user.role||'staff',
    role:user.role||'staff',
    bonusGroup:'staff',
    amount,
    dateKey:summary.dateKey,
    monthKey:summary.dateKey.slice(0,7),
    type:'daily_target_bonus',
    source:'daily_target',
    note:'Bonus target omzet harian tercapai',
    description:'Bonus otomatis karena target omzet harian gabungan tercapai',
    action:'add',
    ...trialRecordFlags(user),
    deleted:false,
    createdAt:serverTimestamp(),
    createdAtMs:Date.now()
  };
}
async function ensureStaffTargetBonusNotification(bonusId,rewardId,row,user,options={}){
  try{
    const force=options.force===true;
    const rewardSnap=await getDocFromServer(doc(db,TARGET_BONUS_REWARDS_TABLE,rewardId)).catch(()=>null);
    const reward=rewardSnap?.exists()?rewardSnap.data()||{}:{};
    if(!force&&(reward.staffNotificationSent===true||Number(reward.staffNotifiedAtMs||0)>0))return false;
    const result=await notifyStaffTargetBonus({id:bonusId,docId:bonusId,...row},user);
    if(!result)return false;
    await setDoc(doc(db,TARGET_BONUS_REWARDS_TABLE,rewardId),{
      staffNotificationSent:true,
      staffNotificationStatus:'sent',
      staffNotifiedAt:serverTimestamp(),
      staffNotifiedAtMs:Date.now()
    },{merge:true}).catch(e=>console.warn('target reward notif flag gagal',e?.code||e?.message||e));
    return true;
  }catch(e){
    console.warn('ensure notif bonus target gagal',e?.code||e?.message||e);
    return false;
  }
}
async function ensureDailyTargetBonusRows(summary,plan,amount){
  const rewarded=[];
  if(Number(amount||0)<=0)return rewarded;
  for(const user of plan.users||[]){
    const username=targetUserKey(user);
    if(!username)continue;
    const bonusId=targetBonusDocId(summary.dateKey,username), rewardId=targetRewardDocId(summary.dateKey,username);
    const existingBonus=await getDocFromServer(doc(db,'manualBonuses',bonusId)).catch(()=>null);
    const existingData=existingBonus?.exists()?existingBonus.data()||{}:null;
    const existingActive=!!existingData&&!deleted(existingData);
    const payload=targetBonusPayload(summary,user,amount);
    if(!existingActive){
      await setDoc(doc(db,'manualBonuses',bonusId),payload,{merge:false});
      if(username===key(state.user?.username)){
        state.data.manual=mergeRowsById(state.data.manual,[{id:bonusId,docId:bonusId,...payload}]).filter(b=>!deleted(b));
      }
    }
    rewarded.push(username);
    const notified=await ensureStaffTargetBonusNotification(bonusId,rewardId,existingActive?{...payload,...existingData,id:bonusId,docId:bonusId}:payload,user,{force:!existingActive});
    await setDoc(doc(db,TARGET_BONUS_REWARDS_TABLE,rewardId),{
      id:rewardId,
      dateKey:summary.dateKey,
      user:username,
      targetUsername:username,
      amount,
      manualBonusId:bonusId,
      type:'daily_target_bonus',
      source:'daily_target',
      rewardedAt:serverTimestamp(),
      rewardedAtMs:Date.now(),
      ...(notified?{staffNotificationSent:true,staffNotificationStatus:'sent',staffNotifiedAt:serverTimestamp(),staffNotifiedAtMs:Date.now()}: {})
    },{merge:true}).catch(e=>console.warn('targetBonusRewards gagal',e?.code||e?.message||e));
  }
  return rewarded;
}
function mergeTargetRewardedUsers(...groups){
  const set=new Set();
  groups.flat().forEach(u=>{const k=key(u);if(k)set.add(k)});
  return [...set];
}
function isTargetAutoBonusRow(row={}){
  return String(row.source||'')==='daily_target'||String(row.type||'')==='daily_target_bonus'||String(row.id||row.docId||'').startsWith('targetbonus_')||String(row.id||row.docId||'').startsWith('trial_targetbonus_');
}
async function revokeDailyTargetBonuses(summary,plan,previous={}){
  const d=String(summary?.dateKey||todayKey()).slice(0,10), nowMs=Date.now();
  const users=mergeTargetRewardedUsers(plan?.rewardedUsers||[],previous?.rewardedUsers||[],state.data.dailyTarget?.rewardedUsers||[],(plan?.users||[]).map(u=>u?.username||u?.id));
  for(const username of users){
    const bonusId=targetBonusDocId(d,username), rewardId=targetRewardDocId(d,username);
    try{
      const snap=await getDocFromServer(doc(db,'manualBonuses',bonusId)).catch(()=>null);
      const row=snap?.exists()?{id:bonusId,...(snap.data()||{})}:null;
      if(row&&!deleted(row)&&isTargetAutoBonusRow(row)){
        await setDoc(doc(db,'manualBonuses',bonusId),{deleted:true,status:'deleted',deletedAt:serverTimestamp(),deletedAtMs:nowMs,deletedBy:'target_auto',deletedByName:'Target Otomatis Rocky',deleteReason:'Omzet turun di bawah target harian',updatedAt:serverTimestamp(),updatedAtMs:nowMs},{merge:true});
        state.data.manual=state.data.manual.map(b=>String(b.id||b.docId)===bonusId?{...b,deleted:true,status:'deleted',deletedAtMs:nowMs}:b).filter(b=>!deleted(b));
      }
      await setDoc(doc(db,TARGET_BONUS_REWARDS_TABLE,rewardId),{deleted:true,resetAt:serverTimestamp(),resetAtMs:nowMs,resetBy:'target_auto',resetByName:'Target Otomatis Rocky',resetReason:'Omzet turun di bawah target harian',staffNotificationSent:false,staffNotificationStatus:'reset',staffNotifiedAtMs:0,updatedAt:serverTimestamp(),updatedAtMs:nowMs},{merge:true}).catch(()=>{});
    }catch(e){
      console.warn('rollback bonus target gagal',username,e?.code||e?.message||e);
    }
  }
  await deleteDoc(doc(db,TARGET_NOTIFICATIONS_TABLE,targetNotificationDocId(d))).catch(()=>{});
  state.data.targetNotification=null;
  await saveDailyTargetStatus({bonusApplied:false,rewardedUsers:[]});
}
async function ensureStaffTargetBonusNotifications(summary,plan,amount){
  if(Number(amount||0)<=0)return [];
  const sent=[];
  for(const user of plan.users||[]){
    const username=targetUserKey(user);
    if(!username)continue;
    const bonusId=targetBonusDocId(summary.dateKey,username), rewardId=targetRewardDocId(summary.dateKey,username);
    const fallback=targetBonusPayload(summary,user,amount);
    const existingBonus=await getDocFromServer(doc(db,'manualBonuses',bonusId)).catch(()=>null);
    const row=existingBonus?.exists()?{...fallback,...(existingBonus.data()||{})}:fallback;
    const ok=await ensureStaffTargetBonusNotification(bonusId,rewardId,row,user);
    if(ok)sent.push(username);
  }
  return sent;
}
async function ensureTargetNotification(summary,plan){
  const id=targetNotificationDocId(summary.dateKey), payload={
    dateKey:summary.dateKey,
    type:'daily_target_reached',
    title:'Target omzet harian tercapai',
    message:`Target omzet harian sudah tercapai. Bonus target Rp ${rp(summary.bonusAmount)} otomatis masuk.`,
    targetAmount:summary.targetAmount,
    totalAmount:summary.totalAmount,
    bonusAmount:summary.bonusAmount,
    reachedAt:serverTimestamp(),
    reachedAtMs:Date.now(),
    read:false,
    createdAt:serverTimestamp(),
    createdAtMs:Date.now()
  };
  const inserted=await insertDocIfAbsent(TARGET_NOTIFICATIONS_TABLE,id,payload);
  if(inserted)state.data.targetNotification={id,...payload};
  if(inserted&&!isTrialUser())await notifyTargetAchievedOnce(summary,plan);
}
async function applyDailyTargetBonus(){
  if(dailyTargetApplying||!state.user)return;
  dailyTargetApplying=true;
  try{
    const summary=dailyTargetSummary();
    if(!summary.reached){
      const plan=dailyTargetRewardPlan(summary.dateKey);
      const latest=await getDocFromServer(doc(db,DAILY_TARGETS_TABLE,targetDailyDocId(summary.dateKey))).catch(()=>null);
      const latestData=latest?.exists()?latest.data():state.data.dailyTarget||{};
      await revokeDailyTargetBonuses(summary,plan,latestData);
      return;
    }
    const latest=await getDocFromServer(doc(db,DAILY_TARGETS_TABLE,targetDailyDocId(summary.dateKey))).catch(()=>null);
    const latestData=latest?.exists()?latest.data():state.data.dailyTarget||{};
    if(latest?.exists())state.data.dailyTarget={...state.data.dailyTarget,...latestData};
    const plan=dailyTargetRewardPlan(summary.dateKey), amount=Number(summary.bonusAmount||0);
    if(latestData?.bonusApplied===true){
      const rewarded=await ensureDailyTargetBonusRows(summary,plan,amount);
      await saveDailyTargetStatus({bonusApplied:true,rewardedUsers:mergeTargetRewardedUsers(latestData.rewardedUsers||[],rewarded)});
      return;
    }
    await saveDailyTargetStatus({});
    if(amount<=0){
      await saveDailyTargetStatus({activeUsers:plan.activeUsers,rewardedUsers:[],bonusApplied:false});
      return;
    }
    const rewarded=await ensureDailyTargetBonusRows(summary,plan,amount);
    await saveDailyTargetStatus({activeUsers:plan.activeUsers,rewardedUsers:rewarded,bonusApplied:true});
    await ensureTargetNotification(summary,{...plan,rewardedUsers:rewarded});
    notifyTargetReachedInApp();
    if(rewarded.includes(key(state.user?.username))){
      render();
      notifyNewManualBonuses();
    }
  }catch(e){
    console.warn('bonus target harian gagal',e?.code||e?.message||e);
  }finally{
    dailyTargetApplying=false;
  }
}
function scheduleDailyTargetCheck(){
  clearTimeout(dailyTargetApplyTimer);
  syncDailyTargetState();
  dailyTargetApplyTimer=setTimeout(()=>applyDailyTargetBonus(),650);
}
function dailyTargetCard(){
  const s=syncDailyTargetState(), pct=Math.max(0,Math.min(100,s.progressPercent||0)), pctText=(Math.round((s.progressPercent||0)*10)/10).toLocaleString('id-ID'), reached=!!s.reached;
  return `<div class="daily-target-line ${reached?'reached':''}"><div class="daily-target-progress"><span style="width:${pct}%"></span></div><span class="daily-target-percent">${pctText}%</span></div>`;
}
function trialModeCard(){
  if(!isTrialUser())return '';
  return `<div class="card trial-mode-card"><div class="between"><div><div class="label">Mode Trial / Akun Dummy</div><div class="hint" style="margin-top:3px">Data percobaan tidak masuk laporan utama.</div></div><span class="pill amber">TRIAL</span></div></div>`;
}


function firstTxPartyKey(){return `${FIRST_TX_PARTY_SEEN}_${key(state.user?.username||'guest')}_${todayKey()}`}
function isFirstTxPartyDue(){
  if(!state.user)return false;
  try{if(localStorage.getItem(firstTxPartyKey())==='1')return false}catch(e){}
  return todayTx().length===0;
}
function markFirstTxPartySeen(){try{localStorage.setItem(firstTxPartyKey(),'1')}catch(e){}}
function closeFirstTxParty(){
  const wrap=document.querySelector('.first-tx-party');
  if(!wrap)return;
  wrap.classList.remove('show');
  setTimeout(()=>wrap.remove(),220);
}
function showFirstTxParty(amount,note){
  markFirstTxPartySeen();
  try{if(navigator.vibrate)navigator.vibrate([70,35,90,35,120])}catch(e){}
  document.querySelectorAll('.first-tx-party').forEach(el=>el.remove());
  const wrap=document.createElement('div');
  wrap.className='first-tx-party';
  const pieces=Array.from({length:30},()=>`<i style="--x:${Math.round(Math.random()*240-120)}px;--y:${Math.round(Math.random()*-180-45)}px;--r:${Math.round(Math.random()*720-360)}deg;--d:${(Math.random()*.18).toFixed(2)}s"></i>`).join('');
  wrap.innerHTML=`<div class="first-tx-confetti">${pieces}</div><div class="first-tx-box"><div class="first-tx-emoji">🎉</div><div class="first-tx-title">Transaksi Pertama Hari Ini!</div><div class="first-tx-sub">الحمد لله</div><div class="first-tx-amount">Rp ${rp(amount)}</div><div class="first-tx-sub">${esc(note||'Transaksi')}</div><button class="first-tx-close" onclick="closeFirstTxParty()" type="button">Tutup</button></div>`;
  (document.querySelector('.app')||document.body).appendChild(wrap);
  requestAnimationFrame(()=>wrap.classList.add('show'));
}

function closeManualBonusParty(){
  const wrap=document.querySelector('.manual-bonus-party');
  if(!wrap)return;
  wrap.classList.remove('show');
  setTimeout(()=>wrap.remove(),220);
}
function showManualBonusParty(amount,note,count=1,options={}){
  document.querySelectorAll('.manual-bonus-party').forEach(el=>el.remove());
  const wrap=document.createElement('div');
  wrap.className='manual-bonus-party';
  const pieces=Array.from({length:34},()=>`<i style="--x:${Math.round(Math.random()*250-125)}px;--y:${Math.round(Math.random()*-190-45)}px;--r:${Math.round(Math.random()*720-360)}deg;--d:${(Math.random()*.20).toFixed(2)}s"></i>`).join('');
  const onlyAmount=options?.amountOnly===true;
  const noteText=manualBonusDisplayNote(note,'');
  const noteHtml=noteText?`<div class="manual-bonus-note">${esc(noteText)}</div>`:'';
  wrap.innerHTML=onlyAmount
    ? `<div class="manual-bonus-confetti">${pieces}</div><div class="manual-bonus-box"><div class="manual-bonus-amount">Rp ${rp(amount)}</div><button class="manual-bonus-close" onclick="closeManualBonusParty()" type="button">Tutup</button></div>`
    : `<div class="manual-bonus-confetti">${pieces}</div><div class="manual-bonus-box"><div class="manual-bonus-emoji">🎁</div><div class="manual-bonus-title">BONUS DARI MIMIN</div><div class="manual-bonus-sub">الحمد لله</div><div class="manual-bonus-amount">Rp ${rp(amount)}</div>${noteHtml}<button class="manual-bonus-close" onclick="closeManualBonusParty()" type="button">Tutup</button></div>`;
  (document.querySelector('.app')||document.body).appendChild(wrap);
  requestAnimationFrame(()=>wrap.classList.add('show'));
}

const SUCCESS_SOUND_SRC='./success.mp3';
const BONUS_SOUND_SRC='./notifkasi_bonus_masuk.mp3';
let successAudio=null;
let bonusAudio=null;
function primeSuccessSound(){try{if(!successAudio){successAudio=new Audio(SUCCESS_SOUND_SRC);successAudio.preload='auto';successAudio.volume=0.9;successAudio.load();}}catch(e){}}
function playSuccessSound(){try{primeSuccessSound();if(!successAudio)return;successAudio.pause();successAudio.currentTime=0;const p=successAudio.play();if(p&&p.catch)p.catch(()=>{});}catch(e){}}
function primeBonusSound(){try{if(!bonusAudio){bonusAudio=new Audio(BONUS_SOUND_SRC);bonusAudio.preload='auto';bonusAudio.volume=1;bonusAudio.load();}}catch(e){}}
function playBonusSound(){try{primeBonusSound();if(!bonusAudio)return;bonusAudio.pause();bonusAudio.currentTime=0;const p=bonusAudio.play();if(p&&p.catch)p.catch(()=>{});}catch(e){}}
function primeAppSounds(){primeSuccessSound();primeBonusSound();}
function onlyDigits(v){return String(v||'').replace(/[^0-9]/g,'')}
function formatRupiahInput(el){const n=onlyDigits(el.value);el.value=n?'Rp '+rp(Number(n)):''}
function modal(title,body,actions='',variant=''){
  closeModal(true); // bersihkan modal lama dulu supaya popup transaksi tidak dobel
  const r=$('modal');
  const variantText=String(variant||'').trim();
  const cls=variantText?` ${variantText}`:'';
  const isTxModal=variantText.includes('tx-modal');
  const isPaymentPicker=variantText.includes('payment-picker-modal');
  const wrap=`modal-wrap show${isTxModal?' tx-backdrop':''}${isPaymentPicker?' payment-picker-wrap':''}`;
  if(!r)return;
  r.removeAttribute('style');
  r.innerHTML=`<div class="modal${cls}" role="dialog" aria-modal="true"><div class="modal-head"><div><div class="modal-title">${esc(title)}</div></div><button type="button" class="icon" onpointerdown="closeModal(true);event.preventDefault()" ontouchstart="closeModal(true);event.preventDefault()" onclick="closeModal(true)">×</button></div>${body}${actions?`<div class="modal-actions">${actions}</div>`:''}</div>`;
  r.className=wrap;
}

function closeModal(force=false){
  try{if(typeof stopPrayerAyat==='function')stopPrayerAyat(false)}catch(e){}
  try{if(document.activeElement&&typeof document.activeElement.blur==='function')document.activeElement.blur()}catch(e){}
  const main=$('modal');
  if(main){
    main.className='modal-wrap';
    main.innerHTML='';
    main.style.cssText='';
  }
  document.querySelectorAll('.modal-wrap').forEach((el)=>{
    if(el!==main)el.remove();
  });
  document.querySelectorAll('.modal[role="dialog"]').forEach((el)=>{
    if(!main||!main.contains(el))el.remove();
  });
}
function getTheme(){return localStorage.getItem('rocky_staff_theme')||'light'}
function setTheme(t){document.documentElement.setAttribute('data-theme',t);localStorage.setItem('rocky_staff_theme',t)}
function toggleTheme(){setTheme(getTheme()==='dark'?'light':'dark');render()}
function pendingKey(){return PENDING_KEY+'_'+key(state.user?.username||'guest')}
function getPending(){try{return JSON.parse(localStorage.getItem(pendingKey())||'[]')}catch(e){return []}}
function setPending(list){localStorage.setItem(pendingKey(),JSON.stringify(list||[]))}
function pendingForUser(){const u=key(state.user?.username);return getPending().filter(x=>!u||key(x.user)===u)}
function addPending(item){const list=getPending().filter(x=>x.id!==item.id);list.push({...item,tries:Number(item.tries||0),lastTryMs:item.lastTryMs||0});setPending(list)}
function removePending(id){setPending(getPending().filter(x=>x.id!==id))}
function isPermissionError(e){return String(e?.code||e?.message||'').toLowerCase().includes('permission')}
function localTxFromPending(p){return {id:p.id,...(p.payload||{}),pending:true}}
function localAttFromPending(p){return {id:p.id,...(p.payload||{}),pending:true}}
function localUnlockFromPending(p){return {id:p.id,...(p.payload||{}),pending:true}}
function applyPendingToState(){
  const p=pendingForUser();
  p.forEach(x=>{
    if(x.type==='tx_add'&&!state.data.tx.some(t=>String(t.id)===String(x.id)))state.data.tx.unshift(localTxFromPending(x));
    if(x.type==='att_add'&&!state.data.att.some(a=>String(a.id)===String(x.id)))state.data.att.unshift(localAttFromPending(x));
    if(x.type==='feature_unlock_request'&&!state.data.unlockRequests.some(t=>String(t.id)===String(x.id)))state.data.unlockRequests.unshift(localUnlockFromPending(x));
  });
  state.data.att=dedupeAttendanceRows(state.data.att);
  state.data.unlockRequests=sortDesc(state.data.unlockRequests||[]);
}
function syncTimeText(){if(!state.lastSyncMs)return 'Belum sync';const diff=Math.max(0,Math.round((Date.now()-state.lastSyncMs)/1000));if(diff<60)return 'baru saja';if(diff<3600)return `${Math.floor(diff/60)} menit lalu`;return timeID(state.lastSyncMs)}
function syncBar(){const pc=pendingForUser().length, cls=pc?'syncbar sync-pending':'syncbar';const msg=pc?`<b>${pc} data belum terkirim</b><span> akan dicoba otomatis</span>`:`<span>Sync terakhir: <b>${syncTimeText()}</b></span>`;const err=state.syncError?`<div class="hint" style="color:var(--red);margin-top:2px">${esc(state.syncError)}</div>`:'';return `<div class="${cls}"><div>${msg}${err}</div><button class="btn sm" onclick="retrySync()">Sync</button></div>`}
function syncHeroLine(){const pc=pendingForUser().length;const msg=pc?`<b>${pc} data belum terkirim</b><span> · dicoba otomatis</span>`:`<span>Sync terakhir: <b>${syncTimeText()}</b></span>`;const err=state.syncError?`<div class="hero-sync-error">${esc(state.syncError)}</div>`:'';return `<div class="hero-sync"><div>${msg}${err}</div><button class="hero-sync-btn" onclick="retrySync()" aria-label="Sync data">Sync</button></div>`}
function top(title,sub){const icon=getTheme()==='dark'?'☀':'☾',back=canAppBack()?'<button class="top-back-btn" onclick="appBack()" aria-label="Kembali">←</button>':'';return `<div class="top">${back}<div class="brand"><div class="title">${esc(title)}</div><div class="sub">${esc(sub||state.user?.name||'Staff')}</div></div><div class="row"><a class="btn sm member" href="${MEMBER_URL}" target="_blank" rel="noopener">Member</a>${headerTxStatus()}<button class="btn sm" onclick="refresh()">Refresh</button><button class="btn sm" onclick="toggleTheme()">${icon}</button><button class="btn sm danger" onclick="logout()">Keluar</button></div></div>`}
function headerGuideNoteText(){
  const note=String(state.data.headerGuideNote||'').trim();
  return note||DEFAULT_HEADER_GUIDE_NOTE;
}
function staffDailyHomeNoteData(){
  const raw=state.data.staffDailyNote||{};
  const enabled=raw.enabled!==false;
  const note=String(raw.note||'').trim();
  return {enabled,note:note||DEFAULT_STAFF_DAILY_NOTE,updatedAtMs:Number(raw.updatedAtMs||0),updatedByName:raw.updatedByName||raw.updatedBy||''};
}
function staffDailyNoteRoleLabel(){return isDaily()?'Karyawan Harian':'Staff'}
function staffDailyNoteCard(){
  if(!state.user)return '';
  const data=staffDailyHomeNoteData();
  if(!data.enabled||!data.note)return '';
  const updated=data.updatedAtMs?`Update ${dateID(todayKey(new Date(data.updatedAtMs)))} · ${timeID(data.updatedAtMs)}`:'Catatan dari admin';
  const by=data.updatedAtMs?' · Pesan dari Markas':'';
  return `<div class="card staff-home-note-card"><div class="staff-home-note-main"><span class="staff-home-note-ico">📌</span><div style="min-width:0;flex:1"><div class="staff-home-note-title">PENTING</div><div class="staff-home-note-text">${linkText(data.note)}</div><div class="staff-home-note-meta">${esc(updated+by)}</div></div></div></div>`;
}
function headerGuideItems(){
  const themeIcon=getTheme()==='dark'?'☀':'☾';
  const lock=missedAttendanceLockForDate();
  const free=isAttendanceFreeUser();
  const locked=!!lock||isClosedToday()||(!free&&!todayAtt());
  const lockIcon=locked?'🔒':'🔓';
  const lockText=lock?`Transaksi terkunci karena tidak ada absen ${dateID(lock.missedDate)}.`:(locked?'Transaksi terkunci karena belum absen atau sudah closing.':(isRismaSpecialUser()?'Risma bebas absen dan siap transaksi.':'Transaksi terbuka dan siap digunakan.'));
  return `<div class="header-guide-grid"><div class="header-guide-item"><span class="header-guide-ico">M</span><div><div class="header-guide-title">Member</div><div class="header-guide-desc">Membuka halaman kode khusus member.</div></div></div><div class="header-guide-item"><span class="header-guide-ico">${lockIcon}</span><div><div class="header-guide-title">Status Transaksi</div><div class="header-guide-desc">${lockText}</div></div></div><div class="header-guide-item"><span class="header-guide-ico">↻</span><div><div class="header-guide-title">Refresh</div><div class="header-guide-desc">Muat ulang data, sync pending, dan update bonus terbaru.</div></div></div><div class="header-guide-item"><span class="header-guide-ico">${themeIcon}</span><div><div class="header-guide-title">Tema</div><div class="header-guide-desc">Ganti tampilan gelap atau terang.</div></div></div><div class="header-guide-item"><span class="header-guide-ico">⏻</span><div><div class="header-guide-title">Keluar</div><div class="header-guide-desc">Logout dari akun staff di perangkat ini.</div></div></div><div class="header-guide-item"><span class="header-guide-ico">!</span><div><div class="header-guide-title">Catatan Refresh</div><div class="header-guide-desc">Pakai refresh saat data belum masuk, bonus belum berubah, atau transaksi gagal.</div></div></div></div>`;
}
function openHeaderGuideDetail(){
  modal('Panduan Icon Header',`<div class="header-guide-modal-note">“${esc(headerGuideNoteText())}”</div>${headerGuideItems()}`,`<button class="btn primary" onclick="closeModal()">Tutup</button>`);
}
function headerIconGuide(){
  return `<div class="card header-guide-card is-compact"><div class="between"><div class="header-guide-mini-row"><span class="header-guide-mini-ico">?</span><div style="min-width:0"><div class="label">Panduan Icon Header</div><div class="hint" style="margin-top:2px">Klik buka untuk lihat pesan dan keterangan tombol atas.</div></div></div><button class="header-guide-open-btn" onclick="openHeaderGuideDetail()" aria-label="Buka Panduan Icon Header">Buka</button></div></div>`;
}
function showStaffAbsenFab(){return !!state.user&&state.page==='home'&&!isAttendanceFreeUser()&&!todayAtt()&&!isClosedToday()&&!missedAttendanceLockForDate()}
function nav(){const n=$('nav'),f=$('fab'),af=$('absenFab');if(!state.user){n.style.display='none';f.style.display='none';if(af)af.style.display='none';return}n.style.display='flex';f.style.display=state.page==='home'?'block':'none';if(af)af.style.display=showStaffAbsenFab()?'flex':'none';document.querySelectorAll('.nav button').forEach(b=>b.classList.remove('active'));$(`nav-${state.page}`)?.classList.add('active')}
function renderLogin(){nav();page.innerHTML=`<div class="login"><div class="login-card"><div class="hero" style="text-align:center"><div class="big">ROCKY HIJAB</div><div class="sub">Koleksi Terbaik Untuk Muslimah Hebat</div></div><div class="card" style="margin-top:10px"><div class="field"><div class="label">Username Staff</div><input id="lu" autocomplete="username" placeholder="username"></div><div class="field"><div class="label">PIN</div><input id="lp" type="password" inputmode="numeric" pattern="[0-9]*" autocomplete="one-time-code" placeholder="PIN"></div><button class="btn primary block" onclick="login()">Masuk</button><div class="hint" style="margin-top:8px;text-align:center">Login Menggunakan Username Masing-Masing</div></div></div></div>`}
let androidStaffSessionSignature='';
function syncAndroidStaffSession(){
  if(!state.user)return;
  try{
    const username=key(state.user.username), actualRole=String(state.user.role||'staff'), dailyMode=isDaily();
    const staffName=String(state.user.name||state.user.username||'');
    const payload={
      username,
      user:username,
      staff:username,
      targetUsername:username,
      name:staffName,
      role:actualRole,
      userRole:actualRole,
      actualRole,
      bonusGroup:dailyMode?'harian':'staff',
      daily:dailyMode,
      isDaily:dailyMode,
      actualDaily:dailyMode,
      dailyMode,
      mode:dailyMode?'harian':'staff',
      notificationAudience:'staff',
      canReceiveStaffNotifications:true
    };
    const signature=JSON.stringify(payload);
    if(signature===androidStaffSessionSignature)return;
    androidStaffSessionSignature=signature;
    const bridges=[window.AndroidStaff,window.Android].filter(Boolean), seen=[];
    bridges.forEach(bridge=>{
      if(seen.includes(bridge))return;
      seen.push(bridge);
      if(typeof bridge.setStaffSession==='function')bridge.setStaffSession(signature);
      if(typeof bridge.registerStaffSession==='function')bridge.registerStaffSession(signature);
      if(typeof bridge.setStaffUser==='function'){
        try{bridge.setStaffUser(username,staffName,actualRole)}catch(e){try{bridge.setStaffUser(username)}catch(err){}}
      }
    });
  }catch(e){}
}
function clearAndroidStaffSession(){
  androidStaffSessionSignature='';
  try{
    const bridges=[window.AndroidStaff,window.Android].filter(Boolean), seen=[];
    bridges.forEach(bridge=>{
      if(seen.includes(bridge))return;
      seen.push(bridge);
      if(typeof bridge.clearStaffSession==='function')bridge.clearStaffSession();
      if(typeof bridge.clearStaffUser==='function')bridge.clearStaffUser();
    });
  }catch(e){}
}
function renderNow(){if(!state.user)return renderLogin();syncAndroidStaffSession();nav();if(state.page==='history')return history();if(state.page==='unlock')return renderUnlockPage();return home()}
let renderFrame=0;
function render(){
  if(renderFrame)return;
  const schedule=window.requestAnimationFrame||((fn)=>setTimeout(fn,16));
  renderFrame=schedule(()=>{renderFrame=0;renderNow()});
}
async function getQueryDocs(makePrimary,makeFallback){try{return await getDocs(makePrimary())}catch(e){console.warn('primary query fallback',e?.code||e);return await getDocs(makeFallback())}}
function clearStaffRealtime(){
  staffRealtimeUnsubs.forEach(fn=>{try{if(typeof fn==='function')fn()}catch(e){console.warn('unsub realtime gagal',e)}});
  staffRealtimeUnsubs=[];
  staffRealtimeStartedFor='';
}
function replaceScopedRows(listName,rows,matchFn){
  const keep=(state.data[listName]||[]).filter(x=>!matchFn(x));
  state.data[listName]=sortDesc([...keep,...(rows||[])]);
}
function mergeRowsById(...lists){
  const map=new Map();
  lists.flat().forEach(x=>{if(x&&x.id)map.set(String(x.id),{...(map.get(String(x.id))||{}),...x})});
  return sortDesc([...map.values()]);
}
function mergeScopedRows(listName,rows){
  const map=new Map();
  (state.data[listName]||[]).forEach(x=>{if(x&&x.id)map.set(String(x.id),x)});
  (rows||[]).forEach(x=>{if(x&&x.id)map.set(String(x.id),{...(map.get(String(x.id))||{}),...x})});
  state.data[listName]=sortDesc([...map.values()]);
}
function handleRealtimeError(label,err){
  console.warn(label+' realtime gagal',err?.code||err?.message||err);
  state.syncError=label+' realtime gagal. Pakai tombol refresh jika data belum masuk.';
  render();
}
function startStaffRealtime(){
  if(!state.user)return;
  const u=key(state.user.username), d=todayKey(), realtimeKey=u+'_'+d;
  if(staffRealtimeStartedFor===realtimeKey&&staffRealtimeUnsubs.length)return;
  clearStaffRealtime();
  staffRealtimeStartedFor=realtimeKey;

  const txQ=query(collection(db,'transactions'),where('user','==',u),where('dateKey','==',d),limit(LIMITS.txToday));
  staffRealtimeUnsubs.push(onSnapshot(txQ,snap=>{
    const rows=sortDesc(snap.docs.map(x=>({id:x.id,...x.data()})));
    // Jangan hapus riwayat lokal hanya karena polling Supabase kebetulan membawa hasil parsial.
    // Server tetap jadi sumber update: row dengan id sama akan dioverwrite, row deleted akan tersaring oleh liveTx().
    mergeScopedRows('tx',rows);
    applyPendingToState();
    state.lastSyncMs=Date.now();
    state.syncError='';
    render();
  },err=>handleRealtimeError('Transaksi hari ini',err)));

  const attQ=query(collection(db,'attendance'),where('user','==',u),where('dateKey','==',d),limit(10));
  staffRealtimeUnsubs.push(onSnapshot(attQ,snap=>{
    const rows=snap.docs.map(x=>({id:x.id,...x.data()}));
    replaceScopedRows('att',rows,t=>key(t.user)===u&&attDate(t)===d);
    state.data.att=dedupeAttendanceRows(state.data.att);
    render();
  },err=>handleRealtimeError('Absen hari ini',err)));

  const manualQ=query(collection(db,'manualBonuses'),where('user','==',u),where('dateKey','==',d),limit(LIMITS.manualToday));
  staffRealtimeUnsubs.push(onSnapshot(manualQ,snap=>{
    const rows=sortDesc(snap.docs.map(x=>({...(x.data()||{}),id:x.id,docId:x.id})).filter(b=>!deleted(b)));
    replaceScopedRows('manual',rows,t=>key(t.user)===u&&manualBonusDate(t)===d);
    render();
    notifyNewManualBonuses();
  },err=>handleRealtimeError('Bonus manual hari ini',err)));

  const prevD=previousDateKey(d);
  if(prevD!==d){
    const prevAttQ=query(collection(db,'attendance'),where('user','==',u),where('dateKey','==',prevD),limit(10));
    staffRealtimeUnsubs.push(onSnapshot(prevAttQ,snap=>{
      const rows=snap.docs.map(x=>({id:x.id,...x.data()}));
      replaceScopedRows('att',rows,t=>key(t.user)===u&&attDate(t)===prevD);
      state.data.att=dedupeAttendanceRows(state.data.att);
      render();
    },err=>handleRealtimeError('Absen kemarin',err)));
  }

  const unlockQ=query(collection(db,STAFF_UNLOCK_TABLE),where('requestKind','==','unlock'),where('user','==',u),limit(LIMITS.unlockRequests));
  staffRealtimeUnsubs.push(onSnapshot(unlockQ,snap=>{
    const rows=sortDesc(snap.docs.map(x=>({...(x.data()||{}),id:x.id,docId:x.id})).filter(r=>isFeatureUnlockRequest(r)&&!deleted(r)));
    replaceScopedRows('unlockRequests',rows,t=>isFeatureUnlockRequest(t)&&key(t.user)===u);
    render();
  },err=>handleRealtimeError('Buka fitur',err)));

  const closingQ=query(collection(db,'closings'),where('dateKey','==',d),limit(LIMITS.closingsToday));
  staffRealtimeUnsubs.push(onSnapshot(closingQ,snap=>{
    const rows=snap.docs.map(x=>({id:x.id,...x.data()})).filter(c=>c.id!=='__bonus_settings'&&c.type!=='bonus_settings');
    replaceScopedRows('closings',rows,t=>String(t.dateKey||'')===d);
    render();
  },err=>handleRealtimeError('Closing hari ini',err)));

  const targetTxQ=query(collection(db,'transactions'),where('dateKey','==',d),limit(LIMITS.targetTxToday));
  staffRealtimeUnsubs.push(onSnapshot(targetTxQ,snap=>{
    state.data.targetTx=sortDesc(snap.docs.map(x=>({id:x.id,...x.data()})));
    syncDailyTargetState();
    render();
    notifyTargetReachedInApp();
    scheduleDailyTargetCheck();
  },err=>console.warn('Target omzet transaksi realtime gagal',err?.code||err?.message||err)));

  const targetAttQ=query(collection(db,'attendance'),where('dateKey','==',d),limit(LIMITS.targetAttToday));
  staffRealtimeUnsubs.push(onSnapshot(targetAttQ,snap=>{
    state.data.targetAtt=dedupeAttendanceRows(snap.docs.map(x=>({id:x.id,...x.data()})));
    syncDailyTargetState();
    render();
    notifyTargetReachedInApp();
    scheduleDailyTargetCheck();
  },err=>console.warn('Target omzet absen realtime gagal',err?.code||err?.message||err)));

  const targetUsersQ=query(collection(db,'users'),limit(LIMITS.targetUsers));
  staffRealtimeUnsubs.push(onSnapshot(targetUsersQ,snap=>{
    state.data.targetUsers=snap.docs.map(x=>({id:x.id,username:key((x.data()||{}).username||x.id),...x.data()}));
    syncDailyTargetState();
    render();
    notifyTargetReachedInApp();
    scheduleDailyTargetCheck();
  },err=>console.warn('Target omzet user realtime gagal',err?.code||err?.message||err)));

  staffRealtimeUnsubs.push(onSnapshot(doc(db,DAILY_TARGETS_TABLE,targetDailyDocId(d)),snap=>{
    if(snap.exists())syncDailyTargetState(snap.data()||{});
    render();
    notifyTargetReachedInApp();
  },err=>console.warn('Status target harian realtime gagal',err?.code||err?.message||err)));

  staffRealtimeUnsubs.push(onSnapshot(doc(db,TARGET_NOTIFICATIONS_TABLE,targetNotificationDocId(d)),snap=>{
    state.data.targetNotification=snap.exists()?{id:snap.id,...(snap.data()||{})}:null;
    notifyTargetReachedInApp();
    render();
  },err=>console.warn('Notifikasi target harian realtime gagal',err?.code||err?.message||err)));

  staffRealtimeUnsubs.push(onSnapshot(doc(db,TARGET_SETTINGS_TABLE,'daily_target'),snap=>{
    if(snap.exists()){
      state.data.targetSettings=normalizeDailyTargetSettings(snap.data()||{});
      syncDailyTargetState();
      render();
      scheduleDailyTargetCheck();
    }
  },err=>console.warn('Setting target harian realtime gagal',err?.code||err?.message||err)));

  staffRealtimeUnsubs.push(onSnapshot(doc(db,'closings','__bonus_settings'),snap=>{
    state.data.bonus=snap.exists()?normalizeBonusSettings(snap.data()):normalizeBonusSettings(DEFAULT_BONUS);
    render();
  },err=>handleRealtimeError('Setting bonus',err)));

  staffRealtimeUnsubs.push(onSnapshot(doc(db,'closings',HEADER_GUIDE_NOTE_DOC_ID),snap=>{
    state.data.headerGuideNote=(snap&&snap.exists())?String((snap.data()||{}).note||DEFAULT_HEADER_GUIDE_NOTE):DEFAULT_HEADER_GUIDE_NOTE;
    render();
  },err=>handleRealtimeError('Catatan panduan',err)));

  staffRealtimeUnsubs.push(onSnapshot(doc(db,'closings',STAFF_DAILY_NOTE_DOC_ID),snap=>{
    state.data.staffDailyNote=(snap&&snap.exists())?{note:DEFAULT_STAFF_DAILY_NOTE,enabled:true,...(snap.data()||{})}:{note:DEFAULT_STAFF_DAILY_NOTE,enabled:true,updatedAtMs:0,updatedByName:''};
    render();
  },err=>handleRealtimeError('Catatan staff',err)));

  staffRealtimeUnsubs.push(onSnapshot(doc(db,'closings',RECEIPT_TEXT_DOC_ID),snap=>{
    state.data.receiptSettings=(snap&&snap.exists())?normalizeReceiptSettings({...(snap.data()||{})}):normalizeReceiptSettings(DEFAULT_RECEIPT_TEXT_SETTINGS);
  },err=>handleRealtimeError('Setting struk',err)));

  staffRealtimeUnsubs.push(onSnapshot(doc(db,'users',u),snap=>{
    if(!snap.exists()){clearSessionAndRender('Akun sudah tidak valid');return}
    const raw=snap.data()||{}, fresh={id:snap.id,username:raw.username||snap.id,...raw};
    if(isAdmin(fresh)||fresh.active===false||deleted(fresh)){clearSessionAndRender('Akun sudah nonaktif / tidak valid');return}
    const deviceInvalidReason=isDeviceSessionInvalid(fresh);
    if(deviceInvalidReason){clearSessionAndRender(deviceInvalidReason);return}
    state.user={
      ...state.user,
      username:key(fresh.username||u),
      name:fresh.name||state.user.name||u,
      role:fresh.role||state.user.role||'staff',
      transactionBonusRate:hasBonusValue(fresh.transactionBonusRate)?Number(fresh.transactionBonusRate):null,
      transactionBonusPercent:hasBonusValue(fresh.transactionBonusPercent)?Number(fresh.transactionBonusPercent):null,
      closingBonusPerMinute:isDailyUser(fresh)?0:(hasBonusValue(fresh.closingBonusPerMinute)?Number(fresh.closingBonusPerMinute):null),
      dailyBonusRate:hasBonusValue(fresh.dailyBonusRate)?Number(fresh.dailyBonusRate):null,
      dailyBonusPercent:hasBonusValue(fresh.dailyBonusPercent)?Number(fresh.dailyBonusPercent):null,
      active:fresh.active!==false,
      deviceId:isDeviceFreeUser(fresh)?'':String(fresh.deviceId||state.user?.deviceId||''),
      deviceResetAtMs:Number(fresh.deviceResetAtMs||state.user?.deviceResetAtMs||0),
      ...trialSessionFields(fresh)
    };
    render();
  },err=>handleRealtimeError('User',err)));
}
async function loadStaffData(opts={}){
  if(!state.user)return;
  const u=key(state.user.username), silent=opts.silent===true, d=todayKey(), prevD=previousDateKey(d), m=monthKey(), monthStart=monthStartKey(m), monthEnd=monthEndKey(m);
  if(!silent)showLoad(true);
  try{
    // Load awal fokus ke bulan berjalan. Ini menjaga kartu hari ini, bonus bulanan,
    // absen bulanan, dan closing tetap sama tanpa mengambil riwayat lintas bulan.
    const [tx,txLegacy,att,attLegacy,prevAtt,manual,manualLegacy,unlockReq,bs,userSnap,guideNoteSnap,staffDailyNoteSnap,receiptTextSnap]=await Promise.all([
      getQueryDocs(
        ()=>query(collection(db,'transactions'),where('user','==',u),where('dateKey','>=',monthStart),where('dateKey','<=',monthEnd),limit(LIMITS.txBonusAll)),
        ()=>query(collection(db,'transactions'),where('user','==',u),limit(LIMITS.txBonusAll))
      ),
      getDocs(query(collection(db,'transactions'),where('user','==',u),limit(INITIAL_LEGACY_LIMITS.tx))).catch(()=>querySnapshot([])),
      getQueryDocs(
        ()=>query(collection(db,'attendance'),where('user','==',u),where('dateKey','>=',monthStart),where('dateKey','<=',monthEnd),limit(LIMITS.attMonth)),
        ()=>query(collection(db,'attendance'),where('user','==',u),limit(LIMITS.attMonth))
      ),
      getDocs(query(collection(db,'attendance'),where('user','==',u),limit(INITIAL_LEGACY_LIMITS.att))).catch(()=>querySnapshot([])),
      getDocs(query(collection(db,'attendance'),where('user','==',u),where('dateKey','==',prevD),limit(10))).catch(()=>querySnapshot([])),
      getQueryDocs(
        ()=>query(collection(db,'manualBonuses'),where('user','==',u),where('dateKey','>=',monthStart),where('dateKey','<=',monthEnd),limit(LIMITS.manualBonusAll)),
        ()=>query(collection(db,'manualBonuses'),where('user','==',u),limit(LIMITS.manualBonusAll))
      ),
      getDocs(query(collection(db,'manualBonuses'),where('user','==',u),limit(INITIAL_LEGACY_LIMITS.manual))).catch(()=>querySnapshot([])),
      getQueryDocs(
        ()=>query(collection(db,STAFF_UNLOCK_TABLE),where('requestKind','==','unlock'),where('user','==',u),limit(LIMITS.unlockRequests)),
        ()=>query(collection(db,STAFF_UNLOCK_TABLE),where('requestKind','==','unlock'),where('user','==',u),limit(LIMITS.unlockRequests))
      ).catch(()=>querySnapshot([])),
      getDocFromServer(doc(db,'closings','__bonus_settings')),
      getDocFromServer(doc(db,'users',u)),
      getDocFromServer(doc(db,'closings',HEADER_GUIDE_NOTE_DOC_ID)).catch(()=>null),
      getDocFromServer(doc(db,'closings',STAFF_DAILY_NOTE_DOC_ID)).catch(()=>null),
      getDocFromServer(doc(db,'closings',RECEIPT_TEXT_DOC_ID)).catch(()=>null)
    ]);

    let closingRows=[];
    try{
      const allRows=await getQueryDocs(
        ()=>query(collection(db,'closings'),where('dateKey','>=',monthStart),where('dateKey','<=',monthEnd),limit(LIMITS.closingsAll)),
        ()=>query(collection(db,'closings'),limit(LIMITS.closingsAll))
      );
      closingRows=allRows.docs.map(x=>({id:x.id,...x.data()}));
    }catch(closeErr){
      console.warn('closing all query gagal',closeErr?.code||closeErr);
      closingRows=[];
    }

    const txRows=mergeRowsById(tx.docs.map(x=>({id:x.id,...x.data()})),txLegacy.docs.map(x=>({id:x.id,...x.data()}))).filter(t=>txMonth(t)===m);
    const attRows=mergeRowsById(att.docs.map(x=>({id:x.id,...x.data()})),attLegacy.docs.map(x=>({id:x.id,...x.data()})),prevAtt.docs.map(x=>({id:x.id,...x.data()}))).filter(a=>attDate(a).startsWith(m)||attDate(a)===prevD);
    const manualRows=mergeRowsById(manual.docs.map(x=>({...(x.data()||{}),id:x.id,docId:x.id})),manualLegacy.docs.map(x=>({...(x.data()||{}),id:x.id,docId:x.id}))).filter(b=>manualBonusDate(b).startsWith(m));
    const unlockRows=unlockReq.docs.map(x=>({...(x.data()||{}),id:x.id,docId:x.id})).filter(r=>isFeatureUnlockRequest(r));
    state.data.tx=sortDesc(txRows);
    state.data.att=dedupeAttendanceRows(attRows);
    state.data.manual=sortDesc(manualRows.filter(b=>!deleted(b)));
    state.data.unlockRequests=sortDesc(unlockRows.filter(r=>!deleted(r)));
    state.data.closings=sortDesc(closingRows.filter(c=>c.id!=='__bonus_settings'&&c.dateKey!=='__bonus_settings'&&c.id!=='__risma_manual_closing'&&c.dateKey!=='__risma_manual_closing'&&c.id!==HEADER_GUIDE_NOTE_DOC_ID&&c.dateKey!==HEADER_GUIDE_NOTE_DOC_ID&&c.id!==STAFF_DAILY_NOTE_DOC_ID&&c.dateKey!==STAFF_DAILY_NOTE_DOC_ID&&c.id!==RECEIPT_TEXT_DOC_ID&&c.dateKey!==RECEIPT_TEXT_DOC_ID&&c.type!=='bonus_settings'&&c.type!=='closing_manual_config'&&c.type!=='header_guide_note'&&c.type!=='staff_daily_home_note'&&c.type!=='receipt_text_settings'));
    state.data.headerGuideNote=(guideNoteSnap&&guideNoteSnap.exists())?String((guideNoteSnap.data()||{}).note||DEFAULT_HEADER_GUIDE_NOTE):DEFAULT_HEADER_GUIDE_NOTE;
    state.data.staffDailyNote=(staffDailyNoteSnap&&staffDailyNoteSnap.exists())?{note:DEFAULT_STAFF_DAILY_NOTE,enabled:true,...(staffDailyNoteSnap.data()||{})}:{note:DEFAULT_STAFF_DAILY_NOTE,enabled:true,updatedAtMs:0,updatedByName:''};
    state.data.receiptSettings=(receiptTextSnap&&receiptTextSnap.exists())?normalizeReceiptSettings({...(receiptTextSnap.data()||{})}):normalizeReceiptSettings(DEFAULT_RECEIPT_TEXT_SETTINGS);
    state.data.bonus=bs.exists()?normalizeBonusSettings(bs.data()):normalizeBonusSettings(DEFAULT_BONUS);
    if(userSnap.exists()){
      const rawUser=userSnap.data()||{}, fresh={id:userSnap.id,username:rawUser.username||userSnap.id,...rawUser};
      if(isAdmin(fresh)||fresh.active===false||deleted(fresh)){clearSessionAndRender('Akun sudah nonaktif / tidak valid');return;}
      const deviceInvalidReason=isDeviceSessionInvalid(fresh);
      if(deviceInvalidReason){clearSessionAndRender(deviceInvalidReason);return;}
      state.user={
        username:key(fresh.username||u),
        name:fresh.name||u,
        pin:String(fresh.pin||state.user.pin||''),
        role:fresh.role||'staff',
        transactionBonusRate:hasBonusValue(fresh.transactionBonusRate)?Number(fresh.transactionBonusRate):null,
        transactionBonusPercent:hasBonusValue(fresh.transactionBonusPercent)?Number(fresh.transactionBonusPercent):null,
        closingBonusPerMinute:isDailyUser(fresh)?0:(hasBonusValue(fresh.closingBonusPerMinute)?Number(fresh.closingBonusPerMinute):null),
        dailyBonusRate:hasBonusValue(fresh.dailyBonusRate)?Number(fresh.dailyBonusRate):null,
        dailyBonusPercent:hasBonusValue(fresh.dailyBonusPercent)?Number(fresh.dailyBonusPercent):null,
        active:fresh.active!==false,
        deviceId:isDeviceFreeUser(fresh)?'':String(fresh.deviceId||state.user?.deviceId||''),
        deviceResetAtMs:Number(fresh.deviceResetAtMs||state.user?.deviceResetAtMs||0),
        ...trialSessionFields(fresh)
      };
      const saved={username:state.user.username,name:state.user.name,pin:state.user.pin,role:state.user.role,transactionBonusRate:state.user.transactionBonusRate,transactionBonusPercent:state.user.transactionBonusPercent,closingBonusPerMinute:isDailyUser(state.user)?0:state.user.closingBonusPerMinute,dailyBonusRate:state.user.dailyBonusRate,dailyBonusPercent:state.user.dailyBonusPercent,active:true,deviceId:state.user.deviceId,deviceResetAtMs:state.user.deviceResetAtMs,...trialSessionFields(state.user)};
      localStorage.setItem(SESSION,JSON.stringify(saved));
    }
    state.lastSyncMs=Date.now();
    state.syncError='';
    applyPendingToState();
  }catch(e){
    console.error(e);
    state.syncError=isPermissionError(e)?'Akses Firebase ditolak.':'Gagal memuat data. Coba refresh manual.';
    applyPendingToState();
  }
  await loadDailyTargetData({apply:true});
  if(!silent)showLoad(false);
  render();
  notifyNewManualBonuses();
  if(!opts.skipFlush)flushPending();
  startStaffRealtime();
}
async function flushPending(){
  if(!state.user||state.syncing)return;
  let list=getPending();
  if(!list.length)return;
  state.syncing=true;
  state.syncError='';
  const remain=[];
  for(const item of list){
    if(key(item.user)!==key(state.user.username)){remain.push(item);continue}
    try{
      if(item.type==='tx_add'){
        const allowed=await verifyTransactionAllowedServer(item.payload?.dateKey||todayKey());
        if(!allowed.ok){
          item.tries=Number(item.tries||0)+1;
          item.lastTryMs=Date.now();
          remain.push(item);
          state.syncError=allowed.msg||'Pending transaksi belum bisa diverifikasi.';
          continue;
        }
        await setDoc(doc(db,'transactions',item.id),{...(item.payload||{}),createdAt:serverTimestamp(),syncedAt:serverTimestamp(),syncedAtMs:Date.now()},{merge:true});
      }
      else if(item.type==='att_add'){
        const payload=item.payload||{};
        const d=String(payload.dateKey||todayKey()).slice(0,10), docId=attendanceDocId(item.user||payload.user,d);
        const existing=await getDocFromServer(doc(db,'attendance',docId));
        if(!existing.exists())await setDoc(doc(db,'attendance',docId),{...payload,clientId:docId,dateKey:d,createdAt:serverTimestamp(),syncedAt:serverTimestamp(),syncedAtMs:Date.now()},{merge:true});
        item.id=docId;
      }
      else if(item.type==='feature_unlock_request'){
        const payload=item.payload||{};
        await setDoc(doc(db,STAFF_UNLOCK_TABLE,item.id),{...payload,createdAt:serverTimestamp(),syncedAt:serverTimestamp(),syncedAtMs:Date.now()},{merge:true});
        notifyAdminFeatureUnlock({id:item.id,...payload,pending:false});
      }
      else remain.push(item);
    }catch(e){
      console.warn('pending sync failed',e?.code||e);
      item.tries=Number(item.tries||0)+1;
      item.lastTryMs=Date.now();
      remain.push(item);
      if(isPermissionError(e)){state.syncError='Akses Firebase ditolak. Data belum bisa sync.';break}
      else state.syncError='Koneksi belum stabil, sync akan dicoba lagi.';
    }
  }
  setPending(remain);
  state.syncing=false;
  applyPendingToState();
  render();
}
async function retrySync(){await flushPending();if(!pendingForUser().length){toast('Semua data sudah sync');await loadStaffData({silent:true,skipFlush:true})}else toast('Masih ada data menunggu sync')}
function liveTx(){return state.data.tx.filter(t=>!deleted(t)&&recordMatchesCurrentAccount(t))}
function todayTx(){const d=todayKey();return liveTx().filter(t=>txDate(t)===d)}
function monthTx(){const m=monthKey();return liveTx().filter(t=>txMonth(t)===m)}
function todayTotal(){return todayTx().reduce((s,t)=>s+Number(t.amount||0),0)}
function monthTotal(){return monthTx().reduce((s,t)=>s+Number(t.amount||0),0)}
function todayAtt(){const d=todayKey(),u=key(state.user?.username);return dedupeAttendanceRows(state.data.att).find(a=>!deleted(a)&&recordMatchesCurrentAccount(a)&&attDate(a)===d&&(!u||key(a.user)===u))}
function monthAttendDays(){const m=monthKey(), set=new Set();dedupeAttendanceRows(state.data.att).forEach(a=>{if(!deleted(a)&&recordMatchesCurrentAccount(a)&&attDate(a).startsWith(m))set.add(attDate(a))});return set.size}
function monthlyAttendancePerformance(){const m=monthKey();const rows=dedupeAttendanceRows(state.data.att).filter(a=>!deleted(a)&&recordMatchesCurrentAccount(a)&&attDate(a).startsWith(m));if(!rows.length)return null;let total=0,count=0;for(const rec of rows){const n=ms(rec);if(!n)continue;const p=parts(new Date(n));total+=Number(p.hour||0)*60+Number(p.minute||0);count++}if(!count)return null;const avg=total/count,h=Math.floor(avg/60),mi=Math.floor(avg%60),label=String(h).padStart(2,'0')+':'+String(mi).padStart(2,'0');return{avgLabel:label,isGood:avg<=440,count}}
function averageAttendanceCard(){if(isDaily()||isRismaSpecialUser())return '';const p=monthlyAttendancePerformance();if(!p)return `<div class="card avg-att-card"><div class="between"><div class="avg-att-head"><span class="avg-att-ico">⏱</span><div><div class="label">Rata-rata Jam Absen</div><div class="hint" style="margin-top:4px">${monthID(monthKey())} · belum ada data</div></div></div><div class="avg-att-metric"><div class="stat-val" style="color:var(--muted)">--:--</div></div></div></div>`;const cls=p.isGood?'avg-good':'avg-warn',pill=p.isGood?'green':'amber',txt=p.isGood?'Performa baik':'Perlu ditingkatkan';return `<div class="card avg-att-card ${cls}"><div class="between"><div class="avg-att-head"><span class="avg-att-ico">⏱</span><div><div class="label">Rata-rata Jam Absen</div><div class="hint" style="margin-top:4px">${monthID(monthKey())} · ${p.count} hari hadir</div></div></div><div class="avg-att-metric"><div class="stat-val">${p.avgLabel}</div><span class="pill ${pill}">${txt}</span></div></div></div>`}
function hasAttendOn(dateKey){const u=key(state.user?.username),d=String(dateKey||'').slice(0,10);return dedupeAttendanceRows(state.data.att).some(a=>!deleted(a)&&recordMatchesCurrentAccount(a)&&attDate(a)===d&&(!u||key(a.user)===u))}
function rate(){return userTransactionBonusRate()}
function manualBonusRowsForDate(dateKey=todayKey()){
  const d=String(dateKey||todayKey()).slice(0,10);
  return sortDesc((state.data.manual||[]).filter(b=>!deleted(b)&&recordMatchesCurrentAccount(b)&&canReceiveManualBonusRow(b)&&manualBonusDate(b)===d&&Number(b.amount||0)!==0));
}
function manualBonusRowsForMonth(month=monthKey()){
  const m=String(month||monthKey()).slice(0,7);
  return sortDesc((state.data.manual||[]).filter(b=>!deleted(b)&&recordMatchesCurrentAccount(b)&&canReceiveManualBonusRow(b)&&manualBonusDate(b).startsWith(m)&&Number(b.amount||0)!==0));
}
function manualBonusRowsAll(){return sortDesc((state.data.manual||[]).filter(b=>!deleted(b)&&recordMatchesCurrentAccount(b)&&canReceiveManualBonusRow(b)&&Number(b.amount||0)!==0))}
function manualBonusToday(){return manualBonusRowsForDate(todayKey()).reduce((sum,b)=>sum+Number(b.amount||0),0)}
function manualBonus(){return manualBonusRowsForMonth(monthKey()).reduce((sum,b)=>sum+Number(b.amount||0),0)}
function activeClosings(){return state.data.closings.filter(c=>c&&c.closed===true&&c.canceled!==true&&!deleted(c))}
function closingScope(c){return String(c?.scope||(c?.user?'user':'global')).toLowerCase()}
function closingBonusValue(c){
  if(!c||c.closed!==true)return 0;
  const u=key(state.user?.username), map=c.bonusByUser||{}, scope=closingScope(c);
  if(scope==='user'||c.user){
    if(Object.keys(map).length)return Number(map[u]||0);
    return key(c.user)===u?Number(c.bonusPerUser||c.totalBonus||0):0;
  }
  // Penting: untuk closing GLOBAL, user hanya dapat bonus kalau index menulis bonusByUser[username].
  // Jangan fallback ke bonusPerUser/totalBonus, karena user yang sudah closing per-user bisa terbaca dobel.
  return Number(map[u]||0);
}
function closingBonusRows(month=monthKey()){
  if(isDaily())return [];
  const m=String(month||monthKey()).slice(0,7);
  const byDate=new Map();
  activeClosings().forEach(c=>{
    const d=String(c.dateKey||'').slice(0,10);
    if(!d||!d.startsWith(m))return;
    const val=Math.round(closingBonusValue(c));
    if(!val)return;
    const scope=closingScope(c), prev=byDate.get(d);
    // Satu user hanya boleh punya 1 bonus closing per tanggal. Jika ada closing per-user dan global, prioritaskan per-user.
    if(!prev||scope==='user'||c.user)byDate.set(d,{record:c,val});
  });
  return [...byDate.values()];
}
function closingBonus(){return closingBonusRows(monthKey()).reduce((sum,x)=>sum+Number(x.val||0),0)}
function closingCount(){return closingBonusRows(monthKey()).length}
function trxBonus(){return txBonusSum(monthTx())}
function totalBonus(){return trxBonus()+closingBonus()+manualBonus()}
function todayClosing(){
  const d=todayKey(), u=key(state.user?.username);
  const rows=state.data.closings.filter(c=>c&&c.closed===true&&c.canceled!==true&&!deleted(c)&&String(c.dateKey||'')===d);
  const userRows=rows.filter(c=>(closingScope(c)==='user'||c.user)&&key(c.user)===u);
  if(userRows.length)return sortDesc(userRows)[0];
  const globalRows=rows.filter(c=>closingScope(c)==='global'||(!c.user&&c.scope!=='user'));
  return sortDesc(globalRows)[0]||null;
}
function closingTimeText(c){
  if(!c)return '--:--';
  const tm=String(c.closingTime||'').match(/^(\d{1,2}):(\d{2})/);
  if(tm)return String(tm[1]).padStart(2,'0')+':'+tm[2];
  const n=Number(c.closedAtMs||c.createdAtMs||c.updatedAtMs||0);
  return n?timeID(n):'--:--';
}
function isClosedToday(){const d=todayKey(), u=key(state.user?.username);return state.data.closings.some(c=>c&&c.closed===true&&c.canceled!==true&&!deleted(c)&&String(c.dateKey||'')===d&&((!c.user&&c.scope!=='user')||c.scope==='global'||key(c.user)===u))}
function delayMin(tm=timeNow(),source=null){const m=String(tm).match(/^(\d{1,2}):(\d{2})$/);if(!m)return 0;return Math.max(0,Number(m[1])*60+Number(m[2])-closingDeadlineMinutes(source))}
function closingNotice(){if(isDaily()||(!todayAtt()&&!(isRismaSpecialUser()&&todayTx().length)))return '';const d=delayMin();if(!d||isClosedToday())return '';const rate=userClosingBonusPerMinute();const est=d*rate;return `<div class="card warn" style="margin-bottom:8px"><b>Estimasi bonus closing</b><div class="hint" style="margin-top:4px;color:inherit">Estimasi bonus kamu Rp ${rp(est)} · sejak ${closingDeadlineTimeLabel()} WIB</div></div>`}
function todayClosingBonusNotice(){if(isDaily())return '';const c=todayClosing();if(!c)return '';const val=Math.round(closingBonusValue(c));if(!val)return '';return `<div class="card closing-bonus-card"><div class="between"><div><div class="label">Bonus Closing Hari Ini</div><div class="stat-val" style="font-size:20px;color:var(--green)">Rp ${rp(val)}</div><div class="hint" style="margin-top:3px">Bonus dari closing index hari ini</div></div><span class="pill green">Dapat</span></div></div>`}
function todayClosingBonusInline(){if(isDaily())return '';const c=todayClosing();if(!c)return '';const val=Math.round(closingBonusValue(c));if(!val)return '';return `<div class="bonus-note" style="margin-top:4px;color:var(--green);font-weight:850">Bonus closing hari ini Rp ${rp(val)}</div>`}
function validDateKey(v){return /^\d{4}-\d{2}-\d{2}$/.test(String(v||'').slice(0,10))}
function shiftDateKey(date,days=0){
  const d=String(date||todayKey()).slice(0,10);
  const base=Date.parse(`${d}T12:00:00+07:00`);
  if(!Number.isFinite(base))return todayKey();
  return todayKey(new Date(base+(Number(days)||0)*86400000));
}
function previousDateKey(date=todayKey()){return shiftDateKey(date,-1)}
function isFeatureUnlockRequest(r){return String(r?.requestKind||'')==='unlock'||String(r?.action||r?.type||'')==='feature_unlock_request'||r?.featureUnlockRequest===true}
function cleanUnlockText(v,max=280){return String(v||'').replace(/[<>]/g,'').replace(/\s+/g,' ').trim().slice(0,max)}
function unlockRows(){return sortDesc((state.data.unlockRequests||[]).filter(r=>isFeatureUnlockRequest(r)&&!deleted(r)))}
function unlockStatus(r){const s=key(r?.status||'pending');if(s==='approved'||s==='diterima')return'approved';if(s==='rejected'||s==='ditolak')return'rejected';if(s==='cancelled'||s==='batal')return'cancelled';return'pending'}
function unlockStatusText(r){const s=unlockStatus(r);if(s==='approved')return'Dibuka';if(s==='rejected')return'Ditolak';if(s==='cancelled')return'Dibatalkan';return'Menunggu'}
function unlockStatusClass(r){const s=unlockStatus(r);if(s==='approved')return'green';if(s==='rejected')return'red';if(s==='cancelled')return'amber';return'blue'}
function unlockTargetDate(r){return String(r?.targetDate||r?.workDate||r?.dateKey||todayKey()).slice(0,10)}
function unlockMissedDate(r){return String(r?.missedDate||r?.missingDate||r?.previousDate||previousDateKey(unlockTargetDate(r))).slice(0,10)}
function isMissedAttendanceUnlock(r){
  const kind=key(r?.lockKind||r?.reasonKind||r?.unlockKind||'');
  return isFeatureUnlockRequest(r)&&(r?.missingAttendance===true||kind==='missed_attendance'||kind==='absen_kemarin'||String(r?.source||'')==='staff_app_missed_attendance_unlock');
}
function missedAttendanceUnlockRows(){return unlockRows().filter(isMissedAttendanceUnlock)}
function unlockRequestDocId(u,nowMs=Date.now()){return `staffunlock_${key(u)}_${nowMs}_${Math.random().toString(36).slice(2,7)}`}
function missedAttendanceLockId(u,missedDate,targetDate){return `missed_attendance_${key(u)}_${String(missedDate).slice(0,10)}_${String(targetDate).slice(0,10)}`}
function approvedUnlockForMissedAttendance(targetDate=todayKey(),missedDate=previousDateKey(targetDate),rows=null){
  const list=(Array.isArray(rows)?rows:missedAttendanceUnlockRows()).filter(isMissedAttendanceUnlock);
  const u=key(state.user?.username), target=String(targetDate||todayKey()).slice(0,10), missed=String(missedDate||previousDateKey(target)).slice(0,10);
  return list.find(r=>key(r.user)===u&&unlockStatus(r)==='approved'&&(unlockTargetDate(r)===target||unlockMissedDate(r)===missed))||null;
}
function pendingUnlockForMissedAttendance(targetDate=todayKey(),missedDate=previousDateKey(targetDate)){
  const u=key(state.user?.username), target=String(targetDate||todayKey()).slice(0,10), missed=String(missedDate||previousDateKey(target)).slice(0,10);
  return missedAttendanceUnlockRows().find(r=>key(r.user)===u&&unlockStatus(r)==='pending'&&(unlockTargetDate(r)===target||unlockMissedDate(r)===missed))||null;
}
function latestMissedAttendanceUnlock(){return missedAttendanceUnlockRows().find(r=>key(r.user)===key(state.user?.username))||null}
function missedAttendanceLockForDate(date=todayKey(),unlockRowsOverride=null){
  if(!state.user||isAttendanceFreeUser()||todayAtt())return null;
  const target=String(date||todayKey()).slice(0,10), missed=previousDateKey(target), u=key(state.user.username);
  if(!validDateKey(target)||!validDateKey(missed))return null;
  if(hasAttendOn(missed))return null;
  if(approvedUnlockForMissedAttendance(target,missed,unlockRowsOverride))return null;
  return {id:missedAttendanceLockId(u,missed,target),user:u,missedDate:missed,targetDate:target,dateKey:target,reason:`Tidak ada absen ${dateID(missed)}`};
}
function missedAttendanceLockText(lock,action='transaksi'){
  const label=action==='absen'?'Absen':'Transaksi';
  return `${label} terkunci karena tidak ada absen tanggal ${dateID(lock?.missedDate)}. Minta buka fitur ke admin dulu.`;
}
function txBlockedMessage(){
  if(isTrialUser())return 'Mode trial siap transaksi';
  const lock=missedAttendanceLockForDate();
  if(lock)return missedAttendanceLockText(lock,'transaksi');
  if(isClosedToday())return 'Transaksi sudah closing hari ini';
  if(isRismaSpecialUser())return 'Risma siap transaksi tanpa absen';
  return 'Absen dulu sebelum transaksi';
}
function canTx(){if(isTrialUser())return true;if(missedAttendanceLockForDate())return false;if(isClosedToday())return false;if(isAttendanceFreeUser())return true;return !!todayAtt()}
function upsertRows(listName,rows){
  if(!Array.isArray(rows)||!rows.length)return;
  const map=new Map((state.data[listName]||[]).map(x=>[String(x.id||''),x]));
  rows.forEach(r=>{if(r&&r.id)map.set(String(r.id),r)});
  state.data[listName]=sortDesc([...map.values()]);
}
function serverClosingHit(c,u,d){
  return c&&c.closed===true&&c.canceled!==true&&!deleted(c)&&String(c.dateKey||'')===d&&((!c.user&&c.scope!=='user')||c.scope==='global'||key(c.user)===u);
}
async function verifyAbsenceOpenServer(dateKeyForWork=todayKey(),action='transaksi'){
  const u=key(state.user?.username), target=String(dateKeyForWork||todayKey()).slice(0,10), missed=previousDateKey(target);
  if(!u)return {ok:false,msg:'Sesi login tidak valid. Login ulang.'};
  if(isAttendanceFreeUser())return {ok:true};
  try{
    const reads=[
      getDocsFromServer(query(collection(db,'attendance'),where('user','==',u),where('dateKey','==',missed),limit(10))),
      getDocsFromServer(query(collection(db,STAFF_UNLOCK_TABLE),where('requestKind','==','unlock'),where('user','==',u),limit(LIMITS.unlockRequests)))
    ];
    if(action!=='absen')reads.push(getDocsFromServer(query(collection(db,'attendance'),where('user','==',u),where('dateKey','==',target),limit(10))));
    const snaps=await Promise.all(reads);
    const missedRows=snaps[0].docs.map(x=>({id:x.id,...x.data()})).filter(a=>!deleted(a)&&key(a.user)===u&&attDate(a)===missed);
    const unlockRowsFresh=snaps[1].docs.map(x=>({id:x.id,...x.data()})).filter(r=>isFeatureUnlockRequest(r)&&key(r.user)===u&&!deleted(r));
    const todayRows=action!=='absen'?snaps[2].docs.map(x=>({id:x.id,...x.data()})).filter(a=>!deleted(a)&&key(a.user)===u&&attDate(a)===target):[];
    upsertRows('att',[...missedRows,...todayRows]);
    upsertRows('unlockRequests',unlockRowsFresh);
    if(missedRows.length||todayRows.length||approvedUnlockForMissedAttendance(target,missed,unlockRowsFresh))return {ok:true};
    render();
    return {ok:false,msg:missedAttendanceLockText({missedDate:missed,targetDate:target},action)};
  }catch(e){
    console.error('cek buka fitur staff gagal',e);
    return {ok:false,msg:isPermissionError(e)?'Akses cek buka fitur ditolak Firebase.':'Gagal cek buka fitur. Pastikan internet aktif.'};
  }
}
async function verifyTransactionAllowedServer(dateKeyForTx=todayKey()){
  const u=key(state.user?.username), d=String(dateKeyForTx||todayKey()).slice(0,10);
  if(!u)return {ok:false,msg:'Sesi login tidak valid. Login ulang.'};
  if(isTrialUser())return {ok:true};
  const unlockCheck=await verifyAbsenceOpenServer(d,'transaksi');
  if(!unlockCheck.ok)return unlockCheck;
  try{
    const reads=[
      getDocsFromServer(query(collection(db,'closings'),where('dateKey','==',d),limit(80)))
    ];
    if(!isAttendanceFreeUser())reads.push(getDocsFromServer(query(collection(db,'attendance'),where('user','==',u),where('dateKey','==',d),limit(10))));
    const snaps=await Promise.all(reads);
    const closingRows=snaps[0].docs.map(x=>({id:x.id,...x.data()}));
    upsertRows('closings',closingRows);
    if(closingRows.some(c=>serverClosingHit(c,u,d))){
      render();
      return {ok:false,msg:'Transaksi sudah closing hari ini.'};
    }
    if(!isAttendanceFreeUser()){
      const attRows=snaps[1].docs.map(x=>({id:x.id,...x.data()}));
      upsertRows('att',attRows);
      const hasAtt=attRows.some(a=>!deleted(a)&&key(a.user)===u&&attDate(a)===d);
      if(!hasAtt){
        state.data.att=(state.data.att||[]).filter(a=>!(key(a.user)===u&&attDate(a)===d));
        render();
        return {ok:false,msg:'Absen hari ini belum ada / sudah dihapus admin.'};
      }
    }
    return {ok:true};
  }catch(e){
    console.error('cek ulang absen/closing gagal',e);
    return {ok:false,msg:isPermissionError(e)?'Akses cek absen/closing ditolak Firebase.':'Gagal cek ulang absen/closing. Pastikan internet aktif.'};
  }
}
async function latestBonusSnapshotForSave(){
  const u=key(state.user?.username);
  if(!u)return {ok:false,msg:'Sesi login tidak valid. Login ulang.'};
  try{
    const [userSnap,bonusSnap]=await Promise.all([
      getDocFromServer(doc(db,'users',u)),
      getDocFromServer(doc(db,'closings','__bonus_settings'))
    ]);
    if(!userSnap.exists())return {ok:false,msg:'User tidak ditemukan. Login ulang.'};
    const raw=userSnap.data()||{};
    const fresh={id:userSnap.id,username:raw.username||userSnap.id,...raw};
    if(isAdmin(fresh)||fresh.active===false||deleted(fresh))return {ok:false,msg:'Akun sudah nonaktif / tidak valid.'};
    const deviceInvalidReason=isDeviceSessionInvalid(fresh);
    if(deviceInvalidReason)return {ok:false,msg:deviceInvalidReason};
    lastDeviceSessionCheckAt=Date.now();
    const freshUser={
      username:key(fresh.username||u),
      name:fresh.name||u,
      pin:String(fresh.pin||state.user.pin||''),
      role:fresh.role||'staff',
      transactionBonusRate:hasBonusValue(fresh.transactionBonusRate)?Number(fresh.transactionBonusRate):null,
      transactionBonusPercent:hasBonusValue(fresh.transactionBonusPercent)?Number(fresh.transactionBonusPercent):null,
      closingBonusPerMinute:isDailyUser(fresh)?0:(hasBonusValue(fresh.closingBonusPerMinute)?Number(fresh.closingBonusPerMinute):null),
      dailyBonusRate:hasBonusValue(fresh.dailyBonusRate)?Number(fresh.dailyBonusRate):null,
      dailyBonusPercent:hasBonusValue(fresh.dailyBonusPercent)?Number(fresh.dailyBonusPercent):null,
      active:fresh.active!==false,
      deviceId:isDeviceFreeUser(fresh)?'':String(fresh.deviceId||state.user?.deviceId||''),
      deviceResetAtMs:Number(fresh.deviceResetAtMs||state.user?.deviceResetAtMs||0),
      ...trialSessionFields(fresh)
    };
    const freshBonus=bonusSnap.exists()?normalizeBonusSettings(bonusSnap.data()):normalizeBonusSettings(DEFAULT_BONUS);
    state.user=freshUser;
    state.data.bonus=freshBonus;
    localStorage.setItem(SESSION,JSON.stringify(freshUser));
    localStorage.setItem(LEGACY_SESSION,JSON.stringify({username:freshUser.username,pin:freshUser.pin}));
    const txRate=userTransactionBonusRate(freshUser);
    const txPercent=Number((txRate*100).toFixed(3));
    const closingSnapshot=userClosingBonusPerMinute(freshUser);
    const userRole=isDailyUser(freshUser)?'harian':'staff';
    return {ok:true,user:freshUser,bonus:freshBonus,txRate,txPercent,closingSnapshot,userRole};
  }catch(e){
    console.error('cek bonus terbaru gagal',e);
    return {ok:false,msg:isPermissionError(e)?'Akses cek bonus ditolak Firebase.':'Gagal cek bonus terbaru. Pastikan internet aktif.'};
  }
}

function statusPill(){if(isTrialUser())return `<span class="pill amber">Mode Trial</span>`;if(isClosedToday())return `<span class="pill amber">Sudah closing</span>`;if(isDaily())return `<span class="pill blue">Karyawan harian</span>`;const a=todayAtt();return a?`<span class="pill green">Sudah absen · ${timeID(ms(a))}</span>`:`<span class="pill red">Belum absen</span>`}
function locText(){if(!state.pos)return `<button class="pill blue" onclick="updateLocation()">Ambil GPS</button>`;const dist=distance(state.pos.lat,state.pos.lng,OFFICE_LOC.lat,OFFICE_LOC.lng);return dist<=RADIUS_LIMIT?`<span class="pill green">Radius · ${Math.round(dist)}m</span>`:`<span class="pill red">Diluar · ${Math.round(dist)}m</span>`}
let txProductDraftItems=[];
function txProductItemsFromText(text,fallback='Transaksi'){
  const lines=String(text||'').split(/\r?\n/).map(x=>x.replace(/\s+/g,' ').trim()).filter(Boolean);
  return lines.length?lines:[fallback];
}
function txProductTextFromItems(items,fallback='Transaksi'){
  const lines=(Array.isArray(items)?items:[]).map(x=>String(x||'').replace(/\s+/g,' ').trim()).filter(Boolean);
  return lines.length?lines.join('\n'):fallback;
}
function txProductDraftText(){return txProductTextFromItems(txProductDraftItems,'Transaksi')}
function txProductItemCount(note){return txProductItemsFromText(note,'Transaksi').length}
function txProductDisplayHtml(note){
  const items=txProductItemsFromText(note,'Transaksi');
  if(items.length===1)return esc(items[0]);
  return `<ol class="tx-product-render-list">${items.map(x=>`<li>${esc(x)}</li>`).join('')}</ol>`;
}
function txProductDetailHtml(note){
  const items=txProductItemsFromText(note,'Transaksi');
  return `<ol class="tx-product-render-list">${items.map(x=>`<li>${esc(x)}</li>`).join('')}</ol>`;
}
function renderTxProductDraft(){
  const list=$('txProductList'), add=$('txAddProductBtn'), input=$('txProductInput');
  if(add)add.disabled=!String(input?.value||'').trim();
  if(!list)return;
  if(!txProductDraftItems.length){
    list.innerHTML='<div class="tx-product-empty">Belum ada barang. Jika kosong, tersimpan sebagai Transaksi.</div>';
    return;
  }
  list.innerHTML=txProductDraftItems.map((item,i)=>`<div class="tx-product-row"><span class="tx-product-no">${i+1}</span><span class="tx-product-name">${esc(item)}</span><button type="button" class="tx-product-remove" onclick="removeTxProductItem(${i})">Hapus</button></div>`).join('');
}
function updateTxProductAddButton(){renderTxProductDraft()}
function addTxProductItem(){
  const input=$('txProductInput');
  const value=String(input?.value||'').replace(/\s+/g,' ').trim();
  if(!value){renderTxProductDraft();input?.focus?.();return}
  txProductDraftItems.push(value);
  if(input)input.value='';
  renderTxProductDraft();
  setTimeout(()=>input?.focus?.(),0);
}
function removeTxProductItem(index){
  const i=Number(index);
  if(Number.isInteger(i)&&i>=0&&i<txProductDraftItems.length)txProductDraftItems.splice(i,1);
  renderTxProductDraft();
  setTimeout(()=>$('txProductInput')?.focus?.(),0);
}
function handleTxProductKey(e){
  if(e?.key==='Enter'){
    e.preventDefault();
    addTxProductItem();
  }
}
function txPaymentBadgeHtml(v){
  const method=normalizeTxPaymentMethod(v);
  if(!method)return '';
  const label=txPaymentLabel(method);
  const cls=method==='cash'?'cash':'qris';
  return `<span class="tx-pay-badge ${cls}">${esc(label)}</span>`;
}
function txItem(t){
  const pending=t.pending===true;
  const tag=pending?'<span class="pending-tag">MENUNGGU SYNC</span>':'';
  const canDelete=txDate(t)===todayKey()&&!pending;
  const id=esc(t.id);
  const detailBtn=`<button class="btn sm" onclick="openTxDetail('${id}')" aria-label="Detail transaksi">Detail</button>`;
  const printBtn=!pending?`<button class="btn sm tx-print-btn" onclick="printReceiptFromTx('${id}')" aria-label="Cetak struk">Cetak</button>`:'';
  const deleteBtn=canDelete?`<button class="btn sm danger tx-del-btn" onclick="delTx('${id}')">Hapus</button>`:'';
  const action=pending?`<div class="tx-action-buttons">${detailBtn}<span class="pill amber">Sync</span></div>`:`<div class="tx-action-buttons">${detailBtn}${printBtn}${deleteBtn}</div>`;
  const payBadge=txPaymentBadgeHtml(t.paymentMethod||t.paymentLabel);
  const cashier=esc(t.name||state.user?.name||t.user||'Staff');
  return `<div class="tx-row"><div class="tx-name"><div class="tx-title"><span class="tx-title-main">${cashier} · ${timeID(ms(t))} · ${txProductItemCount(t.note)} item</span>${payBadge}</div><div class="tx-meta">${dateID(txDate(t))}${tag}</div></div><div class="tx-nominal">Rp ${rp(t.amount)}</div><div class="tx-action">${action}</div></div>`
}
function findTxById(id){return liveTx().find(t=>String(t.id)===String(id))}
function openTxDetail(id){
  const t=findTxById(id);
  if(!t)return toast('Transaksi tidak ditemukan',true);
  const pay=txPaymentLabel(t.paymentMethod||t.paymentLabel)||'Cash';
  const cashier=esc(t.name||state.user?.name||t.user||'Staff');
  const body=`<div class="tx-detail-box"><div class="tx-meta" style="margin-bottom:8px">${cashier} · ${dateID(txDate(t))} · ${timeID(ms(t))} · ${esc(pay)}</div><div class="tx-nominal" style="margin-bottom:10px">Rp ${rp(t.amount)}</div><div class="tx-meta" style="margin-bottom:6px">Daftar Barang</div>${txProductDetailHtml(t.note||'Transaksi')}</div>`;
  modal('Detail Transaksi',body,`<button type="button" class="btn primary" onpointerdown="closeModal(true);event.preventDefault()" ontouchstart="closeModal(true);event.preventDefault()" onclick="closeModal(true)">Kembali</button>`,'tx-modal');
}

function unlockHomeCard(){
  if(!state.user)return '';
  if(isAttendanceFreeUser())return '';
  const lock=missedAttendanceLockForDate();
  if(!lock)return '';
  const pending=pendingUnlockForMissedAttendance(lock.targetDate,lock.missedDate);
  const latestLine=`Tidak ada absen ${dateID(lock.missedDate)}${pending?' - menunggu admin buka fitur':''}`;
  const primary=`<button class="btn primary" onclick="event.stopPropagation();requestFeatureUnlock()" ${pending?'disabled':''}>${pending?'Menunggu Admin':'Minta Buka'}</button>`;
  return `<div class="card leave-home-card" onclick="go('unlock')" role="button" tabindex="0"><div class="leave-home-main"><div class="leave-home-ico">B</div><div class="leave-home-copy"><div class="label">Buka Fitur</div><div class="leave-home-title">Akses kerja terkunci</div><div class="hint">${esc(latestLine)}</div></div></div><div class="leave-home-actions">${primary}</div></div>`;
}
function notifyAdminFeatureUnlock(row={}){
  const staffName=String(row.name||state.user?.name||row.user||state.user?.username||'Staff');
  return notifyRockyAdmin(ROCKY_ADMIN_NOTIFY_UNLOCK_URL,{
    type:'feature_unlock_request',
    title:'Minta Buka Fitur Staff',
    staff:staffName,
    user:staffName,
    username:String(row.user||state.user?.username||''),
    name:staffName,
    action:'request',
    status:String(row.status||'pending'),
    lockId:String(row.parentLockId||row.lockId||''),
    missedDate:String(row.missedDate||''),
    targetDate:String(row.targetDate||row.dateKey||''),
    unlockId:String(row.id||row.clientId||''),
    reason:String(row.reason||row.note||''),
    note:String(row.reason||row.note||''),
    feature:String(row.feature||'attendance_transaction'),
    source:'staff'
  },'Notif buka fitur admin');
}
async function requestFeatureUnlock(){
  if(!state.user)return toast('Silakan login ulang');
  const lock=missedAttendanceLockForDate();
  if(!lock)return toast('Fitur sudah terbuka');
  if(pendingUnlockForMissedAttendance(lock.targetDate,lock.missedDate))return toast('Permintaan buka fitur masih menunggu admin');
  const note=cleanUnlockText(prompt('Alasan minta buka fitur?', `Saya tidak absen ${dateID(lock.missedDate)} dan ingin mulai kerja hari ini`)||'',220);
  if(note.length<5)return toast('Alasan wajib diisi');
  const u=key(state.user.username), nowMs=Date.now(), id=unlockRequestDocId(u,nowMs), staffName=state.user.name||u;
  const payload={
    requestKind:'unlock',
    action:'feature_unlock_request',
    type:'feature_unlock_request',
    featureUnlockRequest:true,
    lockKind:'missed_attendance',
    missingAttendance:true,
    parentLockId:String(lock.id||''),
    lockId:String(lock.id||''),
    missedDate:lock.missedDate,
    targetDate:lock.targetDate,
    user:u,
    name:staffName,
    feature:'attendance_transaction',
    reason:note,
    note,
    status:'pending',
    adminRead:false,
    dateKey:todayKey(),
    monthKey:monthKey(),
    createdAtMs:nowMs,
    createdBy:u,
    createdByName:staffName,
    clientId:id,
    source:'staff_app_missed_attendance_unlock'
  };
  const local={id,...payload,pending:true};
  state.data.unlockRequests=sortDesc([local,...(state.data.unlockRequests||[])]);
  render();
  showLoad(true);
  try{
    await setDoc(doc(db,STAFF_UNLOCK_TABLE,id),{...payload,createdAt:serverTimestamp(),syncedAt:serverTimestamp(),syncedAtMs:Date.now()},{merge:false});
    const saved={id,...payload,pending:false};
    state.data.unlockRequests=sortDesc((state.data.unlockRequests||[]).map(r=>String(r.id)===id?saved:r));
    removePending(id);
    await notifyAdminFeatureUnlock(saved);
    toast('Permintaan buka fitur terkirim');
  }catch(e){
    console.error(e);
    addPending({type:'feature_unlock_request',id,user:u,payload,createdAtMs:nowMs});
    toast('Koneksi gagal. Minta buka fitur akan sync otomatis.');
  }finally{
    showLoad(false);
    render();
  }
}
function unlockItem(r){
  const pending=r.pending===true?'<span class="pending-tag">MENUNGGU SYNC</span>':'';
  return `<div class="leave-row"><div class="leave-row-main"><div class="leave-row-title">Buka fitur kerja</div><div class="leave-row-meta">Tidak absen ${esc(dateID(unlockMissedDate(r)))} - ${esc(r.reason||r.note||'-')} ${pending}</div></div><div class="leave-row-actions"><span class="pill ${unlockStatusClass(r)}">${unlockStatusText(r)}</span></div></div>`;
}
function renderUnlockPage(){
  const rows=missedAttendanceUnlockRows(), pending=rows.filter(r=>unlockStatus(r)==='pending').length;
  const lock=missedAttendanceLockForDate();
  const waiting=lock?pendingUnlockForMissedAttendance(lock.targetDate,lock.missedDate):null;
  const lockCard=lock?`<div class="card warn leave-lock-card"><b>Akses kerja terkunci</b><div class="hint" style="margin-top:4px;color:inherit">${esc(missedAttendanceLockText(lock,isDaily()?'transaksi':'absen'))}</div><button class="btn primary block" style="margin-top:8px" onclick="requestFeatureUnlock()" ${waiting?'disabled':''}>${waiting?'Menunggu admin buka fitur':'Minta Buka Fitur'}</button></div>`:'';
  const body=rows.length?`<div class="leave-list">${rows.map(unlockItem).join('')}</div>`:'<div class="empty">Akses kerja normal. Belum ada permintaan buka fitur.</div>';
  const heroDisabled=lock&&waiting?'disabled':'';
  const heroLabel=lock?(waiting?'Menunggu Admin':'Minta Buka'):'Terbuka';
  page.innerHTML=`${top('Buka Fitur',`${pending} menunggu admin`)}${syncBar()}${lockCard}<div class="card leave-hero-card"><div><div class="label">Akses Kerja</div><div class="leave-hero-title">${lock?'Tidak absen hari sebelumnya':'Fitur terbuka'}</div><div class="hint">${lock?'Kirim permintaan ke admin untuk membuka absen dan transaksi.':'Tidak ada akses yang terkunci sekarang.'}</div></div><button class="btn primary" onclick="requestFeatureUnlock()" ${heroDisabled||(!lock?'disabled':'')}>${heroLabel}</button></div>${body}`;
}

function cleanReceiptLine(v,fallback='',max=42){
  const s=String(v??'').replace(/[\x00-\x1F\x7F]/g,' ').replace(/[\r\n\t]+/g,' ').replace(/\s+/g,' ').trim();
  const out=s||fallback;
  return String(out||'').slice(0,max);
}
function cleanReceiptMultiline(v,fallback='',maxLines=4,maxEach=42){
  const lines=String(v??'').split(/\r?\n/).map(x=>cleanReceiptLine(x,'',maxEach)).filter(Boolean).slice(0,maxLines);
  return lines.length?lines.join('\n'):fallback;
}
function normalizeReceiptSettings(raw={}){
  const m={...DEFAULT_RECEIPT_TEXT_SETTINGS,...(raw||{})};
  const feed=Math.round(Number(m.bottomFeedLines??DEFAULT_RECEIPT_TEXT_SETTINGS.bottomFeedLines));
  const bottomFeedLines=Math.max(0,Math.min(20,Number.isFinite(feed)?feed:DEFAULT_RECEIPT_TEXT_SETTINGS.bottomFeedLines));
  return {
    storeName:cleanReceiptLine(m.storeName,DEFAULT_RECEIPT_TEXT_SETTINGS.storeName),
    storeSubtext:cleanReceiptMultiline(m.storeSubtext,'',4),
    dailyTitle:cleanReceiptLine(m.dailyTitle,DEFAULT_RECEIPT_TEXT_SETTINGS.dailyTitle),
    dateLabel:cleanReceiptLine(m.dateLabel,DEFAULT_RECEIPT_TEXT_SETTINGS.dateLabel,12),
    cashierLabel:cleanReceiptLine(m.cashierLabel,DEFAULT_RECEIPT_TEXT_SETTINGS.cashierLabel,12),
    productLabel:cleanReceiptLine(m.productLabel,DEFAULT_RECEIPT_TEXT_SETTINGS.productLabel,12),
    totalLabel:cleanReceiptLine(m.totalLabel,DEFAULT_RECEIPT_TEXT_SETTINGS.totalLabel,12),
    countLabel:cleanReceiptLine(m.countLabel,DEFAULT_RECEIPT_TEXT_SETTINGS.countLabel,12),
    footerText:cleanReceiptMultiline(m.footerText,DEFAULT_RECEIPT_TEXT_SETTINGS.footerText,3),
    bottomFeedLines
  };
}
function receiptSettings(){return normalizeReceiptSettings(state.data.receiptSettings||DEFAULT_RECEIPT_TEXT_SETTINGS)}
function receiptBottomFeed(s=receiptSettings()){return '\n'.repeat(Math.max(0,Math.min(20,Number(s.bottomFeedLines||0))))}
function receiptLabel(label,width=8){const s=cleanReceiptLine(label,'-',12);return `${s.length<width?s.padEnd(width,' '):s} :`}
function receiptHeaderLines(s=receiptSettings()){const lines=[s.storeName];if(s.storeSubtext)lines.push(...String(s.storeSubtext).split(/\n/).filter(Boolean));return lines.join('\n')}
function receiptFooterLines(s=receiptSettings()){return String(s.footerText||DEFAULT_RECEIPT_TEXT_SETTINGS.footerText).split(/\n/).filter(Boolean).join('\n')}

function receiptProductParts(note){
  const lines=String(note||'Transaksi').split(/\r?\n/).map(x=>cleanReceiptLine(x,'',32)).filter(Boolean);
  return lines.length?lines:['Transaksi'];
}
function receiptProductNumbered(note,indent=''){
  return receiptProductParts(note).map((line,i)=>`${indent}${i+1}. ${line}`).join('\n');
}
function receiptProductBlock(note,{firstPrefix='',indent='',separator=''}={}){
  const lines=receiptProductParts(note);
  return lines.map((line,i)=>`${i===0?firstPrefix:indent}${i+1}. ${line}`).join('\n');
}

function receiptTextForTx(t){
  const s=receiptSettings();
  const tanggal=`${dateID(txDate(t))} ${timeID(ms(t))}`;
  const kasir=state.user?.name||state.user?.username||t.name||t.user||'-';
  const nominal=Number(t.amount||0);
  const pay=txPaymentLabel(t.paymentMethod||t.paymentLabel);
  const paymentLine=pay?`\n${receiptLabel('Bayar')} ${pay}`:'';
  const productLabel=cleanReceiptLine(s.productLabel||'Produk','Produk',12);
  const itemCount=receiptProductParts(t.note).length;
  return `${receiptHeaderLines(s)}
----------------------------------------
${receiptLabel(s.dateLabel)} ${tanggal}
${receiptLabel(s.cashierLabel)} ${kasir}
${productLabel}:
${receiptProductNumbered(t.note)}${paymentLine}

Jumlah item: ${itemCount}
Total Bayar: Rp ${rp(nominal)}
----------------------------------------
${receiptFooterLines(s)}${receiptBottomFeed(s)}
`;
}

let receiptPreviewText='';
let receiptPreviewTitle='Struk Transaksi';

function printReceiptFromTx(id){
  const t=findTxById(id);
  if(!t)return toast('Transaksi tidak ditemukan');
  const text=receiptTextForTx(t);
  openReceiptPreview(text,`Struk · ${dateID(txDate(t))}`);
}

function receiptTextForTodayTransactions(){
  const s=receiptSettings();
  const items=[...todayTx()].sort((a,b)=>ms(a)-ms(b));
  const tanggal=dateID(todayKey());
  const kasir=state.user?.name||state.user?.username||'-';
  const total=items.reduce((sum,t)=>sum+Number(t.amount||0),0);
  const rows=items.map((t,i)=>{
    const no=String(i+1).padStart(2,'0');
    const status=t.pending===true?' (MENUNGGU SYNC)':'';
    const produk=receiptProductNumbered(t.note,'    ');
    const pay=txPaymentLabel(t.paymentMethod||t.paymentLabel);
    const payLine=pay?`\n    Bayar: ${pay}`:'';
    return `${no}. ${timeID(ms(t))}${status}\n    Produk:\n${produk}${payLine}\n    Jumlah item: ${receiptProductParts(t.note).length}\n    Total Bayar: Rp ${rp(t.amount)}`;
  }).join('\n\n');
  return `${receiptHeaderLines(s)}
${s.dailyTitle}
----------------------------------------
${receiptLabel(s.dateLabel)} ${tanggal}
${receiptLabel(s.cashierLabel)} ${kasir}
${receiptLabel(s.countLabel)} ${items.length} trx
----------------------------------------
${rows}
----------------------------------------
TOTAL BAYAR Rp ${rp(total)}
----------------------------------------
${receiptFooterLines(s)}${receiptBottomFeed(s)}
`;
}

function printTodayTransactions(){
  const items=todayTx();
  if(!items.length)return toast('Belum ada transaksi hari ini');
  const text=receiptTextForTodayTransactions();
  openReceiptPreview(text,`Semua Transaksi · ${dateID(todayKey())}`);
}

function openReceiptPreview(text,title='Struk Transaksi'){
  receiptPreviewText=String(text||'');
  receiptPreviewTitle=String(title||'Struk Transaksi');
  const body=`<div class="hint">Preview struk dulu. Struk bisa dibagikan atau langsung dicetak ke printer Android.</div>
    <div class="receipt-preview-box"><pre class="receipt-preview-text">${esc(receiptPreviewText)}</pre></div>
    <div class="receipt-modal-actions">
      <button class="btn" onclick="shareReceiptText()">Bagikan</button>
      <button class="btn primary" onclick="nativePrintReceiptText()">Cetak</button>
    </div>`;
  modal(title,body,'','');
}

async function copyReceiptText(){
  const text=receiptPreviewText||'';
  if(!text)return toast('Struk kosong');
  try{
    if(window.Android&&typeof window.Android.copyReceipt==='function'){
      window.Android.copyReceipt(text);
      return;
    }
    if(navigator.clipboard&&navigator.clipboard.writeText){
      await navigator.clipboard.writeText(text);
    }else{
      const ta=document.createElement('textarea');
      ta.value=text;ta.style.position='fixed';ta.style.left='-9999px';document.body.appendChild(ta);ta.focus();ta.select();document.execCommand('copy');ta.remove();
    }
    toast('Struk disalin');
  }catch(e){
    console.log(text);
    toast('Gagal salin, struk tampil di console');
  }
}

async function shareReceiptText(){
  const text=receiptPreviewText||'';
  if(!text)return toast('Struk kosong');
  try{
    if(window.Android&&typeof window.Android.shareReceipt==='function'){
      window.Android.shareReceipt(text);
      return;
    }
    if(navigator.share){
      await navigator.share({title:receiptPreviewTitle,text});
      return;
    }
    await copyReceiptText();
    toast('Share belum tersedia, struk disalin');
  }catch(e){
    if(String(e?.name||'')!=='AbortError')toast('Gagal bagikan struk');
  }
}

function nativePrintReceiptText(){
  const text=receiptPreviewText||'';
  if(!text)return toast('Struk kosong');
  if(window.Android&&typeof window.Android.printReceipt==='function'){
    window.Android.printReceipt(text);
    return;
  }
  toast('Cetak Android aktif jika dibuka dari APK Android Studio');
}
function directPrintReceiptText(text,title='Struk Transaksi'){
  receiptPreviewText=String(text||'');
  receiptPreviewTitle=String(title||'Struk Transaksi');
  nativePrintReceiptText();
}

function browserPrintReceiptText(){
  const text=receiptPreviewText||'';
  if(!text)return toast('Struk kosong');
  if(window.Android&&typeof window.Android.printPdf==='function'){
    window.Android.printPdf(receiptPreviewTitle||'Struk Transaksi',text);
    return;
  }
  toast('Print/PDF tersedia di APK Android Studio. Untuk browser pakai Salin/Bagikan.');
}


/* ===== JADWAL SHALAT RINGAN - OFFLINE / TANPA API =====
   Koordinat jadwal mengikuti koordinat absen: OFFICE_LOC. */
const PRAYER_COORD={lat:OFFICE_LOC.lat,lng:OFFICE_LOC.lng,tz:7};
const PRAYER_CFG={fajrAngle:20,ishaAngle:18,asrFactor:1,dhuhrOffset:2,maghribOffset:2};
function prayerDayOfYear(d){
  const start=new Date(d.getFullYear(),0,0);
  return Math.floor((d-start)/86400000);
}
function prayerDeg(x){return x*Math.PI/180}
function prayerRad(x){return x*180/Math.PI}
function prayerPad(n){return String(Math.floor(Math.abs(n))).padStart(2,'0')}
function prayerFmtMin(min){
  min=((Math.round(min)%1440)+1440)%1440;
  const h=Math.floor(min/60),m=min%60;
  return `${prayerPad(h)}:${prayerPad(m)}`;
}
function prayerSolarData(date){
  const n=prayerDayOfYear(date);
  const g=2*Math.PI/365*(n-1);
  const eq=229.18*(0.000075+0.001868*Math.cos(g)-0.032077*Math.sin(g)-0.014615*Math.cos(2*g)-0.040849*Math.sin(2*g));
  const dec=0.006918-0.399912*Math.cos(g)+0.070257*Math.sin(g)-0.006758*Math.cos(2*g)+0.000907*Math.sin(2*g)-0.002697*Math.cos(3*g)+0.00148*Math.sin(3*g);
  const noon=720-4*PRAYER_COORD.lng-eq+PRAYER_COORD.tz*60;
  return {dec,noon};
}
function prayerHourAngle(latRad,dec,altDeg){
  const alt=prayerDeg(altDeg);
  const cosH=(Math.sin(alt)-Math.sin(latRad)*Math.sin(dec))/(Math.cos(latRad)*Math.cos(dec));
  return prayerRad(Math.acos(Math.max(-1,Math.min(1,cosH))));
}
function prayerTimesForDate(date=new Date()){
  const {dec,noon}=prayerSolarData(date),lat=prayerDeg(PRAYER_COORD.lat);
  const sunAngle=(angle,afterNoon)=>{const h=prayerHourAngle(lat,dec,-Math.abs(angle));return noon+(afterNoon?h*4:-h*4)};
  const sunrise=sunAngle(0.833,false), sunset=sunAngle(0.833,true);
  const diff=Math.abs(lat-dec);
  const asrAlt=prayerRad(Math.atan(1/(PRAYER_CFG.asrFactor+Math.tan(diff))));
  const asr=noon+prayerHourAngle(lat,dec,asrAlt)*4;
  return [
    {key:'subuh',name:'Subuh',min:sunAngle(PRAYER_CFG.fajrAngle,false)},
    {key:'dzuhur',name:'Dzuhur',min:noon+PRAYER_CFG.dhuhrOffset},
    {key:'ashar',name:'Ashar',min:asr},
    {key:'maghrib',name:'Maghrib',min:sunset+PRAYER_CFG.maghribOffset},
    {key:'isya',name:'Isya',min:sunAngle(PRAYER_CFG.ishaAngle,true)}
  ].map(x=>({...x,time:prayerFmtMin(x.min)}));
}
function nextPrayerInfo(){
  const now=new Date();
  const cur=now.getHours()*60+now.getMinutes();
  const today=prayerTimesForDate(now);
  let next=today.find(p=>Math.round(p.min)>cur);
  let label='hari ini';
  if(!next){
    const tomorrow=new Date(now.getFullYear(),now.getMonth(),now.getDate()+1);
    next=prayerTimesForDate(tomorrow)[0];
    label='besok';
  }
  return {next,label,today};
}
function prayerStatCard(extraClass=''){
  const info=nextPrayerInfo(), p=info.next;
  const tap=` onclick="requestOpenPrayerAyat()" role="button" tabindex="0" title="Double Klik untuk Putar ayat" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();requestOpenPrayerAyat()}"`;
  if(extraClass==='daily'){
    return `<div class="card prayer-daily-banner"${tap}><div class="prayer-daily-icon">☪</div><div class="prayer-daily-main"><div class="prayer-daily-label">Jadwal Shalat</div><div class="prayer-daily-foot">${p.name} ${info.label} · Double Klik untuk Putar ayat</div></div><div class="prayer-daily-time">${p.time}</div></div>`;
  }
  const cls=extraClass?` prayer-${extraClass}-card`:'';
  return `<div class="stat prayer-stat-card${cls}"${tap}><div class="stat-label">Jadwal Shalat</div><div class="stat-val">${p.time}</div><div class="stat-foot">${p.name} · Double Klik untuk Putar ayat</div></div>`;
}


const PRAYER_AYAT_TOTAL=6236;
const PRAYER_AYAT_CACHE_PREFIX='rocky_prayer_random_ayah_';
let prayerAyatAudio=null, prayerAyatAudioSrc='', prayerAyatPlaying=false, prayerAyatPaused=false, lastPrayerAyatTap=0, prayerAyatLoadingPromise=null;
function prayerAyatDateKey(d=new Date()){
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
function prayerAyatSeed(str){
  let h=2166136261;
  str=String(str||'');
  for(let i=0;i<str.length;i++)h=Math.imul(h^str.charCodeAt(i),16777619);
  return h>>>0;
}
function prayerAyatNumber(d=new Date()){
  const seed=prayerAyatSeed(prayerAyatDateKey(d));
  return (seed%PRAYER_AYAT_TOTAL)+1;
}
function prayerAyatCacheKey(dateKey=prayerAyatDateKey()){
  return PRAYER_AYAT_CACHE_PREFIX+dateKey;
}
function prayerAyatAudioUrl(num=prayerAyatNumber()){
  return `https://cdn.islamic.network/quran/audio/128/ar.alafasy/${num}.mp3`;
}
function readPrayerAyatCache(dateKey=prayerAyatDateKey()){
  try{
    const raw=JSON.parse(localStorage.getItem(prayerAyatCacheKey(dateKey))||'null');
    if(raw&&raw.dateKey===dateKey&&Number(raw.num)>0)return raw;
  }catch(e){}
  return null;
}
function savePrayerAyatCache(ayat){
  try{
    if(!ayat||!ayat.dateKey)return;
    localStorage.setItem(prayerAyatCacheKey(ayat.dateKey),JSON.stringify(ayat));
    Object.keys(localStorage).filter(k=>k.indexOf(PRAYER_AYAT_CACHE_PREFIX)===0&&k!==prayerAyatCacheKey(ayat.dateKey)).slice(0,-7).forEach(k=>localStorage.removeItem(k));
  }catch(e){}
}
function cleanPrayerAyatText(v){
  return String(v||'').replace(/<[^>]*>/g,'').replace(/\s+/g,' ').trim();
}
function normalizePrayerAyatApi(json,num,dateKey){
  const rows=Array.isArray(json?.data)?json.data:[];
  const ar=rows.find(x=>String(x?.edition?.language||'').toLowerCase()==='ar')||rows[0]||{};
  const id=rows.find(x=>String(x?.edition?.language||'').toLowerCase()==='id')||rows[1]||{};
  const surah=ar.surah||id.surah||{};
  const surahName=surah.englishName||surah.name||'Al-Qur’an';
  const ayahNo=ar.numberInSurah||id.numberInSurah||num;
  return {
    num,
    dateKey,
    ref:`QS. ${surahName}: ${ayahNo}`,
    arabic:cleanPrayerAyatText(ar.text)||'Teks Arab belum tersedia.',
    translation:cleanPrayerAyatText(id.text)||'Terjemahan belum tersedia.',
    audio:prayerAyatAudioUrl(num),
    loaded:true
  };
}
function currentPrayerAyat(){
  const dateKey=prayerAyatDateKey(), num=prayerAyatNumber();
  const cached=readPrayerAyatCache(dateKey);
  if(cached&&Number(cached.num)===num)return {...cached,audio:cached.audio||prayerAyatAudioUrl(num),loaded:true};
  return {
    num,
    dateKey,
    ref:`Ayat Harian #${num}`,
    arabic:'Memuat ayat harian...',
    translation:'Ayat Al-Qur’an dipilih otomatis setiap ganti tanggal.',
    audio:prayerAyatAudioUrl(num),
    loaded:false
  };
}
async function fetchPrayerAyatToday(force=false){
  const dateKey=prayerAyatDateKey(), num=prayerAyatNumber();
  if(!force){
    const cached=readPrayerAyatCache(dateKey);
    if(cached&&Number(cached.num)===num)return {...cached,audio:cached.audio||prayerAyatAudioUrl(num),loaded:true};
  }
  if(prayerAyatLoadingPromise)return prayerAyatLoadingPromise;
  prayerAyatLoadingPromise=(async()=>{
    try{
      const url=`https://api.alquran.cloud/v1/ayah/${num}/editions/quran-uthmani,id.indonesian`;
      const res=await fetch(url,{cache:'force-cache'});
      if(!res.ok)throw new Error('Gagal memuat ayat');
      const json=await res.json();
      const ayat=normalizePrayerAyatApi(json,num,dateKey);
      savePrayerAyatCache(ayat);
      return ayat;
    }finally{
      prayerAyatLoadingPromise=null;
    }
  })();
  return prayerAyatLoadingPromise;
}
function getPrayerAyatAudio(){
  const ayat=currentPrayerAyat();
  if(!prayerAyatAudio||prayerAyatAudioSrc!==ayat.audio){
    try{if(prayerAyatAudio){prayerAyatAudio.pause();prayerAyatAudio.currentTime=0}}catch(e){}
    prayerAyatAudioSrc=ayat.audio;
    prayerAyatAudio=new Audio(ayat.audio);
    prayerAyatAudio.preload='none';
    prayerAyatAudio.onplaying=()=>{prayerAyatPlaying=true;prayerAyatPaused=false;updatePrayerAyatUI()};
    prayerAyatAudio.onpause=()=>{prayerAyatPlaying=false;prayerAyatPaused=prayerAyatAudio&&prayerAyatAudio.currentTime>0&&!prayerAyatAudio.ended;updatePrayerAyatUI()};
    prayerAyatAudio.onended=()=>{prayerAyatPlaying=false;prayerAyatPaused=false;updatePrayerAyatUI()};
    prayerAyatAudio.onerror=()=>{prayerAyatPlaying=false;prayerAyatPaused=false;updatePrayerAyatUI('Audio tilawah gagal dimuat. Cek internet lalu coba Play lagi.')};
    prayerAyatAudio.onwaiting=()=>updatePrayerAyatUI('Memuat audio tilawah...');
  }
  return prayerAyatAudio;
}
function updatePrayerAyatUI(customStatus=''){
  const btn=$('prayerAyatPlayBtn'), st=$('prayerAyatStatus');
  const audio=prayerAyatAudio;
  const isPlaying=!!(audio&&!audio.paused&&!audio.ended);
  const isPaused=!!(audio&&audio.paused&&audio.currentTime>0&&!audio.ended);
  if(btn)btn.innerHTML=isPlaying?'⏸ Pause':'▶ Play';
  if(st)st.textContent=customStatus||(isPlaying?'Sedang memutar tilawah':(isPaused?'Dijeda':'Klik Play untuk memutar tilawah'));
}
function playPrayerAyat(){
  const audio=getPrayerAyatAudio();
  try{if(audio.ended)audio.currentTime=0}catch(e){}
  updatePrayerAyatUI('Memuat audio tilawah...');
  const playPromise=audio.play();
  if(playPromise&&typeof playPromise.catch==='function'){
    playPromise.catch(()=>{prayerAyatPlaying=false;prayerAyatPaused=false;updatePrayerAyatUI('Klik Play untuk memutar tilawah')});
  }
}
function togglePrayerAyat(){
  const audio=getPrayerAyatAudio();
  try{
    if(!audio.paused&&!audio.ended){audio.pause();updatePrayerAyatUI();return}
    playPrayerAyat();
  }catch(e){toast('Gagal memutar audio')}
}
function stopPrayerAyat(closePopup=true){
  try{
    if(prayerAyatAudio){
      prayerAyatAudio.pause();
      prayerAyatAudio.currentTime=0;
    }
  }catch(e){}
  prayerAyatPlaying=false;prayerAyatPaused=false;updatePrayerAyatUI();
  if(closePopup)closeModal(true);
}
function prayerAyatModalBody(ayat){
  const loading=!ayat.loaded;
  return `<div class="prayer-ayat-box"><div class="prayer-ayat-icon">P</div><div id="prayerAyatRef" class="prayer-ayat-ref">${esc(ayat.ref)}</div><div id="prayerAyatArabic" class="prayer-ayat-arabic">${esc(ayat.arabic)}</div><div id="prayerAyatTranslation" class="prayer-ayat-translation">“${esc(ayat.translation)}”</div><div id="prayerAyatStatus" class="prayer-ayat-status">${loading?'Memuat teks ayat harian...':'Klik Play untuk memutar tilawah'}</div><div class="prayer-ayat-controls"><button id="prayerAyatPlayBtn" type="button" class="btn primary" onclick="togglePrayerAyat()">▶ Play</button><button type="button" class="btn danger" onclick="stopPrayerAyat(true)">Close</button></div></div>`;
}
function updatePrayerAyatContent(ayat){
  const ref=$('prayerAyatRef'), ar=$('prayerAyatArabic'), tr=$('prayerAyatTranslation');
  if(ref)ref.textContent=ayat.ref;
  if(ar)ar.textContent=ayat.arabic;
  if(tr)tr.textContent=`“${ayat.translation}”`;
}
async function loadPrayerAyatForModal(){
  try{
    updatePrayerAyatUI('Memuat teks ayat harian...');
    const ayat=await fetchPrayerAyatToday(false);
    updatePrayerAyatContent(ayat);
    updatePrayerAyatUI();
  }catch(e){
    updatePrayerAyatUI('Teks ayat gagal dimuat. Audio masih bisa dicoba jika internet tersedia.');
  }
}
function requestOpenPrayerAyat(){
  const now=Date.now(), ayat=currentPrayerAyat();
  if(now-lastPrayerAyatTap>2500){
    lastPrayerAyatTap=now;
    toast(`Klik sekali lagi untuk memutar ${ayat.ref}`);
    return;
  }
  lastPrayerAyatTap=0;
  openPrayerAyat(true);
}
function openPrayerAyat(autoPlay=false){
  const ayat=currentPrayerAyat();
  modal('Ayat Hari Ini',prayerAyatModalBody(ayat),'','prayer-ayat-modal');
  updatePrayerAyatUI(ayat.loaded?'Klik Play untuk memutar tilawah':'Memuat teks ayat harian...');
  loadPrayerAyatForModal();
  if(autoPlay)playPrayerAyat();
}

function home(){const tx=todayTx(), a=todayAtt(), c=todayClosing(), emptyStockCard=stockEmptyQuickCard();
  if(isDaily()){
    const todayTxBonus=txBonusSum(tx), manualToday=manualBonusToday(), todayBonus=todayTxBonus+manualToday;
    const manualCard='';
    page.innerHTML=`${top('Mode Harian',state.user?.name||'Karyawan Harian')}${trialModeCard()}${manualBonusNoticeCard()}<div class="hero daily-income-hero"><div class="kicker">Pendapatan Hari Ini</div><div class="big">Rp ${rp(todayTotal())}</div><div class="sub hero-meta-line">${dateID(todayKey()).slice(0,5)} · ${tx.length} trx</div>${syncHeroLine()}</div>${emptyStockCard}<div class="grid2" style="margin-top:8px"><div class="stat"><div class="stat-label">Transaksi Hari Ini</div><div class="stat-val">${tx.length}</div><div class="stat-foot">hari ini</div></div><div class="stat"><div class="stat-label">Gaji Hari Ini</div><div class="stat-val">Rp ${rp(todayBonus)}</div><div class="stat-foot">hari ini</div></div></div>${prayerStatCard('daily')}${manualCard}${staffDailyNoteCard()}<div class="bonus-refresh-note"><span class="note-alert-icon">!</span><span><b>Perhatian:</b> klik ikon refresh saat aplikasi error atau saat Transaksi gagal di lakukan.<br><span style="display:block;margin-top:2px">Copyright © 2026 Program by Alfajri – Rocky Hijab.</span></span></div>${headerIconGuide()}`;
    return;
  }
  const mainLabel=c?'Closing Hari Ini':'Absen Hari Ini';const mainValue=c?closingTimeText(c):(a?timeID(ms(a)):'--:--');const mainFoot=c?'sudah closing':(a?'sudah absen':'belum absen');const mainClass=c?'closed':(a?'ok':'wait');page.innerHTML=`${top('Mode Staff',state.user?.name||'Karyawan Staff')}${trialModeCard()}${targetReachedNoticeCard()}${manualBonusNoticeCard()}${averageAttendanceCard()}${closingNotice()}<div class="hero"><div class="kicker">Pendapatan Hari Ini</div><div class="big">Rp ${rp(todayTotal())}</div><div class="sub hero-meta-line">${dateID(todayKey()).slice(0,5)} · ${tx.length} trx</div>${syncHeroLine()}</div>${dailyTargetCard()}${emptyStockCard}<div class="grid2 staff-stat-grid" style="margin-top:8px"><div class="stat att-status ${mainClass}"><div class="stat-label">${mainLabel}</div><div class="stat-val">${mainValue}</div><div class="stat-foot">${mainFoot}</div></div>${prayerStatCard()}<div class="stat"><div class="stat-label">Transaksi</div><div class="stat-val">${tx.length}</div><div class="stat-foot">hari ini</div></div><div class="stat"><div class="stat-label">Total Masuk Kerja</div><div class="stat-val">${monthAttendDays()} <span style="font-size:13px;font-weight:850;color:var(--muted);letter-spacing:0">Hari</span></div><div class="stat-foot">${monthID(monthKey())}</div></div></div><div class="card bonus-plus-card" style="margin-top:8px"><div class="bonus-plus-head"><div class="label">Bonus Bulan Ini ++</div><button class="refresh-icon-btn" onclick="refresh()" aria-label="Refresh bonus"><svg viewBox="0 0 24 24"><path d="M21 12a9 9 0 0 1-15.2 6.5"/><path d="M3 12A9 9 0 0 1 18.2 5.5"/><path d="M18 2v4h-4"/><path d="M6 22v-4h4"/></svg></button></div><div class="big" style="color:var(--blue)">Rp ${rp(totalBonus())}</div><div class="bonus-note">Bonus bulan ${monthID(monthKey())}</div>${todayClosingBonusInline()}</div>${staffDailyNoteCard()}<div class="bonus-refresh-note"><span class="note-alert-icon">!</span><span><b>Perhatian:</b> klik ikon refresh saat aplikasi error atau saat Transaksi gagal di lakukan.<br><span style="display:block;margin-top:2px">Copyright © 2026 Program by Alfajri – Rocky Hijab.</span></span></div>${headerIconGuide()}`}
function history(){const items=sortDesc(todayTx());const printAllCard=items.length?`<div class="card" style="margin-bottom:8px"><button class="btn primary block" onclick="printTodayTransactions()">🧾 Cetak Semua Transaksi Hari Ini</button><div class="hint" style="margin-top:6px">Cetak ${items.length} transaksi hari ini dalam 1 struk.</div></div>`:'';const body=items.length?`<div class="tx-table"><div class="tx-head"><span>Nama / Jam</span><span>Nominal</span><span style="text-align:right">Aksi</span></div><div class="tx-list">${items.map(txItem).join('')}</div></div>`:'<div class="empty">Belum ada transaksi hari ini.</div>';page.innerHTML=`${top('Riwayat Hari Ini',`${items.length} transaksi · Rp ${rp(todayTotal())}`)}${syncBar()}${printAllCard}${body}`}

const __baseHomeWithUnlockCard=home;
home=function(){
  __baseHomeWithUnlockCard();
  if(isRismaSpecialUser()&&page){
    const stat=page.querySelector('.att-status');
    if(stat){
      stat.classList.remove('wait','closed');
      stat.classList.add('ok');
      const label=stat.querySelector('.stat-label'),val=stat.querySelector('.stat-val'),foot=stat.querySelector('.stat-foot');
      if(label)label.textContent='Akses Hari Ini';
      if(val)val.textContent='OK';
      if(foot)foot.textContent='bebas absen';
    }
  }
  const card=unlockHomeCard();
  if(!card||!page)return;
  const stock=page.querySelector('.stock-empty-card');
  if(stock)stock.insertAdjacentHTML('afterend',card);
  else page.insertAdjacentHTML('beforeend',card);
};

function cleanBelanjaText(v){return String(v||'').replace(/[<>]/g,'').replace(/\s+/g,' ').trim()}
function belanjaSlug(v){return cleanBelanjaText(v).toLowerCase().replace(/\s+/g,'_').replace(/[^a-z0-9_\-.]/g,'').slice(0,70)||'item'}
function getBelanjakuBridgeQueue(){try{const raw=JSON.parse(localStorage.getItem(BELANJAKU_BRIDGE_KEY)||'[]');return Array.isArray(raw)?raw.filter(Boolean):[]}catch(e){return []}}
function setBelanjakuBridgeQueue(list){try{localStorage.setItem(BELANJAKU_BRIDGE_KEY,JSON.stringify((list||[]).slice(-250)))}catch(e){}}
function saveBelanjakuBridgeQueue(item){const list=getBelanjakuBridgeQueue().filter(x=>String(x.id)!==String(item.id));list.push(item);setBelanjakuBridgeQueue(list)}
function stockEmptyQuickCard(){return `<div class="card stock-empty-card"><div class="stock-empty-left"><div class="stock-empty-icon">✘</div><div class="stock-empty-copy"><div class="label">Barang Kosong</div><div class="hint">Laporkan Segera Apabila Ada Stok Barang Yang Menipis Atau Kosong</div></div></div><button class="btn stock-empty-btn" type="button" onclick="openEmptyStock()">Laporkan</button></div>`}
function normalizeEmptyStockVariants(list){
  const merged=[], seen=new Set();
  (Array.isArray(list)?list:[]).forEach(row=>{
    const colorName=cleanBelanjaText(row?.colorName||row?.color||'');
    if(!colorName)return;
    const k=colorName.toLowerCase();
    if(seen.has(k))return;
    seen.add(k);
    // Qty sengaja tidak ditampilkan di STF. Nilai 1 hanya untuk kompatibilitas data Belanjaku.
    merged.push({colorName,qty:1,done:false});
  });
  return merged;
}
function addEmptyStockVariantRow(colorName=''){
  const list=$('emptyStockVariantList');
  if(!list)return;
  const row=document.createElement('div');
  row.className='empty-stock-variant-row';
  row.style.cssText='display:grid;grid-template-columns:minmax(0,1fr) 36px;gap:7px;align-items:end;margin-bottom:8px';
  row.innerHTML=`<div><div class="label" style="font-size:10px;margin-bottom:4px">Warna</div><input class="empty-stock-color" autocomplete="off" placeholder="Warna"></div><button type="button" class="btn danger" style="min-height:42px;padding:0;border-radius:10px" onclick="removeEmptyStockVariantRow(this)">×</button>`;
  const color=row.querySelector('.empty-stock-color');
  if(color)color.value=cleanBelanjaText(colorName);
  list.appendChild(row);
  setTimeout(()=>color?.focus({preventScroll:true}),40);
  return row;
}
function removeEmptyStockVariantRow(btn){
  const list=$('emptyStockVariantList'), row=btn?.closest?.('.empty-stock-variant-row');
  if(!list||!row)return;
  const rows=[...list.querySelectorAll('.empty-stock-variant-row')];
  if(rows.length<=1){
    const color=row.querySelector('.empty-stock-color');
    if(color)color.value='';
    color?.focus();
  }else row.remove();
}
function getEmptyStockVariants(){
  return normalizeEmptyStockVariants([...document.querySelectorAll('.empty-stock-variant-row')].map(row=>({
    colorName:row.querySelector('.empty-stock-color')?.value
  })));
}
function buildBelanjakuEmptyItem({name,variants}){
  const now=Date.now(), nowISO=new Date(now).toISOString(), u=key(state.user?.username||'staff'), displayName=state.user?.name||state.user?.username||'Staff';
  const cleanName=cleanBelanjaText(name);
  const cleanVariants=normalizeEmptyStockVariants(variants);
  const id='stafku_empty_'+belanjaSlug(u)+'_'+now+'_'+Math.random().toString(36).slice(2,7);
  const variantText=cleanVariants.map(v=>v.colorName).join(', ');
  const baseNote=`Dari Stafku (${displayName}) - barang kosong`;
  return {
    id,
    name:cleanName,
    supplierName:'',
    kodiPrice:0,
    variants:cleanVariants,
    unit:'pcs',
    priority:'Tinggi',
    note:variantText?`${baseNote}. Warna: ${variantText}`:baseNote,
    done:false,
    source:'stafku_barang_kosong',
    staffStatus:'inbox',
    staffRequest:true,
    createdBy:u,
    createdByName:displayName,
    createdAt:nowISO,
    updatedAt:nowISO,
    createdAtMs:now,
    updatedAtMs:now
  };
}
async function sendBelanjakuItemToSupabase(item){
  if(typeof fetch!=='function')return false;
  try{
    const url=`${BELANJAKU_SUPABASE_URL.replace(/\/$/,'')}/rest/v1/${BELANJAKU_SUPABASE_ITEMS_TABLE}?on_conflict=id`;
    const res=await fetch(url,{method:'POST',headers:{apikey:BELANJAKU_SUPABASE_ANON_KEY,Authorization:`Bearer ${BELANJAKU_SUPABASE_ANON_KEY}`,'Content-Type':'application/json',Prefer:'resolution=merge-duplicates,return=minimal'},body:JSON.stringify({id:item.id,data:item,updated_at:item.updatedAt||new Date().toISOString()})});
    if(!res.ok)throw new Error('Supabase '+res.status+': '+await res.text());
    return true;
  }catch(e){console.warn('Belanjaku bridge belum terkirim',e);return false;}
}
async function syncBelanjakuBridgeQueue(){
  const queue=getBelanjakuBridgeQueue();
  if(!queue.length)return {sent:0,remaining:0};
  const remaining=[];let sent=0;
  for(const item of queue){
    const ok=await sendBelanjakuItemToSupabase(item);
    if(ok)sent+=1;else remaining.push(item);
  }
  setBelanjakuBridgeQueue(remaining);
  return {sent,remaining:remaining.length};
}
function openEmptyStock(){
  if(!state.user)return toast('Silakan login ulang');
  modal('Barang Kosong',`<div class="tx-chip"><span>Terima Kasih Sudah Ikut Berkontribusi 🔥🔥🔥🔥</span><b>${timeNow()} WIB</b></div><div class="field"><div class="label">Nama Barang</div><input id="emptyStockName" autocomplete="off" placeholder="isi Nama Barang"></div><div class="field"><div class="between" style="margin-bottom:7px;align-items:center"><div class="label" style="margin:0">Warna</div><button type="button" class="btn warn sm" onclick="addEmptyStockVariantRow()">+ Warna</button></div><div id="emptyStockVariantList"><div class="empty-stock-variant-row" style="display:grid;grid-template-columns:minmax(0,1fr) 36px;gap:7px;align-items:end;margin-bottom:8px"><div><div class="label" style="font-size:10px;margin-bottom:4px">Warna</div><input class="empty-stock-color" autocomplete="off" placeholder="Warna"></div><button type="button" class="btn danger" style="min-height:42px;padding:0;border-radius:10px" onclick="removeEmptyStockVariantRow(this)">×</button></div></div><div class="hint" style="display:block;margin-top:2px">Isi nama barang dan warna saja</div></div><div class="tx-note-mini">Setelah dikirim, barang masuk halaman <b>Kiriman Staff</b><b</b>.</div>`,`<button type="button" class="btn danger" onclick="closeModal(true)">Batal</button><button type="button" class="btn warn" onclick="submitEmptyStock()">Kirim</button>`,'tx-modal');
  setTimeout(()=>$('emptyStockName')?.focus(),80);
}
async function submitEmptyStock(){
  const name=cleanBelanjaText($('emptyStockName')?.value), variants=getEmptyStockVariants();
  if(!name)return toast('Nama barang wajib diisi');
  if(!variants.length)return toast('Minimal isi 1 warna');
  const item=buildBelanjakuEmptyItem({name,variants});
  saveBelanjakuBridgeQueue(item);
  closeModal(true);
  toast(`${variants.length} warna disiapkan untuk Belanjaku`);
  const result=await syncBelanjakuBridgeQueue();
  if(result.sent>0)toast('Barang kosong multi warna masuk ke Belanjaku');
  else toast('Tersimpan lokal, buka Belanjaku lalu refresh');
}

async function pinAsk(msg='Masukkan PIN'){return new Promise(res=>{window.__pin=v=>{closeModal();delete window.__pin;res(String(v||''))};modal('Verifikasi PIN',`<div class="hint" style="margin-bottom:8px">${esc(msg)}</div><input id="pinx" type="password" inputmode="numeric" pattern="[0-9]*" autocomplete="one-time-code" placeholder="PIN" style="text-align:center;font-size:22px;font-weight:950">`,`<button class="btn" onclick="__pin('')">Batal</button><button class="btn primary" onclick="__pin(document.getElementById('pinx').value)">Lanjut</button>`);setTimeout(()=>$('pinx')?.focus(),60)})}
const TX_PAYMENT_METHODS={cash:'Cash',qris_transfer:'QRIS / Transfer'};
let pendingTxDraft=null;
let txPaymentSubmitting=false;
function normalizeTxPaymentMethod(v){
  const s=key(v).replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,'');
  if(s==='cash')return 'cash';
  if(s==='qris'||s==='transfer'||s==='qris_transfer'||s==='qris_transfer_bank'||s==='qris_dan_transfer')return 'qris_transfer';
  return '';
}
function txPaymentLabel(v){
  const m=normalizeTxPaymentMethod(v);
  if(m)return TX_PAYMENT_METHODS[m];
  const s=String(v||'').trim();
  return s||'';
}

function notifyAdminNewTransaction(tx={}){
  try{
    if(!ROCKY_ADMIN_NOTIFY_WORKER_URL||!ROCKY_ADMIN_NOTIFY_SECRET)return;
    const amount=Number(tx.amount||0);
    const staffName=String(tx.name||tx.user||state.user?.name||state.user?.username||'Staff');
    const note=String(tx.note||'Transaksi baru');
    fetch(ROCKY_ADMIN_NOTIFY_WORKER_URL,{
      method:'POST',
      headers:{'Content-Type':'application/json','X-Notify-Secret':ROCKY_ADMIN_NOTIFY_SECRET},
      body:JSON.stringify({
        user:staffName,
        username:String(tx.user||state.user?.username||''),
        name:staffName,
        amount,
        note,
        paymentMethod:String(tx.paymentMethod||''),
        paymentLabel:String(tx.paymentLabel||txPaymentLabel(tx.paymentMethod)||''),
        transactionId:String(tx.id||tx.clientId||''),
        dateKey:String(tx.dateKey||todayKey())
      })
    }).then(async r=>{
      const data=await r.json().catch(()=>null);
      if(!r.ok||data?.ok===false)console.warn('Notif admin gagal:',data||r.status);
      return data;
    }).catch(err=>console.warn('Notif admin gagal:',err?.message||err));
  }catch(err){
    console.warn('Notif admin gagal:',err?.message||err);
  }
}

async function notifyRockyAdmin(url,payload={},label='Notif admin'){
  try{
    if(!url||!ROCKY_ADMIN_NOTIFY_SECRET)return null;
    const r=await fetch(url,{
      method:'POST',
      keepalive:true,
      headers:{'Content-Type':'application/json','X-Notify-Secret':ROCKY_ADMIN_NOTIFY_SECRET},
      body:JSON.stringify(payload||{})
    });
    const data=await r.json().catch(()=>null);
    if(!r.ok||data?.ok===false)console.warn(label+' gagal:',data||r.status);
    return data;
  }catch(err){
    console.warn(label+' gagal:',err?.message||err);
    return null;
  }
}
function notifyAdminAttendance(att={},action='masuk'){
  try{
    const createdMs=Number(att.createdAtMs||ms(att)||Date.now());
    const staffName=String(att.name||state.user?.name||att.user||state.user?.username||'Staff');
    return notifyRockyAdmin(ROCKY_ADMIN_NOTIFY_ATTENDANCE_URL,{
      type:'attendance',
      staff:staffName,
      user:staffName,
      username:String(att.user||state.user?.username||''),
      name:staffName,
      action:String(action||'masuk'),
      time:String(att.time||timeID(createdMs)||timeNow()),
      dateKey:String(att.dateKey||todayKey(new Date(createdMs))),
      attendanceId:String(att.id||att.clientId||''),
      source:'staff',
      note:String(att.note||'Absen dari aplikasi staff')
    },'Notif absen admin');
  }catch(err){
    console.warn('Notif absen admin gagal:',err?.message||err);
    return null;
  }
}
function notifyAdminTransactionDelete(tx={}){
  try{
    const staffName=String(tx.deletedByName||state.user?.name||tx.name||tx.user||state.user?.username||'Staff');
    const ownerName=String(tx.name||tx.user||'');
    const paymentLabel=String(tx.paymentLabel||txPaymentLabel(tx.paymentMethod)||tx.payment||'');
    const amount=Number(tx.amount||0);
    const desc=String(tx.note||tx.desc||'Transaksi');
    return notifyRockyAdmin(ROCKY_ADMIN_NOTIFY_TRANSACTION_DELETE_URL,{
      type:'delete_transaction',
      title:'Transaksi Dihapus',
      staff:staffName,
      user:staffName,
      username:String(tx.deletedBy||state.user?.username||tx.user||''),
      name:staffName,
      deletedBy:staffName,
      targetUser:ownerName,
      transactionUser:ownerName,
      amount,
      desc,
      note:desc,
      payment:String(paymentLabel||tx.paymentMethod||''),
      paymentMethod:String(tx.paymentMethod||''),
      paymentLabel,
      transactionId:String(tx.id||tx.clientId||''),
      dateKey:String(tx.dateKey||txDate(tx)||todayKey()),
      time:timeNow(),
      source:'staff'
    },'Notif hapus transaksi admin');
  }catch(err){
    console.warn('Notif hapus transaksi admin gagal:',err?.message||err);
    return null;
  }
}
function openTxLegacy(draft={}){
  if(!state.user)return;
  if(!canTx())return toast(txBlockedMessage());
  primeAppSounds();
  const defaultNote=String(draft?.note||'');
  const defaultAmount=Number(draft?.amount||0);
  const amountValue=defaultAmount>0?`Rp ${rp(defaultAmount)}`:'';
  modal('Transaksi Baru',`<div class="tx-chip"><span>⚡ Input cepat</span><b>${timeNow()} WIB</b></div><div class="field"><div class="label">Nama Produk</div><textarea id="txn" class="tx-product-input" placeholder="Produk...&#10;&#10;&#10;" autocomplete="off" rows="4">${esc(defaultNote)}</textarea></div><div class="field"><div class="label">Nominal Rupiah</div><input id="txa" class="tx-amount-input" type="tel" inputmode="numeric" pattern="[0-9]*" placeholder="Rp 0" value="${esc(amountValue)}" oninput="formatRupiahInput(this)" style="text-align:right;font-size:25px;font-weight:950"></div><div class="tx-note-mini">Klik <b>Simpan</b> atau <b>Cetak</b>, lalu pilih metode pembayaran dulu. Transaksi baru dianggap sukses setelah memilih <b>Cash</b> atau <b>QRIS / Transfer</b>.</div>`,`<div style="display:grid;grid-template-columns:1fr 1fr;gap:7px;width:100%"><button type="button" class="btn danger" onpointerdown="closeModal(true);event.preventDefault()" ontouchstart="closeModal(true);event.preventDefault()" onclick="closeModal(true)" style="grid-column:1 / -1">Batal</button><button class="btn success" onclick="saveTx(true)" style="background:var(--green);border-color:var(--green);color:#fff">Cetak</button><button class="btn primary" onclick="saveTx(false)">Simpan</button></div>`,'tx-modal')
}
function openTx(draft={}){
  if(!state.user)return;
  if(!canTx())return toast(txBlockedMessage());
  primeAppSounds();
  const defaultNote=String(draft?.note||'').trim();
  txProductDraftItems=defaultNote&&defaultNote!=='Transaksi'?txProductItemsFromText(defaultNote,'Transaksi'):[];
  const defaultAmount=Number(draft?.amount||0);
  const amountValue=defaultAmount>0?`Rp ${rp(defaultAmount)}`:'';
  const body=`<div class="tx-chip"><span>Input cepat</span><b>${timeNow()} WIB</b></div>
    <div class="field tx-product-builder">
      <div class="label">Rincian Barang</div>
      <div class="tx-product-entry">
        <input id="txProductInput" class="tx-product-input" type="text" placeholder="Nama barang..." autocomplete="off" oninput="updateTxProductAddButton()" onkeydown="handleTxProductKey(event)">
        <button id="txAddProductBtn" type="button" class="btn primary tx-product-add" onclick="addTxProductItem()" disabled>+ Tambah Barang</button>
      </div>
      <div class="tx-product-list-title">Daftar Barang:</div>
      <div id="txProductList" class="tx-product-list"></div>
    </div>
    <div class="field"><div class="label">Total Bayar</div><input id="txa" class="tx-amount-input" type="tel" inputmode="numeric" pattern="[0-9]*" placeholder="Rp 0" value="${esc(amountValue)}" oninput="formatRupiahInput(this)" style="text-align:right;font-size:25px;font-weight:950"></div>
    <div class="tx-note-mini">Klik <b>Simpan</b> atau <b>Cetak</b>, lalu pilih metode pembayaran dulu. Transaksi baru dianggap sukses setelah memilih <b>Cash</b> atau <b>QRIS / Transfer</b>.</div>`;
  const actions=`<div style="display:grid;grid-template-columns:1fr 1fr;gap:7px;width:100%"><button type="button" class="btn danger" onpointerdown="closeModal(true);event.preventDefault()" ontouchstart="closeModal(true);event.preventDefault()" onclick="closeModal(true)" style="grid-column:1 / -1">Batal</button><button class="btn success" onclick="saveTx(true)" style="background:var(--green);border-color:var(--green);color:#fff">Cetak</button><button class="btn primary" onclick="saveTx(false)">Simpan</button></div>`;
  modal('Transaksi Baru',body,actions,'tx-modal');
  setTimeout(()=>{renderTxProductDraft();$('txProductInput')?.focus?.();},30);
}
function openTxPaymentChoice(draft){
  txPaymentSubmitting=false;
  pendingTxDraft={
    printAfterSave:Boolean(draft?.printAfterSave),
    amount:Number(draft?.amount||0),
    note:String(draft?.note||'Transaksi').trim()||'Transaksi'
  };
  modal('Pilih Pembayaran',`<div class="payment-simple-choices"><button type="button" data-payment-choice="cash" class="payment-simple-btn payment-simple-cash" onpointerdown="confirmTxPayment('cash',this,event)" ontouchstart="confirmTxPayment('cash',this,event)" onclick="confirmTxPayment('cash',this,event)"><span>🌟 Cash 🌟</span></button><button type="button" data-payment-choice="qris_transfer" class="payment-simple-btn payment-simple-qris" onpointerdown="confirmTxPayment('qris_transfer',this,event)" ontouchstart="confirmTxPayment('qris_transfer',this,event)" onclick="confirmTxPayment('qris_transfer',this,event)"><span>Qris / Transfer</span></button></div>`,``,'tx-modal payment-picker-modal')
}

function cancelTxPaymentChoice(){
  txPaymentSubmitting=false;
  pendingTxDraft=null;
  closeModal(true);
}
function reopenTxDraft(){
  txPaymentSubmitting=false;
  const draft=pendingTxDraft?{...pendingTxDraft}:{};
  openTx(draft);
}
async function confirmTxPayment(paymentMethod,btn,event){
  try{if(event&&typeof event.preventDefault==='function')event.preventDefault()}catch(e){}
  try{if(event&&typeof event.stopPropagation==='function')event.stopPropagation()}catch(e){}
  if(txPaymentSubmitting)return;
  txPaymentSubmitting=true;
  const buttons=[...document.querySelectorAll('[data-payment-choice]')];
  buttons.forEach(b=>{
    b.disabled=true;
    b.setAttribute('aria-busy','true');
    b.style.opacity='.72';
  });
  if(btn){
    btn.dataset.originalText=btn.dataset.originalText||btn.textContent;
    btn.textContent='Menyimpan...';
  }
  const draft=pendingTxDraft?{...pendingTxDraft}:null;
  if(!draft){
    txPaymentSubmitting=false;
    buttons.forEach(b=>{b.disabled=false;b.removeAttribute('aria-busy');b.style.opacity='';if(b.dataset.originalText)b.textContent=b.dataset.originalText;});
    return toast('Data transaksi tidak ditemukan. Isi ulang transaksi.');
  }
  try{
    await saveTx(Boolean(draft.printAfterSave),paymentMethod,draft);
  }finally{
    txPaymentSubmitting=false;
    document.querySelectorAll('[data-payment-choice]').forEach(b=>{
      b.disabled=false;
      b.removeAttribute('aria-busy');
      b.style.opacity='';
      if(b.dataset.originalText)b.textContent=b.dataset.originalText;
    });
  }
}
async function saveTx(printAfterSave=false,paymentMethod='',draft=null){
  primeAppSounds();
  const amount=Number(draft?.amount||onlyDigits($('txa')?.value)), note=String(draft?.note??txProductDraftText()).trim()||'Transaksi';
  if(!amount||amount<=0)return toast('Nominal wajib diisi');
  if(!paymentMethod){
    if(!canTx()){
      closeModal();
      return toast(txBlockedMessage());
    }
    return openTxPaymentChoice({printAfterSave,amount,note});
  }
  const payment=normalizeTxPaymentMethod(paymentMethod);
  if(!payment)return toast('Pilih Cash atau QRIS / Transfer dulu');
  const paymentText=txPaymentLabel(payment);
  pendingTxDraft=null;
  if(!canTx()){
    closeModal();
    return toast(txBlockedMessage());
  }
  showLoad(true);
  const verified=await verifyTransactionAllowedServer(todayKey());
  if(!verified.ok){
    showLoad(false);
    closeModal();
    return toast(verified.msg||'Transaksi ditolak. Cek absen ulang.');
  }
  const latest=await latestBonusSnapshotForSave();
  showLoad(false);
  if(!latest.ok)return toast(latest.msg||'Gagal cek bonus terbaru. Transaksi belum disimpan.');
  const firstTxToday=isFirstTxPartyDue();
  const now=Date.now(), u=key(latest.user.username||state.user.username), id='stafftx_'+u+'_'+now+'_'+Math.random().toString(36).slice(2,7);
  const userRole=latest.userRole;
  const txRate=latest.txRate;
  const txPercent=latest.txPercent;
  const closingSnapshot=latest.closingSnapshot;
  const payload={
    user:u,
    name:latest.user.name||state.user.name||u,
    amount,
    note,
    paymentMethod:payment,
    paymentLabel:paymentText,
    paymentStatus:'success',
    paymentCashOutType:payment==='qris_transfer'?'qris':'',
    isNonCashPayment:payment==='qris_transfer',
    paymentConfirmed:true,
    paymentConfirmedAtMs:now,
    dateKey:todayKey(),
    monthKey:monthKey(),
    userRole,
    role:userRole,
    bonusGroup:userRole==='harian'?'harian':'staff',
    bonusRate:txRate,
    bonusPercent:txPercent,
    transactionBonusRate:txRate,
    transactionBonusPercent:txPercent,
    closingBonusPerMinuteSnapshot:closingSnapshot,
    bonusLogicVersion:3,
    ...trialRecordFlags(latest.user||state.user),
    createdAtMs:now,
    deleted:false,
    createdBy:u,
    clientId:id,
    source:'trx_staff_bonus_per_user_check_on_save'
  };
  const local={id,...payload,pending:true};
  closeModal();
  state.data.tx=[local,...state.data.tx];
  render();
  try{
    await setDoc(doc(db,'transactions',id),{...payload,createdAt:serverTimestamp(),syncedAt:serverTimestamp(),syncedAtMs:Date.now()},{merge:false});
    const savedTx={id,...payload,pending:false};
    state.data.tx=state.data.tx.map(t=>t.id===id?savedTx:t);
    removePending(id);
    playSuccessSound();
    await logAudit('transaction_create',{id,user:u,amount,note,paymentMethod:payment,paymentLabel:paymentText});
    if(!isTrialRecord(savedTx))notifyAdminNewTransaction(savedTx);
    state.data.targetTx=mergeRowsById(state.data.targetTx,[savedTx]);
    syncDailyTargetState();
    scheduleDailyTargetCheck();
    render();
    if(firstTxToday)showFirstTxParty(amount,note);
    if(printAfterSave){
      toast(`Transaksi ${paymentText} tersimpan, mencetak struk`);
      setTimeout(()=>directPrintReceiptText(receiptTextForTx(savedTx),'Struk Transaksi Baru'),120);
    }else{
      toast(`Transaksi sukses via ${paymentText}`);
    }
  }catch(e){
    console.error(e);
    if(isPermissionError(e)){
      state.data.tx=state.data.tx.filter(t=>t.id!==id);
      state.syncError='Akses Firebase menolak transaksi.';
      toast('Akses Firebase menolak transaksi');
    }else{
      state.data.tx=state.data.tx.filter(t=>t.id!==id);
      state.syncError='Transaksi gagal tersimpan. Cek koneksi, lalu coba simpan ulang.';
      toast('Gagal menyimpan. Transaksi staff wajib online.');
    }
    render();
  }
}
async function delTx(id){
  const t=state.data.tx.find(x=>String(x.id)===String(id));
  if(!t)return toast('Transaksi tidak ditemukan');
  if(key(t.user)!==key(state.user.username))return toast('Tidak bisa hapus transaksi orang lain');
  const p=await pinAsk(`Hapus transaksi Rp ${rp(t.amount)}?`);
  if(!p)return;
  if(String(p)!==String(state.user.pin))return toast('PIN salah');
  showLoad(true);
  try{
    await setDoc(doc(db,'transactions',id),{deleted:true,deletedAt:serverTimestamp(),deletedAtMs:Date.now(),deletedBy:state.user.username,deletedByName:state.user.name},{merge:true});
    state.data.tx=state.data.tx.map(x=>String(x.id)===String(id)?{...x,deleted:true,deletedAtMs:Date.now(),deletedBy:state.user.username,deletedByName:state.user.name}:x);
    state.data.targetTx=state.data.targetTx.map(x=>String(x.id)===String(id)?{...x,deleted:true,deletedAtMs:Date.now(),deletedBy:state.user.username,deletedByName:state.user.name}:x);
    syncDailyTargetState();
    scheduleDailyTargetCheck();
    await logAudit('transaction_soft_delete',{id,user:t.user,amount:Number(t.amount||0),note:t.note||'Transaksi'});
    await notifyAdminTransactionDelete({...t,id,deletedBy:state.user.username,deletedByName:state.user.name});
    toast('Transaksi dihapus aman');
  }catch(e){
    console.error(e);
    toast(isPermissionError(e)?'Akses Firebase menolak hapus':'Gagal hapus transaksi');
  }
  showLoad(false);
  render();
}
function getPosition(){return new Promise((res,rej)=>{if(!navigator.geolocation)return rej(new Error('GPS tidak tersedia'));navigator.geolocation.getCurrentPosition(p=>res({lat:p.coords.latitude,lng:p.coords.longitude,accuracy:p.coords.accuracy||0}),e=>rej(e),{enableHighAccuracy:true,timeout:12000,maximumAge:0})})}
function distance(a,b,c,d){const R=6371000,rad=x=>x*Math.PI/180,da=rad(c-a),db=rad(d-b),x=Math.sin(da/2)**2+Math.cos(rad(a))*Math.cos(rad(c))*Math.sin(db/2)**2;return R*2*Math.atan2(Math.sqrt(x),Math.sqrt(1-x))}
async function updateLocation(){try{state.pos=await getPosition();render()}catch(e){toast('GPS gagal / izin lokasi ditolak')}}
async function serverAttendanceToday(user,dateKey=todayKey()){
  const u=key(user), d=String(dateKey||todayKey()).slice(0,10), docId=attendanceDocId(u,d);
  try{
    const direct=await getDocFromServer(doc(db,'attendance',docId));
    if(direct.exists())return {id:direct.id,...direct.data()};
  }catch(e){console.warn('cek absen direct gagal',e?.code||e)}
  try{
    const snap=await getDocsFromServer(query(collection(db,'attendance'),where('user','==',u),where('dateKey','==',d),limit(10)));
    const rows=snap.docs.map(x=>({id:x.id,...x.data()})).filter(a=>!deleted(a)&&key(a.user)===u&&attDate(a)===d);
    if(rows.length)return sortDesc(rows)[0];
  }catch(e){console.warn('cek absen tanggal gagal',e?.code||e)}
  return null;
}
async function clockIn(){
  if(clockInInFlight||state.busy)return toast('Absen sedang diproses');
  if(!state.user)return toast('Silakan login ulang');
  if(isRismaSpecialUser())return toast('Risma tidak perlu absen. Transaksi sudah terbuka.');
  const u=key(state.user.username), d=todayKey(), id=attendanceDocId(u,d);
  const lock=missedAttendanceLockForDate(d);
  if(lock)return toast(missedAttendanceLockText(lock,'absen'));
  if(todayAtt())return toast('Sudah absen hari ini');
  clockInInFlight=true;
  let pos=state.pos;
  try{
    if(!await validateCurrentDeviceSession({silent:true,force:true}))return;
    const unlockCheck=await verifyAbsenceOpenServer(d,'absen');
    if(!unlockCheck.ok)return toast(unlockCheck.msg);
    const existing=await serverAttendanceToday(u,d);
    if(existing){
      state.data.att=dedupeAttendanceRows([{id:existing.id,...existing},...state.data.att]);
      state.data.targetAtt=dedupeAttendanceRows([{id:existing.id,...existing},...state.data.targetAtt]);
      syncDailyTargetState();
      render();
      return toast('Sudah absen hari ini');
    }
    try{if(!pos)pos=await getPosition();state.pos=pos}catch(e){return toast('GPS gagal / izin lokasi ditolak')}
    const dist=distance(pos.lat,pos.lng,OFFICE_LOC.lat,OFFICE_LOC.lng);
    if(dist>RADIUS_LIMIT)return toast(`Diluar radius (${Math.round(dist)}m)`);
    showLoad(true);
    const now=Date.now();
    const payload={user:u,name:state.user.name||u,dateKey:d,monthKey:d.slice(0,7),createdAtMs:now,loc:pos,lat:pos.lat,lng:pos.lng,...trialRecordFlags(state.user),deleted:false,clientId:id,source:'trx_staff_sesuai_index'};
    const local={id,...payload,pending:true};
    state.data.att=dedupeAttendanceRows([local,...state.data.att]);
    render();
    await setDoc(doc(db,'attendance',id),{...payload,createdAt:serverTimestamp(),syncedAt:serverTimestamp(),syncedAtMs:Date.now()},{merge:true});
    state.data.att=dedupeAttendanceRows(state.data.att.map(a=>a.id===id?{...a,pending:false}:a));
    state.data.targetAtt=dedupeAttendanceRows([{id,...payload,pending:false},...state.data.targetAtt]);
    syncDailyTargetState();
    removePending(id);
    if(!isTrialRecord(payload))await notifyAdminAttendance({id,...payload,pending:false},'masuk');
    toast('Absen berhasil');
  }catch(e){
    console.error(e);
    if(isPermissionError(e)){
      state.data.att=state.data.att.filter(a=>a.id!==id);
      state.syncError='Akses Firebase menolak absen.';
      toast('Akses Firebase menolak absen');
    }else{
      const now=Date.now();
      const payload={user:u,name:state.user.name||u,dateKey:d,monthKey:d.slice(0,7),createdAtMs:now,loc:pos||null,lat:pos?.lat||null,lng:pos?.lng||null,...trialRecordFlags(state.user),deleted:false,clientId:id,source:'trx_staff_sesuai_index'};
      state.data.att=dedupeAttendanceRows([{id,...payload,pending:true},...state.data.att]);
      state.data.targetAtt=dedupeAttendanceRows([{id,...payload,pending:true},...state.data.targetAtt]);
      syncDailyTargetState();
      addPending({type:'att_add',id,user:u,payload,createdAtMs:now});
      toast('Koneksi gagal. Absen disimpan lokal & akan sync otomatis.');
    }
  }finally{
    showLoad(false);
    clockInInFlight=false;
    render();
  }
}
async function logAudit(action,detail={}){try{await addDoc(collection(db,'audit_logs'),{action,detail,user:state.user?.username||'',name:state.user?.name||'',createdAt:serverTimestamp(),createdAtMs:Date.now()})}catch(e){console.warn('audit skipped',e?.code||e)}}
async function login(){
  const u=key($('lu')?.value), p=String($('lp')?.value||'').trim();
  if(!u||!p)return toast('Isi username dan PIN');
  showLoad(true);
  try{
    const snap=await getDocFromServer(doc(db,'users',u));
    if(!snap.exists())throw new Error('User tidak ditemukan');
    const raw=snap.data()||{};
    const data={id:snap.id,username:raw.username||snap.id,...raw};
    if(isAdmin(data))throw new Error('Aplikasi ini khusus staff');
    if(data.active===false||deleted(data))throw new Error('User nonaktif');
    if(String(data.pin||'')!==p)throw new Error('PIN salah');
    const lock=await verifyDeviceLockForLogin(data,u);
    if(!lock.ok)throw new Error(lock.msg);
    state.user={username:key(data.username||u),name:data.name||u,pin:String(data.pin||''),role:data.role||'staff',transactionBonusRate:hasBonusValue(data.transactionBonusRate)?Number(data.transactionBonusRate):null,transactionBonusPercent:hasBonusValue(data.transactionBonusPercent)?Number(data.transactionBonusPercent):null,closingBonusPerMinute:isDailyUser(data)?0:(hasBonusValue(data.closingBonusPerMinute)?Number(data.closingBonusPerMinute):null),dailyBonusRate:hasBonusValue(data.dailyBonusRate)?Number(data.dailyBonusRate):null,dailyBonusPercent:hasBonusValue(data.dailyBonusPercent)?Number(data.dailyBonusPercent):null,active:data.active!==false,deviceId:lock.deviceId,deviceResetAtMs:Number(data.deviceResetAtMs||0),...trialSessionFields(data)};
    const saved={username:state.user.username,name:state.user.name,pin:state.user.pin,role:state.user.role,transactionBonusRate:state.user.transactionBonusRate,transactionBonusPercent:state.user.transactionBonusPercent,closingBonusPerMinute:isDailyUser(state.user)?0:state.user.closingBonusPerMinute,dailyBonusRate:state.user.dailyBonusRate,dailyBonusPercent:state.user.dailyBonusPercent,active:true,deviceId:state.user.deviceId,deviceResetAtMs:state.user.deviceResetAtMs,...trialSessionFields(state.user)};
    localStorage.setItem(SESSION,JSON.stringify(saved));
    localStorage.setItem(LEGACY_SESSION,JSON.stringify({username:state.user.username,pin:state.user.pin}));
    showLoad(false);
    await loadStaffData();
    toast(`Masuk: ${state.user.name}`);
  }catch(e){
    console.error(e);
    showLoad(false);
    toast(e.message||'Login gagal');
    renderLogin();
  }
}
async function boot(){
  setTheme(getTheme());
  const raw=[];
  try{raw.push(JSON.parse(localStorage.getItem(SESSION)||'null'))}catch(e){}
  try{raw.push(JSON.parse(localStorage.getItem(LEGACY_SESSION)||'null'))}catch(e){}
  for(const s of raw.filter(Boolean)){
    try{
      const username=key(s.username||s.user||s.id);
      const pin=String(s.pin||'');
      if(!username||!pin)continue;
      const snap=await getDocFromServer(doc(db,'users',username));
      if(!snap.exists())continue;
      const rawData=snap.data()||{};
      const data={id:snap.id,username:rawData.username||snap.id,...rawData};
      if(!isAdmin(data)&&data.active!==false&&!deleted(data)&&String(data.pin||'')===pin){
        if(!isDeviceFreeUser(data)&&Number(data.deviceResetAtMs||0)>Number(s.deviceResetAtMs||0)){
          const serverDeviceId=String(data.deviceId||'').trim();
          const thisDeviceId=getDeviceId();
          // Kalau reset lama sudah dipakai login ulang di device ini, jangan logout lagi.
          // Ini penting untuk sesi lama/legacy yang belum menyimpan deviceResetAtMs.
          if(serverDeviceId&&serverDeviceId===thisDeviceId){
            s.deviceResetAtMs=Number(data.deviceResetAtMs||0);
            s.deviceId=serverDeviceId;
            try{localStorage.setItem(SESSION,JSON.stringify({...s,deviceResetAtMs:s.deviceResetAtMs,deviceId:s.deviceId}))}catch(e){}
          }else{
            localStorage.removeItem(SESSION);
            localStorage.removeItem(LEGACY_SESSION);
            toast('Device akun ini sudah direset admin. Silakan login ulang.');
            break;
          }
        }
        const lock=await verifyDeviceLockForLogin(data,username);
        if(!lock.ok){
          localStorage.removeItem(SESSION);
          localStorage.removeItem(LEGACY_SESSION);
          toast(lock.msg);
          break;
        }
        state.user={username:key(data.username||username),name:data.name||username,pin:String(data.pin||''),role:data.role||'staff',transactionBonusRate:hasBonusValue(data.transactionBonusRate)?Number(data.transactionBonusRate):null,transactionBonusPercent:hasBonusValue(data.transactionBonusPercent)?Number(data.transactionBonusPercent):null,closingBonusPerMinute:isDailyUser(data)?0:(hasBonusValue(data.closingBonusPerMinute)?Number(data.closingBonusPerMinute):null),dailyBonusRate:hasBonusValue(data.dailyBonusRate)?Number(data.dailyBonusRate):null,dailyBonusPercent:hasBonusValue(data.dailyBonusPercent)?Number(data.dailyBonusPercent):null,active:data.active!==false,deviceId:lock.deviceId,deviceResetAtMs:Number(data.deviceResetAtMs||0),...trialSessionFields(data)};
        const saved={username:state.user.username,name:state.user.name,pin:state.user.pin,role:state.user.role,transactionBonusRate:state.user.transactionBonusRate,transactionBonusPercent:state.user.transactionBonusPercent,closingBonusPerMinute:isDailyUser(state.user)?0:state.user.closingBonusPerMinute,dailyBonusRate:state.user.dailyBonusRate,dailyBonusPercent:state.user.dailyBonusPercent,active:true,deviceId:state.user.deviceId,deviceResetAtMs:state.user.deviceResetAtMs,...trialSessionFields(state.user)};
        localStorage.setItem(SESSION,JSON.stringify(saved));
        localStorage.setItem(LEGACY_SESSION,JSON.stringify({username:state.user.username,pin:state.user.pin}));
        await loadStaffData();
        return;
      }
    }catch(e){console.warn(e)}
  }
  localStorage.removeItem(SESSION);
  localStorage.removeItem(LEGACY_SESSION);
  renderLogin();
}
function logout(){clearSessionAndRender('')}
function canAppBack(){return !!state.user&&state.page!=='home'}
function syncAppBrowserHistory(p,mode='push'){
  try{
    const url=location.pathname+location.search+'#'+encodeURIComponent(p||'home');
    const data={rockyApp:true,page:p||'home'};
    if(mode==='replace')history.replaceState(data,'',url);
    else history.pushState(data,'',url);
  }catch(e){}
}
function go(p,opts={}){
  p=p==='leave'?'unlock':p;
  p=(p==='history'||p==='unlock')?p:'home';
  if(state.page===p){render();return}
  state.page=p;
  syncAppBrowserHistory(p,opts.replace?'replace':'push');
  render();
}
function appBack(opts={}){
  const silentHome=!!(opts&&opts.silentHome);
  const source=opts&&opts.source?String(opts.source):'';
  try{closeModal()}catch(e){}
  if(!state.user)return false;
  if(state.page!=='home'){
    state.page='home';
    syncAppBrowserHistory('home','push');
    render();
    return true;
  }
  syncAppBrowserHistory('home','replace');
  if(source==='nativeBack')return false;
  if(!silentHome)toast('Sudah di halaman utama');
  return true;
}
function installSafeBackNavigation(){
  if(window.__ROCKY_SAFE_BACK_INSTALLED__)return;
  window.__ROCKY_SAFE_BACK_INSTALLED__=true;
  syncAppBrowserHistory(state.page||'home','replace');
  window.addEventListener('popstate',()=>{
    if(document.querySelector('.modal-wrap.show')){try{closeModal()}catch(e){} syncAppBrowserHistory(state.page||'home','push');return}
    if(state.user&&state.page!=='home'){
      state.page='home';
      render();
      syncAppBrowserHistory('home','push');
      return;
    }
    if(state.user){
      toast('Gunakan tombol Keluar untuk logout');
      syncAppBrowserHistory(state.page||'home','push');
    }
  });
}
function installSwipeBack(){
  if(window.__ROCKY_SWIPE_BACK_INSTALLED__)return;
  window.__ROCKY_SWIPE_BACK_INSTALLED__=true;
  const hint=$('swipeBackHint');
  const ptr={active:false,startX:0,startY:0,lastX:0,lastY:0,ready:false};
  const blocked=t=>t&&t.closest&&t.closest('input,textarea,select,button,a,.modal-wrap,.nav');
  const showHint=show=>{if(!hint)return;hint.classList.toggle('show',!!show)};
  const reset=()=>{ptr.active=false;ptr.ready=false;showHint(false);if(page){page.style.transition='';page.style.transform='';page.style.willChange=''}};
  document.addEventListener('touchstart',e=>{
    if(!state.user||state.page==='home'||e.touches.length!==1)return;
    if(document.querySelector('.modal-wrap.show'))return;
    if(blocked(e.target))return;
    const t=e.touches[0];
    ptr.active=true;ptr.ready=false;ptr.startX=t.clientX;ptr.startY=t.clientY;ptr.lastX=t.clientX;ptr.lastY=t.clientY;
    if(page)page.style.willChange='transform';
  },{passive:true});
  document.addEventListener('touchmove',e=>{
    if(!ptr.active||e.touches.length!==1)return;
    const t=e.touches[0],dx=t.clientX-ptr.startX,dy=t.clientY-ptr.startY;
    ptr.lastX=t.clientX;ptr.lastY=t.clientY;
    if(Math.abs(dy)>42&&Math.abs(dy)>Math.abs(dx)){reset();return}
    if(dx<-14&&Math.abs(dx)>Math.abs(dy)*1.25){
      e.preventDefault();
      const move=Math.max(-64,dx*.28);
      ptr.ready=dx<-74;
      showHint(ptr.ready);
      if(page){page.style.transition='';page.style.transform=`translateX(${move}px)`}
    }
  },{passive:false});
  document.addEventListener('touchend',()=>{
    if(!ptr.active)return;
    const dx=ptr.lastX-ptr.startX,dy=ptr.lastY-ptr.startY,ok=ptr.ready&&dx<-74&&Math.abs(dx)>Math.abs(dy)*1.35;
    reset();
    if(ok)appBack({silentHome:true,source:'swipe'});
  },{passive:true});
  document.addEventListener('touchcancel',reset,{passive:true});
}
async function refresh(){
  const now=Date.now();
  if(now-lastManualRefreshAt<REFRESH_COOLDOWN_MS){
    const wait=Math.ceil((REFRESH_COOLDOWN_MS-(now-lastManualRefreshAt))/1000);
    return toast(`Tunggu ${wait} detik sebelum refresh lagi`);
  }
  lastManualRefreshAt=now;
  const changed=await checkDateChange();
  if(!changed){
    await flushPending();
    await loadStaffData({skipFlush:true});
  }
  toast('Data diperbarui');
}

function hardRefreshApp(){
  try{closeModal()}catch(e){}
  toast('Memuat ulang aplikasi...');
  setTimeout(()=>{
    try{
      if(window.Android&&typeof window.Android.reloadApp==='function'){
        window.Android.reloadApp();
        return;
      }
    }catch(e){}
    try{location.reload()}catch(e){location.href=location.href}
  },260);
}
function installPullToReload(){
  if(window.__ROCKY_PULL_TO_RELOAD_INSTALLED__)return;
  window.__ROCKY_PULL_TO_RELOAD_INSTALLED__=true;
  const indicator=document.createElement('div');
  indicator.id='pullReloadIndicator';
  indicator.textContent='Tarik untuk reload';
  indicator.style.cssText='position:fixed;left:50%;top:10px;transform:translate(-50%,-70px);z-index:120;background:#0f172a;color:#fff;border-radius:999px;padding:8px 12px;font-size:12px;font-weight:900;box-shadow:0 10px 26px rgba(15,23,42,.25);opacity:0;transition:transform .18s ease,opacity .18s ease;pointer-events:none;white-space:nowrap';
  document.body.appendChild(indicator);
  const ptr={active:false,startY:0,ready:false,dist:0,reloading:false};
  const scrollTop=()=>Math.max(window.scrollY||0,document.documentElement.scrollTop||0,document.body.scrollTop||0);
  const isBlockedTarget=(target)=>target&&target.closest&&target.closest('input,textarea,select,button,a,.modal-wrap,.nav');
  const resetPull=()=>{
    ptr.active=false;ptr.ready=false;ptr.dist=0;
    indicator.textContent='Tarik untuk reload';
    indicator.style.opacity='0';
    indicator.style.transform='translate(-50%,-70px)';
    if(page){
      page.style.transition='transform .18s ease';
      page.style.transform='translateY(0)';
      setTimeout(()=>{try{page.style.transition='';page.style.willChange=''}catch(e){}},220);
    }
  };
  document.addEventListener('touchstart',e=>{
    if(ptr.reloading||state.busy||e.touches.length!==1)return;
    if(scrollTop()>0)return;
    if(document.querySelector('.modal-wrap.show'))return;
    if(isBlockedTarget(e.target))return;
    ptr.active=true;ptr.ready=false;ptr.startY=e.touches[0].clientY;ptr.dist=0;
    if(page){page.style.willChange='transform'}
  },{passive:true});
  document.addEventListener('touchmove',e=>{
    if(!ptr.active||ptr.reloading||e.touches.length!==1)return;
    const dy=e.touches[0].clientY-ptr.startY;
    if(dy<=0){resetPull();return}
    if(scrollTop()>0){resetPull();return}
    if(dy>8)e.preventDefault();
    ptr.dist=Math.min(78,dy*.55);
    ptr.ready=dy>96;
    indicator.textContent=ptr.ready?'Lepas untuk reload':'Tarik untuk reload';
    indicator.style.opacity=String(Math.min(1,dy/75));
    indicator.style.transform=`translate(-50%,${Math.min(12,ptr.dist-42)}px)`;
    if(page){
      page.style.transition='';
      page.style.transform=`translateY(${ptr.dist}px)`;
    }
  },{passive:false});
  const endPull=()=>{
    if(!ptr.active)return;
    const shouldReload=ptr.ready;
    resetPull();
    if(shouldReload){
      ptr.reloading=true;
      indicator.textContent='Memuat ulang...';
      indicator.style.opacity='1';
      indicator.style.transform='translate(-50%,12px)';
      setTimeout(()=>hardRefreshApp(),180);
    }
  };
  document.addEventListener('touchend',endPull,{passive:true});
  document.addEventListener('touchcancel',resetPull,{passive:true});
}


// FIX: penjaga pergantian tanggal WIB.
// Absen dan closing dihitung per dateKey, jadi UI wajib dirender ulang saat hari berganti.
let APP_DATE_KEY=todayKey();
let APP_DATE_CHECKING=false;
async function checkDateChange(){
  if(APP_DATE_CHECKING)return false;
  const nowKey=todayKey();
  if(nowKey===APP_DATE_KEY)return false;
  APP_DATE_CHECKING=true;
  APP_DATE_KEY=nowKey;
  try{
    closeModal();
    state.lastSyncMs=0;
    clearStaffRealtime();
    if(state.user){
      toast('Tanggal berganti. Absen & closing diperbarui.');
      await loadStaffData({silent:true});
    }else{
      render();
    }
  }catch(e){
    console.warn('date change refresh failed',e);
    render();
  }finally{
    APP_DATE_CHECKING=false;
  }
  return true;
}
function setupAutoSync(){
  // Tidak ada lagi refresh besar tiap 60 detik.
  // Realtime kecil yang menjaga transaksi/absen/closing hari ini tetap update.
  setInterval(async()=>{await checkDateChange();},60000);
  window.addEventListener('online',()=>{
    if(state.user){
      toast('Online lagi, sync pending dicek');
      flushPending();
      startStaffRealtime();
    }
  });
  window.addEventListener('focus',async()=>{
    const changed=await checkDateChange();
    if(!changed&&state.user){
      startStaffRealtime();
      render();
    }
  });
  document.addEventListener('visibilitychange',async()=>{
    if(!document.hidden){
      const changed=await checkDateChange();
      if(!changed&&state.user){
        startStaffRealtime();
        render();
      }
    }
  });
}

try{
  window.addEventListener('online',()=>{syncBelanjakuBridgeQueue().then(r=>{if(r.sent)toast('Data Belanjaku terkirim')}).catch(()=>{})});
  window.addEventListener('focus',()=>{syncBelanjakuBridgeQueue().catch(()=>{})});
}catch(e){}
window.requestOpenPrayerAyat=requestOpenPrayerAyat;window.openPrayerAyat=openPrayerAyat;window.togglePrayerAyat=togglePrayerAyat;window.stopPrayerAyat=stopPrayerAyat;window.openStaffNoteLink=openStaffNoteLink;window.login=login;window.logout=logout;window.toggleTheme=toggleTheme;window.openHeaderGuideDetail=openHeaderGuideDetail;window.dismissManualBonusNotice=dismissManualBonusNotice;window.dismissTargetNotice=dismissTargetNotice;window.refresh=refresh;window.hardRefreshApp=hardRefreshApp;window.retrySync=retrySync;window.go=go;window.appBack=appBack;window.openTx=openTx;window.requestFeatureUnlock=requestFeatureUnlock;window.openEmptyStock=openEmptyStock;window.addEmptyStockVariantRow=addEmptyStockVariantRow;window.removeEmptyStockVariantRow=removeEmptyStockVariantRow;window.submitEmptyStock=submitEmptyStock;window.syncBelanjakuBridgeQueue=syncBelanjakuBridgeQueue;window.saveTx=saveTx;window.confirmTxPayment=confirmTxPayment;window.reopenTxDraft=reopenTxDraft;window.cancelTxPaymentChoice=cancelTxPaymentChoice;window.delTx=delTx;window.closeModal=closeModal;window.closeFirstTxParty=closeFirstTxParty;window.closeManualBonusParty=closeManualBonusParty;window.updateLocation=updateLocation;window.clockIn=clockIn;window.formatRupiahInput=formatRupiahInput;window.printReceiptFromTx=printReceiptFromTx;window.printTodayTransactions=printTodayTransactions;window.copyReceiptText=copyReceiptText;window.shareReceiptText=shareReceiptText;window.nativePrintReceiptText=nativePrintReceiptText;window.directPrintReceiptText=directPrintReceiptText;window.browserPrintReceiptText=browserPrintReceiptText;
window.addTxProductItem=addTxProductItem;window.removeTxProductItem=removeTxProductItem;window.handleTxProductKey=handleTxProductKey;window.updateTxProductAddButton=updateTxProductAddButton;window.openTxDetail=openTxDetail;
statusPill=function(){if(isTrialUser())return `<span class="pill amber">Mode Trial</span>`;if(isClosedToday())return `<span class="pill amber">Sudah closing</span>`;if(isRismaSpecialUser())return `<span class="pill green">Risma bebas absen</span>`;if(isDaily())return `<span class="pill blue">Karyawan harian</span>`;const a=todayAtt();return a?`<span class="pill green">Sudah absen · ${timeID(ms(a))}</span>`:`<span class="pill red">Belum absen</span>`};
setupAutoSync();
// Pull-to-refresh dimatikan supaya tidak ada read tambahan tanpa sengaja.
// Refresh manual dan realtime tetap aktif.
// installPullToReload();
boot();

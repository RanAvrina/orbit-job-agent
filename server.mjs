import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
const PORT=Number(process.env.PORT||3000);
const TOKEN=process.env.ORBIT_ADMIN_TOKEN||'';
const IS_PROD=process.env.NODE_ENV==='production';
if(IS_PROD && TOKEN.length<32)throw Error('Production requires ORBIT_ADMIN_TOKEN with at least 32 characters');
const MAX_JOBS=Number(process.env.MAX_STORED_JOBS||3000);
const MAX_AGE_DAYS=Number(process.env.JOB_RETENTION_DAYS||90);
const APPLY_ENABLED=false; // Deliberately no automatic submissions until approved integrations exist.
const limits=new Map();
function rateLimited(req){const ip=req.socket.remoteAddress||'unknown',now=Date.now();const x=limits.get(ip)||{count:0,until:now+60000};if(now>x.until){x.count=0;x.until=now+60000}x.count++;limits.set(ip,x);if(limits.size>5000)limits.clear();return x.count>90}
function secureHeaders(res){res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('Referrer-Policy','no-referrer');res.setHeader('X-Frame-Options','DENY');res.setHeader('Permissions-Policy','camera=(), microphone=(), geolocation=()');res.setHeader('Cache-Control','no-store');if(IS_PROD)res.setHeader('Strict-Transport-Security','max-age=31536000; includeSubDomains')}

const BOARDS=(process.env.GREENHOUSE_BOARDS??'monday,appsflyer,similarweb,taboola,wix').split(',').map(s=>s.trim()).filter(s=>/^[\w-]+$/.test(s));
const LEVER=(process.env.LEVER_SITES??'').split(',').map(s=>s.trim()).filter(s=>/^[\w-]+$/.test(s));
const ASHBY=(process.env.ASHBY_BOARDS??'').split(',').map(s=>s.trim()).filter(s=>/^[\w-]+$/.test(s));
const STORE=process.env.ORBIT_DATA_FILE||path.join(dir,'data.json');
const SB_URL=(process.env.SUPABASE_URL||'').replace(/\/$/,'');
const SB_KEY=process.env.SUPABASE_SECRET_KEY||'';
const USE_SB=!!(SB_URL&&SB_KEY);
if ((SB_URL&&!SB_KEY)||(!SB_URL&&SB_KEY)) throw Error('Set both SUPABASE_URL and SUPABASE_SECRET_KEY');
if (USE_SB && !/^https:\/\/[a-z0-9-]+\.supabase\.co$/i.test(SB_URL)) throw Error('Invalid SUPABASE_URL');
async function sb(table,{method='GET',query='',body,prefer}={}){
  const r=await fetch(`${SB_URL}/rest/v1/${table}${query}`,{method,headers:{'apikey':SB_KEY,'Authorization':`Bearer ${SB_KEY}`,'Content-Type':'application/json',...(prefer?{'Prefer':prefer}:{})},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(20000)});
  const txt=await r.text();if(!r.ok)throw Error(`Supabase ${table}: ${r.status} ${txt.slice(0,300)}`);
  return txt?JSON.parse(txt):null;
}
const jobToRow=j=>({source:j.source,external_id:j.id,title:j.title,company:j.company||null,location:j.location||null,description:j.description||null,url:j.url||null,match_score:j.score,status:'new',discovered_at:j.firstSeen,updated_at:j.lastSeen});
const rowToJob=r=>({id:r.external_id,source:r.source,title:r.title,company:r.company||'',location:r.location||'',description:r.description||'',url:r.url,score:r.match_score,reason:'התאמה לפי תפקיד ומיקום',firstSeen:r.discovered_at,lastSeen:r.updated_at});
async function loadSupabase(){
 const rows=await sb('jobs',{query:'?select=*&order=updated_at.desc&limit=3000'});
 const apps=await sb('applications',{query:'?select=job_id,status,submitted_at,created_at,jobs(external_id)&limit=3000'});
 const scans=await sb('scan_logs',{query:'?select=created_at,status,error_message&order=created_at.desc&limit=1'});
 db.jobs=rows.map(rowToJob);
 db.applications=apps.filter(a=>a.jobs?.external_id).map(a=>({jobId:a.jobs.external_id,status:a.status,updatedAt:a.submitted_at||a.created_at,mode:'manual'}));
 db.lastScan=scans[0]?.created_at||null;db.lastError=scans[0]?.error_message||null;
}
async function saveJobsSupabase(jobs){
 for(let i=0;i<jobs.length;i+=100)await sb('jobs',{method:'POST',query:'?on_conflict=source,external_id',body:jobs.slice(i,i+100).map(jobToRow),prefer:'resolution=merge-duplicates,return=minimal'});
}
async function logScan(source,found,status,error){await sb('scan_logs',{method:'POST',body:{source,jobs_found:found,status,error_message:error||null},prefer:'return=minimal'});}
async function saveApplicationSupabase(x){
 const rows=await sb('jobs',{query:`?select=id&external_id=eq.${encodeURIComponent(x.jobId)}&limit=1`});
 if(!rows.length)throw Error('Job not found in Supabase');
 await sb('applications',{method:'POST',query:'?on_conflict=job_id',body:{job_id:rows[0].id,status:x.status,submitted_at:x.status==='applied'?new Date().toISOString():null},prefer:'resolution=merge-duplicates,return=minimal'});
}

const INTERVAL=Math.max(5,Number(process.env.SCAN_INTERVAL_MINUTES||30))*60000;
const FETCH_TIMEOUT=12000;
let db={jobs:[],applications:[],lastScan:null,lastError:null,scans:0};
if(!USE_SB){try{db={...db,...JSON.parse(await fs.readFile(STORE,'utf8'))}}catch(e){if(e.code!=='ENOENT')console.error('Data load:',e.message)}}
let running=false;
const clean=(s,max=1800)=>String(s||'').replace(/<[^>]*>/g,' ').replace(/&nbsp;/g,' ').replace(/&amp;/g,'&').replace(/\s+/g,' ').trim().slice(0,max);
const role=/(project manager|project coordinator|project analyst|project control|\bpmo\b|program coordinator|implementation|information systems|business analyst|systems analyst|data analyst|bi analyst|erp|crm|priority|salesforce|monday\.com|automation specialist|ai agent|מנהל.{0,4}פרויקט|ניהול פרויקטים|רכז.{0,4}פרויקט|מערכות מידע|מיישמ|מטמיע|ניתוח מערכות|אנליסט|אוטומציה)/i;
const junior=/(junior|entry[ -]?level|graduate|associate|intern|0[–-]2|0[–-]3|ללא ניסיון|ג׳וניור|ג'וניור|מתחיל)/i;
const senior=/(senior|principal|director|head of|staff engineer|team lead|ראש צוות|מנהל מחלקה|סמנכ)/i;
const place=/(tel[ -]?aviv|תל אביב|ramat gan|רמת גן|be.?er sheva|beer sheva|באר שבע|ashdod|אשדוד|ashkelon|אשקלון|omer|עומר|lehavim|להבים|kiryat gat|קריית גת|קרית גת|netivot|נתיבות|ofakim|אופקים|sderot|שדרות|dimona|דימונה|southern district|south district|מחוז הדרום)/i;
const remote=/(remote|work from anywhere|עבודה מהבית|מרחוק)/i;
const israel=/(israel|ישראל)/i;
const countryMismatch=/(germany|deutschland|united states|usa|united kingdom|uk|poland|france|spain|portugal|canada|australia|india|berlin|london|new york|warsaw)/i;
function evaluate(j){const title=clean(j.title,300),body=clean(j.description,3000),loc=clean(j.location,200);const titleHit=role.test(title),bodyHit=role.test(body),seniorTitle=senior.test(title);const locationHit=place.test(loc);const locationAllowed=locationHit || (remote.test(loc)&&israel.test(loc)&&!countryMismatch.test(loc));const eligible=(titleHit||bodyHit)&&locationAllowed&&!seniorTitle;let score=15+(titleHit?42:bodyHit?12:0)+(junior.test(title)?20:junior.test(body)?8:0)+(locationHit?10:5)+(/sql|javascript|node\.js|typescript|priority|erp|crm|מערכות מידע/i.test(body)?8:0)-(seniorTitle?60:0);return {eligible,score:Math.max(0,Math.min(99,score)),reason:seniorTitle?'תפקיד בכיר':!locationAllowed?'מיקום לא מתאים':!titleHit?'התאמה לפי תיאור בלבד':'התאמה לפי תפקיד ומיקום'};}
async function persist(){if(USE_SB)return;await fs.mkdir(path.dirname(STORE),{recursive:true});const temp=STORE+'.tmp';await fs.writeFile(temp,JSON.stringify(db,null,2),{mode:0o600});await fs.rename(temp,STORE)}
async function getJSON(url){const res=await fetch(url,{signal:AbortSignal.timeout(FETCH_TIMEOUT),headers:{'User-Agent':'OrbitJobAgent/5.0'}});if(!res.ok)throw Error(`HTTP ${res.status}`);return res.json()}
async function sourceJobs(){const tasks=[]; if(process.env.INCLUDE_ARBEITNOW==='true')tasks.push({name:'Arbeitnow',run:async()=>{const d=await getJSON('https://www.arbeitnow.com/api/job-board-api');return(d.data||[]).map(x=>({id:`arbeitnow:${x.slug||x.url}`,title:clean(x.title),company:clean(x.company_name),location:clean(x.location),description:clean(x.description),url:x.url,source:'Arbeitnow',publishedAt:x.created_at?new Date(x.created_at*1000).toISOString():null}))}});
for(const board of BOARDS)tasks.push({name:`Greenhouse/${board}`,run:async()=>{const d=await getJSON(`https://boards-api.greenhouse.io/v1/boards/${board}/jobs?content=true`);return(d.jobs||[]).map(x=>({id:`greenhouse:${board}:${x.id}`,title:clean(x.title),company:board,location:clean(x.location?.name),description:clean(x.content),url:x.absolute_url,source:'Greenhouse',publishedAt:x.first_published||null}))}});
for(const site of LEVER)tasks.push({name:`Lever/${site}`,run:async()=>{const d=await getJSON(`https://api.lever.co/v0/postings/${site}?mode=json`);return(d||[]).map(x=>({id:`lever:${site}:${x.id}`,title:clean(x.text),company:site,location:clean(x.categories?.location||x.workplaceType),description:clean(x.descriptionPlain||x.description),url:x.hostedUrl,source:'Lever',publishedAt:x.createdAt?new Date(x.createdAt).toISOString():null}))}});
for(const board of ASHBY)tasks.push({name:`Ashby/${board}`,run:async()=>{const d=await getJSON(`https://api.ashbyhq.com/posting-api/job-board/${board}?includeCompensation=false`);return(d.jobs||[]).filter(x=>x.isListed!==false).map(x=>({id:`ashby:${board}:${x.id||x.jobUrl}`,title:clean(x.title),company:board,location:clean(x.location||x.address?.postalAddress?.addressLocality),description:clean(x.descriptionPlain||x.descriptionHtml||x.description),url:x.jobUrl||x.applyUrl,source:'Ashby',publishedAt:x.publishedAt||null}))}});
const outcomes=await Promise.allSettled(tasks.map(t=>t.run()));let jobs=[],errors=[];outcomes.forEach((r,i)=>{if(r.status==='fulfilled')jobs.push(...r.value);else errors.push(`${tasks[i].name}: ${r.reason.message}`)});return {jobs,errors,sources:tasks.length};}
async function scan(){if(running)return {busy:true};running=true;try{const {jobs,errors,sources}=await sourceJobs();const eligibleCount=jobs.filter(j=>/^https:\/\//i.test(j.url||'')&&evaluate(j).eligible).length;console.log('ORBIT SCAN',JSON.stringify({fetched:jobs.length,eligible:eligibleCount,rejected:jobs.length-eligibleCount,sources,errors}));let added=0,updated=0,skipped=0;const existing=new Map(db.jobs.map(j=>[j.id,j]));const now=new Date().toISOString();for(const j of jobs){if(!/^https:\/\//i.test(j.url||'')){skipped++;continue}const match=evaluate(j);if(!match.eligible){skipped++;continue}const old=existing.get(j.id);const item={...j,score:match.score,reason:match.reason,firstSeen:old?.firstSeen||now,lastSeen:now};if(old){Object.assign(old,item);updated++}else{db.jobs.push(item);existing.set(j.id,item);added++}}db.jobs=db.jobs.filter(j=>Date.now()-new Date(j.lastSeen).getTime()<MAX_AGE_DAYS*86400000).sort((a,b)=>b.score-a.score||b.firstSeen.localeCompare(a.firstSeen)).slice(0,MAX_JOBS);if(USE_SB){await saveJobsSupabase(db.jobs);await logScan('all',jobs.length,errors.length?'partial':'success',errors.join('; ')||null)}db.lastScan=now;db.lastError=errors.join('; ')||null;db.scans++;await persist();return {added,updated,skipped,checked:jobs.length,eligible:eligibleCount,sources,errors}}catch(e){db.lastError=e.message;try{if(USE_SB)await logScan('all',0,'error',e.message);else await persist()}catch(logErr){console.error('Scan log:',logErr.message)}return {error:e.message}}finally{running=false}}
const json=(res,status,obj)=>{res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});res.end(JSON.stringify(obj))};
const authorized=req=>{const supplied=req.headers.authorization?.replace(/^Bearer /,'')||'';if(!TOKEN||!supplied)return false;const a=Buffer.from(TOKEN),b=Buffer.from(supplied);return a.length===b.length&&crypto.timingSafeEqual(a,b)};
async function handler(req,res){try{secureHeaders(res);if(rateLimited(req))return json(res,429,{error:'יותר מדי בקשות'});const u=new URL(req.url,'http://localhost');if(u.pathname.startsWith('/api/')&&!authorized(req))return json(res,401,{error:'נדרש מפתח גישה'});if(u.pathname==='/api/status'&&req.method==='GET')return json(res,200,{lastScan:db.lastScan,lastError:db.lastError,total:db.jobs.length,scans:db.scans,scanRunning:running,scanEnabled:!!TOKEN,greenhouseBoards:BOARDS.length,leverSites:LEVER.length,ashbyBoards:ASHBY.length,sources:[...(process.env.INCLUDE_ARBEITNOW==='true'?['Arbeitnow']:[]),...(BOARDS.length?['Greenhouse']:[]),...(LEVER.length?['Lever']:[]),...(ASHBY.length?['Ashby']:[])],autoApplyEnabled:APPLY_ENABLED,security:{privateApi:true,automaticSubmission:false}});if(u.pathname==='/api/jobs'&&req.method==='GET')return json(res,200,db.jobs.map(({description,...j})=>({...j,description:description.slice(0,320)})));if(u.pathname==='/api/applications'&&req.method==='GET')return json(res,200,db.applications);if(u.pathname==='/api/scan'&&req.method==='POST')return authorized(req)?json(res,200,await scan()):json(res,401,{error:'לא מורשה'});if(u.pathname==='/api/applications'&&req.method==='POST'){if(!authorized(req))return json(res,401,{error:'לא מורשה'});let body='';for await(const c of req){body+=c;if(body.length>10000)return json(res,413,{error:'בקשה גדולה מדי'})}let x;try{x=JSON.parse(body)}catch{return json(res,400,{error:'JSON לא תקין'})}if(!db.jobs.some(j=>j.id===x.jobId)||!['saved','applied','interview','rejected'].includes(x.status))return json(res,400,{error:'נתונים לא תקינים'});db.applications=db.applications.filter(a=>a.jobId!==x.jobId);db.applications.push({jobId:x.jobId,status:x.status,updatedAt:new Date().toISOString(),mode:'manual'});if(USE_SB)await saveApplicationSupabase(x);else await persist();return json(res,200,{ok:true})}if(u.pathname!=='/'&&u.pathname!=='/index.html')return json(res,404,{error:'לא נמצא'});const html=await fs.readFile(path.join(dir,'index.html'));res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Content-Security-Policy':"default-src 'self' https://fonts.googleapis.com https://fonts.gstatic.com 'unsafe-inline'; connect-src 'self'; img-src 'self' data:;",'X-Content-Type-Options':'nosniff'});res.end(html)}catch(e){console.error(e);if(!res.headersSent)json(res,500,{error:'שגיאת שרת'})}}
if(process.env.NODE_ENV!=='test'){http.createServer(handler).listen(PORT,()=>console.log(`Orbit http://localhost:${PORT}`));if(TOKEN){const begin=async()=>{if(USE_SB){try{await loadSupabase()}catch(e){console.error('Supabase initialization failed:',e.message);return}}await scan();setInterval(scan,INTERVAL)};begin()}}
export {evaluate,scan,handler,authorized};

import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
process.env.NODE_ENV='test';
process.env.ORBIT_ADMIN_TOKEN='abcdefghijklmnopqrstuvwxyz1234567890';
const {handler,authorized}=await import('./server.mjs');
test('authorization rejects missing and incorrect tokens',()=>{assert.equal(authorized({headers:{}}),false);assert.equal(authorized({headers:{authorization:'Bearer wrong'}}),false);assert.equal(authorized({headers:{authorization:'Bearer '+process.env.ORBIT_ADMIN_TOKEN}}),true)});
test('private APIs are protected',async()=>{const server=http.createServer(handler);await new Promise(r=>server.listen(0,'127.0.0.1',r));try{const base='http://127.0.0.1:'+server.address().port;for(const endpoint of ['/api/status','/api/jobs','/api/applications']){const r=await fetch(base+endpoint);assert.equal(r.status,401)}const r=await fetch(base+'/api/status',{headers:{Authorization:'Bearer '+process.env.ORBIT_ADMIN_TOKEN}});assert.equal(r.status,200);const d=await r.json();assert.equal(d.autoApplyEnabled,false);assert.equal(r.headers.get('x-frame-options'),'DENY')}finally{server.close()}});

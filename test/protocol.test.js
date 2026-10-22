const {test}=require('node:test');const assert=require('node:assert/strict');const vm=require('node:vm');const fs=require('node:fs');const path=require('node:path');const os=require('node:os');
// Exercise actual routes and Socket.IO handlers without opening network ports.
function harness(){const temp=fs.mkdtempSync(path.join(os.tmpdir(),'worktimer-test-'));const routes=new Map(),connections=[],events=[];let io;const app={disable(){},set(){},use(){},get(p,...h){routes.set('GET '+p,h);},post(p,...h){routes.set('POST '+p,h);}};function express(){return app;}express.json=express.urlencoded=express.static=()=>()=>{};
 class Io{constructor(){io=this;this.engine={use(){}};this.sockets={sockets:new Map()};}on(e,h){if(e==='connection')connections.push(h);}to(room){return {emit:(event,payload)=>events.push({room,event,payload})};}}
 function session(){return ()=>{};}
 const sandbox={require(name){if(name==='express')return express;if(name==='express-session')return session;if(name==='session-file-store')return ()=>class{};if(name==='socket.io')return {Server:Io};if(name==='dotenv')return {config(){}};if(name==='http')return {createServer(){return {listen(){}}}};if(name==='./timer-state')return require('../timer-state');return require(name);},__dirname:temp,process:{env:{}},console:{log(){}},setTimeout:()=>1,clearTimeout(){}};
 vm.createContext(sandbox);vm.runInContext(fs.readFileSync(path.join(__dirname,'../server.js'),'utf8')+'\nthis.testApi={states,startShift,finishShift,safeWorkerState};',sandbox);
 function request(method,p,user,body={}){const req={body,ip:'test-ip',session:{user}},res={statusCode:200,status(code){this.statusCode=code;return this;},json(data){this.data=data;}};const handlers=routes.get(method+' '+p);let i=0;const next=()=>handlers[i++]?.(req,res,next);next();return res;}
 function socket(user,id){const h=new Map();const s={id,request:{session:{user}},join(){},emit:(event,payload)=>events.push({room:id,event,payload}),disconnect(){this.disconnected=true;},on:(e,f)=>h.set(e,f)};io.sockets.sockets.set(id,s);connections[0](s);return {socket:s,send:(e,p)=>h.get(e)?.(p)};}
 return {temp,request,socket,events,...sandbox.testApi,cleanup:()=>fs.rmSync(temp,{recursive:true,force:true})};}
test('all four built-in worker credentials authenticate with renamed display labels and worker routes remain protected',()=>{const h=harness();try{for(const [name,id,label]of[['john','worker1','Worker 1'],['mark','worker2','Worker 2'],['anne','worker3','Worker 3'],['thing','worker4','Worker 4']]){const r=h.request('POST','/api/login',null,{username:name,password:name+'123'});assert.equal(r.statusCode,200);assert.equal(r.data.user.id,id);assert.equal(r.data.user.name,label);}assert.equal(h.request('POST','/api/worker/pause',null).statusCode,403);}finally{h.cleanup();}});
test('camera requests work before Start and during Pause; Start/Pause/Resume preserve an active broadcast',()=>{const h=harness();try{const u={role:'worker',id:'worker1',username:'john'};const w=h.socket(u,'w1');assert.notEqual(w.socket.disconnected,true);w.send('worker-capture-ready');const a=h.socket({role:'admin'},'a1');a.send('admin-live-request',{workerId:'worker1',mode:'camera'});const s=h.states.get('worker1');assert.equal(s.status,'OFFLINE');assert.equal(s.liveMode,'CAMERA_PENDING');w.send('worker-live-started',{workerId:'worker1',adminSocketId:'a1',mode:'camera'});assert.equal(s.liveMode,'CAMERA');h.request('POST','/api/worker/start',u);const shift=s.sessionId;h.request('POST','/api/worker/pause',u);assert.equal(s.status,'PAUSED');assert.equal(s.liveMode,'CAMERA');assert.equal(s.socketId,'w1');assert.equal(s.online,true);w.send('worker-heartbeat');h.request('POST','/api/worker/resume',u);assert.equal(s.status,'WORKING');assert.equal(s.sessionId,shift);assert.equal(s.liveMode,'CAMERA');assert.equal(h.events.filter(e=>e.event==='stop-live').length,0);}finally{h.cleanup();}});
test('ending a paused shift records only accumulated work and excludes the pause',()=>{const h=harness();try{const u={role:'worker',id:'worker1',username:'john'};h.request('POST','/api/worker/start',u);const s=h.states.get('worker1');s.status='PAUSED';s.startedAt=null;s.accumulatedMs=15000;h.finishShift(s,'test');const records=JSON.parse(fs.readFileSync(path.join(h.temp,'data','sessions.json'),'utf8'));assert.equal(records[0].durationMs,15000);}finally{h.cleanup();}});

test('admin timer reset clears the shift without disconnecting an active camera feed',()=>{
 const h=harness();try{
  const workerUser={role:'worker',id:'worker1',username:'john'};
  const worker=h.socket(workerUser,'worker-socket');
  worker.send('worker-capture-ready');
  const admin=h.socket({role:'admin',username:'admin'},'admin-socket');
  admin.send('admin-live-request',{workerId:'worker1',mode:'camera'});
  worker.send('worker-live-started',{workerId:'worker1',adminSocketId:'admin-socket',mode:'camera'});
  h.request('POST','/api/worker/start',workerUser);
  const shiftId=h.states.get('worker1').sessionId;
  admin.send('admin-timer-control',{workerId:'worker1',action:'reset'});
  const state=h.states.get('worker1');
  assert.equal(state.status,'STOPPED');
  assert.equal(state.startedAt,null);
  assert.equal(state.accumulatedMs,0);
  assert.equal(state.sessionId,null);
  assert.equal(state.liveMode,'CAMERA');
  assert.equal(state.socketId,'worker-socket');
  assert.equal(state.online,true);
  const sessions=JSON.parse(fs.readFileSync(path.join(h.temp,'data','sessions.json'),'utf8'));
  const ended=sessions.find(x=>x.id===shiftId);
  assert.ok(ended.endedAt);
  assert.equal(ended.reason,'admin-reset');
 }finally{h.cleanup();}
});

const {test}=require('node:test');
const assert=require('node:assert/strict');
const {elapsed,pause,resume}=require('../timer-state');
test('multiple pauses preserve accumulated time and exclude paused intervals',()=>{
 const s={status:'WORKING',startedAt:1000,accumulatedMs:0,sessionId:'shift-1',liveMode:'CAMERA',socketId:'worker-socket'};
 assert.equal(pause(s,6000),true); assert.equal(elapsed(s,11000),5000);
 assert.equal(pause(s,12000),false); assert.equal(resume(s,20000),true);
 assert.equal(resume(s,21000),false); assert.equal(elapsed(s,25000),10000);
 pause(s,30000);assert.equal(elapsed(s,99000),15000);
 assert.equal(s.liveMode,'CAMERA');assert.equal(s.socketId,'worker-socket');assert.equal(s.sessionId,'shift-1');
});
test('idle Resume does not create a shift',()=>{
 const s={status:'STOPPED',accumulatedMs:0,startedAt:null,sessionId:null};
 assert.equal(resume(s,5000),false);assert.equal(elapsed(s,10000),0);
});

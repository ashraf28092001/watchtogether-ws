const WebSocket = require('ws');
const PORT = Number(process.env.PORT || 8080);
const wss = new WebSocket.Server({ host: '0.0.0.0', port: PORT });
const rooms = new Map(); // roomCode => Map(socket => participant)
console.log(`WatchTogether WebSocket server running on port ${PORT}`);
function send(socket, data) { if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(data)); }
function broadcast(roomCode, data, except = null) { const room=rooms.get(roomCode); if(!room)return; for(const [socket] of room){if(socket!==except)send(socket,data);} }
function participants(roomCode){const room=rooms.get(roomCode);if(!room)return [];const byId=new Map();for(const p of room.values())byId.set(String(p.userId),{userId:p.userId,userName:p.userName});return [...byId.values()];}
function sendUsers(roomCode){broadcast(roomCode,{type:'users',roomCode,users:participants(roomCode)});}
wss.on('connection',(socket)=>{socket.roomCode=null;socket.participant=null;socket.isAlive=true;socket.on('pong',()=>socket.isAlive=true);
 socket.on('message',(raw)=>{let data;try{data=JSON.parse(raw.toString())}catch(e){return;}const roomCode=String(data.roomCode||'').toUpperCase();if(!/^[A-Z0-9]{4,12}$/.test(roomCode))return;const userId=Number(data.userId);const userName=String(data.userName||'Guest').slice(0,100);
 if(data.type==='join'){if(socket.roomCode&&rooms.has(socket.roomCode)){rooms.get(socket.roomCode).delete(socket);sendUsers(socket.roomCode);}socket.roomCode=roomCode;socket.participant={userId,userName};if(!rooms.has(roomCode))rooms.set(roomCode,new Map());rooms.get(roomCode).set(socket,socket.participant);send(socket,{type:'notice',roomCode,message:`You joined room ${roomCode}.`});sendUsers(roomCode);return;}
 if(socket.roomCode!==roomCode||!rooms.has(roomCode))return;
 if(data.type==='chat'){const message=String(data.message||'').trim().slice(0,1000);if(!message)return;broadcast(roomCode,{type:'chat',roomCode,userId,userName,message,time:Date.now()});}
 else if(data.type==='playback'){const action=['play','pause','seek','time'].includes(data.action)?data.action:null;const currentTime=Number(data.currentTime);if(!action||!Number.isFinite(currentTime)||currentTime<0)return;broadcast(roomCode,{type:'playback',roomCode,userId,userName,action,currentTime},socket);}
 else if(data.type==='file-selected'){broadcast(roomCode,{type:'notice',roomCode,message:`${userName} selected a video. Select the same file on your device to sync.`},socket);}
 else if(data.type==='leave'){rooms.get(roomCode).delete(socket);socket.roomCode=null;sendUsers(roomCode);}
 });
 socket.on('close',()=>{const code=socket.roomCode;if(code&&rooms.has(code)){rooms.get(code).delete(socket);if(rooms.get(code).size===0)rooms.delete(code);else sendUsers(code);}});
});
const heartbeat=setInterval(()=>{wss.clients.forEach(s=>{if(s.isAlive===false)return s.terminate();s.isAlive=false;s.ping();});},30000);
wss.on('close',()=>clearInterval(heartbeat));

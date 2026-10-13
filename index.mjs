// Apex Drive's online server, ready for a cloud host (Render, Railway…): relays player
// positions over WebSocket. Opened in a browser it answers "çalışıyor" with the player count.
//
//   npm install && npm start          → http://localhost:8080  (PORT to change)
//
// Players join a room (any name; the default is "türkiye") and send their car's pose a dozen
// times a second. The server keeps the last pose of everyone in the room, hands it to whoever
// joins, and forwards each update to the others. There is no server-side physics: every player
// runs their own car, and sees the others as smoothly moving ghosts they can bump into.
import { createReadStream, existsSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';

const PORT = Number(process.env.PORT ?? 8080);
const ROOT = resolve(fileURLToPath(new URL('.', import.meta.url)), '..', 'dist');
/** Players in a room at most; the world is big, but a browser can only draw so many cars. */
const ROOM_LIMIT = 32;
/** A client that sends nothing for this long (ms) is dropped. */
const TIMEOUT = 20000;

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.wasm': 'application/wasm',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
};

const http = createServer((req, res) => {
  const url = new URL(req.url ?? '/', 'http://x');
  let file = normalize(join(ROOT, decodeURIComponent(url.pathname)));
  if (!file.startsWith(ROOT)) {
    res.writeHead(403).end();
    return;
  }
  if (!existsSync(file) || statSync(file).isDirectory()) file = join(ROOT, 'index.html');
  if (!existsSync(file)) {
    // No web build beside it (the usual case in the cloud): a status page.
    let players = 0;
    for (const room of rooms.values()) players += room.size;
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' }).end(`Apex Drive sunucusu çalışıyor ✓ — şu an ${players} oyuncu, ${rooms.size} oda`);
    return;
  }
  res.writeHead(200, {
    'content-type': TYPES[extname(file)] ?? 'application/octet-stream',
    'cache-control': file.endsWith('index.html') ? 'no-cache' : 'public, max-age=31536000, immutable',
  });
  createReadStream(file).pipe(res);
});

/** @type {Map<string, Map<number, {ws: import('ws').WebSocket, id: number, name: string, vehicle: string, character: string, state: unknown, seen: number}>>} */
const rooms = new Map();
let nextId = 1;

const send = (ws, msg) => {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
};
const broadcast = (room, msg, except) => {
  for (const p of room.values()) if (p.id !== except) send(p.ws, msg);
};
const BAD = ['amk', 'aq', 'orospu', 'piç', 'sik', 'yarrak', 'göt', 'ibne', 'pezevenk', 'kahpe', 'fuck', 'shit', 'bitch'];
const filter = (s) => BAD.reduce((t, w) => t.replace(new RegExp(`(^|[^\\p{L}])${w}(?=[^\\p{L}]|$)`, 'giu'), (m, pre) => pre + '*'.repeat(w.length)), s);
const clean = (s, max) => filter(String(s ?? '').replace(/[<>]/g, '').trim().slice(0, max));

const wss = new WebSocketServer({ server: http });
wss.on('connection', (ws) => {
  let me = null;
  let room = null;
  let roomName = '';
  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(String(raw));
    } catch {
      return;
    }
    if (msg.t === 'join' && !me) {
      roomName = clean(msg.room, 32).toLocaleLowerCase('tr-TR') || 'türkiye';
      room = rooms.get(roomName);
      if (!room) {
        room = new Map();
        rooms.set(roomName, room);
      }
      if (room.size >= ROOM_LIMIT) {
        send(ws, { t: 'full' });
        ws.close();
        return;
      }
      me = { ws, id: nextId++, name: clean(msg.name, 20) || `Sürücü ${nextId}`, vehicle: clean(msg.vehicle, 40), character: clean(msg.character, 20), state: null, seen: Date.now() };
      room.set(me.id, me);
      send(ws, {
        t: 'welcome',
        id: me.id,
        room: roomName,
        players: [...room.values()].filter((p) => p.id !== me.id).map((p) => ({ id: p.id, name: p.name, vehicle: p.vehicle, character: p.character, state: p.state })),
      });
      broadcast(room, { t: 'join', id: me.id, name: me.name, vehicle: me.vehicle, character: me.character }, me.id);
      console.log(`+ ${me.name} → ${roomName} (${room.size})`);
      return;
    }
    if (!me || !room) return;
    me.seen = Date.now();
    if (msg.t === 'state') {
      me.state = msg.s;
      broadcast(room, { t: 'state', id: me.id, s: msg.s }, me.id);
    } else if (msg.t === 'chat') {
      broadcast(room, { t: 'chat', id: me.id, name: me.name, text: clean(msg.text, 140) }, me.id);
    } else if (msg.t === 'vehicle') {
      me.vehicle = clean(msg.vehicle, 40);
      broadcast(room, { t: 'vehicle', id: me.id, vehicle: me.vehicle }, me.id);
    }
  });
  const leave = () => {
    if (!me || !room) return;
    room.delete(me.id);
    broadcast(room, { t: 'leave', id: me.id });
    console.log(`- ${me.name} ← ${roomName} (${room.size})`);
    if (room.size === 0) rooms.delete(roomName);
    me = null;
  };
  ws.on('close', leave);
  ws.on('error', leave);
});

// Drop the silent.
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) for (const p of room.values()) if (now - p.seen > TIMEOUT) p.ws.terminate();
}, 5000);

http.listen(PORT, '0.0.0.0', () => {
  console.log(`Oyun sunucusu: http://localhost:${PORT}  (dist: ${ROOT})`);
  console.log('Aynı ağdaki oyuncular bilgisayarın IP adresiyle, internetten oyuncular port yönlendirmesi ya da bir bulut sunucuyla bağlanır.');
});

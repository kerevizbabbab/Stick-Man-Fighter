/*
 * Stickman Dövüş - Online Sunucu
 * -------------------------------------------------------------
 * Bağımlılık YOK. Sadece Node.js (v16+) yeterli:   node server.js
 *
 *  - Aynı klasördeki oyun .html dosyasını da sunar (http://SUNUCU:8080)
 *  - WebSocket ile oda kurma / koda katılma / rastgele eşleşme
 *  - İki oyuncu arasındaki mesajları aktarır (host-authoritative oyun,
 *    oyun mantığı sunucuda değil, host oyuncunun tarayıcısında çalışır)
 *
 * Ortam değişkenleri:  PORT (varsayılan 8080)
 */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT, 10) || 8080;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_MSG = 64 * 1024;              // tek mesaj üst sınırı
const MAX_RATE = 240;                   // saniyede en fazla mesaj
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const RELAY = new Set(['hello', 'start', 'k', 'st', 'rs', 'nr', 'rm', 'p', 'q', 'bye']);

// ---------------------------------------------------------------- HTTP
function findGameFile() {
    const dir = __dirname;
    const idx = path.join(dir, 'index.html');
    if (fs.existsSync(idx)) return idx;
    const f = fs.readdirSync(dir).find(n => /\.html?$/i.test(n));
    return f ? path.join(dir, f) : null;
}

const server = http.createServer((req, res) => {
    const url = (req.url || '/').split('?')[0];
    if (url === '/health' || url === '/stats') {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: true, online: clients.size, rooms: rooms.size, queue: queue ? 1 : 0 }));
        return;
    }
    if (url === '/' || url === '/index.html') {
        const f = findGameFile();
        if (f) {
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
            fs.createReadStream(f).pipe(res);
            return;
        }
    }
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Stickman online sunucusu çalışıyor. Oyun dosyasını (.html) bu klasöre koyarsan burada açılır.');
});

// ----------------------------------------------------------- WebSocket
class Client {
    constructor(socket) {
        this.socket = socket;
        this.buf = Buffer.alloc(0);
        this.frag = [];
        this.fragLen = 0;
        this.alive = true;
        this.lastSeen = Date.now();
        this.name = 'Oyuncu';
        this.room = null;       // oda kodu
        this.peer = null;       // eşleşilen Client
        this.winStart = Date.now();
        this.winCount = 0;
    }
    sendRaw(str) {
        if (!this.alive) return;
        const payload = Buffer.from(str, 'utf8');
        const len = payload.length;
        let head;
        if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
        else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
        else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
        head[0] = 0x81;
        try { this.socket.write(Buffer.concat([head, payload])); } catch (e) { this.close(); }
    }
    send(obj) { this.sendRaw(JSON.stringify(obj)); }
    sendControl(op, data) {
        if (!this.alive) return;
        const d = data || Buffer.alloc(0);
        try { this.socket.write(Buffer.concat([Buffer.from([0x80 | op, d.length]), d])); } catch (e) { this.close(); }
    }
    close() {
        if (!this.alive) return;
        this.alive = false;
        try { this.socket.destroy(); } catch (e) {}
        onDisconnect(this);
    }
    feed(chunk) {
        this.buf = Buffer.concat([this.buf, chunk]);
        for (;;) {
            if (this.buf.length < 2) return;
            const b0 = this.buf[0], b1 = this.buf[1];
            const fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
            let len = b1 & 0x7f, off = 2;
            if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
            else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
            if (len > MAX_MSG) { this.close(); return; }
            if (!masked) { this.close(); return; }                 // istemci çerçeveleri maskeli olmalı
            if (this.buf.length < off + 4 + len) return;
            const mask = this.buf.slice(off, off + 4);
            const data = Buffer.from(this.buf.slice(off + 4, off + 4 + len));
            for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
            this.buf = this.buf.slice(off + 4 + len);
            this.lastSeen = Date.now();

            if (op === 0x8) { this.sendControl(0x8); this.close(); return; }
            if (op === 0x9) { this.sendControl(0xA, data); continue; }
            if (op === 0xA) continue;
            if (op === 0x1 || op === 0x0) {
                this.frag.push(data); this.fragLen += data.length;
                if (this.fragLen > MAX_MSG) { this.close(); return; }
                if (fin) {
                    const msg = Buffer.concat(this.frag).toString('utf8');
                    this.frag = []; this.fragLen = 0;
                    onMessage(this, msg);
                }
            }
        }
    }
}

const clients = new Set();
const rooms = new Map();      // kod -> { host: Client, guest: Client|null }
let queue = null;             // rastgele eşleşme bekleyen

server.on('upgrade', (req, socket) => {
    const key = req.headers['sec-websocket-key'];
    if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
    const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
    socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
    socket.setNoDelay(true);
    const c = new Client(socket);
    clients.add(c);
    socket.on('data', d => { try { c.feed(d); } catch (e) { c.close(); } });
    socket.on('error', () => c.close());
    socket.on('close', () => c.close());
    c.send({ t: 'welcome', online: clients.size });
});

function cleanName(n) {
    return String(n || 'Oyuncu').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 15) || 'Oyuncu';
}
function newCode() {
    for (let k = 0; k < 50; k++) {
        let c = '';
        for (let i = 0; i < 4; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
        if (!rooms.has(c)) return c;
    }
    return null;
}
function leaveAll(c) {
    if (queue === c) queue = null;
    if (c.room) {
        const r = rooms.get(c.room);
        if (r) {
            if (r.host === c) { if (r.guest) { r.guest.peer = null; r.guest.room = null; r.guest.send({ t: 'left' }); } rooms.delete(c.room); }
            else if (r.guest === c) { r.guest = null; }
        }
        c.room = null;
    }
    if (c.peer) {
        const p = c.peer;
        c.peer = null;
        if (p.peer === c) { p.peer = null; p.send({ t: 'left' }); if (p.room) { rooms.delete(p.room); p.room = null; } }
    }
}
function pair(host, guest, code) {
    host.peer = guest; guest.peer = host;
    host.room = guest.room = code;
    rooms.set(code, { host, guest });
    host.send({ t: 'paired', role: 'host', code, opp: guest.name });
    guest.send({ t: 'paired', role: 'guest', code, opp: host.name });
}
function onDisconnect(c) {
    clients.delete(c);
    leaveAll(c);
}

function onMessage(c, raw) {
    // hız sınırı
    const now = Date.now();
    if (now - c.winStart > 1000) { c.winStart = now; c.winCount = 0; }
    if (++c.winCount > MAX_RATE) return;

    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    switch (m.t) {
        case 'create': {
            leaveAll(c);
            c.name = cleanName(m.name);
            const code = newCode();
            if (!code) { c.send({ t: 'err', msg: 'Oda kodu üretilemedi, tekrar dene.' }); return; }
            c.room = code;
            rooms.set(code, { host: c, guest: null });
            c.send({ t: 'created', code });
            return;
        }
        case 'join': {
            leaveAll(c);
            c.name = cleanName(m.name);
            const code = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 4);
            const r = rooms.get(code);
            if (!r || !r.host.alive) { c.send({ t: 'err', msg: 'Bu kodla bir oda bulunamadı.' }); return; }
            if (r.guest) { c.send({ t: 'err', msg: 'Oda dolu.' }); return; }
            pair(r.host, c, code);
            return;
        }
        case 'quick': {
            leaveAll(c);
            c.name = cleanName(m.name);
            if (queue && queue !== c && queue.alive) {
                const host = queue; queue = null;
                const code = newCode() || 'RAND';
                pair(host, c, code);
            } else {
                queue = c;
                c.send({ t: 'queued' });
            }
            return;
        }
        case 'cancel':
            leaveAll(c);
            c.send({ t: 'cancelled' });
            return;
        default:
            if (RELAY.has(m.t) && c.peer && c.peer.alive) c.peer.sendRaw(raw);
    }
}

// ------------------------------------------------------ canlılık kontrolü
setInterval(() => {
    const now = Date.now();
    for (const c of clients) {
        if (now - c.lastSeen > 60000) { c.close(); continue; }
        c.sendControl(0x9);
    }
}, 20000);

server.listen(PORT, () => {
    console.log('Stickman online sunucusu hazır: http://localhost:' + PORT);
    console.log('WebSocket adresi: ws://localhost:' + PORT);
    const f = findGameFile();
    console.log(f ? 'Oyun dosyası sunuluyor: ' + path.basename(f) : 'Uyarı: oyun .html dosyası bulunamadı (sadece WebSocket çalışır).');
});

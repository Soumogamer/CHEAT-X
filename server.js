const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');
const FILE = path.join(process.env.DATA_DIR || __dirname, 'data.json');
const RU = process.env.UPSTASH_REDIS_REST_URL, RT = process.env.UPSTASH_REDIS_REST_TOKEN;
const ICE = process.env.ICE_JSON ? JSON.parse(process.env.ICE_JSON) : [
  { urls: ['stun:stun.l.google.com:19302', 'stun:stun.cloudflare.com:3478'] },
  { urls: ['turn:openrelay.metered.ca:80', 'turn:openrelay.metered.ca:443?transport=tcp'], username: 'openrelayproject', credential: 'openrelayproject' }];
let db = { users: {} }, tm;
const redis = cmd => fetch(RU, { method: 'POST', headers: { Authorization: 'Bearer ' + RT }, body: JSON.stringify(cmd) }).then(r => r.json());
async function load() {
  try { if (RU) { const j = await redis(['GET', 'bluffdb']); if (j.result) db = JSON.parse(j.result); } else db = JSON.parse(fs.readFileSync(FILE)); } catch {}
}
const save = () => { clearTimeout(tm); tm = setTimeout(() => { const s = JSON.stringify(db); RU ? redis(['SET', 'bluffdb', s]).catch(console.error) : fs.writeFile(FILE, s, () => {}); }, 500); };
const rid = n => crypto.randomBytes(n).toString('hex');
const hash = (p, s) => crypto.scryptSync(String(p), s, 32).toString('hex');
const clean = (s, n) => String(s || '').trim().slice(0, n);
const av = a => typeof a === 'string' && (a.length <= 8 || (a.startsWith('data:image/') && a.length < 30000)) ? a : '🙂';
const send = (w, o) => w && w.readyState === 1 && w.send(JSON.stringify(o));
const err = (p, m) => send(p.ws, { t: 'err', m });
const RK = 'A,2,3,4,5,6,7,8,9,10,J,Q,K'.split(',');
const L = new Map(), P = new Map();
const log = (g, s) => { g.log.push(s); g.log = g.log.slice(-4); };
const nxt = (g, id) => g.order[(g.order.indexOf(id) + 1) % g.order.length];

const server = http.createServer((q, r) => fs.readFile(path.join(__dirname, 'index.html'), (e, d) => {
  r.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); r.end(d);
}));
const wss = new WebSocketServer({ server, maxPayload: 60000 });

const view = (l, p) => {
  const g = l.g;
  return { t: 'lobby', host: l.host, me: p.id, set: l.set, res: l.res,
    players: l.ps.map(q => ({ id: q.id, name: q.name, av: q.av, off: !!q.off, n: g ? (g.h[q.id] || []).length : 0 })),
    g: g && { turn: g.turn, rank: g.rank, pile: g.pile.length, pend: !!g.pend, rev: g.rev, log: g.log, hand: g.h[p.id],
      last: g.last && { by: g.last.by, rank: g.last.rank, n: g.last.ids.length } } };
};
function push(l) { l.ps.forEach(p => send(p.ws, view(l, p))); kick(l); }
const me = p => { const u = p.u && db.users[p.u];
  send(p.ws, { t: 'me', uid: p.u, name: p.name, av: p.av, ice: ICE, inl: !!p.l, ...(u && { tok: u.tok, hist: u.hist, wins: u.wins || 0, games: u.games || 0 }) }); };
const login = (p, id) => { const u = db.users[id]; p.u = id; p.name = u.name; p.av = u.av; p.ready = 1; };

// offline players: the server plays for them so the table never stalls
function kick(l) {
  const g = l.g; if (!g || g.auto || g.pend) return;
  const c = l.ps.find(q => q.id === g.turn);
  if (c && c.off) g.auto = setTimeout(() => {
    g.auto = null; if (l.g !== g || g.pend || g.turn !== c.id || !c.off) return;
    if (g.last) doPass(l, c); else { const k = g.h[c.id][0]; doPlay(l, c, [k], k < 52 ? k % 13 : 0); }
  }, 2500);
}

function win(l, p) {
  const g = l.g; if (!g) return;
  clearTimeout(g.pend); clearTimeout(g.auto);
  const e0 = { t: Date.now(), w: p.name, ps: l.ps.map(q => q.name) };
  l.ps.forEach(q => {
    const e = { ...e0, won: q === p };
    if (q.u) { const u = db.users[q.u]; u.hist.unshift(e); u.hist.length = Math.min(u.hist.length, 20); u.games = (u.games || 0) + 1; if (q === p) u.wins = (u.wins || 0) + 1; me(q); }
    send(q.ws, { t: 'end', h: e });
  });
  save(); l.g = null; l.res = p.name + ' won the match! 🏆'; push(l);
}

function leave(p) {
  const l = p.l; clearTimeout(p.dc); P.delete(p.pid); if (!l) return;
  l.ps = l.ps.filter(q => q !== p); p.l = null; p.off = false;
  if (l.g) { clearTimeout(l.g.pend); clearTimeout(l.g.auto); l.g = null; l.res = p.name + ' left, so the match ended'; }
  if (!l.ps.length) return L.delete(l.code);
  if (l.host === p.id) l.host = l.ps[0].id;
  push(l);
}

function doPlay(l, p, cards, r) {
  const g = l.g; if (!g || g.turn !== p.id || g.pend || !Array.isArray(cards)) return;
  const ids = [...new Set(cards)].filter(i => g.h[p.id].includes(i));
  const rk = g.rank == null ? r : g.rank; // first player sets the rank, everyone else must follow it
  if (!ids.length || !(rk >= 0 && rk < 13)) return;
  g.h[p.id] = g.h[p.id].filter(i => !ids.includes(i));
  g.pile.push(...ids); g.rank = rk; g.last = { by: p.id, rank: rk, ids }; g.passes = 0; g.rev = null;
  log(g, `${p.name} played ${ids.length} × ${RK[rk]}`);
  if (!g.h[p.id].length) { log(g, `${p.name} is out of cards!`); g.pend = setTimeout(() => { if (l.g === g) win(l, p); }, 6000); }
  g.turn = nxt(g, p.id); push(l);
}
function doPass(l, p) {
  const g = l.g; if (!g || g.turn !== p.id || g.pend || !g.last) return;
  g.passes++; g.rev = null;
  if (g.passes >= g.order.length - 1) {
    log(g, 'Everyone passed. Pile cleared.');
    g.turn = nxt(g, g.last.by); g.pile = []; g.last = null; g.passes = 0; g.rank = null;
  } else { log(g, `${p.name} passed`); g.turn = nxt(g, p.id); }
  push(l);
}
function doBluff(l, p) {
  const g = l.g; if (!g || !g.last || g.last.by === p.id) return;
  clearTimeout(g.pend); g.pend = null;
  const x = g.last, by = l.ps.find(q => q.id === x.by);
  const ok = x.ids.every(i => i < 52 ? i % 13 === x.rank : l.set.wild);
  const loser = ok ? p : by;
  g.h[loser.id].push(...g.pile);
  g.rev = { ids: x.ids, rank: x.rank, ok, by: by.name, caller: p.name, loser: loser.id };
  g.pile = []; g.last = null; g.passes = 0;
  log(g, `${p.name} called bluff: ${ok ? 'it was true' : 'it was a bluff'}! ${loser.name} takes the pile`);
  if (ok && !g.h[by.id].length) return win(l, by);
  g.rank = null; g.turn = p.id; push(l); // the player who called bluff starts the next round
}

function handle(ws, m) {
  let p = ws.p; const l = p.l, g = l && l.g;
  switch (m.t) {
    case 'hello': {
      const pid = clean(m.pid, 40) || rid(8), old = P.get(pid);
      if (old && old.ws !== ws) { clearTimeout(old.dc); old.off = false; old.ws = ws; ws.p = p = old; } else p.pid = pid;
      if (m.tok) { const id = Object.keys(db.users).find(k => db.users[k].tok === m.tok); if (id) login(p, id); }
      else if (m.name) { p.name = clean(m.name, 14); p.av = av(m.av); p.ready = 1; }
      me(p); if (p.l) push(p.l); break;
    }
    case 'guest': p.name = clean(m.name, 14) || 'Guest'; p.av = av(m.av); p.ready = 1; me(p); break;
    case 'signup': {
      const id = clean(m.uid, 20).toLowerCase();
      if (!/^\w{3,20}$/.test(id) || String(m.pw || '').length < 4) return err(p, 'ID: 3-20 letters or numbers. Password: 4+ characters.');
      if (db.users[id]) return err(p, 'That ID is taken');
      const salt = rid(8);
      db.users[id] = { salt, h: hash(m.pw, salt), name: clean(m.name, 14) || id, av: av(m.av), hist: [], wins: 0, games: 0, tok: rid(16) };
      save(); login(p, id); me(p); break;
    }
    case 'login': {
      const id = clean(m.uid, 20).toLowerCase(), u = db.users[id];
      if (!u || hash(m.pw, u.salt) !== u.h) return err(p, 'Wrong ID or password');
      login(p, id); me(p); break;
    }
    case 'profile':
      p.name = clean(m.name, 14) || p.name; p.av = av(m.av);
      if (p.u) { Object.assign(db.users[p.u], { name: p.name, av: p.av }); save(); }
      me(p); if (l) push(l); break;
    case 'list':
      send(p.ws, { t: 'list', ls: [...L.values()].filter(x => !x.g && x.ps.length < 4)
        .map(x => ({ code: x.code, host: x.ps.find(q => q.id === x.host).name, n: x.ps.length, avs: x.ps.map(q => q.av) })) }); break;
    case 'users':
      send(p.ws, { t: 'users', us: [...wss.clients].map(c => c.p).filter(q => q && q.ready)
        .map(q => ({ name: q.name, av: q.av, st: q.l ? (q.l.g ? 'In a match' : 'In a lobby') : 'Online' })) }); break;
    case 'rank':
      send(p.ws, { t: 'rank', r: Object.values(db.users).map(u => ({ name: u.name, av: u.av, w: u.wins || 0, g: u.games || 0 }))
        .sort((a, b) => b.w - a.w || a.g - b.g).slice(0, 20) }); break;
    case 'create': {
      if (l) return;
      let code; do { code = rid(2); } while (L.has(code));
      const nl = { code, host: p.id, ps: [p], set: { wild: true }, g: null, res: '' };
      L.set(code, nl); p.l = nl; p.pid = p.pid || rid(8); P.set(p.pid, p); push(nl); break;
    }
    case 'join': {
      if (l) return;
      const t = L.get(clean(m.code, 8));
      if (!t) return err(p, 'Lobby not found');
      if (t.ps.length >= 4) return err(p, 'Lobby is full');
      if (t.g) return err(p, 'Match already started');
      t.ps.push(p); p.l = t; t.res = ''; p.pid = p.pid || rid(8); P.set(p.pid, p); push(t); break;
    }
    case 'leave': leave(p); send(p.ws, { t: 'left' }); break;
    case 'set': if (l && !g && l.host === p.id) { l.set = { wild: !!m.wild }; push(l); } break;
    case 'start': {
      if (!l || g || l.host !== p.id) return;
      if (l.ps.length < 2) return err(p, 'Need at least 2 players');
      const d = [...Array(54).keys()];
      for (let i = 53; i > 0; i--) { const j = crypto.randomInt(i + 1); [d[i], d[j]] = [d[j], d[i]]; }
      const h = {}; l.ps.forEach(q => h[q.id] = []);
      d.forEach((c, i) => h[l.ps[i % l.ps.length].id].push(c));
      l.res = '';
      l.g = { h, order: l.ps.map(q => q.id), turn: l.ps[0].id, pile: [], last: null, passes: 0, log: ['Match started'], rev: null, pend: null, auto: null, rank: null };
      push(l); break;
    }
    case 'play': doPlay(l, p, m.cards, m.rank); break;
    case 'pass': doPass(l, p); break;
    case 'bluff': doBluff(l, p); break;
    case 'emo': if (l) l.ps.forEach(q => send(q.ws, { t: 'emo', from: p.id, e: clean(m.e, 4) })); break;
    case 'rtc': {
      const q = l && l.ps.find(x => x.id === m.to);
      if (q) send(q.ws, { t: 'rtc', from: p.id, data: m.data });
      break;
    }
  }
}

wss.on('connection', ws => {
  ws.p = { ws, id: rid(4), name: '', av: '🙂', u: null, l: null, ready: 0 };
  ws.on('message', raw => { let m; try { m = JSON.parse(raw); } catch { return; } try { handle(ws, m); } catch (e) { console.error(e); } });
  ws.on('close', () => {
    const p = ws.p; if (p.ws !== ws || !p.l) return;
    p.off = true; push(p.l); clearTimeout(p.dc);
    p.dc = setTimeout(() => leave(p), p.l.g ? 120000 : 20000); // time to come back and rejoin
  });
});
load().then(() => server.listen(process.env.PORT || 3000, () => console.log('Bluff server running')));

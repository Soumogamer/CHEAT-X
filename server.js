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
const av = a => typeof a === 'string' && (a.length <= 8 || (a.startsWith('data:image/') && a.length < 30000)) ? a : 'c:0';
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
  return { t: 'lobby', host: l.host, me: p.id, watch: !!p.spec, specN: (l.spec || []).length, set: l.set, res: l.res,
    players: l.ps.map(q => ({ id: q.id, name: q.name, av: q.av, bot: !!q.bot, off: !!q.off, n: g ? (g.h[q.id] || []).length : 0 })),
    g: g && { turn: g.turn, rank: g.rank, left: Math.max(0, (g.until || 0) - Date.now()), pile: g.pile.length, pend: !!g.pend, rev: g.rev, log: g.log, hand: g.h[p.id] || [],
      last: g.last && { by: g.last.by, rank: g.last.rank, n: g.last.ids.length } } };
};
function push(l) { kick(l); [...l.ps, ...(l.spec || [])].forEach(p => send(p.ws, view(l, p))); }
const me = p => { const u = p.u && db.users[p.u];
  send(p.ws, { t: 'me', uid: p.u, name: p.name, av: p.av, ice: ICE, inl: !!p.l, store: RU ? 'cloud' : 'local', ...(u && { tok: u.tok, hist: u.hist, wins: u.wins || 0, games: u.games || 0 }) }); };
const login = (p, id) => { const u = db.users[id]; p.u = id; p.name = u.name; p.av = u.av; p.ready = 1; };

// 45 seconds per turn; offline players are played for after a short wait
function timeUp(l, g) {
  if (l.g !== g) return;
  const c = l.ps.find(q => q.id === g.turn); if (!c) return;
  log(g, `${c.name} ran out of time`);
  if (g.last) doPass(l, c); else { const k = g.h[c.id][0]; doPlay(l, c, [k], k < 52 ? k % 13 : 0); }
}
function kick(l) {
  const g = l.g; if (!g) return;
  if (g.tn !== g.n) { clearTimeout(g.tm); g.tn = g.n; g.until = Date.now() + 45000; g.tm = setTimeout(() => timeUp(l, g), 45000); }
  const c = l.ps.find(q => q.id === g.turn);
  if (c && c.bot && !g.bt) g.bt = setTimeout(() => { g.bt = null; if (l.g !== g) return; if (g.turn === c.id) botAct(l, c); else kick(l); }, botDelay(l));
  if (c && c.off && !g.auto) g.auto = setTimeout(() => {
    g.auto = null; if (l.g !== g || g.turn !== c.id || !c.off) return;
    if (g.last) doPass(l, c); else { const k = g.h[c.id][0]; doPlay(l, c, [k], k < 52 ? k % 13 : 0); }
  }, 2500);
}

// ---------- bots ----------
const BOTN = ['Ace', 'Jack', 'Queen', 'King', 'Nova', 'Blaze', 'Dealer', 'Shark'];
const isRank = (l, c, r) => c < 52 ? c % 13 === r : l.set.wild; // jokers match when wild
function suspect(l, g, b) { // chance the bot thinks the last claim is a lie
  const x = g.last, tot = l.set.wild ? 6 : 4, smart = b.lvl !== 'easy';
  if (g.pend) return 1; // someone is out of cards: calling costs nothing
  const own = g.h[b.id].filter(c => isRank(l, c, x.rank)).length, n = x.ids.length, avail = tot - own;
  if (n > avail) return smart ? 1 : 0.6; // more copies claimed than can exist
  let p = 0.12 + 0.55 * (n / Math.max(avail, 1));
  if (g.h[x.by].length <= 2) p += 0.25; // close to winning
  p -= Math.min(0.25, g.pile.length / 60); // a big pile is costly if wrong
  return smart ? Math.max(0.05, Math.min(0.9, p)) : Math.max(0, Math.min(0.4, p * 0.5));
}
const botDelay = l => { const v = l.set.speed || 'slow'; return v === 'fast' ? 700 + Math.random() * 800 : v === 'normal' ? 1800 + Math.random() * 1700 : 3500 + Math.random() * 3000; };
function botAct(l, b) {
  const g = l.g; if (!g || g.turn !== b.id) return;
  if (g.pend) return doBluff(l, b);
  const h = g.h[b.id], smart = b.lvl !== 'easy', tot = l.set.wild ? 6 : 4;
  if (g.last && g.last.by !== b.id && Math.random() < suspect(l, g, b)) return doBluff(l, b);
  let r = g.rank;
  if (r == null) {
    const cnt = {}; h.forEach(c => { if (c < 52) cnt[c % 13] = (cnt[c % 13] || 0) + 1; });
    r = +Object.keys(cnt).sort((a, c) => cnt[c] - cnt[a])[0];
    if (isNaN(r) || (!smart && Math.random() < 0.5)) r = Math.floor(Math.random() * 13);
  }
  const mt = h.filter(c => isRank(l, c, r)), junk = h.filter(c => !isRank(l, c, r));
  let pick = [];
  if (mt.length) {
    pick = smart ? mt.slice(0, 4) : mt.slice(0, Math.ceil(Math.random() * mt.length));
    if (smart && junk.length && Math.random() < 0.25) pick.push(junk[0]); // hide a bluff card among real ones
  } else {
    if (g.last && Math.random() < (smart ? 0.35 : 0.5)) return doPass(l, b);
    pick = junk.slice(0, 1 + (smart && junk.length > 6 && Math.random() < 0.5 ? 1 : 0));
  }
  pick = pick.slice(0, tot);
  if (!pick.length) pick = [h[0]];
  doPlay(l, b, pick, r);
}

function win(l, p) {
  const g = l.g; if (!g) return;
  clearTimeout(g.tm); clearTimeout(g.auto); clearTimeout(g.bt);
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
  if (p.spec) { l.spec = l.spec.filter(q => q !== p); p.l = null; p.spec = false; push(l); return; }
  l.ps = l.ps.filter(q => q !== p); p.l = null; p.off = false;
  if (l.g) { clearTimeout(l.g.tm); clearTimeout(l.g.auto); clearTimeout(l.g.bt); l.g = null; l.res = p.name + ' left, so the match ended'; }
  const hum = l.ps.filter(q => !q.bot);
  if (!hum.length) { (l.spec || []).forEach(q => { q.l = null; q.spec = false; send(q.ws, { t: 'left' }); }); return L.delete(l.code); }
  if (l.host === p.id) l.host = hum[0].id;
  push(l);
}

function doPlay(l, p, cards, r) {
  const g = l.g; if (!g || g.turn !== p.id || g.pend || !Array.isArray(cards)) return;
  const ids = [...new Set(cards)].filter(i => g.h[p.id].includes(i));
  const rk = g.rank == null ? r : g.rank; // first player sets the rank, everyone else must follow it
  if (!ids.length || !(rk >= 0 && rk < 13)) return;
  g.h[p.id] = g.h[p.id].filter(i => !ids.includes(i));
  g.n++; g.pn = g.n; g.pile.push(...ids); g.rank = rk; g.last = { by: p.id, rank: rk, ids }; g.passes = 0; g.rev = null;
  log(g, `${p.name} played ${ids.length} × ${RK[rk]}`);
  if (!g.h[p.id].length) { log(g, `${p.name} is out of cards!`); g.pend = p.id; } // no timer: everyone else must bluff or pass
  g.turn = nxt(g, p.id); push(l);
}
function doPass(l, p) {
  const g = l.g; if (!g || g.turn !== p.id || !g.last) return;
  g.passes++; g.n++; g.rev = null;
  if (g.passes >= g.order.length - 1) {
    if (g.pend) return win(l, l.ps.find(q => q.id === g.pend));
    log(g, 'Everyone passed. Pile cleared.');
    g.turn = nxt(g, g.last.by); g.pile = []; g.last = null; g.passes = 0; g.rank = null;
  } else { log(g, `${p.name} passed`); g.turn = nxt(g, p.id); }
  push(l);
}
function doBluff(l, p) {
  const g = l.g; if (!g || !g.last || g.last.by === p.id || g.turn !== p.id) return;
  g.pend = null; g.n++;
  const x = g.last, by = l.ps.find(q => q.id === x.by);
  const ok = x.ids.every(i => i < 52 ? i % 13 === x.rank : l.set.wild);
  const loser = ok ? p : by;
  g.h[loser.id].push(...g.pile);
  g.rev = { ids: x.ids, rank: x.rank, ok, by: by.name, caller: p.name, loser: loser.id };
  g.pile = []; g.last = null; g.passes = 0;
  log(g, `${p.name} called bluff: ${ok ? 'it was true' : 'it was a bluff'}! ${loser.name} takes the pile`);
  if (ok && !g.h[by.id].length) return win(l, by);
  g.rank = null; g.turn = ok ? by.id : p.id; g.rev.next = g.turn; push(l); // the winner of the challenge starts the next round
}

function handle(ws, m) {
  let p = ws.p; const l = p.l, g = l && l.g;
  switch (m.t) {
    case 'hello': {
      const pid = clean(m.pid, 40) || rid(8), old = P.get(pid);
      if (old && old.ws !== ws) { clearTimeout(old.dc); old.off = false; old.ws = ws; ws.p = p = old; } else p.pid = pid;
      if (m.tok) {
        let id = Object.keys(db.users).find(k => db.users[k].tok === m.tok);
        const a = m.acct, u = a && String(a.uid || '').toLowerCase();
        // server data was lost (free hosting restarts): rebuild this account from the phone's saved copy
        if (!id && u && /^\w{3,20}$/.test(u) && !db.users[u] && /^[0-9a-f]{32}$/.test(m.tok)) {
          db.users[u] = { salt: '', h: '', name: clean(a.name, 14) || u, av: av(a.av), tok: m.tok,
            hist: Array.isArray(a.hist) ? a.hist.slice(0, 20).map(e => ({ t: +e.t || 0, w: clean(e.w, 14), won: !!e.won, ps: (e.ps || []).slice(0, 4).map(x => clean(x, 14)) })) : [],
            wins: Math.min(+a.wins || 0, 9999), games: Math.min(+a.games || 0, 9999) };
          id = u; save();
        }
        if (id) login(p, id);
      }
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
      send(p.ws, { t: 'list', ls: [...L.values()]
        .map(x => ({ code: x.code, host: x.ps.find(q => q.id === x.host).name, n: x.ps.length, avs: x.ps.map(q => q.av), live: !!x.g, w: (x.spec || []).length })) }); break;
    case 'users':
      send(p.ws, { t: 'users', us: [...wss.clients].map(c => c.p).filter(q => q && q.ready)
        .map(q => ({ name: q.name, av: q.av, st: q.l ? (q.l.g ? 'In a match' : 'In a lobby') : 'Online' })) }); break;
    case 'rank':
      send(p.ws, { t: 'rank', r: Object.values(db.users).map(u => ({ name: u.name, av: u.av, w: u.wins || 0, g: u.games || 0 }))
        .sort((a, b) => b.w - a.w || a.g - b.g).slice(0, 20) }); break;
    case 'create': {
      if (l) return;
      let code; do { code = rid(2); } while (L.has(code));
      const nl = { code, host: p.id, ps: [p], set: { wild: true, level: 'smart', speed: 'slow' }, g: null, res: '', spec: [] };
      L.set(code, nl); p.l = nl; p.pid = p.pid || rid(8); P.set(p.pid, p); push(nl); break;
    }
    case 'join': {
      if (l) return;
      const t = L.get(clean(m.code, 8));
      if (!t) return err(p, 'Lobby not found');
      if (m.watch) { t.spec = t.spec || []; t.spec.push(p); p.spec = true; p.l = t; p.pid = p.pid || rid(8); P.set(p.pid, p); push(t); break; }
      if (t.ps.length >= 4) return err(p, 'Lobby is full');
      if (t.g) return err(p, 'Match already started');
      t.ps.push(p); p.l = t; t.res = ''; p.pid = p.pid || rid(8); P.set(p.pid, p); push(t); break;
    }
    case 'sit': {
      if (!l || !p.spec || g || l.ps.length >= 4) return;
      l.spec = l.spec.filter(q => q !== p); p.spec = false; l.ps.push(p); l.res = ''; push(l); break;
    }
    case 'leave': leave(p); send(p.ws, { t: 'left' }); break;
    case 'set':
      if (l && l.host === p.id && m.speed) { l.set = { ...l.set, speed: ['slow', 'normal', 'fast'].includes(m.speed) ? m.speed : 'slow' }; push(l); }
      else if (l && !g && l.host === p.id) {
        l.set = { ...l.set, ...(m.wild !== undefined && { wild: !!m.wild }), ...(m.level && { level: m.level === 'easy' ? 'easy' : 'smart' }) };
        l.ps.forEach(q => { if (q.bot) q.lvl = l.set.level; }); push(l);
      } break;
    case 'bot': {
      if (!l || g || l.host !== p.id) return;
      if (m.op === 'add') {
        if (l.ps.length >= 4) return err(p, 'The table is full');
        const nm = BOTN.find(n => !l.ps.some(q => q.name === n)) || 'Bot';
        l.ps.push({ id: rid(4), name: nm, av: 'c:' + crypto.randomInt(20), bot: 1, lvl: l.set.level, ws: null, l, u: null, ready: 1 });
      } else if (m.op === 'del') l.ps = l.ps.filter(q => !(q.bot && q.id === m.id));
      push(l); break;
    }
    case 'start': {
      if (!l || g || l.host !== p.id) return;
      if (l.ps.length < 2) return err(p, 'Need at least 2 players');
      const d = [...Array(54).keys()];
      for (let i = 53; i > 0; i--) { const j = crypto.randomInt(i + 1); [d[i], d[j]] = [d[j], d[i]]; }
      const h = {}; l.ps.forEach(q => h[q.id] = []);
      d.forEach((c, i) => h[l.ps[i % l.ps.length].id].push(c));
      l.res = '';
      l.g = { h, order: l.ps.map(q => q.id), turn: l.ps[0].id, pile: [], last: null, passes: 0, log: ['Match started'], rev: null, pend: null, auto: null, rank: null, n: 0, tn: -1 };
      push(l); break;
    }
    case 'play': doPlay(l, p, m.cards, m.rank); break;
    case 'pass': doPass(l, p); break;
    case 'bluff': doBluff(l, p); break;
    case 'emo': if (l) [...l.ps, ...(l.spec || [])].forEach(q => send(q.ws, { t: 'emo', from: p.id, e: clean(m.e, 24) })); break;
    case 'rtc': {
      const q = l && l.ps.find(x => x.id === m.to);
      if (q) send(q.ws, { t: 'rtc', from: p.id, data: m.data });
      break;
    }
  }
}

wss.on('connection', ws => {
  ws.p = { ws, id: rid(4), name: '', av: 'c:0', u: null, l: null, ready: 0 };
  ws.on('message', raw => { let m; try { m = JSON.parse(raw); } catch { return; } try { handle(ws, m); } catch (e) { console.error(e); } });
  ws.on('close', () => {
    const p = ws.p; if (p.ws !== ws || !p.l) return;
    p.off = true; push(p.l); clearTimeout(p.dc);
    p.dc = setTimeout(() => leave(p), p.l.g ? 120000 : 20000); // time to come back and rejoin
  });
});
load().then(() => server.listen(process.env.PORT || 3000, () => console.log('Bluff server running')));

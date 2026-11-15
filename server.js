#!/usr/bin/env node
// swisspop-relay — ретрансляция Radio Swiss Pop через бесплатный хостинг.
// Зависимостей нет: достаточно Node.js 18+.
//
// Эндпоинты:
//   /         — поток AAC 192 kbit/s (браузер вместо потока увидит тест-страницу с плеером)
//   /192      — поток AAC 192 kbit/s
//   /96       — поток AAC 96 kbit/s (запасной, на плохой вечер)
//   /healthz  — «я жив» (для аптайм-мониторов и проверок площадки)
//
// Главная фишка: сервер держит соединение с клиентом открытым и сам
// переподключается к апстриму (швейцарский балансировщик закрывает
// соединения каждые ~6–8 минут, а при сбое — когда угодно), не роняя
// соединение с клиентом. AIMP ничего не замечает.
//
// Переменные окружения:
//   PORT               — порт, который задаёт площадка (по умолчанию 8080;
//                        в Dockerfile для HF Spaces зашит 7860)
//   UPSTREAM_BASE      — не трогайте, это адрес потока SRG SSR
//   RELAY_TEST_KILL_MS — только для теста: убивать апстрим каждые N мс,
//                        чтобы проверить бесшовное переподключение

'use strict';

const http = require('http');
const { Readable } = require('stream');

const UPSTREAM_BASE = process.env.UPSTREAM_BASE || 'https://stream.srg-ssr.ch/srgssr/rsp/aac/';
const PORT = Number(process.env.PORT || 8080);
const TEST_KILL_MS = Number(process.env.RELAY_TEST_KILL_MS || 0);

// эти заголовки апстрима пробрасываем клиенту (AIMP показывает название/битрейт)
const PASS_HEADERS = [
  'content-type',
  'icy-name', 'icy-br', 'icy-genre', 'icy-url',
  'icy-description', 'icy-metaint', 'icy-samplerate', 'icy-pub', 'ice-audio-info',
];

const TEST_PAGE = `<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Radio Swiss Pop — релей</title></head>
<body style="font-family:system-ui,sans-serif;max-width:640px;margin:48px auto;padding:0 16px">
<h2>📻 Radio Swiss Pop — релей работает</h2>
<audio controls autoplay style="width:100%" src="/192"></audio>
<p>Прямые ссылки для плееров (AIMP и др.):</p>
<ul>
<li><code>/192</code> — AAC 192 kbit/s</li>
<li><code>/96</code> — AAC 96 kbit/s</li>
</ul>
</body></html>`;

function log(...a) { console.log(new Date().toISOString(), ...a); }

// ждём, пока клиентское соединение снова сможет принять данные
// (backpressure), либо пока клиент не отключится
function waitDrainOrClose(res) {
  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      res.removeListener('drain', finish);
      res.removeListener('close', finish);
      resolve();
    };
    const timer = setTimeout(finish, 30000); // страховка от вечного ожидания
    res.once('drain', finish);
    res.once('close', finish);
  });
}

function serveStream(req, res, bitrate) {
  const started = Date.now();
  let bytes = 0;
  let headSent = false;
  let clientGone = false;
  let currentAc = null;
  let failStreak = 0;

  const onGone = () => {
    clientGone = true;
    if (currentAc) currentAc.abort();
  };
  res.once('close', onGone);
  res.on('error', onGone);

  (async () => {
    while (!clientGone) {
      const ac = new AbortController();
      currentAc = ac;
      const killTimer = TEST_KILL_MS > 0 ? setTimeout(() => ac.abort(), TEST_KILL_MS) : null;
      try {
        const upstream = await fetch(UPSTREAM_BASE + bitrate, {
          redirect: 'follow',
          signal: ac.signal,
          headers: { 'User-Agent': 'swisspop-relay/1.0', Accept: '*/*' },
        });
        if (!upstream.ok || !upstream.body) throw new Error('upstream HTTP ' + upstream.status);

        if (!headSent) {
          const h = { 'cache-control': 'no-store' };
          for (const k of PASS_HEADERS) {
            const v = upstream.headers.get(k);
            if (v) h[k] = v;
          }
          if (!h['content-type']) h['content-type'] = 'audio/aac';
          res.writeHead(200, h);
          res.flushHeaders();
          headSent = true;
          log('client', (req.socket.remoteAddress || '?'), '-> /' + bitrate);
        } else {
          log('upstream reconnected, continuing same client response');
        }

        let gotData = false;
        for await (const chunk of Readable.fromWeb(upstream.body)) {
          if (clientGone) break;
          if (!gotData) { gotData = true; failStreak = 0; }
          bytes += chunk.length;
          if (!res.write(chunk)) await waitDrainOrClose(res);
        }
      } catch (err) {
        if (!clientGone) {
          failStreak += 1;
          log('upstream error:', err && err.name, '-', err && err.message, '| retry 2s (streak ' + failStreak + ')');
          if (failStreak >= 15) {
            log('giving up after 15 failed rounds');
            if (!headSent) {
              res.writeHead(502, { 'content-type': 'text/plain' });
              res.end('upstream unavailable');
            }
            return;
          }
          await new Promise((r) => setTimeout(r, 2000));
        }
      } finally {
        if (killTimer) clearTimeout(killTimer);
        if (currentAc === ac) currentAc = null;
      }
      // апстрим закрыл соединение штатно (он делает это каждые ~6–8 минут) —
      // мгновенно подключаемся снова и продолжаем лить в тот же ответ
      if (!clientGone) log('upstream finished its turn, reconnecting seamlessly');
    }
  })()
    .catch((e) => log('relay loop crashed:', (e && e.stack) || e))
    .finally(() => {
      try { if (!res.writableEnded) res.end(); } catch (_) { /* соединение уже закрыто */ }
      log('client finished: bytes=' + bytes + ' duration=' + Math.round((Date.now() - started) / 1000) + 's');
    });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const acceptHtml = (req.headers.accept || '').includes('text/html');

  if (url.pathname === '/healthz') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('ok');
    return;
  }

  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405, { 'content-type': 'text/plain' });
    res.end('GET only');
    return;
  }

  // браузеру отдаём тестовую страничку с плеером, плеерам (AIMP, curl) — поток
  if (url.pathname === '/' && acceptHtml) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    res.end(TEST_PAGE);
    return;
  }

  let bitrate = null;
  if (url.pathname === '/' || url.pathname === '/192') bitrate = '192';
  else if (url.pathname === '/96') bitrate = '96';

  if (bitrate) {
    if (req.method === 'HEAD') {
      res.writeHead(200, { 'content-type': 'audio/aac' });
      res.end();
      return;
    }
    serveStream(req, res, bitrate);
    return;
  }

  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('not found: use / , /192 , /96 , /healthz');
});

// важно: без этого Node сам рубит «долгие» запросы (по умолчанию 300 с),
// а наш ответ — бесконечный поток
server.requestTimeout = 0;
server.headersTimeout = 0;
server.timeout = 0;

server.listen(PORT, '0.0.0.0', () => {
  log('swisspop-relay up on 0.0.0.0:' + PORT + ', upstream: ' + UPSTREAM_BASE);
});

server.on('error', (e) => { log('server error:', e.message); process.exit(1); });
process.on('SIGTERM', () => { log('SIGTERM, bye'); process.exit(0); });
process.on('uncaughtException', (e) => log('uncaughtException:', (e && e.stack) || e));
process.on('unhandledRejection', (e) => log('unhandledRejection:', (e && (e.stack || e)) || e));

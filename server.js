const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// ---------- Config (set these as environment variables) ----------
const PORT = process.env.PORT || 3000;
const TEACHER_KEY = process.env.TEACHER_KEY || 'teacher123';
const BUCKET = process.env.BUCKET_NAME || '';          // empty = save results locally
const REGION = process.env.AWS_REGION || 'ap-south-1'; // must match your bucket's region

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const s3 = new S3Client({ region: REGION }); // on EC2, credentials come from the IAM role automatically

app.use(express.static(path.join(__dirname, 'public')));
app.get('/teacher', (req, res) => res.sendFile(path.join(__dirname, 'public', 'teacher.html')));
app.get('/health', (req, res) => res.send('ok'));

// ---------- In-memory state (no database) ----------
let poll = null;          // { id, question, options: [{ text, votes }] }
let pollHistory = [];     // earlier polls from this session
let voters = new Set();   // clientIds that voted on the current poll
let questions = [];       // { id, text, votes, voters:Set }
let nextId = 1;

function publicState() {
  return {
    poll: poll ? { id: poll.id, question: poll.question, options: poll.options } : null,
    questions: [...questions]
      .sort((a, b) => b.votes - a.votes)
      .map(q => ({ id: q.id, text: q.text, votes: q.votes }))
  };
}

function broadcast() {
  io.emit('state', publicState());
}

function clean(text, max) {
  return String(text || '').trim().slice(0, max);
}

async function exportSession() {
  const data = {
    exportedAt: new Date().toISOString(),
    polls: [...pollHistory, ...(poll ? [poll] : [])],
    questions: questions.map(q => ({ text: q.text, votes: q.votes }))
  };
  const body = JSON.stringify(data, null, 2);
  const key = `sessions/session-${Date.now()}.json`;

  if (!BUCKET) {
    fs.mkdirSync('exports', { recursive: true });
    const file = path.join('exports', path.basename(key));
    fs.writeFileSync(file, body);
    return `Saved locally: ${file}`;
  }

  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: body,
    ContentType: 'application/json'
  }));
  return `Saved to S3: s3://${BUCKET}/${key}`;
}

// ---------- Real-time events ----------
io.on('connection', (socket) => {
  socket.emit('state', publicState());

  // Teacher: start a new poll
  socket.on('createPoll', ({ key, question, options }, cb = () => {}) => {
    if (key !== TEACHER_KEY) return cb({ ok: false, message: 'Wrong teacher key' });
    const q = clean(question, 200);
    const opts = (options || []).map(o => clean(o, 80)).filter(Boolean).slice(0, 6);
    if (!q || opts.length < 2) return cb({ ok: false, message: 'Need a question and at least 2 options' });

    if (poll) pollHistory.push(poll);
    poll = { id: nextId++, question: q, options: opts.map(text => ({ text, votes: 0 })) };
    voters = new Set();
    broadcast();
    cb({ ok: true });
  });

  // Student: vote (one vote per client per poll)
  socket.on('vote', ({ clientId, index }, cb = () => {}) => {
    if (!poll || !poll.options[index]) return cb({ ok: false });
    if (voters.has(clientId)) return cb({ ok: false, message: 'Already voted' });
    voters.add(clientId);
    poll.options[index].votes++;
    broadcast();
    cb({ ok: true });
  });

  // Student: ask a question
  socket.on('askQuestion', ({ text }, cb = () => {}) => {
    const t = clean(text, 300);
    if (!t) return cb({ ok: false });
    questions.push({ id: nextId++, text: t, votes: 0, voters: new Set() });
    broadcast();
    cb({ ok: true });
  });

  // Student: upvote a question (one upvote per client per question)
  socket.on('upvote', ({ clientId, id }, cb = () => {}) => {
    const q = questions.find(x => x.id === id);
    if (!q || q.voters.has(clientId)) return cb({ ok: false });
    q.voters.add(clientId);
    q.votes++;
    broadcast();
    cb({ ok: true });
  });

  // Teacher: end session, save results (S3), and reset
  socket.on('endSession', async ({ key }, cb = () => {}) => {
    if (key !== TEACHER_KEY) return cb({ ok: false, message: 'Wrong teacher key' });
    try {
      const message = await exportSession();
      poll = null; pollHistory = []; voters = new Set(); questions = [];
      broadcast();
      cb({ ok: true, message });
    } catch (err) {
      console.error(err);
      cb({ ok: false, message: 'Export failed: ' + err.message });
    }
  });
});

server.listen(PORT, () => console.log(`Live Q&A running on port ${PORT}`));

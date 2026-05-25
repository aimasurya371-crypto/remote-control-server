const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 500 * 1024 * 1024 // 500MB max
});

const agents = new Map();
const admins = new Map();

function generateSessionId() {
  let id;
  do {
    const n = Math.floor(100000 + Math.random() * 900000).toString();
    id = n.slice(0, 3) + '-' + n.slice(3);
  } while ([...agents.values()].some(a => a.sessionId === id));
  return id;
}

io.on('connection', (socket) => {
  console.log('Socket connected:', socket.id);

  socket.on('register-agent', ({ machineId, name }) => {
    const sessionId = generateSessionId();
    agents.set(sessionId, { socketId: socket.id, name, machineId, sessionId });
    socket.data.role = 'agent';
    socket.data.sessionId = sessionId;
    socket.data.name = name;
    socket.emit('session-id', { sessionId });
    console.log(`Agent: ${name} → Session: ${sessionId}`);
    broadcastAgentList();
  });

  socket.on('register-admin', ({ adminId }) => {
    admins.set(socket.id, adminId);
    socket.data.role = 'admin';
    socket.data.adminId = adminId;
    socket.emit('agents-list', getAgentList());
    console.log(`Admin registered: ${adminId}`);
  });

  socket.on('lookup-agent', ({ sessionId }) => {
    const agent = agents.get(sessionId.trim());
    if (!agent) {
      socket.emit('lookup-result', { found: false, sessionId });
    } else {
      socket.emit('lookup-result', {
        found: true, sessionId,
        agentSocketId: agent.socketId,
        agentName: agent.name,
        machineId: agent.machineId
      });
    }
  });

  socket.on('request-control', ({ agentSocketId, sessionId }) => {
    const agent = agents.get(sessionId);
    if (!agent || agent.socketId !== agentSocketId) {
      socket.emit('error-msg', { message: 'Agent not found.' });
      return;
    }
    io.to(agentSocketId).emit('control-request', {
      adminSocketId: socket.id,
      adminId: socket.data.adminId || 'Admin'
    });
  });

  socket.on('respond-control', ({ adminSocketId, accepted }) => {
    io.to(adminSocketId).emit('control-response', { agentSocketId: socket.id, accepted });
  });

  // ── FILE TRANSFER (no permission needed, direct send) ────────────────────
  socket.on('file-data', ({ agentSocketId, fileName, fileType, fileData, savePath }) => {
    console.log(`File transfer: ${fileName} → agent ${agentSocketId}`);
    io.to(agentSocketId).emit('file-receive', { fileName, fileType, fileData, savePath });
  });

  socket.on('file-received', ({ adminSocketId, fileName, savedAt }) => {
    io.to(adminSocketId).emit('file-delivered', { fileName, savedAt });
  });

  // ── FILE CHUNK TRANSFER (for large files >50MB) ──────────────────────────
  socket.on('file-chunk-start', ({ agentSocketId, transferId, fileName, fileType, totalChunks, totalSize, savePath }) => {
    io.to(agentSocketId).emit('file-chunk-start', { transferId, fileName, fileType, totalChunks, totalSize, savePath });
  });

  socket.on('file-chunk', ({ agentSocketId, transferId, chunkIndex, chunkData }) => {
    io.to(agentSocketId).emit('file-chunk', { transferId, chunkIndex, chunkData });
  });

  socket.on('file-chunk-end', ({ agentSocketId, transferId, fileName }) => {
    io.to(agentSocketId).emit('file-chunk-end', { transferId, fileName });
  });

  socket.on('file-chunk-received', ({ adminSocketId, fileName, savedAt }) => {
    io.to(adminSocketId).emit('file-delivered', { fileName, savedAt });
  });

  // ── COMMAND EXECUTION (run any shell command on agent) ───────────────────
  socket.on('run-command', ({ agentSocketId, command, cmdId }) => {
    console.log(`Admin running command on agent: ${command}`);
    io.to(agentSocketId).emit('run-command', { command, cmdId, adminSocketId: socket.id });
  });

  socket.on('command-result', ({ adminSocketId, cmdId, output, error }) => {
    io.to(adminSocketId).emit('command-result', { cmdId, output, error });
  });

  // ── DIRECTORY LISTING ────────────────────────────────────────────────────
  socket.on('list-directory', ({ agentSocketId, dirPath }) => {
    io.to(agentSocketId).emit('list-directory', { dirPath, adminSocketId: socket.id });
  });

  socket.on('directory-listing', ({ adminSocketId, dirPath, items, error }) => {
    io.to(adminSocketId).emit('directory-listing', { dirPath, items, error });
  });

  // ── FILE READ FROM AGENT ─────────────────────────────────────────────────
  socket.on('read-file', ({ agentSocketId, filePath }) => {
    io.to(agentSocketId).emit('read-file', { filePath, adminSocketId: socket.id });
  });

  socket.on('file-read-result', ({ adminSocketId, filePath, fileData, fileType, error }) => {
    io.to(adminSocketId).emit('file-read-result', { filePath, fileData, fileType, error });
  });

  // ── WebRTC signaling ──────────────────────────────────────────────────────
  socket.on('signal', ({ targetSocketId, data }) => {
    io.to(targetSocketId).emit('signal', { senderSocketId: socket.id, data });
  });

  socket.on('disconnect', () => {
    if (socket.data.role === 'agent' && socket.data.sessionId) {
      agents.delete(socket.data.sessionId);
      console.log(`Agent disconnected: ${socket.data.name}`);
      broadcastAgentList();
    } else if (socket.data.role === 'admin') {
      admins.delete(socket.id);
    }
  });
});

function getAgentList() {
  return [...agents.values()].map(a => ({
    sessionId: a.sessionId, name: a.name,
    machineId: a.machineId, socketId: a.socketId
  }));
}

function broadcastAgentList() { io.emit('agents-list', getAgentList()); }

server.listen(3001, () => console.log('Signaling server running on port 3001'));

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] } });

const agents = new Map();  // sessionId -> { socketId, name, machineId, socket }
const socketToSession = new Map();  // socketId -> sessionId
const admins = new Map();  // socketId -> { adminId, socket }

function generateSessionId() {
  let id;
  do {
    const n = Math.floor(100000 + Math.random() * 900000).toString();
    id = n.slice(0,3) + '-' + n.slice(3);
  } while ([...agents.values()].find(a => a.sessionId === id));
  return id;
}

function broadcastAgentsList() {
  const list = [...agents.values()].map(a => ({
    socketId: a.socketId, name: a.name,
    machineId: a.machineId, sessionId: a.sessionId
  }));
  for (const admin of admins.values()) {
    admin.socket.emit('agents-list', list);
  }
}

io.on('connection', (socket) => {
  console.log('Socket connected:', socket.id);

  socket.on('register-agent', ({ name, machineId }) => {
    const sessionId = generateSessionId();
    const agentData = { socketId: socket.id, name, machineId: machineId || 'PC', sessionId, socket };
    agents.set(sessionId, agentData);
    socketToSession.set(socket.id, sessionId);
    console.log('Agent:', name, '-> Session:', sessionId);
    socket.emit('assigned-id', { shareId: sessionId, sessionId, name });
    broadcastAgentsList();
  });

  socket.on('register-admin', ({ adminId }) => {
    admins.set(socket.id, { adminId, socket });
    console.log('Admin registered:', adminId);
    const list = [...agents.values()].map(a => ({
      socketId: a.socketId, name: a.name,
      machineId: a.machineId, sessionId: a.sessionId
    }));
    socket.emit('agents-list', list);
  });

  socket.on('lookup-agent', ({ sessionId }) => {
    const agent = agents.get(sessionId);
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
    const admin = admins.get(socket.id);
    let agent = null;
    if (agentSocketId) {
      for (const a of agents.values()) {
        if (a.socketId === agentSocketId) { agent = a; break; }
      }
    } else if (sessionId) {
      agent = agents.get(sessionId);
    }
    if (!agent) { socket.emit('error-msg', { message: 'Agent not found.' }); return; }
    console.log('Admin', socket.id, 'requesting control of', agent.sessionId);
    agent.socket.emit('control-request', {
      adminSocketId: socket.id,
      adminId: admin ? admin.adminId : 'Admin'
    });
  });

  socket.on('respond-control', ({ adminSocketId, accepted }) => {
    const admin = admins.get(adminSocketId);
    if (admin) {
      admin.socket.emit('control-response', { agentSocketId: socket.id, accepted });
    }
  });

  socket.on('connect-by-id', ({ shareId }) => {
    const agent = agents.get(shareId);
    const admin = admins.get(socket.id);
    if (!agent) { socket.emit('connect-error', { message: 'ID not found.' }); return; }
    agent.socket.emit('control-request', {
      adminSocketId: socket.id,
      adminId: admin ? admin.adminId : 'Admin'
    });
  });

  socket.on('signal', ({ targetSocketId, data }) => {
    io.to(targetSocketId).emit('signal', { senderSocketId: socket.id, data });
  });

  socket.on('list-directory', ({ agentSocketId, dirPath }) => {
    io.to(agentSocketId).emit('list-directory', { adminSocketId: socket.id, dirPath });
  });

  socket.on('directory-listing', ({ adminSocketId, dirPath, items, error }) => {
    io.to(adminSocketId).emit('directory-listing', { dirPath, items, error });
  });

  socket.on('run-command', ({ agentSocketId, command, cmdId }) => {
    io.to(agentSocketId).emit('run-command', { adminSocketId: socket.id, command, cmdId });
  });

  socket.on('command-result', ({ adminSocketId, cmdId, output, error }) => {
    io.to(adminSocketId).emit('command-result', { cmdId, output, error });
  });

  socket.on('file-data', ({ agentSocketId, fileName, fileType, fileData, savePath }) => {
    io.to(agentSocketId).emit('file-data', { fileName, fileType, fileData, savePath, adminSocketId: socket.id });
  });

  socket.on('file-delivered', ({ adminSocketId, fileName, savedAt }) => {
    io.to(adminSocketId).emit('file-delivered', { fileName, savedAt });
  });

  socket.on('disconnect', () => {
    const sessionId = socketToSession.get(socket.id);
    if (sessionId) {
      agents.delete(sessionId);
      socketToSession.delete(socket.id);
      console.log('Agent disconnected:', sessionId);
      broadcastAgentsList();
    } else if (admins.has(socket.id)) {
      admins.delete(socket.id);
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log('Signaling server running on port', PORT));

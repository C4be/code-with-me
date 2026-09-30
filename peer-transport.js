const ALLOWED_PATHS = new Set(["/api/room", "/api/files", "/api/file", "/api/rename", "/api/export"]);
const FRAME_SIZE = 12000;

function signalURL(base, roomId, params) {
  const url = new URL(`/signal/${roomId}`, base);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
  return url;
}

function waitForOpen(socket) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => fail(), 12000);
    const cleanup = () => {
      clearTimeout(timer);
      socket.removeEventListener("open", opened);
      socket.removeEventListener("error", fail);
      socket.removeEventListener("close", fail);
    };
    const opened = () => { cleanup(); resolve(); };
    const fail = () => {
      cleanup();
      socket.close();
      reject(new Error("Сервер знакомства недоступен"));
    };
    socket.addEventListener("open", opened);
    socket.addEventListener("error", fail);
    socket.addEventListener("close", fail);
  });
}

function candidateDetails(candidate) {
  const raw = candidate?.candidate || "";
  const type = candidate?.type || raw.match(/\btyp (host|srflx|prflx|relay)\b/)?.[1] || "unknown";
  const protocol = candidate?.protocol || raw.split(/\s+/)[2]?.toLowerCase() || "unknown";
  return { type, protocol };
}

function countCandidate(counts, candidate) {
  const { type, protocol } = candidateDetails(candidate);
  const key = `${type}/${protocol}`;
  counts[key] = (counts[key] || 0) + 1;
}

function selectedRoute(stats) {
  const values = [...stats.values()];
  const transport = values.find((item) => item.type === "transport" && item.selectedCandidatePairId);
  const pair = stats.get(transport?.selectedCandidatePairId)
    || values.find((item) => item.type === "candidate-pair" && (item.selected || (item.nominated && item.state === "succeeded")));
  if (!pair) return null;
  const local = stats.get(pair.localCandidateId);
  const remote = stats.get(pair.remoteCandidateId);
  if (!local?.candidateType || !remote?.candidateType) return null;
  return {
    kind: local.candidateType === "relay" || remote.candidateType === "relay" ? "relay" : "direct",
    localType: local.candidateType,
    remoteType: remote.candidateType,
    localProtocol: local.protocol || "unknown",
    remoteProtocol: remote.protocol || "unknown",
    rttMs: Number.isFinite(pair.currentRoundTripTime) ? Math.round(pair.currentRoundTripTime * 1000) : null,
  };
}

function observePeer(pc, onRoute, onDiagnostics) {
  let active = true;
  let timer = 0;
  let last = "";
  let sampling = false;
  const pairHistory = new Map();
  const started = performance.now();
  const diagnostics = {
    startedAt: new Date().toISOString(),
    iceGatheringState: pc.iceGatheringState,
    iceConnectionState: pc.iceConnectionState,
    connectionState: pc.connectionState,
    localCandidates: {},
    remoteCandidates: {},
    candidatePairs: [],
    directPairsObserved: 0,
    directChecksSent: null,
    directResponsesReceived: null,
    errors: [],
    connectedInMs: null,
    dataChannelOpenInMs: null,
    applicationRttMs: null,
    selectedRoute: null,
  };
  const publish = () => onDiagnostics?.(JSON.parse(JSON.stringify(diagnostics)));
  pc.addEventListener("icecandidate", (event) => {
    if (event.candidate) countCandidate(diagnostics.localCandidates, event.candidate);
    publish();
  });
  pc.addEventListener("icecandidateerror", (event) => {
    if (diagnostics.errors.length >= 12) return;
    diagnostics.errors.push({ code: event.errorCode, server: event.url || "" });
    publish();
  });
  for (const [name, property] of [
    ["icegatheringstatechange", "iceGatheringState"],
    ["iceconnectionstatechange", "iceConnectionState"],
    ["connectionstatechange", "connectionState"],
  ]) {
    pc.addEventListener(name, () => {
      diagnostics[property] = pc[property];
      if (name === "connectionstatechange" && pc.connectionState === "connected" && diagnostics.connectedInMs === null) {
        diagnostics.connectedInMs = Math.round(performance.now() - started);
      }
      publish();
      if (name !== "icegatheringstatechange") sample();
    });
  }
  const sample = async () => {
    if (!active || pc.connectionState === "closed" || sampling) return;
    sampling = true;
    try {
      const stats = await pc.getStats();
      if (!active) return;
      const route = selectedRoute(stats);
      for (const item of stats.values()) {
        if (item.type !== "candidate-pair") continue;
        pairHistory.set(item.id, {
          local: stats.get(item.localCandidateId)?.candidateType || "unknown",
          remote: stats.get(item.remoteCandidateId)?.candidateType || "unknown",
          localProtocol: stats.get(item.localCandidateId)?.protocol || "unknown",
          remoteProtocol: stats.get(item.remoteCandidateId)?.protocol || "unknown",
          state: item.state || "unknown",
          nominated: Boolean(item.nominated),
          requestsSent: Number.isFinite(item.requestsSent) ? item.requestsSent : null,
          responsesReceived: Number.isFinite(item.responsesReceived) ? item.responsesReceived : null,
          rttMs: Number.isFinite(item.currentRoundTripTime) ? Math.round(item.currentRoundTripTime * 1000) : null,
        });
      }
      diagnostics.candidatePairs = [...pairHistory.values()].slice(0, 60);
      const directPairs = [...pairHistory.values()].filter((pair) => pair.local !== "relay" && pair.remote !== "relay");
      diagnostics.directPairsObserved = directPairs.length;
      diagnostics.directChecksSent = directPairs.some((pair) => pair.requestsSent !== null)
        ? directPairs.reduce((sum, pair) => sum + (pair.requestsSent || 0), 0) : null;
      diagnostics.directResponsesReceived = directPairs.some((pair) => pair.responsesReceived !== null)
        ? directPairs.reduce((sum, pair) => sum + (pair.responsesReceived || 0), 0) : null;
      if (route) {
        const current = JSON.stringify(route);
        if (current !== last) { last = current; onRoute(route); }
        diagnostics.selectedRoute = route;
      }
      publish();
    } catch { /* Some WebViews expose candidate stats only after ICE settles. */ }
    finally { sampling = false; }
    if (!active) return;
    clearTimeout(timer);
    timer = setTimeout(sample, diagnostics.connectedInMs === null ? 500 : 4000);
  };
  sample();
  return {
    recordRemoteCandidate(candidate) {
      if (!candidate) return;
      countCandidate(diagnostics.remoteCandidates, candidate);
      publish();
    },
    markDataOpen() {
      diagnostics.dataChannelOpenInMs = Math.round(performance.now() - started);
      publish();
      sample();
    },
    markApplicationRtt(milliseconds) {
      diagnostics.applicationRttMs = Math.round(milliseconds);
      publish();
    },
    stop() { active = false; clearTimeout(timer); },
  };
}

function startChannelPings(packets, probe) {
  const pending = new Map();
  const ping = () => {
    if (packets.channel.readyState !== "open") return;
    const id = crypto.randomUUID();
    pending.set(id, performance.now());
    packets.send({ type: "latency:ping", id }).catch(() => pending.delete(id));
    for (const [key, started] of pending) if (performance.now() - started > 15000) pending.delete(key);
  };
  ping();
  const timer = setInterval(ping, 4000);
  return {
    receive(id) {
      const started = pending.get(id);
      if (started === undefined) return;
      pending.delete(id);
      probe?.markApplicationRtt(performance.now() - started);
    },
    stop() { clearInterval(timer); pending.clear(); },
  };
}

class PacketChannel {
  constructor(channel, onMessage) {
    this.channel = channel;
    this.onMessage = onMessage;
    this.pending = new Map();
    channel.bufferedAmountLowThreshold = 128 * 1024;
    channel.onmessage = (event) => {
      let frame;
      try { frame = JSON.parse(event.data); } catch { return; }
      if (!Number.isInteger(frame.index) || !Number.isInteger(frame.total) || frame.total < 1 || frame.total > 20000 || frame.index < 0 || frame.index >= frame.total || typeof frame.data !== "string") return;
      const current = this.pending.get(frame.id) || { parts: new Array(frame.total), received: 0, created: Date.now() };
      if (current.parts.length !== frame.total) return;
      if (current.parts[frame.index] === undefined) { current.parts[frame.index] = frame.data; current.received++; }
      this.pending.set(frame.id, current);
      if (current.received === current.parts.length) {
        this.pending.delete(frame.id);
        try { this.onMessage(JSON.parse(current.parts.join(""))); } catch { /* malformed packet */ }
      }
      for (const [id, item] of this.pending) if (Date.now() - item.created > 60000) this.pending.delete(id);
    };
  }

  async send(value) {
    const serialized = JSON.stringify(value);
    const id = crypto.randomUUID();
    const total = Math.max(1, Math.ceil(serialized.length / FRAME_SIZE));
    for (let index = 0; index < total; index++) {
      if (this.channel.readyState !== "open") throw new Error("Прямое соединение прервано");
      while (this.channel.bufferedAmount > 256 * 1024) {
        await new Promise((resolve) => {
          this.channel.addEventListener("bufferedamountlow", resolve, { once: true });
          setTimeout(resolve, 1000);
        });
        if (this.channel.readyState !== "open") throw new Error("Прямое соединение прервано");
      }
      this.channel.send(JSON.stringify({ id, index, total, data: serialized.slice(index * FRAME_SIZE, (index + 1) * FRAME_SIZE) }));
    }
  }
}

export class HostBridge {
  constructor(info) {
    this.info = info;
    this.peers = new Map();
  }

  async connect() {
    if (!globalThis.RTCPeerConnection) throw new Error("На этом компьютере WebRTC недоступен");
    const socket = new WebSocket(signalURL(new URL(this.info.inviteUrl).origin, this.info.roomId, { host: "true", secret: this.info.hostSecret }));
    this.socket = socket;
    await waitForOpen(socket);
    socket.onmessage = (event) => this.handleSignal(JSON.parse(event.data));
    // Existing data channels remain usable if the rendezvous service restarts.
    socket.onclose = () => {};
  }

  async handleSignal(message) {
    if (message.type === "hello") { this.iceServers = message.iceServers; return; }
    if (message.type === "peer-left") { this.closePeer(message.peerId); return; }
    if (message.type === "peer-joined") { this.iceServers = message.iceServers || this.iceServers; await this.addPeer(message.peerId); return; }
    const peer = this.peers.get(message.from);
    if (!peer) return;
    try {
      if (message.type === "answer") {
        await peer.pc.setRemoteDescription(message.description);
        for (const candidate of peer.candidates.splice(0)) await peer.pc.addIceCandidate(candidate);
      } else if (message.type === "ice") {
        peer.probe?.recordRemoteCandidate(message.candidate);
        if (peer.pc.remoteDescription) await peer.pc.addIceCandidate(message.candidate);
        else peer.candidates.push(message.candidate);
      }
    } catch (error) { console.warn("WebRTC signaling", error); }
  }

  async addPeer(id) {
    const pc = new RTCPeerConnection({ iceServers: this.iceServers || [], iceTransportPolicy: "all" });
    const peer = { pc, candidates: [], roomSocket: null, packets: null, probe: null, pings: null };
    this.peers.set(id, peer);
    peer.probe = observePeer(pc, (route) => this.onRoute?.(id, route), (report) => this.onDiagnostics?.(id, report));
    pc.onicecandidate = ({ candidate }) => { if (candidate) this.sendSignal({ type: "ice", to: id, candidate }); };
    pc.onconnectionstatechange = () => { if (["failed", "closed"].includes(pc.connectionState)) this.closePeer(id); };
    const channel = pc.createDataChannel("room", { ordered: true });
    channel.onopen = () => {
      peer.packets = new PacketChannel(channel, (message) => this.handlePacket(peer, message));
      peer.probe?.markDataOpen();
      peer.pings = startChannelPings(peer.packets, peer.probe);
    };
    try {
      await pc.setLocalDescription(await pc.createOffer());
      this.sendSignal({ type: "offer", to: id, description: pc.localDescription });
    } catch (error) { console.warn("WebRTC offer", error); this.closePeer(id); }
  }

  sendSignal(value) { if (this.socket?.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify(value)); }

  async handlePacket(peer, message) {
    if (message.type === "latency:ping") { peer.packets?.send({ type: "latency:pong", id: message.id }).catch(() => {}); return; }
    if (message.type === "latency:pong") { peer.pings?.receive(message.id); return; }
    if (message.type === "rpc") {
      const { id, path, method = "GET", body = null } = message;
      if (!ALLOWED_PATHS.has(path) || !["GET", "POST", "PUT", "DELETE"].includes(method)) {
        peer.packets.send({ type: "rpc-result", id, status: 403, body: "Запрос запрещён" }); return;
      }
      try {
        const url = new URL(path, this.info.localBase);
        url.searchParams.set("code", this.info.inviteCode);
        for (const [key, value] of Object.entries(message.query || {})) url.searchParams.set(key, value);
        const response = await fetch(url, { method, body, headers: method === "POST" ? { "Content-Type": "application/json" } : {} });
        await peer.packets.send({ type: "rpc-result", id, status: response.status, body: await response.text() });
      } catch (error) { peer.packets.send({ type: "rpc-result", id, status: 503, body: String(error) }); }
      return;
    }
    if (message.type === "ws-open") {
      peer.roomSocket?.close();
      const url = new URL("/ws", this.info.localBase);
      url.protocol = "ws:";
      url.searchParams.set("code", this.info.inviteCode);
      url.searchParams.set("name", String(message.name || "Гость").slice(0, 40));
      const socket = new WebSocket(url);
      peer.roomSocket = socket;
      socket.onmessage = (event) => peer.packets?.send({ type: "ws-event", data: event.data });
      socket.onclose = () => peer.packets?.send({ type: "ws-close" });
    }
    if (message.type === "ws-send" && peer.roomSocket?.readyState === WebSocket.OPEN) peer.roomSocket.send(message.data);
    if (message.type === "ws-close") peer.roomSocket?.close();
  }

  closePeer(id) { const peer = this.peers.get(id); if (!peer) return; peer.pings?.stop(); peer.probe?.stop(); peer.roomSocket?.close(); peer.pc.close(); this.peers.delete(id); this.onPeerClose?.(id); }
  close() {
    this.socket?.close();
    for (const [id, peer] of this.peers) {
      if (peer.packets) {
        peer.packets.send({ type: "room-ended" }).catch(() => {});
        setTimeout(() => this.closePeer(id), 400);
      } else this.closePeer(id);
    }
  }
}

export class GuestTransport {
  constructor(info) {
    this.info = info;
    this.pending = new Map();
    this.pendingEvents = [];
    this.virtualSocket = {
      readyState: WebSocket.CONNECTING,
      onmessage: null,
      onclose: null,
      onerror: null,
      send: (data) => this.packets?.send({ type: "ws-send", data }),
      close: () => this.close(),
    };
    Object.defineProperty(this.virtualSocket, "onmessage", {
      get: () => this.messageHandler,
      set: (handler) => {
        this.messageHandler = handler;
        if (handler) for (const data of this.pendingEvents.splice(0)) handler({ data });
      },
    });
  }

  async connect(name) {
    if (!globalThis.RTCPeerConnection) throw new Error("Этот браузер не поддерживает WebRTC");
    this.name = name;
    const url = signalURL(new URL(this.info.inviteUrl).origin, this.info.roomId, { code: this.info.inviteCode });
    this.socket = new WebSocket(url);
    await waitForOpen(this.socket);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.close();
        reject(new Error("Не удалось установить соединение за 25 секунд"));
      }, 25000);
      this.socket.onmessage = async (event) => {
        const message = JSON.parse(event.data);
        try {
          if (message.type === "room-ended") {
            clearTimeout(timer);
            this.virtualSocket.readyState = WebSocket.CLOSED;
            this.virtualSocket.onclose?.();
            reject(new Error("Ведущий завершил комнату"));
            return;
          }
          if (message.type === "hello") { this.iceServers = message.iceServers; return; }
          if (message.type === "offer") { await this.acceptOffer(message); return; }
          if (message.type === "ice") {
            if (this.pc?.remoteDescription) {
              this.probe?.recordRemoteCandidate(message.candidate);
              await this.pc.addIceCandidate(message.candidate);
            }
            else (this.candidates ||= []).push(message.candidate);
          }
        } catch (error) { clearTimeout(timer); reject(error); }
      };
      this.socket.onclose = () => { if (!this.packets) { clearTimeout(timer); reject(new Error("Сервер знакомства отключился")); } };
      this.ready = () => { clearTimeout(timer); resolve(this.virtualSocket); };
    });
  }

  async acceptOffer(message) {
    this.hostId = message.from;
    const pc = new RTCPeerConnection({ iceServers: this.iceServers || [], iceTransportPolicy: "all" });
    this.pc = pc;
    this.probe = observePeer(pc, (route) => this.onRoute?.(route), (report) => this.onDiagnostics?.(report));
    pc.onconnectionstatechange = () => {
      if (["failed", "closed"].includes(pc.connectionState)) this.virtualSocket.onclose?.();
    };
    pc.onicecandidate = ({ candidate }) => { if (candidate && this.socket.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify({ type: "ice", to: this.hostId, candidate })); };
    pc.ondatachannel = ({ channel }) => {
      channel.onopen = () => {
        this.packets = new PacketChannel(channel, (packet) => this.handlePacket(packet));
        this.probe?.markDataOpen();
        this.pings = startChannelPings(this.packets, this.probe);
        this.virtualSocket.readyState = WebSocket.OPEN;
        this.packets.send({ type: "ws-open", name: this.name });
        this.ready();
      };
      channel.onclose = () => { this.virtualSocket.readyState = WebSocket.CLOSED; this.virtualSocket.onclose?.(); };
    };
    await pc.setRemoteDescription(message.description);
    for (const candidate of this.candidates || []) {
      this.probe?.recordRemoteCandidate(candidate);
      await pc.addIceCandidate(candidate);
    }
    this.candidates = [];
    await pc.setLocalDescription(await pc.createAnswer());
    this.socket.send(JSON.stringify({ type: "answer", to: this.hostId, description: pc.localDescription }));
  }

  handlePacket(packet) {
    if (packet.type === "latency:ping") { this.packets?.send({ type: "latency:pong", id: packet.id }).catch(() => {}); return; }
    if (packet.type === "latency:pong") { this.pings?.receive(packet.id); return; }
    if (packet.type === "room-ended") { this.virtualSocket.readyState = WebSocket.CLOSED; this.virtualSocket.onclose?.(); return; }
    if (packet.type === "ws-event") {
      if (this.virtualSocket.onmessage) this.virtualSocket.onmessage({ data: packet.data });
      else this.pendingEvents.push(packet.data);
    }
    if (packet.type === "ws-close") { this.virtualSocket.readyState = WebSocket.CLOSED; this.virtualSocket.onclose?.(); }
    if (packet.type === "rpc-result") { const pending = this.pending.get(packet.id); if (pending) { this.pending.delete(packet.id); pending(packet); } }
  }

  request(path, options = {}, query = {}) {
    if (!this.packets) return Promise.reject(new Error("Прямое соединение ещё не готово"));
    const id = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error("Ведущий не ответил на запрос")); }, 120000);
      this.pending.set(id, (packet) => { clearTimeout(timer); if (packet.status >= 400) reject(new Error(packet.body)); else resolve(packet.body); });
      this.packets.send({ type: "rpc", id, path, method: options.method || "GET", body: options.body || null, query }).catch(reject);
    });
  }

  close() { this.pings?.stop(); this.probe?.stop(); this.virtualSocket.readyState = WebSocket.CLOSED; this.pc?.close(); this.socket?.close(); for (const pending of this.pending.values()) pending({ status: 503, body: "Соединение закрыто" }); this.pending.clear(); }
}

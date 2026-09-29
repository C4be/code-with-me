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

async function selectedRoute(pc) {
  const stats = await pc.getStats();
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
    rttMs: Number.isFinite(pair.currentRoundTripTime) ? Math.round(pair.currentRoundTripTime * 1000) : null,
  };
}

function observeRoute(pc, onRoute) {
  let active = true;
  let timer = 0;
  let attempts = 0;
  let last = "";
  const sample = async () => {
    if (!active || pc.connectionState === "closed") return;
    try {
      const route = await selectedRoute(pc);
      if (route) {
        const current = JSON.stringify(route);
        if (current !== last) { last = current; onRoute(route); }
        timer = setTimeout(sample, 4000);
        return;
      }
    } catch { /* Some WebViews expose candidate stats only after ICE settles. */ }
    if (++attempts < 12) timer = setTimeout(sample, 500);
  };
  sample();
  return () => { active = false; clearTimeout(timer); };
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
        if (peer.pc.remoteDescription) await peer.pc.addIceCandidate(message.candidate);
        else peer.candidates.push(message.candidate);
      }
    } catch (error) { console.warn("WebRTC signaling", error); }
  }

  async addPeer(id) {
    const pc = new RTCPeerConnection({ iceServers: this.iceServers || [] });
    const peer = { pc, candidates: [], roomSocket: null, packets: null, stopRoute: null };
    this.peers.set(id, peer);
    pc.onicecandidate = ({ candidate }) => { if (candidate) this.sendSignal({ type: "ice", to: id, candidate }); };
    pc.onconnectionstatechange = () => { if (["failed", "closed"].includes(pc.connectionState)) this.closePeer(id); };
    const channel = pc.createDataChannel("room", { ordered: true });
    channel.onopen = () => {
      peer.packets = new PacketChannel(channel, (message) => this.handlePacket(peer, message));
      peer.stopRoute = observeRoute(pc, (route) => this.onRoute?.(id, route));
    };
    try {
      await pc.setLocalDescription(await pc.createOffer());
      this.sendSignal({ type: "offer", to: id, description: pc.localDescription });
    } catch (error) { console.warn("WebRTC offer", error); this.closePeer(id); }
  }

  sendSignal(value) { if (this.socket?.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify(value)); }

  async handlePacket(peer, message) {
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

  closePeer(id) { const peer = this.peers.get(id); if (!peer) return; peer.stopRoute?.(); peer.roomSocket?.close(); peer.pc.close(); this.peers.delete(id); this.onPeerClose?.(id); }
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
            if (this.pc?.remoteDescription) await this.pc.addIceCandidate(message.candidate);
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
    const pc = new RTCPeerConnection({ iceServers: this.iceServers || [] });
    this.pc = pc;
    pc.onconnectionstatechange = () => {
      if (["failed", "closed"].includes(pc.connectionState)) this.virtualSocket.onclose?.();
    };
    pc.onicecandidate = ({ candidate }) => { if (candidate && this.socket.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify({ type: "ice", to: this.hostId, candidate })); };
    pc.ondatachannel = ({ channel }) => {
      channel.onopen = () => {
        this.packets = new PacketChannel(channel, (packet) => this.handlePacket(packet));
        this.stopRoute = observeRoute(pc, (route) => this.onRoute?.(route));
        this.virtualSocket.readyState = WebSocket.OPEN;
        this.packets.send({ type: "ws-open", name: this.name });
        this.ready();
      };
      channel.onclose = () => { this.virtualSocket.readyState = WebSocket.CLOSED; this.virtualSocket.onclose?.(); };
    };
    await pc.setRemoteDescription(message.description);
    for (const candidate of this.candidates || []) await pc.addIceCandidate(candidate);
    this.candidates = [];
    await pc.setLocalDescription(await pc.createAnswer());
    this.socket.send(JSON.stringify({ type: "answer", to: this.hostId, description: pc.localDescription }));
  }

  handlePacket(packet) {
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

  close() { this.stopRoute?.(); this.virtualSocket.readyState = WebSocket.CLOSED; this.pc?.close(); this.socket?.close(); for (const pending of this.pending.values()) pending({ status: 503, body: "Соединение закрыто" }); this.pending.clear(); }
}

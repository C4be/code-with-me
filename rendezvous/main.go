package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const maxParticipants = 10

type room struct {
	id, inviteURL        string
	inviteHash, hostHash [32]byte
	host                 *peer
	guests               map[string]*peer
	created              time.Time
}

type peer struct {
	id   string
	conn *websocket.Conn
	mu   sync.Mutex
}

func (p *peer) send(value any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return p.conn.WriteJSON(value)
}

type service struct {
	mu                                                         sync.Mutex
	rooms                                                      map[string]*room
	publicURL, legacyURL, turnHost, turnLegacyHost, turnSecret string
}

type registerRequest struct {
	RoomID, InviteCode, HostSecret, InviteURL string
}

type signal struct {
	Type        string          `json:"type"`
	To          string          `json:"to,omitempty"`
	From        string          `json:"from,omitempty"`
	Description json.RawMessage `json:"description,omitempty"`
	Candidate   json.RawMessage `json:"candidate,omitempty"`
}

func main() {
	s := &service{rooms: map[string]*room{}, publicURL: strings.TrimRight(os.Getenv("PUBLIC_URL"), "/"), legacyURL: strings.TrimRight(os.Getenv("LEGACY_PUBLIC_URL"), "/"), turnHost: os.Getenv("TURN_HOST"), turnLegacyHost: os.Getenv("TURN_LEGACY_HOST"), turnSecret: os.Getenv("TURN_SECRET")}
	if s.publicURL == "" {
		log.Fatal("PUBLIC_URL is required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /api/rooms", s.register)
	mux.HandleFunc("GET /api/rooms/{id}", s.info)
	mux.HandleFunc("DELETE /api/rooms/{id}", s.delete)
	mux.HandleFunc("GET /signal/{id}", s.connect)
	server := &http.Server{Addr: "127.0.0.1:8787", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}

func digest(value string) [32]byte { return sha256.Sum256([]byte(value)) }
func same(stored [32]byte, supplied string) bool {
	candidate := digest(supplied)
	return subtle.ConstantTimeCompare(stored[:], candidate[:]) == 1
}
func token() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func validToken(value string) bool {
	if len(value) != 32 && len(value) != 36 {
		return false
	}
	for _, c := range value {
		if !strings.ContainsRune("0123456789abcdef-", c) {
			return false
		}
	}
	return true
}
func jsonReply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func (s *service) register(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != "" {
		http.Error(w, "desktop host only", http.StatusForbidden)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var request registerRequest
	if json.NewDecoder(r.Body).Decode(&request) != nil || !validToken(request.RoomID) || !validToken(request.InviteCode) || !validToken(request.HostSecret) {
		http.Error(w, "invalid room", http.StatusBadRequest)
		return
	}
	path := "/?room=" + request.RoomID + "&code=" + request.InviteCode
	if request.InviteURL != s.publicURL+path && (s.legacyURL == "" || request.InviteURL != s.legacyURL+path) {
		http.Error(w, "invalid room", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, room := range s.rooms {
		if room.host == nil && time.Since(room.created) > 2*time.Minute {
			delete(s.rooms, id)
		}
	}
	if len(s.rooms) >= 1000 {
		http.Error(w, "capacity reached", http.StatusServiceUnavailable)
		return
	}
	if _, exists := s.rooms[request.RoomID]; exists {
		http.Error(w, "room exists", http.StatusConflict)
		return
	}
	s.rooms[request.RoomID] = &room{id: request.RoomID, inviteURL: request.InviteURL, inviteHash: digest(request.InviteCode), hostHash: digest(request.HostSecret), guests: map[string]*peer{}, created: time.Now()}
	log.Printf("registered room %s", request.RoomID)
	jsonReply(w, http.StatusCreated, map[string]any{"roomId": request.RoomID})
}

func (s *service) lookup(r *http.Request) (*room, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	room := s.rooms[r.PathValue("id")]
	if room == nil || !same(room.inviteHash, r.URL.Query().Get("code")) {
		return nil, errors.New("room not found")
	}
	return room, nil
}

func (s *service) info(w http.ResponseWriter, r *http.Request) {
	room, err := s.lookup(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	count := len(room.guests) + 1
	online := room.host != nil && room.host.conn != nil
	s.mu.Unlock()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	jsonReply(w, http.StatusOK, map[string]any{"roomId": room.id, "inviteCode": r.URL.Query().Get("code"), "inviteUrl": room.inviteURL, "participantCount": count, "maxParticipants": maxParticipants, "online": online})
}

func (s *service) delete(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	room := s.rooms[r.PathValue("id")]
	if room == nil || !same(room.hostHash, r.Header.Get("X-Host-Secret")) {
		s.mu.Unlock()
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	delete(s.rooms, room.id)
	peers := []*peer{}
	if room.host != nil {
		peers = append(peers, room.host)
	}
	for _, guest := range room.guests {
		peers = append(peers, guest)
	}
	s.mu.Unlock()
	for _, p := range peers {
		if p.conn == nil {
			continue
		}
		if p != room.host {
			_ = p.send(map[string]any{"type": "room-ended"})
		}
		p.conn.Close()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *service) iceServers(id string) []map[string]any {
	if s.turnHost == "" || s.turnSecret == "" {
		return []map[string]any{}
	}
	username := fmt.Sprintf("%d:%s", time.Now().Add(time.Hour).Unix(), id)
	h := hmac.New(sha1.New, []byte(s.turnSecret))
	h.Write([]byte(username))
	urls := []string{"stun:" + s.turnHost + ":3478", "turn:" + s.turnHost + ":3478?transport=udp", "turn:" + s.turnHost + ":3478?transport=tcp"}
	if s.turnLegacyHost != "" && s.turnLegacyHost != s.turnHost {
		urls = append(urls, "stun:"+s.turnLegacyHost+":3478", "turn:"+s.turnLegacyHost+":3478?transport=udp", "turn:"+s.turnLegacyHost+":3478?transport=tcp")
	}
	return []map[string]any{{"urls": urls, "username": username, "credential": base64.StdEncoding.EncodeToString(h.Sum(nil))}}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	// The desktop WebView has a platform-specific Origin; possession of the room credential is required.
	switch origin {
	case "", "tauri://localhost", "http://tauri.localhost", "https://tauri.localhost", "http://localhost:1420", "http://127.0.0.1:1420":
		return true
	default:
		return origin == "https://"+r.Host
	}
}}

func (s *service) connect(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	room := s.rooms[r.PathValue("id")]
	host := r.URL.Query().Get("host") == "true"
	authorized := room != nil && ((host && same(room.hostHash, r.URL.Query().Get("secret"))) || (!host && same(room.inviteHash, r.URL.Query().Get("code"))))
	if !authorized || (host && room.host != nil) || (!host && (room.host == nil || room.host.conn == nil || len(room.guests) >= maxParticipants-1)) {
		s.mu.Unlock()
		log.Printf("signal rejected for room %s, host=%t, authorized=%t", r.PathValue("id"), host, authorized)
		http.Error(w, "room unavailable", http.StatusForbidden)
		return
	}
	// Reserve a guest slot before the WebSocket upgrade to enforce the room limit.
	id := token()
	reservation := &peer{id: id}
	if host {
		room.host = reservation
	} else {
		room.guests[id] = reservation
	}
	s.mu.Unlock()
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed for room %s: %v", room.id, err)
		s.remove(room, reservation, host)
		return
	}
	s.mu.Lock()
	reservation.conn = conn
	s.mu.Unlock()
	log.Printf("signal connected for room %s, host=%t", room.id, host)
	defer func() { conn.Close(); s.remove(room, reservation, host) }()
	conn.SetReadLimit(1024 * 1024)
	if err := reservation.send(map[string]any{"type": "hello", "id": id, "iceServers": s.iceServers(id)}); err != nil {
		log.Printf("signal hello failed for room %s: %v", room.id, err)
		return
	}
	if !host {
		s.mu.Lock()
		owner := room.host
		s.mu.Unlock()
		if owner == nil || owner.conn == nil || owner.send(map[string]any{"type": "peer-joined", "peerId": id, "iceServers": s.iceServers(owner.id)}) != nil {
			return
		}
	}
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			log.Printf("signal disconnected for room %s, host=%t: %v", room.id, host, err)
			return
		}
		var message signal
		if json.Unmarshal(payload, &message) != nil || (message.Type != "offer" && message.Type != "answer" && message.Type != "ice") {
			continue
		}
		if message.Type == "ice" && len(message.Candidate) > 8192 {
			continue
		}
		if message.Type != "ice" && len(message.Description) > 100000 {
			continue
		}
		message.From = id
		s.mu.Lock()
		var target *peer
		if host {
			target = room.guests[message.To]
		} else if room.host != nil && message.To == room.host.id {
			target = room.host
		}
		s.mu.Unlock()
		if target != nil && target.conn != nil {
			target.send(message)
		}
	}
}

func (s *service) remove(room *room, p *peer, host bool) {
	s.mu.Lock()
	guestsToClose := []*peer{}
	if host && room.host == p {
		room.host = nil
		delete(s.rooms, room.id)
		for _, guest := range room.guests {
			guestsToClose = append(guestsToClose, guest)
		}
	}
	if !host && room.guests[p.id] == p {
		delete(room.guests, p.id)
	}
	owner := room.host
	s.mu.Unlock()
	for _, guest := range guestsToClose {
		if guest.conn != nil {
			_ = guest.send(map[string]any{"type": "room-ended"})
			guest.conn.Close()
		}
	}
	if !host && owner != nil && owner.conn != nil {
		owner.send(map[string]any{"type": "peer-left", "peerId": p.id})
	}
}

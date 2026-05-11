// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replsource

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/jackc/pglogrepl"
)

// Slot is the local state for a PostgreSQL logical replication slot.
type Slot struct {
	Name              string
	Plugin            string
	Database          string
	Temporary         bool
	Active            bool
	ActivePID         int32
	OwnerPID          int32
	RestartLSN        pglogrepl.LSN
	ConfirmedFlushLSN pglogrepl.LSN
	TwoPhase          bool
	TotalTxns         int64
	TotalBytes        int64
}

// Sender is the local state for one active START_REPLICATION stream.
type Sender struct {
	ID              uint64
	SlotName        string
	Publications    []string
	PID             int32
	User            string
	ApplicationName string
	ClientAddr      string
	ClientHostname  string
	ClientPort      int32
	BackendStart    time.Time
	State           string
	SentLSN         pglogrepl.LSN
	WriteLSN        pglogrepl.LSN
	FlushLSN        pglogrepl.LSN
	ReplayLSN       pglogrepl.LSN
	ReplyTime       time.Time
}

// SenderInfo contains caller-provided metadata for a replication sender.
type SenderInfo struct {
	SlotName        string
	Publications    []string
	PID             int32
	User            string
	ApplicationName string
	RemoteAddr      net.Addr
	StartLSN        pglogrepl.LSN
}

type registry struct {
	mu          sync.Mutex
	nextID      uint64
	slots       map[string]*Slot
	senders     map[uint64]*Sender
	queues      map[uint64]chan WALMessage
	current     pglogrepl.LSN
	systemID    string
	journal     []journalEntry
	storageFS   filesys.Filesys
	storagePath string
}

// WALMessage is one CopyData XLogData payload destined for a logical replication sender.
type WALMessage struct {
	WALStart     pglogrepl.LSN
	ServerWALEnd pglogrepl.LSN
	WALData      []byte
}

type journalEntry struct {
	Publications []string
	Messages     []WALMessage
}

type persistentState struct {
	Version  int                      `json:"version"`
	NextID   uint64                   `json:"next_id"`
	Current  uint64                   `json:"current"`
	SystemID string                   `json:"system_id"`
	Slots    []persistentSlot         `json:"slots"`
	Journal  []persistentJournalEntry `json:"journal"`
}

type persistentSlot struct {
	Name              string `json:"name"`
	Plugin            string `json:"plugin"`
	Database          string `json:"database"`
	RestartLSN        uint64 `json:"restart_lsn"`
	ConfirmedFlushLSN uint64 `json:"confirmed_flush_lsn"`
	TwoPhase          bool   `json:"two_phase"`
	TotalTxns         int64  `json:"total_txns"`
	TotalBytes        int64  `json:"total_bytes"`
}

type persistentJournalEntry struct {
	Publications []string               `json:"publications"`
	Messages     []persistentWALMessage `json:"messages"`
}

type persistentWALMessage struct {
	WALStart     uint64 `json:"wal_start"`
	ServerWALEnd uint64 `json:"server_wal_end"`
	WALData      []byte `json:"wal_data"`
}

var defaultRegistry = &registry{
	slots:    make(map[string]*Slot),
	senders:  make(map[uint64]*Sender),
	queues:   make(map[uint64]chan WALMessage),
	systemID: "7500000000000000001",
}

// SystemID returns the stable logical-replication system identifier reported by IDENTIFY_SYSTEM.
func SystemID() string {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	return defaultRegistry.systemID
}

// CurrentLSN returns the highest observed source-side logical replication LSN.
func CurrentLSN() pglogrepl.LSN {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	return defaultRegistry.current
}

// HasActiveSenders returns whether any logical replication sender is active.
func HasActiveSenders() bool {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	return len(defaultRegistry.senders) > 0
}

// HasSlots returns whether any logical replication slot exists.
func HasSlots() bool {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	return len(defaultRegistry.slots) > 0
}

// AdvanceLSN records a local WAL-producing change and returns the new source LSN.
func AdvanceLSN() pglogrepl.LSN {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	defaultRegistry.current += 0x10
	if len(defaultRegistry.slots) > 0 || len(defaultRegistry.journal) > 0 {
		defaultRegistry.persistBestEffortLocked()
	}
	return defaultRegistry.current
}

// ConfigureStorage loads and persists logical replication source state in the supplied filesystem.
func ConfigureStorage(fs filesys.Filesys, storagePath string) error {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	defaultRegistry.storageFS = fs
	defaultRegistry.storagePath = storagePath
	defaultRegistry.nextID = 0
	defaultRegistry.slots = make(map[string]*Slot)
	defaultRegistry.senders = make(map[uint64]*Sender)
	defaultRegistry.queues = make(map[uint64]chan WALMessage)
	defaultRegistry.current = 0
	defaultRegistry.journal = nil
	defaultRegistry.systemID = "7500000000000000001"
	if fs == nil || storagePath == "" {
		return nil
	}
	return defaultRegistry.loadLocked()
}

// ResetForTests clears all in-memory logical replication state.
func ResetForTests() {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	defaultRegistry.nextID = 0
	defaultRegistry.slots = make(map[string]*Slot)
	defaultRegistry.senders = make(map[uint64]*Sender)
	defaultRegistry.queues = make(map[uint64]chan WALMessage)
	defaultRegistry.current = 0
	defaultRegistry.journal = nil
	defaultRegistry.storageFS = nil
	defaultRegistry.storagePath = ""
	defaultRegistry.systemID = "7500000000000000001"
}

// CreateSlot creates a logical replication slot in the local registry.
func CreateSlot(name string, plugin string, database string, temporary bool, twoPhase bool, ownerPID int32) (Slot, error) {
	if name == "" {
		return Slot{}, errors.New("replication slot name is required")
	}
	if !validSlotName(name) {
		return Slot{}, errors.Errorf("invalid replication slot name %q", name)
	}
	if plugin == "" {
		plugin = "pgoutput"
	}
	if !strings.EqualFold(plugin, "pgoutput") {
		return Slot{}, errors.Errorf("logical decoding output plugin %q is not supported", plugin)
	}

	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	if _, ok := defaultRegistry.slots[name]; ok {
		return Slot{}, errors.Errorf(`replication slot "%s" already exists`, name)
	}
	slot := Slot{
		Name:              name,
		Plugin:            plugin,
		Database:          database,
		Temporary:         temporary,
		OwnerPID:          ownerPID,
		RestartLSN:        defaultRegistry.current,
		ConfirmedFlushLSN: defaultRegistry.current,
		TwoPhase:          twoPhase,
	}
	defaultRegistry.slots[name] = &slot
	if !temporary {
		if err := defaultRegistry.persistLocked(); err != nil {
			delete(defaultRegistry.slots, name)
			return Slot{}, err
		}
	}
	return slot, nil
}

func validSlotName(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	for _, ch := range name {
		if ch >= 'a' && ch <= 'z' {
			continue
		}
		if ch >= '0' && ch <= '9' {
			continue
		}
		if ch == '_' {
			continue
		}
		return false
	}
	return true
}

// DropSlot drops an inactive logical replication slot.
func DropSlot(name string) error {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	slot, ok := defaultRegistry.slots[name]
	if !ok {
		return errors.Errorf(`replication slot "%s" does not exist`, name)
	}
	if slot.Active {
		return errors.Errorf(`replication slot "%s" is active`, name)
	}
	delete(defaultRegistry.slots, name)
	defaultRegistry.pruneJournalLocked()
	if err := defaultRegistry.persistLocked(); err != nil {
		defaultRegistry.slots[name] = slot
		return err
	}
	return nil
}

// DropTemporarySlotsForPID drops temporary slots created by the given connection.
func DropTemporarySlotsForPID(pid int32) {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	for name, slot := range defaultRegistry.slots {
		if slot.Temporary && slot.OwnerPID == pid {
			delete(defaultRegistry.slots, name)
		}
	}
}

// ListSlots returns a stable snapshot of all local logical replication slots.
func ListSlots() []Slot {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	slots := make([]Slot, 0, len(defaultRegistry.slots))
	for _, slot := range defaultRegistry.slots {
		slots = append(slots, *slot)
	}
	slices.SortFunc(slots, func(a, b Slot) int {
		return strings.Compare(a.Name, b.Name)
	})
	return slots
}

// GetSlot returns a snapshot of a logical replication slot.
func GetSlot(name string) (Slot, bool) {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	slot, ok := defaultRegistry.slots[name]
	if !ok {
		return Slot{}, false
	}
	return *slot, true
}

// RegisterSender marks a slot as active and records a new replication sender.
func RegisterSender(info SenderInfo) (Sender, <-chan WALMessage, error) {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	slot, ok := defaultRegistry.slots[info.SlotName]
	if !ok {
		return Sender{}, nil, errors.Errorf(`replication slot "%s" does not exist`, info.SlotName)
	}
	if slot.Active {
		return Sender{}, nil, errors.Errorf(`replication slot "%s" is active`, info.SlotName)
	}
	defaultRegistry.nextID++
	host, port := splitAddr(info.RemoteAddr)
	publications := compactLowerStrings(info.Publications)
	replay, replayTxns, replayBytes := defaultRegistry.replayMessagesLocked(publications, info.StartLSN)
	sender := Sender{
		ID:              defaultRegistry.nextID,
		SlotName:        info.SlotName,
		Publications:    publications,
		PID:             info.PID,
		User:            info.User,
		ApplicationName: info.ApplicationName,
		ClientAddr:      host,
		ClientPort:      port,
		BackendStart:    time.Now(),
		State:           "streaming",
		SentLSN:         maxLSN(defaultRegistry.current, info.StartLSN),
		WriteLSN:        info.StartLSN,
		FlushLSN:        info.StartLSN,
		ReplayLSN:       info.StartLSN,
	}
	if replayTxns > 0 {
		slot.TotalTxns += replayTxns
		slot.TotalBytes += replayBytes
		if err := defaultRegistry.persistLocked(); err != nil {
			slot.TotalTxns -= replayTxns
			slot.TotalBytes -= replayBytes
			return Sender{}, nil, err
		}
	}
	slot.Active = true
	slot.ActivePID = info.PID
	defaultRegistry.senders[sender.ID] = &sender
	queueSize := 256
	if len(replay) > queueSize {
		queueSize = len(replay) + 256
	}
	queue := make(chan WALMessage, queueSize)
	for _, message := range replay {
		sender.SentLSN = maxLSN(sender.SentLSN, message.ServerWALEnd)
		queue <- message
	}
	defaultRegistry.queues[sender.ID] = queue
	return sender, queue, nil
}

// UpdateStandbyStatus records a standby status update from the replication client.
func UpdateStandbyStatus(senderID uint64, writeLSN pglogrepl.LSN, flushLSN pglogrepl.LSN, replayLSN pglogrepl.LSN, replyTime time.Time) {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	sender, ok := defaultRegistry.senders[senderID]
	if !ok {
		return
	}
	if flushLSN == 0 {
		flushLSN = writeLSN
	}
	if replayLSN == 0 {
		replayLSN = flushLSN
	}
	sender.WriteLSN = writeLSN
	sender.FlushLSN = flushLSN
	sender.ReplayLSN = replayLSN
	sender.ReplyTime = replyTime
	if slot, ok := defaultRegistry.slots[sender.SlotName]; ok {
		slot.ConfirmedFlushLSN = maxLSN(slot.ConfirmedFlushLSN, minLSN(flushLSN, sender.SentLSN))
		slot.RestartLSN = slot.ConfirmedFlushLSN
	}
	defaultRegistry.pruneJournalLocked()
	defaultRegistry.persistBestEffortLocked()
}

// UnregisterSender clears active state for the sender's slot.
func UnregisterSender(senderID uint64) {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	sender, ok := defaultRegistry.senders[senderID]
	if !ok {
		return
	}
	delete(defaultRegistry.senders, senderID)
	if queue, ok := defaultRegistry.queues[senderID]; ok {
		close(queue)
		delete(defaultRegistry.queues, senderID)
	}
	if slot, ok := defaultRegistry.slots[sender.SlotName]; ok {
		if slot.Temporary {
			delete(defaultRegistry.slots, sender.SlotName)
		} else {
			slot.Active = false
			slot.ActivePID = 0
		}
	}
	defaultRegistry.persistBestEffortLocked()
}

// TerminateSenderByPID clears an active replication sender by backend PID.
func TerminateSenderByPID(pid int32) bool {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	terminated := false
	for senderID, sender := range defaultRegistry.senders {
		if sender.PID != pid {
			continue
		}
		delete(defaultRegistry.senders, senderID)
		if queue, ok := defaultRegistry.queues[senderID]; ok {
			close(queue)
			delete(defaultRegistry.queues, senderID)
		}
		if slot, ok := defaultRegistry.slots[sender.SlotName]; ok {
			if slot.Temporary {
				delete(defaultRegistry.slots, sender.SlotName)
			} else {
				slot.Active = false
				slot.ActivePID = 0
			}
		}
		terminated = true
	}
	if terminated {
		defaultRegistry.persistBestEffortLocked()
	}
	return terminated
}

// Broadcast records logical WAL messages and sends them to matching active senders.
func Broadcast(publications []string, messages []WALMessage) error {
	if len(messages) == 0 {
		return nil
	}
	publications = compactLowerStrings(publications)
	totalBytes := replicationMessageBytes(messages)
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	if len(defaultRegistry.slots) > 0 {
		defaultRegistry.journal = append(defaultRegistry.journal, journalEntry{
			Publications: append([]string(nil), publications...),
			Messages:     cloneWALMessages(messages),
		})
	}
	countedSlots := make(map[string]struct{})
	for senderID, sender := range defaultRegistry.senders {
		if !publicationSetsOverlap(sender.Publications, publications) {
			continue
		}
		if _, ok := defaultRegistry.queues[senderID]; !ok {
			continue
		}
		if slot, ok := defaultRegistry.slots[sender.SlotName]; ok {
			slot.TotalTxns++
			slot.TotalBytes += totalBytes
			countedSlots[sender.SlotName] = struct{}{}
		}
	}
	if len(defaultRegistry.slots) > 0 {
		if err := defaultRegistry.persistLocked(); err != nil {
			defaultRegistry.journal = defaultRegistry.journal[:len(defaultRegistry.journal)-1]
			for slotName := range countedSlots {
				if slot, ok := defaultRegistry.slots[slotName]; ok {
					slot.TotalTxns--
					slot.TotalBytes -= totalBytes
				}
			}
			return err
		}
	}
	for senderID, sender := range defaultRegistry.senders {
		if !publicationSetsOverlap(sender.Publications, publications) {
			continue
		}
		queue, ok := defaultRegistry.queues[senderID]
		if !ok {
			continue
		}
		for _, message := range messages {
			sender.SentLSN = maxLSN(sender.SentLSN, message.ServerWALEnd)
			select {
			case queue <- message:
			default:
				sender.State = "catchup"
			}
		}
	}
	return nil
}

// ListSenders returns a stable snapshot of active logical replication senders.
func ListSenders() []Sender {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	senders := make([]Sender, 0, len(defaultRegistry.senders))
	for _, sender := range defaultRegistry.senders {
		senders = append(senders, *sender)
	}
	slices.SortFunc(senders, func(a, b Sender) int {
		return strings.Compare(a.SlotName, b.SlotName)
	})
	return senders
}

func splitAddr(addr net.Addr) (string, int32) {
	if addr == nil {
		return "", 0
	}
	host, portString, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String(), 0
	}
	port, err := strconv.ParseInt(portString, 10, 32)
	if err != nil {
		return host, 0
	}
	return host, int32(port)
}

func maxLSN(a pglogrepl.LSN, b pglogrepl.LSN) pglogrepl.LSN {
	if a > b {
		return a
	}
	return b
}

func minLSN(a pglogrepl.LSN, b pglogrepl.LSN) pglogrepl.LSN {
	if a < b {
		return a
	}
	return b
}

func compactLowerStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	ret := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			ret = append(ret, value)
		}
	}
	slices.Sort(ret)
	return slices.Compact(ret)
}

func publicationSetsOverlap(senderPublications []string, changePublications []string) bool {
	if len(senderPublications) == 0 || len(changePublications) == 0 {
		return true
	}
	for _, senderPublication := range senderPublications {
		if _, ok := slices.BinarySearch(changePublications, senderPublication); ok {
			return true
		}
	}
	return false
}

func cloneWALMessages(messages []WALMessage) []WALMessage {
	ret := make([]WALMessage, len(messages))
	for i, message := range messages {
		ret[i] = WALMessage{
			WALStart:     message.WALStart,
			ServerWALEnd: message.ServerWALEnd,
			WALData:      append([]byte(nil), message.WALData...),
		}
	}
	return ret
}

func replicationMessageBytes(messages []WALMessage) int64 {
	var total int64
	for _, message := range messages {
		total += int64(len(message.WALData))
	}
	return total
}

func (r *registry) replayMessagesLocked(publications []string, startLSN pglogrepl.LSN) ([]WALMessage, int64, int64) {
	if len(r.journal) == 0 {
		return nil, 0, 0
	}
	var replay []WALMessage
	var txns int64
	var bytes int64
	for _, entry := range r.journal {
		if !publicationSetsOverlap(publications, entry.Publications) {
			continue
		}
		if entryEndLSN(entry) <= startLSN {
			continue
		}
		txns++
		bytes += replicationMessageBytes(entry.Messages)
		replay = append(replay, cloneWALMessages(entry.Messages)...)
	}
	return replay, txns, bytes
}

func (r *registry) pruneJournalLocked() {
	if len(r.slots) == 0 {
		r.journal = nil
		return
	}
	minConfirmed := pglogrepl.LSN(0)
	first := true
	for _, slot := range r.slots {
		if first || slot.ConfirmedFlushLSN < minConfirmed {
			minConfirmed = slot.ConfirmedFlushLSN
			first = false
		}
	}
	keep := r.journal[:0]
	for _, entry := range r.journal {
		if entryEndLSN(entry) > minConfirmed {
			keep = append(keep, entry)
		}
	}
	r.journal = keep
}

func entryEndLSN(entry journalEntry) pglogrepl.LSN {
	var end pglogrepl.LSN
	for _, message := range entry.Messages {
		end = maxLSN(end, message.ServerWALEnd)
	}
	return end
}

func (r *registry) loadLocked() error {
	exists, isDir := r.storageFS.Exists(r.storagePath)
	if !exists {
		return nil
	}
	if isDir {
		return errors.Errorf("logical replication source state path %q is a directory", r.storagePath)
	}
	data, err := r.storageFS.ReadFile(r.storagePath)
	if err != nil {
		return err
	}
	var state persistentState
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.SystemID != "" {
		r.systemID = state.SystemID
	}
	r.nextID = state.NextID
	r.current = pglogrepl.LSN(state.Current)
	for _, storedSlot := range state.Slots {
		slot := Slot{
			Name:              storedSlot.Name,
			Plugin:            storedSlot.Plugin,
			Database:          storedSlot.Database,
			RestartLSN:        pglogrepl.LSN(storedSlot.RestartLSN),
			ConfirmedFlushLSN: pglogrepl.LSN(storedSlot.ConfirmedFlushLSN),
			TwoPhase:          storedSlot.TwoPhase,
			TotalTxns:         storedSlot.TotalTxns,
			TotalBytes:        storedSlot.TotalBytes,
		}
		if slot.Plugin == "" {
			slot.Plugin = "pgoutput"
		}
		r.slots[slot.Name] = &slot
	}
	r.journal = make([]journalEntry, 0, len(state.Journal))
	for _, storedEntry := range state.Journal {
		entry := journalEntry{
			Publications: compactLowerStrings(storedEntry.Publications),
			Messages:     make([]WALMessage, len(storedEntry.Messages)),
		}
		for i, storedMessage := range storedEntry.Messages {
			entry.Messages[i] = WALMessage{
				WALStart:     pglogrepl.LSN(storedMessage.WALStart),
				ServerWALEnd: pglogrepl.LSN(storedMessage.ServerWALEnd),
				WALData:      append([]byte(nil), storedMessage.WALData...),
			}
		}
		r.journal = append(r.journal, entry)
	}
	r.pruneJournalLocked()
	return nil
}

func (r *registry) persistBestEffortLocked() {
	_ = r.persistLocked()
}

func (r *registry) persistLocked() error {
	if r.storageFS == nil || r.storagePath == "" {
		return nil
	}
	if len(r.slots) == 0 && len(r.journal) == 0 {
		if exists, isDir := r.storageFS.Exists(r.storagePath); exists && !isDir {
			return r.storageFS.DeleteFile(r.storagePath)
		}
		return nil
	}
	dir := filepath.Dir(r.storagePath)
	if dir != "." && dir != "" {
		if err := r.storageFS.MkDirs(dir); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(r.toPersistentStateLocked(), "", "  ")
	if err != nil {
		return err
	}
	return r.storageFS.WriteFile(r.storagePath, data, os.ModePerm)
}

func (r *registry) toPersistentStateLocked() persistentState {
	state := persistentState{
		Version:  1,
		NextID:   r.nextID,
		Current:  uint64(r.current),
		SystemID: r.systemID,
		Slots:    make([]persistentSlot, 0, len(r.slots)),
		Journal:  make([]persistentJournalEntry, 0, len(r.journal)),
	}
	for _, slot := range r.slots {
		if slot.Temporary {
			continue
		}
		state.Slots = append(state.Slots, persistentSlot{
			Name:              slot.Name,
			Plugin:            slot.Plugin,
			Database:          slot.Database,
			RestartLSN:        uint64(slot.RestartLSN),
			ConfirmedFlushLSN: uint64(slot.ConfirmedFlushLSN),
			TwoPhase:          slot.TwoPhase,
			TotalTxns:         slot.TotalTxns,
			TotalBytes:        slot.TotalBytes,
		})
	}
	slices.SortFunc(state.Slots, func(a, b persistentSlot) int {
		return strings.Compare(a.Name, b.Name)
	})
	for _, entry := range r.journal {
		storedEntry := persistentJournalEntry{
			Publications: append([]string(nil), entry.Publications...),
			Messages:     make([]persistentWALMessage, len(entry.Messages)),
		}
		for i, message := range entry.Messages {
			storedEntry.Messages[i] = persistentWALMessage{
				WALStart:     uint64(message.WALStart),
				ServerWALEnd: uint64(message.ServerWALEnd),
				WALData:      append([]byte(nil), message.WALData...),
			}
		}
		state.Journal = append(state.Journal, storedEntry)
	}
	return state
}

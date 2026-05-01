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
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
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
	RestartLSN        pglogrepl.LSN
	ConfirmedFlushLSN pglogrepl.LSN
	TwoPhase          bool
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
	mu       sync.Mutex
	nextID   uint64
	slots    map[string]*Slot
	senders  map[uint64]*Sender
	queues   map[uint64]chan WALMessage
	current  pglogrepl.LSN
	systemID string
}

// WALMessage is one CopyData XLogData payload destined for a logical replication sender.
type WALMessage struct {
	WALStart     pglogrepl.LSN
	ServerWALEnd pglogrepl.LSN
	WALData      []byte
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

// AdvanceLSN records a local WAL-producing change and returns the new source LSN.
func AdvanceLSN() pglogrepl.LSN {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	defaultRegistry.current += 0x10
	return defaultRegistry.current
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
}

// CreateSlot creates a logical replication slot in the local registry.
func CreateSlot(name string, plugin string, database string, temporary bool) (Slot, error) {
	if name == "" {
		return Slot{}, errors.New("replication slot name is required")
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
		RestartLSN:        defaultRegistry.current,
		ConfirmedFlushLSN: defaultRegistry.current,
	}
	defaultRegistry.slots[name] = &slot
	return slot, nil
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
	return nil
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
	sender := Sender{
		ID:              defaultRegistry.nextID,
		SlotName:        info.SlotName,
		Publications:    compactLowerStrings(info.Publications),
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
	slot.Active = true
	slot.ActivePID = info.PID
	defaultRegistry.senders[sender.ID] = &sender
	queue := make(chan WALMessage, 256)
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
	}
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
}

// Broadcast sends logical WAL messages to matching active senders.
func Broadcast(publications []string, messages []WALMessage) {
	if len(messages) == 0 {
		return
	}
	publications = compactLowerStrings(publications)
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
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

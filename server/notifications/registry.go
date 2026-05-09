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

package notifications

import (
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
)

const maxPayloadBytes = 8000

type sender func(*pgproto3.NotificationResponse) error

type listenerAction struct {
	channel string
	listen  bool
	all     bool
}

type pendingNotification struct {
	sourcePID uint32
	channel   string
	payload   string
}

type listener struct {
	connectionID uint32
	send         sender
}

type connectionState struct {
	send    sender
	listens map[string]struct{}

	inTransaction bool
	actions       []listenerAction
	notifications []pendingNotification
	seen          map[string]struct{}
}

var globalRegistry = struct {
	sync.RWMutex
	connections map[uint32]*connectionState
	channels    map[string]map[uint32]struct{}
}{
	connections: make(map[uint32]*connectionState),
	channels:    make(map[string]map[uint32]struct{}),
}

// Register adds a connection to the notification registry. The caller is
// responsible for unregistering the connection when it closes.
func Register(connectionID uint32, send sender) {
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	globalRegistry.connections[connectionID] = &connectionState{
		send:    send,
		listens: make(map[string]struct{}),
	}
}

// Unregister removes a connection and all of its active LISTEN registrations.
func Unregister(connectionID uint32) {
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	state := globalRegistry.connections[connectionID]
	if state != nil {
		for channel := range state.listens {
			removeListenerLocked(connectionID, channel)
		}
	}
	delete(globalRegistry.connections, connectionID)
}

// Begin starts transaction-scoped LISTEN/UNLISTEN/NOTIFY bookkeeping.
func Begin(connectionID uint32) {
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	if state := globalRegistry.connections[connectionID]; state != nil {
		state.inTransaction = true
		state.actions = nil
		state.notifications = nil
		state.seen = nil
	}
}

// Listen registers interest in a channel, applying at transaction commit when
// the connection is inside a transaction block.
func Listen(connectionID uint32, channel string) {
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	state := globalRegistry.connections[connectionID]
	if state == nil {
		return
	}
	if state.inTransaction {
		state.actions = append(state.actions, listenerAction{channel: channel, listen: true})
		return
	}
	addListenerLocked(connectionID, state, channel)
}

// Unlisten removes interest in a single channel. Inside a transaction block,
// the removal applies only when that transaction commits.
func Unlisten(connectionID uint32, channel string) {
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	state := globalRegistry.connections[connectionID]
	if state == nil {
		return
	}
	if state.inTransaction {
		state.actions = append(state.actions, listenerAction{channel: channel})
		return
	}
	removeListenerLocked(connectionID, channel)
	delete(state.listens, channel)
}

// UnlistenAll removes all channel registrations. Inside a transaction block,
// the removal applies only when that transaction commits.
func UnlistenAll(connectionID uint32) {
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	state := globalRegistry.connections[connectionID]
	if state == nil {
		return
	}
	if state.inTransaction {
		state.actions = append(state.actions, listenerAction{all: true})
		return
	}
	for channel := range state.listens {
		removeListenerLocked(connectionID, channel)
	}
	state.listens = make(map[string]struct{})
}

// Queue appends a notification to the current transaction. Duplicate
// channel/payload pairs from the same transaction are folded like PostgreSQL.
func Queue(sourcePID uint32, channel string, payload string) error {
	if len(payload) >= maxPayloadBytes {
		return fmt.Errorf("payload string too long")
	}
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	state := globalRegistry.connections[sourcePID]
	if state == nil {
		return nil
	}
	key := channel + "\x00" + payload
	if _, ok := state.seen[key]; ok {
		return nil
	}
	if state.seen == nil {
		state.seen = make(map[string]struct{})
	}
	state.seen[key] = struct{}{}
	state.notifications = append(state.notifications, pendingNotification{
		sourcePID: sourcePID,
		channel:   channel,
		payload:   payload,
	})
	return nil
}

// Commit applies pending LISTEN/UNLISTEN actions and delivers pending
// notifications. It is also used at successful autocommit statement boundaries.
func Commit(connectionID uint32) error {
	actions, pending := takePending(connectionID, true)
	applyActions(connectionID, actions)
	return deliver(pending)
}

// Rollback discards pending LISTEN/UNLISTEN actions and pending notifications.
func Rollback(connectionID uint32) {
	_, _ = takePending(connectionID, false)
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	if state := globalRegistry.connections[connectionID]; state != nil {
		state.inTransaction = false
	}
}

func takePending(connectionID uint32, committed bool) ([]listenerAction, []pendingNotification) {
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	state := globalRegistry.connections[connectionID]
	if state == nil {
		return nil, nil
	}
	actions := state.actions
	pending := state.notifications
	state.actions = nil
	state.notifications = nil
	state.seen = nil
	if committed {
		state.inTransaction = false
	}
	return actions, pending
}

func applyActions(connectionID uint32, actions []listenerAction) {
	if len(actions) == 0 {
		return
	}
	globalRegistry.Lock()
	defer globalRegistry.Unlock()
	state := globalRegistry.connections[connectionID]
	if state == nil {
		return
	}
	for _, action := range actions {
		if action.all {
			for channel := range state.listens {
				removeListenerLocked(connectionID, channel)
			}
			state.listens = make(map[string]struct{})
		} else if action.listen {
			addListenerLocked(connectionID, state, action.channel)
		} else {
			removeListenerLocked(connectionID, action.channel)
			delete(state.listens, action.channel)
		}
	}
}

func deliver(pending []pendingNotification) error {
	if len(pending) == 0 {
		return nil
	}
	for _, notification := range pending {
		for _, listener := range listenersForChannel(notification.channel) {
			err := listener.send(&pgproto3.NotificationResponse{
				PID:     notification.sourcePID,
				Channel: notification.channel,
				Payload: notification.payload,
			})
			if err != nil {
				Unregister(listener.connectionID)
			}
		}
	}
	return nil
}

func listenersForChannel(channel string) []listener {
	globalRegistry.RLock()
	defer globalRegistry.RUnlock()
	listeners := globalRegistry.channels[channel]
	if len(listeners) == 0 {
		return nil
	}
	result := make([]listener, 0, len(listeners))
	for connectionID := range listeners {
		if state := globalRegistry.connections[connectionID]; state != nil {
			result = append(result, listener{
				connectionID: connectionID,
				send:         state.send,
			})
		}
	}
	return result
}

func addListenerLocked(connectionID uint32, state *connectionState, channel string) {
	if _, ok := state.listens[channel]; ok {
		return
	}
	state.listens[channel] = struct{}{}
	listeners := globalRegistry.channels[channel]
	if listeners == nil {
		listeners = make(map[uint32]struct{})
		globalRegistry.channels[channel] = listeners
	}
	listeners[connectionID] = struct{}{}
}

func removeListenerLocked(connectionID uint32, channel string) {
	listeners := globalRegistry.channels[channel]
	if listeners == nil {
		return
	}
	delete(listeners, connectionID)
	if len(listeners) == 0 {
		delete(globalRegistry.channels, channel)
	}
}

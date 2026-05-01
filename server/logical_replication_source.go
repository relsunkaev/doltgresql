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

package server

import (
	"encoding/binary"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dolthub/doltgresql/server/replsource"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const microsecFromUnixEpochToY2K = 946684800 * 1000000

func (h *ConnectionHandler) handleReplicationQuery(query string) (handled bool, endOfMessages bool, err error) {
	statement := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	upper := strings.ToUpper(statement)
	switch {
	case upper == "IDENTIFY_SYSTEM":
		return true, true, h.sendIdentifySystem()
	case strings.HasPrefix(upper, "CREATE_REPLICATION_SLOT "):
		return true, true, h.createReplicationSlot(statement)
	case strings.HasPrefix(upper, "DROP_REPLICATION_SLOT "):
		return true, true, h.dropReplicationSlot(statement)
	case strings.HasPrefix(upper, "START_REPLICATION "):
		return true, false, h.startLogicalReplication(statement)
	default:
		return false, true, nil
	}
}

func (h *ConnectionHandler) sendIdentifySystem() error {
	return h.sendReplicationRows(
		[]string{"systemid", "timeline", "xlogpos", "dbname"},
		[][][]byte{{
			[]byte(replsource.SystemID()),
			[]byte("1"),
			[]byte(formatReplicationLSN(replsource.CurrentLSN())),
			[]byte(h.database),
		}},
		"IDENTIFY_SYSTEM",
	)
}

func (h *ConnectionHandler) createReplicationSlot(statement string) error {
	fields := strings.Fields(statement)
	if len(fields) < 4 {
		return errors.Errorf("invalid CREATE_REPLICATION_SLOT command")
	}
	slotName := normalizeReplicationIdentifier(fields[1])
	idx := 2
	temporary := false
	if strings.EqualFold(fields[idx], "TEMPORARY") {
		temporary = true
		idx++
	}
	if idx >= len(fields) || !strings.EqualFold(fields[idx], "LOGICAL") {
		return errors.Errorf("only logical replication slots are supported")
	}
	if idx+1 >= len(fields) {
		return errors.Errorf("logical replication output plugin is required")
	}
	plugin := normalizeReplicationIdentifier(fields[idx+1])
	slot, err := replsource.CreateSlot(slotName, plugin, h.database, temporary)
	if err != nil {
		existing, ok := replsource.GetSlot(slotName)
		if !ok || existing.Active || existing.Temporary || temporary || !strings.EqualFold(existing.Plugin, plugin) || existing.Database != h.database {
			return err
		}
		slot = existing
	}
	return h.sendReplicationRows(
		[]string{"slot_name", "consistent_point", "snapshot_name", "output_plugin"},
		[][][]byte{{
			[]byte(slot.Name),
			[]byte(formatReplicationLSN(slot.ConfirmedFlushLSN)),
			nil,
			[]byte(slot.Plugin),
		}},
		"CREATE_REPLICATION_SLOT",
	)
}

func (h *ConnectionHandler) dropReplicationSlot(statement string) error {
	fields := strings.Fields(statement)
	if len(fields) < 2 {
		return errors.Errorf("invalid DROP_REPLICATION_SLOT command")
	}
	if err := replsource.DropSlot(normalizeReplicationIdentifier(fields[1])); err != nil {
		return err
	}
	return h.send(&pgproto3.CommandComplete{CommandTag: []byte("DROP_REPLICATION_SLOT")})
}

func (h *ConnectionHandler) startLogicalReplication(statement string) error {
	fields := strings.Fields(statement)
	if len(fields) < 5 || !strings.EqualFold(fields[1], "SLOT") {
		return errors.Errorf("invalid START_REPLICATION command")
	}
	if !strings.EqualFold(fields[3], "LOGICAL") {
		return errors.Errorf("only logical START_REPLICATION is supported")
	}
	slotName := normalizeReplicationIdentifier(fields[2])
	startLSN, err := pglogrepl.ParseLSN(fields[4])
	if err != nil {
		return err
	}
	sender, queue, err := replsource.RegisterSender(replsource.SenderInfo{
		SlotName:        slotName,
		Publications:    replicationPublicationNames(statement),
		PID:             int32(h.mysqlConn.ConnectionID),
		User:            h.mysqlConn.User,
		ApplicationName: h.startupParams["application_name"],
		RemoteAddr:      h.Conn().RemoteAddr(),
		StartLSN:        startLSN,
	})
	if err != nil {
		return err
	}
	h.replicationSenderID = sender.ID
	if err = h.send(&pgproto3.CopyBothResponse{}); err != nil {
		h.closeReplicationSender()
		return err
	}
	if err = h.sendPrimaryKeepalive(false); err != nil {
		h.closeReplicationSender()
		return err
	}
	go h.runReplicationSender(queue)
	return nil
}

func (h *ConnectionHandler) runReplicationSender(queue <-chan replsource.WALMessage) {
	for message := range queue {
		if err := h.sendXLogData(message); err != nil {
			h.closeReplicationSender()
			return
		}
	}
}

func (h *ConnectionHandler) handleReplicationCopyData(message *pgproto3.CopyData) (stop bool, endOfMessages bool, err error) {
	if len(message.Data) == 0 {
		return false, false, nil
	}
	switch message.Data[0] {
	case pglogrepl.StandbyStatusUpdateByteID:
		status, err := parseStandbyStatusUpdate(message.Data)
		if err != nil {
			return false, false, err
		}
		replsource.UpdateStandbyStatus(h.replicationSenderID, status.writeLSN, status.flushLSN, status.replayLSN, status.clientTime)
		if status.replyRequested {
			return false, false, h.sendPrimaryKeepalive(false)
		}
	}
	return false, false, nil
}

func (h *ConnectionHandler) handleReplicationCopyDone(_ *pgproto3.CopyDone) (stop bool, endOfMessages bool, err error) {
	h.closeReplicationSender()
	if err = h.send(&pgproto3.CopyDone{}); err != nil {
		return false, false, err
	}
	if err = h.send(&pgproto3.CommandComplete{CommandTag: []byte("START_REPLICATION")}); err != nil {
		return false, false, err
	}
	return false, true, nil
}

func (h *ConnectionHandler) handleReplicationCopyFail(message *pgproto3.CopyFail) (stop bool, endOfMessages bool, err error) {
	h.closeReplicationSender()
	return false, true, errors.Errorf("replication stream aborted: %s", message.Message)
}

func (h *ConnectionHandler) closeReplicationSender() {
	if h.replicationSenderID == 0 {
		return
	}
	replsource.UnregisterSender(h.replicationSenderID)
	h.replicationSenderID = 0
}

func (h *ConnectionHandler) sendReplicationRows(fieldNames []string, rows [][][]byte, commandTag string) error {
	fields := make([]pgproto3.FieldDescription, len(fieldNames))
	for i, name := range fieldNames {
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(name),
			DataTypeOID:  25,
			DataTypeSize: -1,
			TypeModifier: -1,
		}
	}
	if err := h.send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return err
	}
	for _, row := range rows {
		if err := h.send(&pgproto3.DataRow{Values: row}); err != nil {
			return err
		}
	}
	return h.send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
}

func (h *ConnectionHandler) sendPrimaryKeepalive(replyRequested bool) error {
	data := make([]byte, 0, 18)
	data = append(data, pglogrepl.PrimaryKeepaliveMessageByteID)
	data = binary.BigEndian.AppendUint64(data, uint64(replsource.CurrentLSN()))
	data = binary.BigEndian.AppendUint64(data, uint64(timeToPgTime(time.Now())))
	if replyRequested {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	return h.send(&pgproto3.CopyData{Data: data})
}

func (h *ConnectionHandler) sendXLogData(message replsource.WALMessage) error {
	data := make([]byte, 0, 25+len(message.WALData))
	data = append(data, pglogrepl.XLogDataByteID)
	data = binary.BigEndian.AppendUint64(data, uint64(message.WALStart))
	data = binary.BigEndian.AppendUint64(data, uint64(message.ServerWALEnd))
	data = binary.BigEndian.AppendUint64(data, uint64(timeToPgTime(time.Now())))
	data = append(data, message.WALData...)
	return h.send(&pgproto3.CopyData{Data: data})
}

type standbyStatusUpdate struct {
	writeLSN       pglogrepl.LSN
	flushLSN       pglogrepl.LSN
	replayLSN      pglogrepl.LSN
	clientTime     time.Time
	replyRequested bool
}

func parseStandbyStatusUpdate(data []byte) (standbyStatusUpdate, error) {
	if len(data) != 34 {
		return standbyStatusUpdate{}, errors.Errorf("StandbyStatusUpdate must be 34 bytes, got %d", len(data))
	}
	return standbyStatusUpdate{
		writeLSN:       pglogrepl.LSN(binary.BigEndian.Uint64(data[1:])),
		flushLSN:       pglogrepl.LSN(binary.BigEndian.Uint64(data[9:])),
		replayLSN:      pglogrepl.LSN(binary.BigEndian.Uint64(data[17:])),
		clientTime:     pgTimeToTime(int64(binary.BigEndian.Uint64(data[25:]))),
		replyRequested: data[33] != 0,
	}, nil
}

func normalizeReplicationIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		return strings.ReplaceAll(value[1:len(value)-1], `""`, `"`)
	}
	return value
}

func replicationPublicationNames(statement string) []string {
	lower := strings.ToLower(statement)
	idx := strings.Index(lower, "publication_names")
	if idx < 0 {
		return nil
	}
	tail := statement[idx+len("publication_names"):]
	tail = strings.TrimSpace(tail)
	if strings.HasPrefix(tail, `"`) {
		tail = strings.TrimSpace(tail[1:])
	}
	if len(tail) == 0 {
		return nil
	}
	quote := tail[0]
	if quote != '\'' && quote != '"' {
		return nil
	}
	raw, ok := readReplicationOptionString(tail[1:], quote)
	if !ok {
		return nil
	}
	values := strings.Split(raw, ",")
	for i := range values {
		values[i] = normalizeReplicationIdentifier(strings.TrimSpace(values[i]))
	}
	return values
}

func readReplicationOptionString(value string, quote byte) (string, bool) {
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != quote {
			builder.WriteByte(value[i])
			continue
		}
		if i+1 < len(value) && value[i+1] == quote {
			builder.WriteByte(quote)
			i++
			continue
		}
		return builder.String(), true
	}
	return "", false
}

func formatReplicationLSN(lsn pglogrepl.LSN) string {
	return pgtypes.FormatPgLsn(uint64(lsn))
}

func timeToPgTime(t time.Time) int64 {
	return t.UnixMicro() - microsecFromUnixEpochToY2K
}

func pgTimeToTime(microsecSinceY2K int64) time.Time {
	return time.UnixMicro(microsecFromUnixEpochToY2K + microsecSinceY2K)
}

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
	"context"
	"encoding/binary"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
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
	if err := h.requireReplicationRole(); err != nil {
		return err
	}
	fields := strings.Fields(statement)
	if len(fields) < 3 {
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
	twoPhase, err := h.parseCreateReplicationSlotOptions(fields[idx+2:])
	if err != nil {
		return err
	}
	slot, err := replsource.CreateSlot(slotName, plugin, h.database, temporary, twoPhase, int32(h.mysqlConn.ConnectionID))
	if err != nil {
		existing, ok := replsource.GetSlot(slotName)
		if !ok || existing.Active || existing.Temporary || temporary || !strings.EqualFold(existing.Plugin, plugin) || existing.Database != h.database || existing.TwoPhase != twoPhase {
			return err
		}
		slot = existing
	}
	snapshotName := []byte(replicationSlotSnapshotName(slot.Name))
	if replicationSlotNoExportSnapshot(statement) {
		snapshotName = nil
	}
	return h.sendReplicationRows(
		[]string{"slot_name", "consistent_point", "snapshot_name", "output_plugin"},
		[][][]byte{{
			[]byte(slot.Name),
			[]byte(formatReplicationLSN(slot.ConfirmedFlushLSN)),
			snapshotName,
			[]byte(slot.Plugin),
		}},
		"CREATE_REPLICATION_SLOT",
	)
}

func (h *ConnectionHandler) parseCreateReplicationSlotOptions(options []string) (bool, error) {
	twoPhase := false
	for _, option := range options {
		switch strings.ToUpper(option) {
		case "EXPORT_SNAPSHOT", "NOEXPORT_SNAPSHOT":
		case "USE_SNAPSHOT":
			if !h.inTransaction {
				return false, errors.Errorf("CREATE_REPLICATION_SLOT USE_SNAPSHOT must be called in a transaction")
			}
		case "TWO_PHASE":
			twoPhase = true
		default:
			return false, errors.Errorf("unrecognized CREATE_REPLICATION_SLOT option %q", option)
		}
	}
	return twoPhase, nil
}

func (h *ConnectionHandler) dropReplicationSlot(statement string) error {
	if err := h.requireReplicationRole(); err != nil {
		return err
	}
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
	if err := h.requireReplicationRole(); err != nil {
		return err
	}
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
	options, err := h.parseStartReplicationOptions(statement)
	if err != nil {
		return err
	}
	sender, queue, err := replsource.RegisterSender(replsource.SenderInfo{
		SlotName:        slotName,
		Publications:    options.publications,
		Messages:        options.messages,
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

func (h *ConnectionHandler) requireReplicationRole() error {
	if auth.CanReplicate(h.mysqlConn.User) {
		return nil
	}
	return errors.Errorf("permission denied to use replication")
}

type replicationStartOptions struct {
	publications []string
	messages     bool
}

func (h *ConnectionHandler) parseStartReplicationOptions(statement string) (replicationStartOptions, error) {
	options := replicationStartOptions{
		messages: true,
	}
	if !strings.Contains(statement, "(") && !strings.Contains(statement, ")") {
		return options, nil
	}
	values, err := parseReplicationPluginOptions(statement)
	if err != nil {
		return replicationStartOptions{}, err
	}
	protoVersion, ok := values["proto_version"]
	if !ok {
		return replicationStartOptions{}, errors.Errorf("START_REPLICATION requires pgoutput option proto_version")
	}
	parsedProtoVersion, err := strconv.Atoi(protoVersion)
	if err != nil || (parsedProtoVersion != 1 && parsedProtoVersion != 2) {
		return replicationStartOptions{}, errors.Errorf("invalid pgoutput proto_version %q", protoVersion)
	}
	publicationNames, ok := values["publication_names"]
	if !ok {
		return replicationStartOptions{}, errors.Errorf("START_REPLICATION requires pgoutput option publication_names")
	}
	options.publications, err = parseReplicationPublicationNameList(publicationNames)
	if err != nil {
		return replicationStartOptions{}, err
	}
	if len(options.publications) == 0 {
		return replicationStartOptions{}, errors.Errorf("START_REPLICATION requires at least one publication")
	}
	if err = h.validateReplicationPublications(options.publications); err != nil {
		return replicationStartOptions{}, err
	}
	for key, value := range values {
		switch key {
		case "proto_version", "publication_names":
		case "binary", "messages", "streaming":
			parsed, err := parseReplicationBool(value)
			if err != nil {
				return replicationStartOptions{}, errors.Errorf("invalid pgoutput %s option %q", key, value)
			}
			if key == "messages" {
				options.messages = parsed
			}
		default:
			return replicationStartOptions{}, errors.Errorf("unrecognized pgoutput option %q", key)
		}
	}
	return options, nil
}

func (h *ConnectionHandler) validateReplicationPublications(publications []string) error {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return err
	}
	collection, err := core.GetPublicationsCollectionFromContext(sqlCtx)
	if err != nil {
		return err
	}
	for _, publication := range publications {
		if !collection.HasPublication(sqlCtx, id.NewPublication(publication)) {
			return errors.Errorf(`publication "%s" does not exist`, publication)
		}
	}
	return nil
}

func parseReplicationPluginOptions(statement string) (map[string]string, error) {
	start := strings.Index(statement, "(")
	end := strings.LastIndex(statement, ")")
	if start < 0 || end < start {
		return nil, errors.Errorf("START_REPLICATION requires pgoutput options")
	}
	parts, err := splitReplicationOptionList(statement[start+1 : end])
	if err != nil {
		return nil, err
	}
	options := make(map[string]string, len(parts))
	for _, part := range parts {
		key, rest, ok := readReplicationOptionKey(part)
		if !ok {
			return nil, errors.Errorf("invalid pgoutput option %q", part)
		}
		value, ok := readReplicationOptionValue(strings.TrimSpace(rest))
		if !ok {
			return nil, errors.Errorf("invalid pgoutput option value for %q", key)
		}
		options[key] = value
	}
	return options, nil
}

func splitReplicationOptionList(value string) ([]string, error) {
	var parts []string
	start := 0
	var quote byte
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(value) && value[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == ',' {
			part := strings.TrimSpace(value[start:i])
			if part != "" {
				parts = append(parts, part)
			}
			start = i + 1
		}
	}
	if quote != 0 {
		return nil, errors.Errorf("unterminated pgoutput option string")
	}
	part := strings.TrimSpace(value[start:])
	if part != "" {
		parts = append(parts, part)
	}
	return parts, nil
}

func readReplicationOptionKey(value string) (string, string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", false
	}
	if value[0] == '"' {
		raw, end, ok := readReplicationQuotedToken(value, '"')
		if !ok {
			return "", "", false
		}
		return strings.ToLower(raw), value[end:], true
	}
	for i := 0; i < len(value); i++ {
		if value[i] == ' ' || value[i] == '\t' || value[i] == '\n' || value[i] == '\r' {
			return strings.ToLower(value[:i]), value[i:], true
		}
	}
	return "", "", false
}

func readReplicationOptionValue(value string) (string, bool) {
	if value == "" {
		return "", false
	}
	if value[0] == '\'' || value[0] == '"' {
		raw, end, ok := readReplicationQuotedToken(value, value[0])
		if !ok || strings.TrimSpace(value[end:]) != "" {
			return "", false
		}
		return raw, true
	}
	fields := strings.Fields(value)
	if len(fields) != 1 {
		return "", false
	}
	return fields[0], true
}

func readReplicationQuotedToken(value string, quote byte) (string, int, bool) {
	var builder strings.Builder
	for i := 1; i < len(value); i++ {
		if value[i] != quote {
			builder.WriteByte(value[i])
			continue
		}
		if i+1 < len(value) && value[i+1] == quote {
			builder.WriteByte(quote)
			i++
			continue
		}
		return builder.String(), i + 1, true
	}
	return "", 0, false
}

func parseReplicationPublicationNameList(value string) ([]string, error) {
	parts, err := splitReplicationOptionList(value)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(parts))
	for _, part := range parts {
		names = append(names, normalizeReplicationIdentifier(strings.TrimSpace(part)))
	}
	return names, nil
}

func parseReplicationBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "on", "true":
		return true, nil
	case "0", "off", "false":
		return false, nil
	default:
		return false, errors.Errorf("invalid boolean %q", value)
	}
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

func replicationSlotSnapshotName(slotName string) string {
	return "doltgres-snapshot-" + slotName
}

func replicationSlotNoExportSnapshot(statement string) bool {
	return strings.Contains(strings.ToUpper(statement), "NOEXPORT_SNAPSHOT")
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

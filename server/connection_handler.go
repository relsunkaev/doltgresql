// Copyright 2023 Dolthub, Inc.
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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mitchellh/go-ps"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/dataloader"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	psql "github.com/dolthub/doltgresql/postgres/parser/parser/sql"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/ast"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/deferrable"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/functionstats"
	"github.com/dolthub/doltgresql/server/largeobject"
	"github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/notifications"
	"github.com/dolthub/doltgresql/server/replsource"
	"github.com/dolthub/doltgresql/server/rowsecurity"
	"github.com/dolthub/doltgresql/server/sessionstate"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	pgtables "github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// ConnectionHandler is responsible for the entire lifecycle of a user connection: receiving messages they send,
// executing queries, sending the correct messages in return, and terminating the connection when appropriate.
type ConnectionHandler struct {
	mysqlConn           *mysql.Conn
	preparedStatements  map[string]PreparedStatementData
	planCacheGeneration uint64
	portals             map[string]PortalData
	doltgresHandler     *DoltgresHandler
	backend             *pgproto3.Backend
	sendMu              sync.Mutex

	waitForSync bool
	// copyFromStdinState is set when this connection is in the COPY FROM STDIN mode, meaning it is waiting on
	// COPY DATA messages from the client to import data into tables.
	copyFromStdinState *copyFromStdinState
	// copyFromStdinFailed is set after a COPY DATA error has already ended the client-visible COPY operation. A
	// trailing CopyDone or CopyFail from that aborted stream should be consumed without sending another response.
	copyFromStdinFailed bool
	// inTransaction is set to true with BEGIN query and false with COMMIT query.
	inTransaction bool
	// transactionStatementCount tracks successful statements after BEGIN for
	// PostgreSQL's SET TRANSACTION "before any query" validation.
	transactionStatementCount int
	// transactionSnapshotAllowed tracks whether SET TRANSACTION SNAPSHOT is
	// valid for the current explicit transaction's isolation mode.
	transactionSnapshotAllowed bool
	// pendingReplicationCaptures stores row changes produced inside an explicit transaction until COMMIT.
	pendingReplicationCaptures []*replicationChangeCapture
	// pendingReplicationAdvance records a row-producing transaction without an active logical sender.
	pendingReplicationAdvance bool
	// pendingReplicationSavepoints records replication-buffer positions for transaction savepoints.
	pendingReplicationSavepoints []replicationSavepointState
	// cursors stores simple SQL cursor materializations for this connection.
	cursors map[string]*cursorData
	// replicationMode is true when the client connected with replication=database.
	replicationMode bool
	// startupParams stores startup parameters that are needed after authentication, such as application_name.
	startupParams map[string]string
	// database is the selected database for this connection.
	database string
	// replicationSenderID is set while this connection is in START_REPLICATION copy-both mode.
	replicationSenderID uint64
	// cancelSecretKey is the random 32-bit secret paired with the
	// connection ID and reported to the client via BackendKeyData
	// at startup. PostgreSQL's CancelRequest startup-message variant
	// presents this pair to authorize cancellation of the active
	// query — so the value lives for the connection's lifetime and
	// must be nonzero to distinguish from "uninitialized" sessions.
	cancelSecretKey uint32
}

type replicationSavepointState struct {
	name       string
	captures   int
	advanceLSN bool
}

type cursorData struct {
	fields []pgproto3.FieldDescription
	rows   []Row
	pos    int
	hold   bool
}

type sqlCursorDeclareStatement struct {
	Name  string
	Hold  bool
	Query string
}

func (s sqlCursorDeclareStatement) String() string {
	return "DECLARE CURSOR"
}

func (s sqlCursorDeclareStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return s, nil
}

type sqlCursorFetchStatement struct {
	Name      string
	Direction string
}

func (s sqlCursorFetchStatement) String() string {
	return "FETCH"
}

func (s sqlCursorFetchStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return s, nil
}

type sqlCursorCloseStatement struct {
	Name string
}

func (s sqlCursorCloseStatement) String() string {
	return "CLOSE CURSOR"
}

func (s sqlCursorCloseStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return s, nil
}

// Set this env var to disable panic handling in the connection, which is useful when debugging a panic
const disablePanicHandlingEnvVar = "DOLT_PGSQL_PANIC"

// HandlePanics determines whether panics should be handled in the connection handler. See |disablePanicHandlingEnvVar|.
var HandlePanics = true

func init() {
	if _, ok := os.LookupEnv(disablePanicHandlingEnvVar); ok {
		HandlePanics = false
	} else {
		// This checks if the Go debugger is attached, so that we can disable panic catching automatically
		pid := os.Getppid()
		for pid != 0 {
			p, err := ps.FindProcess(pid)
			if err != nil || p == nil {
				break
			} else if strings.HasPrefix(p.Executable(), "dlv") {
				HandlePanics = false
				break
			} else {
				pid = p.PPid()
			}
		}
	}
}

// NewConnectionHandler returns a new ConnectionHandler for the connection provided
func NewConnectionHandler(conn net.Conn, handler mysql.Handler, sel server.ServerEventListener) *ConnectionHandler {
	mysqlConn := &mysql.Conn{
		Conn:        conn,
		PrepareData: make(map[uint32]*mysql.PrepareData),
	}
	mysqlConn.ConnectionID = atomic.AddUint32(&connectionIDCounter, 1)
	cancelSecretKey := generateSecretKey()

	// Postgres has a two-stage procedure for prepared queries. First the query is parsed via a |Parse| message, and
	// the result is stored in the |preparedStatements| map by the name provided. Then one or more |Bind| messages
	// provide parameters for the query, and the result is stored in |portals|. Finally, a call to |Execute| executes
	// the named portal.
	preparedStatements := make(map[string]PreparedStatementData)
	portals := make(map[string]PortalData)

	// TODO: possibly should define engine and session manager ourselves
	//  instead of depending on the GetRunningServer method.
	server := sqlserver.GetRunningServer()
	server.Engine.Analyzer.Catalog.DbProvider = pgtables.WrapDatabaseProvider(server.Engine.Analyzer.Catalog.DbProvider)
	doltgresHandler := &DoltgresHandler{
		e:                 server.Engine,
		sm:                server.SessionManager(),
		readTimeout:       0,     // cfg.ConnReadTimeout,
		encodeLoggedQuery: false, // cfg.EncodeLoggedQuery,
		pgTypeMap:         pgtype.NewMap(),
	}
	if sel != nil {
		doltgresHandler.sel = sel
	}

	return &ConnectionHandler{
		mysqlConn:          mysqlConn,
		preparedStatements: preparedStatements,
		portals:            portals,
		doltgresHandler:    doltgresHandler,
		backend:            pgproto3.NewBackend(conn, conn),
		cursors:            make(map[string]*cursorData),
		cancelSecretKey:    cancelSecretKey,
	}
}

// HandleConnection handles a connection's session, reading messages, executing queries, and sending responses.
// Expected to run in a goroutine per connection.
func (h *ConnectionHandler) HandleConnection() {
	var returnErr error
	if HandlePanics {
		defer func() {
			if r := recover(); r != nil {
				// debug.Stack() here prints the stack trace of the original panic, not the lexical stack of this defer function
				stackTrace := string(debug.Stack())
				logrus.Errorf("Listener recovered panic: %v: %s", r, stackTrace)

				var eomErr error
				if returnErr != nil {
					eomErr = returnErr
				} else {
					eomErr = errors.Errorf("Listener recovered panic: %v: %s", r, stackTrace)
				}

				// Sending eom can panic, which means we must recover again
				defer func() {
					if r := recover(); r != nil {
						logrus.Errorf("Listener recovered panic: %v: %s", r, string(debug.Stack()))
					}
				}()
				h.endOfMessages(eomErr)
			}

			if returnErr != nil {
				fmt.Println(returnErr.Error())
			}
		}()
	}
	defer func() {
		if err := h.Conn().Close(); err != nil {
			fmt.Printf("Failed to properly close connection:\n%v\n", err)
		}
	}()
	h.doltgresHandler.NewConnection(h.mysqlConn)
	defer func() {
		h.closeReplicationSender()
		replsource.DropTemporarySlotsForPID(int32(h.mysqlConn.ConnectionID))
		sessionstate.DeleteAllPreparedStatements(h.mysqlConn.ConnectionID)
		functionstats.DeleteAll(h.mysqlConn.ConnectionID)
		globalCancelRegistry.unregister(h.mysqlConn.ConnectionID, h.cancelSecretKey)
		_ = auth.RollbackTransaction(h.mysqlConn.ConnectionID)
		h.doltgresHandler.ConnectionClosed(h.mysqlConn)
	}()

	if proceed, err := h.handleStartup(); err != nil || !proceed {
		returnErr = err
		return
	}
	notifications.Register(h.mysqlConn.ConnectionID, func(message *pgproto3.NotificationResponse) error {
		return h.send(message)
	})
	defer notifications.Unregister(h.mysqlConn.ConnectionID)
	defer deferrable.Rollback(h.mysqlConn.ConnectionID)

	// Main session loop: read messages one at a time off the connection until we receive a |Terminate| message, in
	// which case we hang up, or the connection is closed by the client, which generates an io.EOF from the connection.
	for {
		stop, err := h.receiveMessage()
		if err != nil {
			returnErr = err
			break
		}

		if stop {
			break
		}
	}
}

// Conn returns the underlying net.Conn for this connection.
func (h *ConnectionHandler) Conn() net.Conn {
	return h.mysqlConn.Conn
}

// setConn sets a new underlying net.Conn for this connection.
func (h *ConnectionHandler) setConn(conn net.Conn) {
	h.mysqlConn.Conn = conn
	h.backend = pgproto3.NewBackend(conn, conn)
}

// handleStartup handles the entire startup routine, including SSL requests, authentication, etc. Returns false if the
// connection has been terminated, or if we should not proceed with the message loop.
func (h *ConnectionHandler) handleStartup() (bool, error) {
	startupMessage, err := h.backend.ReceiveStartupMessage()
	if err == io.EOF {
		// Receiving EOF means that the connection has terminated, so we should just return
		return false, nil
	} else if err != nil {
		return false, errors.Errorf("error receiving startup message: %w", err)
	}

	switch sm := startupMessage.(type) {
	case *pgproto3.StartupMessage:
		h.startupParams = make(map[string]string, len(sm.Parameters))
		for name, value := range sm.Parameters {
			h.startupParams[strings.ToLower(name)] = value
		}
		replicationParam := strings.ToLower(sm.Parameters["replication"])
		h.replicationMode = replicationParam == "database" || replicationParam == "true" || replicationParam == "on" || replicationParam == "1"
		if err = h.handleAuthentication(sm); err != nil {
			return false, err
		}
		if err = h.sendClientStartupMessages(); err != nil {
			return false, err
		}
		if err = h.chooseInitialParameters(sm); err != nil {
			return false, err
		}
		return true, h.send(&pgproto3.ReadyForQuery{
			TxStatus: byte(ReadyForQueryTransactionIndicator_Idle),
		})
	case *pgproto3.SSLRequest:
		hasCertificate := len(certificate.Certificate) > 0
		var performSSL = []byte("N")
		if hasCertificate {
			performSSL = []byte("S")
		}
		_, err = h.Conn().Write(performSSL)
		if err != nil {
			return false, errors.Errorf("error sending SSL request: %w", err)
		}
		// If we have a certificate and the client has asked for SSL support, then we switch here.
		// This involves swapping out our underlying net connection for a new one.
		// We can't start in SSL mode, as the client does not attempt the handshake until after our response.
		if hasCertificate {
			h.setConn(tls.Server(h.Conn(), &tls.Config{
				Certificates: []tls.Certificate{certificate},
			}))
		}
		return h.handleStartup()
	case *pgproto3.GSSEncRequest:
		// we don't support GSSAPI
		_, err = h.Conn().Write([]byte("N"))
		if err != nil {
			return false, errors.Errorf("error sending response to GSS Enc Request: %w", err)
		}
		return h.handleStartup()
	case *pgproto3.CancelRequest:
		// CancelRequest is the only startup-message variant that does
		// not expect any reply: per the PG protocol the server merely
		// kills the running query and closes the connection. We look
		// up the (ProcessID, SecretKey) pair against the registry,
		// and on a match ask the engine's process list to interrupt
		// every query for that connection. A miss is silently ignored
		// (no response is permitted, so the only safe action is to
		// drop the connection).
		h.handleCancelRequest(sm)
		return false, nil
	default:
		return false, errors.Errorf("terminating connection: unexpected start message: %#v", startupMessage)
	}
}

// sendClientStartupMessages sends introductory messages to the client and returns any error.
// The set mirrors what real PostgreSQL emits at startup: drivers and ORMs key behavior off
// these (JDBC reads integer_datetimes for binary date encoding, node-postgres caches the
// encoding pair for transcoding, SQLAlchemy reads DateStyle / IntervalStyle, GUI editors
// surface application_name and session_authorization).
func (h *ConnectionHandler) sendClientStartupMessages() error {
	statuses := []pgproto3.ParameterStatus{
		{Name: "server_version", Value: "15.17"},
		{Name: "server_encoding", Value: "UTF8"},
		{Name: "client_encoding", Value: "UTF8"},
		{Name: "standard_conforming_strings", Value: "on"},
		{Name: "in_hot_standby", Value: "off"},
		{Name: "DateStyle", Value: h.startupParam("datestyle", "ISO, MDY")},
		{Name: "IntervalStyle", Value: h.startupParam("intervalstyle", "postgres")},
		{Name: "TimeZone", Value: h.startupParam("timezone", "UTC")},
		// integer_datetimes has been "on" since PG 10. Drivers branch on
		// this to choose binary vs. floating-point timestamp encoding.
		{Name: "integer_datetimes", Value: "on"},
		// is_superuser is reported as advisory; doltgres lacks the role
		// concept that distinguishes it, so report off by default.
		{Name: "is_superuser", Value: "off"},
		{Name: "session_authorization", Value: h.mysqlConn.User},
		{Name: "application_name", Value: h.startupParam("application_name", "")},
	}
	for i := range statuses {
		if err := h.send(&statuses[i]); err != nil {
			return err
		}
	}
	// Real PG advertises a per-connection (ProcessID, SecretKey) pair
	// so any process holding the same pair can issue CancelRequest
	// for that connection. Use the GMS connection id as ProcessID
	// (already unique per session) and a per-session random secret.
	globalCancelRegistry.register(h.mysqlConn.ConnectionID, h.cancelSecretKey, h.mysqlConn.ConnectionID)
	return h.send(&pgproto3.BackendKeyData{
		ProcessID: h.mysqlConn.ConnectionID,
		SecretKey: h.cancelSecretKey,
	})
}

// handleCancelRequest looks up the cancel registry for a presented
// (ProcessID, SecretKey) pair and, on match, asks the engine's
// process list to interrupt the active query on the matching
// connection. A non-matching pair is silently ignored — the
// PostgreSQL protocol forbids sending any response on the cancel
// connection, and a wrong/stale pair is the most common case
// (clients reconnect and ask the registry for a stale entry).
func (h *ConnectionHandler) handleCancelRequest(req *pgproto3.CancelRequest) {
	if req == nil || req.SecretKey == 0 {
		return
	}
	connID, ok := globalCancelRegistry.lookup(req.ProcessID, req.SecretKey)
	if !ok {
		return
	}
	server := sqlserver.GetRunningServer()
	if server == nil || server.Engine == nil {
		return
	}
	if pl := server.Engine.ProcessList; pl != nil {
		pl.Kill(connID)
	}
}

// startupParam returns the value the client supplied for a given startup
// parameter (case-insensitive) or fallback when the client did not send one.
func (h *ConnectionHandler) startupParam(name, fallback string) string {
	if v, ok := h.startupParams[strings.ToLower(name)]; ok && v != "" {
		return v
	}
	return fallback
}

// chooseInitialParameters attempts to choose the initial parameter settings for the connection,
// if one is specified in the startup message provided.
func (h *ConnectionHandler) chooseInitialParameters(startupMessage *pgproto3.StartupMessage) error {
	for name, value := range startupMessage.Parameters {
		// TODO: handle other parameters defined in StartupMessage
		switch strings.ToLower(name) {
		case "application_name":
			sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "")
			if err != nil {
				return err
			}
			err = sqlCtx.SetSessionVariable(sqlCtx, "application_name", value)
			if err != nil {
				return err
			}
		case "datestyle":
			err := h.doltgresHandler.InitSessionParameterDefault(context.Background(), h.mysqlConn, "DateStyle", value)
			if err != nil {
				return err
			}
		case "timezone":
			sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "")
			if err != nil {
				return err
			}
			if err = sqlCtx.SetSessionVariable(sqlCtx, "TimeZone", value); err != nil {
				return err
			}
		}
	}
	// set initial database
	db, ok := startupMessage.Parameters["database"]
	dbSpecified := ok && len(db) > 0
	if !dbSpecified {
		db = h.mysqlConn.User
	}
	h.database = db
	useStmt := fmt.Sprintf("SET database TO '%s';", db)
	postgresParser := psql.PostgresParser{}
	parsed, err := postgresParser.ParseSimple(useStmt)
	if err != nil {
		return err
	}
	err = h.doltgresHandler.ComQuery(context.Background(), h.mysqlConn, useStmt, parsed, func(_ *sql.Context, _ *Result) error {
		return nil
	})
	// If a database isn't specified, then we attempt to connect to a database with the same name as the user,
	// ignoring any error
	if err != nil && dbSpecified {
		_ = h.send(&pgproto3.ErrorResponse{
			Severity: string(ErrorResponseSeverity_Fatal),
			Code:     "3D000",
			Message:  fmt.Sprintf(`"database "%s" does not exist"`, db),
			Routine:  "InitPostgres",
		})
		return err
	}
	if err != nil {
		return nil
	}
	if err = h.checkDatabaseConnectPrivilege(db); err != nil {
		_ = h.send(&pgproto3.ErrorResponse{
			Severity: string(ErrorResponseSeverity_Fatal),
			Code:     pgcode.InsufficientPrivilege.String(),
			Message:  err.Error(),
			Routine:  "InitPostgres",
		})
		return err
	}
	return nil
}

func (h *ConnectionHandler) checkDatabaseConnectPrivilege(database string) error {
	var allowed bool
	var err error
	auth.LockRead(func() {
		role := auth.GetRole(h.mysqlConn.User)
		if !role.IsValid() {
			err = errors.Errorf(`role "%s" does not exist`, h.mysqlConn.User)
			return
		}
		publicRole := auth.GetRole("public")
		if !publicRole.IsValid() {
			err = errors.New(`role "public" does not exist`)
			return
		}
		roleKey := auth.DatabasePrivilegeKey{
			Role: role.ID(),
			Name: database,
		}
		publicKey := auth.DatabasePrivilegeKey{
			Role: publicRole.ID(),
			Name: database,
		}
		allowed = auth.HasDatabasePrivilege(roleKey, auth.Privilege_CONNECT) || auth.HasDatabasePrivilege(publicKey, auth.Privilege_CONNECT)
	})
	if err != nil {
		return err
	}
	if !allowed {
		return errors.Errorf("permission denied for database %s", database)
	}
	return nil
}

// receiveMessage reads a single message off the connection and processes it, returning an error if no message could be
// received from the connection. Otherwise, (a message is received successfully), the message is processed and any
// error is handled appropriately. The return value indicates whether the connection should be closed.
func (h *ConnectionHandler) receiveMessage() (bool, error) {
	var endOfMessages bool
	// For the time being, we handle panics in this function and treat them the same as errors so that they don't
	// forcibly close the connection. Contrast this with the panic handling logic in HandleConnection, where we treat any
	// panic as unrecoverable to the connection. As we fill out the implementation, we can revisit this decision and
	// rethink our posture over whether panics should terminate a connection.
	if HandlePanics {
		defer func() {
			if r := recover(); r != nil {
				stackTrace := string(debug.Stack())
				logrus.Errorf("Listener recovered panic: %v: %s", r, stackTrace)

				eomErr := errors.Errorf("receiveMessage recovered panic: %v: %s", r, stackTrace)
				if !endOfMessages && h.waitForSync {
					if syncErr := h.discardToSync(); syncErr != nil {
						fmt.Println(syncErr.Error())
					}
				}
				h.endOfMessages(eomErr)
			}
		}()
	}

	msg, err := h.backend.Receive()
	if err != nil {
		return false, errors.Errorf("error receiving message: %w", err)
	}

	if m, ok := msg.(json.Marshaler); ok && logrus.IsLevelEnabled(logrus.DebugLevel) {
		msgInfo, err := m.MarshalJSON()
		if err != nil {
			return false, err
		}
		logrus.Debugf("Received message: %s", string(msgInfo))
	} else {
		logrus.Debugf("Received message: %t", msg)
	}

	var stop bool
	stop, endOfMessages, err = h.handleMessage(msg)
	if err != nil {
		if !h.inTransaction {
			notifications.Rollback(h.mysqlConn.ConnectionID)
			functions.RollbackSessionLogicalDecodingMessages(h.mysqlConn.ConnectionID)
		}
		if !endOfMessages && h.waitForSync {
			if syncErr := h.discardToSync(); syncErr != nil {
				fmt.Println(syncErr.Error())
			}
		}
		h.endOfMessages(err)
	} else if endOfMessages {
		h.endOfMessages(nil)
	}

	return stop, nil
}

// handleMessages processes the message provided and returns status flags indicating what the connection should do next.
// If the |stop| response parameter is true, it indicates that the connection should be closed by the caller. If the
// |endOfMessages| response parameter is true, it indicates that no more messages are expected for the current operation
// and a READY FOR QUERY message should be sent back to the client, so it can send the next query.
func (h *ConnectionHandler) handleMessage(msg pgproto3.Message) (stop, endOfMessages bool, err error) {
	if h.replicationSenderID != 0 {
		switch message := msg.(type) {
		case *pgproto3.CopyData:
			return h.handleReplicationCopyData(message)
		case *pgproto3.CopyDone:
			return h.handleReplicationCopyDone(message)
		case *pgproto3.CopyFail:
			return h.handleReplicationCopyFail(message)
		}
	}

	switch message := msg.(type) {
	case *pgproto3.Terminate:
		return true, false, nil
	case *pgproto3.Sync:
		h.waitForSync = false
		return false, true, nil
	case *pgproto3.Flush:
		return false, false, h.flush()
	case *pgproto3.Query:
		endOfMessages, err = h.handleQuery(message)
		return false, endOfMessages, err
	case *pgproto3.Parse:
		return false, false, h.handleParse(message)
	case *pgproto3.Describe:
		return false, false, h.handleDescribe(message)
	case *pgproto3.Bind:
		return false, false, h.handleBind(message)
	case *pgproto3.Execute:
		return false, false, h.handleExecute(message)
	case *pgproto3.Close:
		if message.ObjectType == 'S' {
			delete(h.preparedStatements, message.Name)
			if message.Name != "" {
				sessionstate.DeletePreparedStatement(h.mysqlConn.ConnectionID, message.Name)
			}
		} else {
			delete(h.portals, message.Name)
		}
		h.sendBuffered(&pgproto3.CloseComplete{})
		return false, false, nil
	case *pgproto3.CopyData:
		return h.handleCopyData(message)
	case *pgproto3.CopyDone:
		return h.handleCopyDone(message)
	case *pgproto3.CopyFail:
		return h.handleCopyFail(message)
	default:
		return false, true, errors.Errorf(`unhandled message "%T"`, message)
	}
}

// handleQuery handles a query message, and returns a boolean flag, |endOfMessages| indicating if no other messages are
// expected as part of this query, in which case the server will send a READY FOR QUERY message back to the client so
// that it can send its next query.
func (h *ConnectionHandler) handleQuery(message *pgproto3.Query) (endOfMessages bool, err error) {
	if h.replicationMode {
		handled, endOfMessages, err := h.handleReplicationQuery(message.String)
		if handled || err != nil {
			return endOfMessages, err
		}
	}

	handled, err := h.handledPSQLCommands(message.String)
	if handled || err != nil {
		return true, err
	}
	handled, err = h.handleSQLCursorCommand(message.String)
	if handled || err != nil {
		h.releaseXactAdvisoryLocksIfOutsideTransaction()
		return true, err
	}

	queries, err := h.convertQuery(message.String)
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("Error parsing query: %+v\n", err)
		}
		return true, err
	}

	// A query message destroys the unnamed statement and the unnamed portal
	delete(h.preparedStatements, "")
	delete(h.portals, "")

	if len(queries) == 1 {
		// empty query special case
		if queries[0].AST == nil {
			return true, h.send(&pgproto3.EmptyQueryResponse{})
		}
		if err = h.rejectLockTableOutsideTransaction(queries[0]); err != nil {
			return true, err
		}
		handled, endOfMessages, err = h.handleQueryOutsideEngine(queries[0])
		if handled {
			h.releaseXactAdvisoryLocksIfOutsideTransaction()
			return endOfMessages, err
		}
		if err = h.rejectConcurrentIndexInTransaction(queries[0]); err != nil {
			return true, err
		}
		err = h.query(queries[0])
		if err == nil {
			err = h.applyXactVarSavepointHook(queries[0])
		}
		if err == nil {
			h.markTransactionStatement(queries[0])
		}
		h.releaseXactAdvisoryLocksIfOutsideTransaction()
		return true, err
	}

	for _, query := range queries {
		if err = h.rejectLockTableOutsideTransaction(query); err != nil {
			return true, err
		}
		handled, _, err = h.handleQueryOutsideEngine(query)
		if err != nil {
			return true, err
		}
		if handled {
			continue
		}
		if err = h.rejectConcurrentIndexInTransaction(query); err != nil {
			return true, err
		}
		err = h.query(query)
		if err == nil {
			err = h.applyXactVarSavepointHook(query)
		}
		if err == nil {
			h.markTransactionStatement(query)
		}
		h.releaseXactAdvisoryLocksIfOutsideTransaction()
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

var (
	declareCursorPattern = regexp.MustCompile(`(?is)^\s*DECLARE\s+([A-Za-z_][A-Za-z0-9_$]*)\s+(?:BINARY\s+)?(?:INSENSITIVE\s+)?(?:(?:NO\s+)?SCROLL\s+)?CURSOR(?:\s+(WITH|WITHOUT)\s+HOLD)?\s+FOR\s+(.+?)\s*;?\s*$`)
	closeCursorPattern   = regexp.MustCompile(`(?is)^\s*CLOSE\s+(ALL|[A-Za-z_][A-Za-z0-9_$]*)\s*;?\s*$`)
)

func (h *ConnectionHandler) handleSQLCursorCommand(query string) (bool, error) {
	converted, ok, err := h.convertSQLCursorCommand(query)
	if err != nil || !ok {
		return ok, err
	}
	return true, h.executeSQLCursorCommand(converted)
}

func (h *ConnectionHandler) convertSQLCursorCommand(query string) (ConvertedQuery, bool, error) {
	if matches := declareCursorPattern.FindStringSubmatch(query); matches != nil {
		return ConvertedQuery{
			String:       query,
			AST:          sqlparser.InjectedStatement{Statement: sqlCursorDeclareStatement{Name: matches[1], Hold: strings.EqualFold(matches[2], "WITH"), Query: strings.TrimSpace(matches[3])}},
			StatementTag: "DECLARE CURSOR",
		}, true, nil
	}
	if name, direction, ok := parseFetchCursor(query); ok {
		return ConvertedQuery{
			String:       query,
			AST:          sqlparser.InjectedStatement{Statement: sqlCursorFetchStatement{Name: name, Direction: direction}},
			StatementTag: "FETCH",
		}, true, nil
	}
	if matches := closeCursorPattern.FindStringSubmatch(query); matches != nil {
		return ConvertedQuery{
			String:       query,
			AST:          sqlparser.InjectedStatement{Statement: sqlCursorCloseStatement{Name: matches[1]}},
			StatementTag: "CLOSE CURSOR",
		}, true, nil
	}
	return ConvertedQuery{}, false, nil
}

func (h *ConnectionHandler) executeSQLCursorCommand(query ConvertedQuery) error {
	injected, ok := query.AST.(sqlparser.InjectedStatement)
	if !ok {
		return errors.Errorf("expected injected cursor statement")
	}
	switch stmt := injected.Statement.(type) {
	case sqlCursorDeclareStatement:
		return h.declareSQLCursor(stmt.Name, stmt.Hold, stmt.Query)
	case sqlCursorFetchStatement:
		return h.fetchSQLCursor(stmt.Name, stmt.Direction)
	case sqlCursorCloseStatement:
		return h.closeSQLCursor(stmt.Name)
	default:
		return errors.Errorf("unexpected cursor statement: %T", stmt)
	}
}

func (h *ConnectionHandler) cursorReturnFields(query ConvertedQuery) []pgproto3.FieldDescription {
	injected, ok := query.AST.(sqlparser.InjectedStatement)
	if !ok {
		return nil
	}
	stmt, ok := injected.Statement.(sqlCursorFetchStatement)
	if !ok {
		return nil
	}
	if cursor, ok := h.cursors[normalizeCursorName(stmt.Name)]; ok {
		return cloneCursorFields(cursor.fields)
	}
	return nil
}

func parseFetchCursor(query string) (string, string, bool) {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	parts := strings.Fields(trimmed)
	if len(parts) < 2 || !strings.EqualFold(parts[0], "FETCH") {
		return "", "", false
	}
	direction := "NEXT"
	idx := 1
	switch {
	case strings.EqualFold(parts[idx], "NEXT"):
		idx++
	case strings.EqualFold(parts[idx], "ALL"):
		direction = "ALL"
		idx++
	case strings.EqualFold(parts[idx], "FORWARD") && idx+1 < len(parts) && parts[idx+1] == "1":
		idx += 2
	}
	if idx < len(parts) && strings.EqualFold(parts[idx], "FROM") {
		idx++
	}
	if idx != len(parts)-1 {
		return "", "", false
	}
	return parts[idx], direction, true
}

func (h *ConnectionHandler) declareSQLCursor(name string, hold bool, selectSQL string) error {
	name = normalizeCursorName(name)
	if name == "" {
		return pgerror.New(pgcode.InvalidCursorName, "invalid cursor name")
	}
	if _, ok := h.cursors[name]; ok {
		return pgerror.Newf(pgcode.DuplicateCursor, `cursor "%s" already exists`, name)
	}
	queries, err := h.convertQuery(selectSQL)
	if err != nil {
		return err
	}
	if len(queries) != 1 {
		return pgerror.New(pgcode.InvalidCursorDefinition, "cursor declaration must contain a single query")
	}
	cursorQuery := queries[0]
	if !returnsRow(cursorQuery) {
		return pgerror.New(pgcode.InvalidCursorDefinition, "cursor query must return rows")
	}

	cursor := &cursorData{
		hold: hold,
	}
	err = h.doltgresHandler.ComQuery(context.Background(), h.mysqlConn, cursorQuery.String, cursorQuery.AST, func(ctx *sql.Context, res *Result) error {
		if cursor.fields == nil {
			cursor.fields = cloneCursorFields(res.Fields)
		}
		for _, row := range res.Rows {
			cursor.rows = append(cursor.rows, cloneCursorRow(row))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if cursor.fields == nil {
		cursor.fields = []pgproto3.FieldDescription{}
	}
	h.cursors[name] = cursor
	h.markCursorTransactionStatement()
	h.sendBuffered(makeCommandComplete("DECLARE CURSOR", 0))
	return nil
}

func (h *ConnectionHandler) fetchSQLCursor(name string, direction string) error {
	cursor, ok := h.cursors[normalizeCursorName(name)]
	if !ok {
		return pgerror.Newf(pgcode.InvalidCursorName, `cursor "%s" does not exist`, name)
	}
	h.sendBuffered(&pgproto3.RowDescription{
		Fields: cloneCursorFields(cursor.fields),
	})
	var fetched int32
	switch direction {
	case "ALL":
		for cursor.pos < len(cursor.rows) {
			h.sendBuffered(&pgproto3.DataRow{Values: cloneCursorRow(cursor.rows[cursor.pos]).val})
			cursor.pos++
			fetched++
		}
	default:
		if cursor.pos < len(cursor.rows) {
			h.sendBuffered(&pgproto3.DataRow{Values: cloneCursorRow(cursor.rows[cursor.pos]).val})
			cursor.pos++
			fetched = 1
		}
	}
	h.markCursorTransactionStatement()
	h.sendBuffered(makeCommandComplete("FETCH", fetched))
	return nil
}

func (h *ConnectionHandler) closeSQLCursor(name string) error {
	if strings.EqualFold(name, "ALL") {
		clear(h.cursors)
		h.sendBuffered(makeCommandComplete("CLOSE CURSOR", 0))
		return nil
	}
	normalized := normalizeCursorName(name)
	if _, ok := h.cursors[normalized]; !ok {
		return pgerror.Newf(pgcode.InvalidCursorName, `cursor "%s" does not exist`, name)
	}
	delete(h.cursors, normalized)
	h.markCursorTransactionStatement()
	h.sendBuffered(makeCommandComplete("CLOSE CURSOR", 0))
	return nil
}

func (h *ConnectionHandler) closeNonHoldCursors() {
	for name, cursor := range h.cursors {
		if !cursor.hold {
			delete(h.cursors, name)
		}
	}
}

func (h *ConnectionHandler) markCursorTransactionStatement() {
	if h.inTransaction {
		h.transactionStatementCount++
	}
}

func normalizeCursorName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func cloneCursorFields(fields []pgproto3.FieldDescription) []pgproto3.FieldDescription {
	if fields == nil {
		return nil
	}
	cloned := make([]pgproto3.FieldDescription, len(fields))
	copy(cloned, fields)
	for i := range cloned {
		cloned[i].Name = slices.Clone(fields[i].Name)
	}
	return cloned
}

func cloneCursorRow(row Row) Row {
	if row.val == nil {
		return Row{}
	}
	values := make([][]byte, len(row.val))
	for i := range row.val {
		values[i] = slices.Clone(row.val[i])
	}
	return Row{val: values}
}

// releaseXactAdvisoryLocksIfOutsideTransaction releases every transaction-scope
// advisory lock the session holds when the wire-protocol transaction has ended
// (either via COMMIT/ROLLBACK or because we are running in autocommit mode and
// the previous statement just finished). Also rolls back transaction-local
// GUC writes (SET LOCAL / set_config(..., true)). Mirrors PostgreSQL's
// automatic cleanup at transaction end.
func (h *ConnectionHandler) releaseXactAdvisoryLocksIfOutsideTransaction() {
	if h.inTransaction || h.mysqlConn == nil {
		return
	}
	hasLocks := functions.HasSessionXactLocks(h.mysqlConn.ConnectionID)
	hasVars := functions.HasSessionXactVars(h.mysqlConn.ConnectionID)
	if !hasLocks && !hasVars {
		return
	}
	ctx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return
	}
	if hasLocks {
		_ = functions.ReleaseSessionXactLocksWithSubsystem(ctx, h.doltgresHandler.e.LS)
		node.ReleaseSessionRowLocks(h.mysqlConn.ConnectionID)
	}
	if hasVars {
		_ = functions.ReleaseSessionXactVars(ctx)
	}
}

// handleQueryOutsideEngine handles any queries that should be handled by the handler directly, rather than being
// passed to the engine. The response parameter |handled| is true if the query was handled, |endOfMessages| is true
// if no more messages are expected for this query and server should send the client a READY FOR QUERY message,
// and any error that occurred while handling the query.
func (h *ConnectionHandler) handleQueryOutsideEngine(query ConvertedQuery) (handled bool, endOfMessages bool, err error) {
	switch stmt := query.AST.(type) {
	case *sqlparser.Begin:
		transactionCharacteristic := sql.ReadWrite
		if query.UsesDefaultTransactionReadMode {
			sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.StatementTag)
			if err != nil {
				return false, true, err
			}
			readOnly, err := defaultTransactionReadOnly(sqlCtx)
			if err != nil {
				return false, true, err
			}
			if readOnly {
				transactionCharacteristic = sql.ReadOnly
			}
		} else if stmt.TransactionCharacteristic == sqlparser.TxReadOnly {
			transactionCharacteristic = sql.ReadOnly
		}
		h.inTransaction = true
		h.transactionStatementCount = 0
		h.transactionSnapshotAllowed = query.UsesExplicitTransactionIsolation
		return true, true, h.beginTransaction(query, transactionCharacteristic)
	case *sqlparser.Commit:
		h.inTransaction = false
		h.transactionStatementCount = 0
		h.transactionSnapshotAllowed = false
		h.closeNonHoldCursors()
	case *sqlparser.Rollback:
		h.inTransaction = false
		h.transactionStatementCount = 0
		h.transactionSnapshotAllowed = false
		h.closeNonHoldCursors()
	case *sqlparser.Deallocate:
		return true, true, h.deallocatePreparedStatement(stmt.Name, h.preparedStatements, query)
	case sqlparser.InjectedStatement:
		switch injectedStmt := stmt.Statement.(type) {
		case node.SetTransaction:
			return true, true, h.setTransaction(injectedStmt, query)
		case node.SetSessionCharacteristics:
			return true, true, h.setSessionCharacteristics(injectedStmt, query)
		case sqlCursorDeclareStatement, sqlCursorFetchStatement, sqlCursorCloseStatement:
			return true, true, h.executeSQLCursorCommand(query)
		}
		if err := h.rejectReadOnlyTransactionWrite(query); err != nil {
			if copyFrom, ok := stmt.Statement.(*node.CopyFrom); ok && copyFrom.Stdin {
				h.copyFromStdinState = nil
				h.copyFromStdinFailed = true
			}
			return true, true, err
		}
		switch injectedStmt := stmt.Statement.(type) {
		case node.DiscardStatement:
			switch injectedStmt.Mode {
			case node.DiscardModeTemp:
				return true, true, h.discardTemp(query)
			default:
				return true, true, h.discardAll(query)
			}
		case node.PrepareStatement:
			return true, true, h.prepareSQLStatement(injectedStmt, query)
		case node.ExecuteStatement:
			return true, true, h.executeSQLStatement(injectedStmt)
		case node.CreateTableAsExecuteStatement:
			return true, true, h.createTableAsExecuteSQLStatement(injectedStmt, query)
		case node.ListenStatement:
			return true, true, h.listen(injectedStmt, query)
		case node.UnlistenStatement:
			return true, true, h.unlisten(injectedStmt, query)
		case node.NotifyStatement:
			return true, true, h.notify(injectedStmt, query)
		case node.PrepareTransaction:
			return true, true, h.prepareTransaction(injectedStmt, query)
		case node.CommitPrepared:
			return true, true, h.commitPrepared(injectedStmt, query)
		case node.RollbackPrepared:
			return true, true, h.rollbackPrepared(injectedStmt, query)
		case *node.CopyFrom:
			// When copying data from STDIN, the data is sent to the server as CopyData messages
			// We send endOfMessages=false since the server will be in COPY DATA mode and won't
			// be ready for more queries util COPY DATA mode is completed.
			if injectedStmt.Stdin {
				return true, false, h.handleCopyFromStdinQuery(injectedStmt, h.Conn())
			} else {
				// copying from a file is handled in a single message
				return true, true, h.copyFromFileQuery(injectedStmt)
			}
		case *node.CopyTo:
			return true, true, h.handleCopyToStdoutQuery(injectedStmt)
		}
	}
	return false, true, nil
}

func (h *ConnectionHandler) beginTransaction(query ConvertedQuery, transactionCharacteristic sql.TransactionCharacteristic) error {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	ts, ok := sqlCtx.Session.(sql.TransactionSession)
	if ok {
		currentTx := sqlCtx.GetTransaction()
		if currentTx != nil {
			if err := ts.CommitTransaction(sqlCtx, currentTx); err != nil {
				return err
			}
		}
		transaction, err := ts.StartTransaction(sqlCtx, transactionCharacteristic)
		if err != nil {
			return err
		}
		sqlCtx.SetTransaction(transaction)
		sqlCtx.SetIgnoreAutoCommit(true)
	}
	h.sendBuffered(makeCommandComplete(query.StatementTag, 0))
	return h.finishNotifications(query)
}

func (h *ConnectionHandler) setTransaction(stmt node.SetTransaction, query ConvertedQuery) error {
	if stmt.Snapshot != "" {
		if !h.inTransaction || !h.transactionSnapshotAllowed {
			return pgerror.New(pgcode.InvalidParameterValue, "must have isolation level SERIALIZABLE or REPEATABLE READ")
		}
		return pgerror.New(pgcode.InvalidParameterValue, "invalid snapshot identifier")
	}
	if stmt.Isolation && h.transactionStatementCount > 0 {
		return pgerror.New(pgcode.ActiveSQLTransaction, "SET TRANSACTION ISOLATION LEVEL must be called before any query")
	}
	if stmt.DeferrableMode != node.TransactionDeferrableUnspecified && h.transactionStatementCount > 0 {
		return pgerror.New(pgcode.ActiveSQLTransaction, "SET TRANSACTION [NOT] DEFERRABLE must be called before any query")
	}
	if stmt.ReadWriteMode != node.TransactionReadWriteUnspecified && h.transactionStatementCount > 0 {
		return pgerror.New(pgcode.ActiveSQLTransaction, "transaction read-write mode must be set before any query")
	}
	if h.inTransaction && stmt.ReadWriteMode != node.TransactionReadWriteUnspecified {
		transactionCharacteristic := sql.ReadWrite
		if stmt.ReadWriteMode == node.TransactionReadOnly {
			transactionCharacteristic = sql.ReadOnly
		}
		if err := h.restartCurrentTransaction(query, transactionCharacteristic); err != nil {
			return err
		}
	}
	if h.inTransaction && stmt.Isolation {
		h.transactionSnapshotAllowed = true
	}
	h.sendBuffered(makeCommandComplete(query.StatementTag, 0))
	return h.finishNotifications(query)
}

func (h *ConnectionHandler) restartCurrentTransaction(query ConvertedQuery, transactionCharacteristic sql.TransactionCharacteristic) error {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	ts, ok := sqlCtx.Session.(sql.TransactionSession)
	if !ok {
		return nil
	}
	if currentTx := sqlCtx.GetTransaction(); currentTx != nil {
		if err = ts.Rollback(sqlCtx, currentTx); err != nil {
			return err
		}
		sqlCtx.SetTransaction(nil)
	}
	transaction, err := ts.StartTransaction(sqlCtx, transactionCharacteristic)
	if err != nil {
		return err
	}
	sqlCtx.SetTransaction(transaction)
	sqlCtx.SetIgnoreAutoCommit(true)
	return nil
}

func (h *ConnectionHandler) setSessionCharacteristics(stmt node.SetSessionCharacteristics, query ConvertedQuery) error {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	switch stmt.ReadWriteMode {
	case node.TransactionReadOnly:
		err = sqlCtx.SetSessionVariable(sqlCtx, "default_transaction_read_only", int8(1))
	case node.TransactionReadWrite:
		err = sqlCtx.SetSessionVariable(sqlCtx, "default_transaction_read_only", int8(0))
	}
	if err != nil {
		return err
	}
	h.sendBuffered(makeCommandComplete(query.StatementTag, 0))
	return h.finishNotifications(query)
}

func defaultTransactionReadOnly(ctx *sql.Context) (bool, error) {
	value, err := ctx.GetSessionVariable(ctx, "default_transaction_read_only")
	if err != nil {
		return false, err
	}
	switch v := value.(type) {
	case int:
		return v != 0, nil
	case int8:
		return v != 0, nil
	case int16:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case uint:
		return v != 0, nil
	case uint8:
		return v != 0, nil
	case uint16:
		return v != 0, nil
	case uint32:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case bool:
		return v, nil
	case string:
		normalized := strings.ToLower(strings.TrimSpace(v))
		switch normalized {
		case "on", "true", "yes":
			return true, nil
		case "off", "false", "no":
			return false, nil
		}
		parsed, err := strconv.ParseInt(normalized, 10, 64)
		if err == nil {
			return parsed != 0, nil
		}
		return false, nil
	default:
		return false, nil
	}
}

func (h *ConnectionHandler) queryHandledOutsideEngine(query ConvertedQuery) bool {
	switch stmt := query.AST.(type) {
	case *sqlparser.Deallocate:
		return true
	case sqlparser.InjectedStatement:
		switch stmt.Statement.(type) {
		case node.SetTransaction, node.SetSessionCharacteristics,
			node.DiscardStatement, node.PrepareStatement, node.ExecuteStatement, node.CreateTableAsExecuteStatement, node.PrepareTransaction,
			node.CommitPrepared, node.RollbackPrepared, node.ListenStatement, node.UnlistenStatement,
			node.NotifyStatement, *node.CopyFrom, *node.CopyTo,
			sqlCursorDeclareStatement, sqlCursorFetchStatement, sqlCursorCloseStatement:
			return true
		}
	}
	return false
}

func (h *ConnectionHandler) rejectConcurrentIndexInTransaction(query ConvertedQuery) error {
	if !h.inTransaction {
		return nil
	}
	if createIndexConcurrentlyPattern.MatchString(query.String) {
		return pgerror.Newf(pgcode.ActiveSQLTransaction,
			"CREATE INDEX CONCURRENTLY cannot run inside a transaction block")
	}
	if dropIndexConcurrentlyPattern.MatchString(query.String) {
		return pgerror.Newf(pgcode.ActiveSQLTransaction,
			"DROP INDEX CONCURRENTLY cannot run inside a transaction block")
	}
	return nil
}

func (h *ConnectionHandler) rejectLockTableOutsideTransaction(query ConvertedQuery) error {
	if h.inTransaction || strings.ToUpper(query.StatementTag) != "LOCK TABLE" {
		return nil
	}
	return pgerror.New(pgcode.NoActiveSQLTransaction, "LOCK TABLE can only be used in transaction blocks")
}

func (h *ConnectionHandler) rejectReadOnlyTransactionWrite(query ConvertedQuery) error {
	if !h.inTransaction || !queryWritesPersistentState(query) {
		return nil
	}
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	tx := sqlCtx.GetTransaction()
	if tx == nil || !tx.IsReadOnly() {
		return nil
	}
	return readOnlyTransactionError()
}

func readOnlyTransactionError() error {
	return pgerror.New(pgcode.ReadOnlySQLTransaction, "cannot execute statement in a read-only transaction (READ ONLY transaction)")
}

func isReadOnlyTransactionError(err error) bool {
	return sql.ErrReadOnlyTransaction.Is(err) || strings.Contains(err.Error(), "READ ONLY transaction")
}

func queryWritesPersistentState(query ConvertedQuery) bool {
	if stmt, ok := query.AST.(sqlparser.InjectedStatement); ok {
		return injectedStatementWritesPersistentState(stmt.Statement)
	}
	switch strings.ToUpper(query.StatementTag) {
	case "ALTER TABLE", "DROP TABLE", "CREATE INDEX", "DROP INDEX", "ALTER INDEX", "DROP SEQUENCE":
		return true
	case "CREATE TABLE", "CREATE SEQUENCE":
		return !readOnlyTemporaryCreatePattern.MatchString(query.String)
	default:
		return false
	}
}

func injectedStatementWritesPersistentState(stmt any) bool {
	if stmt == nil {
		return false
	}
	switch stmt.(type) {
	case node.SetTransaction, node.SetSessionCharacteristics, node.NoOp,
		node.ListenStatement, node.UnlistenStatement, node.NotifyStatement:
		return false
	}
	if sqlNode, ok := stmt.(sql.Node); ok {
		return !sqlNode.IsReadOnly()
	}
	return false
}

func (h *ConnectionHandler) markTransactionStatement(query ConvertedQuery) {
	if !h.inTransaction || isTransactionControlQuery(query) {
		return
	}
	h.transactionStatementCount++
}

func isTransactionControlQuery(query ConvertedQuery) bool {
	switch query.AST.(type) {
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback,
		*sqlparser.Savepoint, *sqlparser.RollbackSavepoint, *sqlparser.ReleaseSavepoint:
		return true
	case sqlparser.InjectedStatement:
		stmt := query.AST.(sqlparser.InjectedStatement)
		switch stmt.Statement.(type) {
		case node.SetTransaction:
			return true
		}
	}
	return false
}

func (h *ConnectionHandler) listen(stmt node.ListenStatement, query ConvertedQuery) error {
	notifications.Listen(h.mysqlConn.ConnectionID, stmt.Channel)
	h.sendBuffered(makeCommandComplete(query.StatementTag, 0))
	return h.finishNotifications(query)
}

func (h *ConnectionHandler) unlisten(stmt node.UnlistenStatement, query ConvertedQuery) error {
	if stmt.All {
		notifications.UnlistenAll(h.mysqlConn.ConnectionID)
	} else {
		notifications.Unlisten(h.mysqlConn.ConnectionID, stmt.Channel)
	}
	h.sendBuffered(makeCommandComplete(query.StatementTag, 0))
	return h.finishNotifications(query)
}

func (h *ConnectionHandler) notify(stmt node.NotifyStatement, query ConvertedQuery) error {
	if err := notifications.Queue(h.mysqlConn.ConnectionID, stmt.Channel, stmt.Payload); err != nil {
		return err
	}
	h.sendBuffered(makeCommandComplete(query.StatementTag, 0))
	return h.finishNotifications(query)
}

func (h *ConnectionHandler) prepareTransaction(stmt node.PrepareTransaction, query ConvertedQuery) error {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	replication := h.preparedReplicationState()
	if err = sessionstate.PrepareTransaction(sqlCtx, stmt.GID, replication); err != nil {
		return err
	}
	h.clearPendingReplication()
	h.inTransaction = false
	h.transactionSnapshotAllowed = false
	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.StatementTag),
	})
}

func (h *ConnectionHandler) commitPrepared(stmt node.CommitPrepared, query ConvertedQuery) error {
	if h.inTransaction {
		return errors.Errorf("COMMIT PREPARED cannot run inside a transaction block")
	}
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	replication, _ := sessionstate.GetPreparedReplication(stmt.GID)
	if err = sessionstate.CommitPreparedTransaction(sqlCtx, stmt.GID); err != nil {
		return err
	}
	if err = publishPreparedReplicationState(sqlCtx, replication); err != nil {
		return err
	}
	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.StatementTag),
	})
}

func (h *ConnectionHandler) rollbackPrepared(stmt node.RollbackPrepared, query ConvertedQuery) error {
	if h.inTransaction {
		return errors.Errorf("ROLLBACK PREPARED cannot run inside a transaction block")
	}
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	if err := sessionstate.RollbackPreparedTransaction(sqlCtx, stmt.GID); err != nil {
		return err
	}
	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.StatementTag),
	})
}

func (h *ConnectionHandler) preparedReplicationState() *sessionstate.PreparedReplicationState {
	if len(h.pendingReplicationCaptures) == 0 && !h.pendingReplicationAdvance {
		return nil
	}
	state := &sessionstate.PreparedReplicationState{
		Advance: h.pendingReplicationAdvance,
	}
	for _, capture := range h.pendingReplicationCaptures {
		if capture == nil {
			continue
		}
		state.Captures = append(state.Captures, capture.toPreparedReplicationCapture())
	}
	return state
}

func (capture *replicationChangeCapture) toPreparedReplicationCapture() sessionstate.PreparedReplicationCapture {
	prepared := sessionstate.PreparedReplicationCapture{
		Action:       byte(capture.action),
		Schema:       capture.schema,
		Table:        capture.table,
		RowsAffected: capture.rowsAffected,
	}
	for _, field := range capture.fields {
		prepared.Fields = append(prepared.Fields, sessionstate.PreparedReplicationField{
			Name:         string(field.Name),
			DataTypeOID:  field.DataTypeOID,
			TypeModifier: field.TypeModifier,
		})
	}
	for _, row := range capture.rows {
		preparedRow := make([][]byte, len(row.val))
		for i, value := range row.val {
			preparedRow[i] = append([]byte(nil), value...)
		}
		prepared.Rows = append(prepared.Rows, preparedRow)
	}
	for _, row := range capture.oldRows {
		preparedRow := make([][]byte, len(row.val))
		for i, value := range row.val {
			preparedRow[i] = append([]byte(nil), value...)
		}
		prepared.OldRows = append(prepared.OldRows, preparedRow)
	}
	return prepared
}

func publishPreparedReplicationState(ctx *sql.Context, state *sessionstate.PreparedReplicationState) error {
	if state == nil {
		return nil
	}
	if len(state.Captures) > 0 {
		captures := make([]*replicationChangeCapture, 0, len(state.Captures))
		for _, prepared := range state.Captures {
			captures = append(captures, replicationChangeCaptureFromPrepared(prepared))
		}
		return publishReplicationCaptures(ctx, captures)
	}
	if state.Advance {
		replsource.AdvanceLSN()
	}
	return nil
}

func replicationChangeCaptureFromPrepared(prepared sessionstate.PreparedReplicationCapture) *replicationChangeCapture {
	capture := &replicationChangeCapture{
		action:       replicationChangeAction(prepared.Action),
		schema:       prepared.Schema,
		table:        prepared.Table,
		rowsAffected: prepared.RowsAffected,
	}
	for _, field := range prepared.Fields {
		capture.fields = append(capture.fields, pgproto3.FieldDescription{
			Name:         []byte(field.Name),
			DataTypeOID:  field.DataTypeOID,
			TypeModifier: field.TypeModifier,
		})
	}
	for _, row := range prepared.Rows {
		captureRow := Row{val: make([][]byte, len(row))}
		for i, value := range row {
			captureRow.val[i] = append([]byte(nil), value...)
		}
		capture.rows = append(capture.rows, captureRow)
	}
	for _, row := range prepared.OldRows {
		captureRow := Row{val: make([][]byte, len(row))}
		for i, value := range row {
			captureRow.val[i] = append([]byte(nil), value...)
		}
		capture.oldRows = append(capture.oldRows, captureRow)
	}
	return capture
}

func (h *ConnectionHandler) prepareSQLStatement(stmt node.PrepareStatement, query ConvertedQuery) error {
	if _, ok := h.preparedStatements[stmt.Name]; ok {
		return errors.Errorf("prepared statement %s already exists", stmt.Name)
	}

	queries, err := h.convertQuery(stmt.Statement)
	if err != nil {
		return err
	}
	if len(queries) != 1 {
		return errors.Errorf("cannot insert multiple commands into a prepared statement")
	}
	preparedQuery := queries[0]
	if preparedQuery.AST == nil {
		return errors.Errorf("cannot prepare an empty query")
	}

	parsedQuery, fields, err := h.doltgresHandler.ComPrepareParsed(context.Background(), h.mysqlConn, preparedQuery.String, preparedQuery.AST)
	if err != nil {
		return err
	}
	analyzedPlan, ok := parsedQuery.(sql.Node)
	if !ok {
		return errors.Errorf("expected a sql.Node, got %T", parsedQuery)
	}

	bindVarTypes, err := h.resolvePreparedStatementTypes(stmt.ParameterTypes, analyzedPlan)
	if err != nil {
		return err
	}

	preparedData := PreparedStatementData{
		Query:        preparedQuery,
		ReturnFields: fields,
		BindVarTypes: bindVarTypes,
		FromSQL:      true,
	}
	if h.preparedPlanCacheable(preparedQuery, bindVarTypes) {
		cacheCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, preparedQuery.String)
		if err != nil {
			return err
		}
		h.cachePreparedPlan(cacheCtx, &preparedData, analyzedPlan)
	}
	h.preparedStatements[stmt.Name] = preparedData
	h.recordPreparedStatement(stmt.Name, query.String, fields, bindVarTypes, true)
	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.StatementTag),
	})
}

func (h *ConnectionHandler) resolvePreparedStatementTypes(typeNames []string, analyzedPlan sql.Node) ([]uint32, error) {
	ctx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "PREPARE")
	if err != nil {
		return nil, err
	}
	if len(typeNames) == 0 {
		return extractBindVarTypes(ctx, analyzedPlan)
	}

	typeOIDs := make([]uint32, len(typeNames))
	for i, typeName := range typeNames {
		typeOID, err := resolvePreparedStatementTypeOID(ctx, typeName)
		if err != nil {
			return nil, err
		}
		typeOIDs[i] = typeOID
	}
	return typeOIDs, nil
}

func resolvePreparedStatementTypeOID(ctx *sql.Context, typeName string) (uint32, error) {
	schemaName, normalized, err := preparedStatementTypeNameParts(typeName)
	if err != nil {
		return 0, err
	}
	canonical := canonicalPreparedStatementTypeName(normalized)

	typeCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return 0, err
	}
	lookupInSchema := func(schema string) (*pgtypes.DoltgresType, error) {
		if schema == "pg_catalog" {
			if internalID, ok := pgtypes.NameToInternalID[canonical]; ok && internalID.SchemaName() == schema {
				if dgType := pgtypes.GetTypeByID(internalID); dgType != nil {
					return dgType, nil
				}
			}
		}
		dgType, err := typeCollection.GetType(ctx, id.NewType(schema, normalized))
		if err != nil || dgType != nil || canonical == normalized {
			return dgType, err
		}
		return typeCollection.GetType(ctx, id.NewType(schema, canonical))
	}

	if schemaName != "" {
		dgType, err := lookupInSchema(schemaName)
		if err != nil {
			return 0, err
		}
		if dgType != nil {
			return id.Cache().ToOID(dgType.ID.AsId()), nil
		}
		return 0, errors.Errorf("type %s does not exist", typeName)
	}

	searchPath, err := core.SearchPath(ctx)
	if err != nil {
		return 0, err
	}
	for _, schema := range searchPath {
		dgType, err := lookupInSchema(schema)
		if err != nil {
			return 0, err
		}
		if dgType != nil {
			return id.Cache().ToOID(dgType.ID.AsId()), nil
		}
	}
	return 0, errors.Errorf("type %s does not exist", typeName)
}

func canonicalPreparedStatementTypeName(typeName string) string {
	switch typeName {
	case "int", "integer":
		return "int4"
	case "smallint":
		return "int2"
	case "bigint":
		return "int8"
	case "double precision":
		return "float8"
	case "real":
		return "float4"
	case "boolean":
		return "bool"
	case "character varying":
		return "varchar"
	}
	return typeName
}

func preparedStatementTypeNameParts(typeName string) (schemaName string, baseName string, err error) {
	parts, err := splitPreparedStatementTypeName(stripPreparedStatementTypeModifiers(typeName))
	if err != nil {
		return "", "", err
	}
	switch len(parts) {
	case 1:
		return "", parts[0], nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", errors.Errorf("type %s does not exist", typeName)
	}
}

func stripPreparedStatementTypeModifiers(typeName string) string {
	var b strings.Builder
	inQuotes := false
	trimmed := strings.TrimSpace(typeName)
	for i := 0; i < len(trimmed); i++ {
		r := rune(trimmed[i])
		if r == '"' {
			b.WriteRune(r)
			if inQuotes && i+1 < len(trimmed) && trimmed[i+1] == '"' {
				i++
				b.WriteRune('"')
			} else {
				inQuotes = !inQuotes
			}
			continue
		}
		if r == '(' && !inQuotes {
			break
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

func splitPreparedStatementTypeName(typeName string) ([]string, error) {
	if typeName == "" {
		return nil, errors.Errorf("invalid name syntax")
	}
	var parts []string
	var b strings.Builder
	inQuotes := false
	for i := 0; i < len(typeName); i++ {
		r := rune(typeName[i])
		switch r {
		case '"':
			if inQuotes && i+1 < len(typeName) && typeName[i+1] == '"' {
				b.WriteRune('"')
				i++
			} else {
				inQuotes = !inQuotes
			}
		case '.':
			if inQuotes {
				b.WriteRune(r)
			} else {
				part := strings.TrimSpace(b.String())
				if part == "" {
					return nil, errors.Errorf("invalid name syntax")
				}
				parts = append(parts, part)
				b.Reset()
			}
		default:
			if inQuotes {
				b.WriteRune(r)
			} else {
				b.WriteRune(unicode.ToLower(r))
			}
		}
	}
	if inQuotes {
		return nil, errors.Errorf("invalid name syntax")
	}
	part := strings.TrimSpace(b.String())
	if part == "" {
		return nil, errors.Errorf("invalid name syntax")
	}
	parts = append(parts, part)
	return parts, nil
}

func (h *ConnectionHandler) executeSQLStatement(stmt node.ExecuteStatement) error {
	preparedData, ok := h.preparedStatements[stmt.Name]
	if !ok {
		return errors.Errorf("prepared statement %s does not exist", stmt.Name)
	}
	if stmt.DiscardRows {
		return errors.Errorf("EXECUTE DISCARD ROWS is not supported")
	}
	if len(stmt.Params) != len(preparedData.BindVarTypes) {
		return errors.Errorf("prepared statement %s expected %d parameters but got %d", stmt.Name, len(preparedData.BindVarTypes), len(stmt.Params))
	}

	params := make([][]byte, len(stmt.Params))
	for i, param := range stmt.Params {
		value, err := h.evaluateExecuteParameter(param)
		if err != nil {
			return err
		}
		params[i] = value
	}

	var fields []pgproto3.FieldDescription
	var boundPlan sql.Node
	var err error
	if len(params) == 0 {
		cacheCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, preparedData.Query.String)
		if err != nil {
			return err
		}
		if cachedPlan, ok := h.cachedPreparedPlan(cacheCtx, preparedData); ok {
			boundPlan = cachedPlan
			fields, err = schemaToFieldDescriptionsWithSource(cacheCtx, boundPlan.Schema(cacheCtx), boundPlan, nil)
			if err != nil {
				return err
			}
		}
	}
	if boundPlan == nil {
		analyzedPlan, bindFields, err := h.doltgresHandler.ComBind(
			context.Background(),
			h.mysqlConn,
			preparedData.Query.String,
			preparedData.Query.AST,
			BindVariables{
				varTypes:   preparedData.BindVarTypes,
				parameters: params,
			},
			nil,
		)
		if err != nil {
			return err
		}
		fields = bindFields
		var ok bool
		boundPlan, ok = analyzedPlan.(sql.Node)
		if !ok {
			return errors.Errorf("expected a sql.Node, got %T", analyzedPlan)
		}
		if len(params) == 0 {
			cacheCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, preparedData.Query.String)
			if err != nil {
				return err
			}
			h.cachePreparedPlan(cacheCtx, &preparedData, boundPlan)
			h.preparedStatements[stmt.Name] = preparedData
		}
	}
	if err := validatePreparedStatementResultShape(preparedData.Query, preparedData.ReturnFields, fields); err != nil {
		return err
	}

	query := preparedData.Query
	query.StatementTag = preparedData.Query.StatementTag
	if err := h.validateReplicationChangeReplicaIdentity(query); err != nil {
		return err
	}
	rowsAffected := int32(0)
	executionQuery := query
	executionPlan := boundPlan
	executionFields := fields
	var replicationCapture *replicationChangeCapture
	var replicationQuery ConvertedQuery
	var hasReplicationCapture bool
	var executionFormatCodes []int16
	advanceLSN := false
	if replsource.HasSlots() {
		replicationCapture, replicationQuery, hasReplicationCapture, err = h.prepareReplicationChangeQuery(preparedData.Query)
		if err != nil {
			return err
		}
		if !hasReplicationCapture {
			replicationCapture = nil
		} else {
			replicationPlan, replicationFields, err := h.doltgresHandler.ComBind(
				context.Background(),
				h.mysqlConn,
				replicationQuery.String,
				replicationQuery.AST,
				BindVariables{
					varTypes:   preparedData.BindVarTypes,
					parameters: params,
				},
				nil,
			)
			if err != nil {
				return err
			}
			var ok bool
			executionPlan, ok = replicationPlan.(sql.Node)
			if !ok {
				return errors.Errorf("expected a sql.Node, got %T", replicationPlan)
			}
			replicationCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, replicationQuery.String)
			if err != nil {
				return err
			}
			executionFields = replicationFields
			executionPlan, err = wrapReplicationCapturePlan(replicationCtx, executionPlan, replicationCapture)
			if err != nil {
				return err
			}
			executionQuery = replicationQuery
			executionFormatCodes = make([]int16, len(replicationFields))
		}
	} else if _, ok := replicationChangeCaptureFromStatement(query.AST); ok {
		advanceLSN = true
	}
	if err = h.executeBoundWithReplication(query, executionQuery, executionPlan, executionFormatCodes, executionFields, replicationCapture, advanceLSN, &rowsAffected, false); err != nil {
		return err
	}

	sessionstate.IncrementPreparedStatementPlanCount(h.mysqlConn.ConnectionID, stmt.Name, len(stmt.Params) == 0)
	return h.send(makeCommandComplete(query.StatementTag, rowsAffected))
}

func (h *ConnectionHandler) createTableAsExecuteSQLStatement(stmt node.CreateTableAsExecuteStatement, query ConvertedQuery) error {
	preparedData, ok := h.preparedStatements[stmt.Execute.Name]
	if !ok {
		return errors.Errorf("prepared statement %s does not exist", stmt.Execute.Name)
	}
	if stmt.Execute.DiscardRows {
		return errors.Errorf("EXECUTE DISCARD ROWS is not supported")
	}
	if len(stmt.Execute.Params) != len(preparedData.BindVarTypes) {
		return errors.Errorf("prepared statement %s expected %d parameters but got %d", stmt.Execute.Name, len(preparedData.BindVarTypes), len(stmt.Execute.Params))
	}

	params := make([][]byte, len(stmt.Execute.Params))
	for i, param := range stmt.Execute.Params {
		value, err := h.evaluateExecuteParameter(param)
		if err != nil {
			return err
		}
		params[i] = value
	}

	sourceQuery := strings.TrimSpace(preparedData.Query.String)
	for strings.HasSuffix(sourceQuery, ";") {
		sourceQuery = strings.TrimSpace(strings.TrimSuffix(sourceQuery, ";"))
	}
	createQueryString := stmt.CreatePrefix + sourceQuery
	if stmt.WithNoData {
		createQueryString += " WITH NO DATA"
	}
	createQueries, err := h.convertQuery(createQueryString)
	if err != nil {
		return err
	}
	if len(createQueries) != 1 || createQueries[0].AST == nil {
		return errors.Errorf("CREATE TABLE AS EXECUTE must expand to a single statement")
	}
	createQuery := createQueries[0]
	boundQuery, fields, err := h.doltgresHandler.ComBind(
		context.Background(),
		h.mysqlConn,
		createQuery.String,
		createQuery.AST,
		BindVariables{
			varTypes:   preparedData.BindVarTypes,
			parameters: params,
		},
		nil,
	)
	if err != nil {
		return err
	}
	boundPlan, ok := boundQuery.(sql.Node)
	if !ok {
		return errors.Errorf("expected a sql.Node, got %T", boundQuery)
	}

	rowsAffected := int32(0)
	if err = h.executeBoundWithReplication(query, createQuery, boundPlan, nil, fields, nil, false, &rowsAffected, false); err != nil {
		return err
	}
	h.invalidatePreparedPlanCacheIfNeeded(createQuery)
	sessionstate.IncrementPreparedStatementPlanCount(h.mysqlConn.ConnectionID, stmt.Execute.Name, len(stmt.Execute.Params) == 0)
	return h.send(makeCommandComplete(query.StatementTag, rowsAffected))
}

func (h *ConnectionHandler) evaluateExecuteParameter(param string) ([]byte, error) {
	queries, err := h.convertQuery("SELECT " + param)
	if err != nil {
		return nil, err
	}
	if len(queries) != 1 || queries[0].AST == nil {
		return nil, errors.Errorf("EXECUTE parameter must be a single expression")
	}

	var value []byte
	seen := false
	callback := func(ctx *sql.Context, res *Result) error {
		for _, row := range res.Rows {
			if len(row.val) != 1 {
				return errors.Errorf("EXECUTE parameter expression returned %d columns", len(row.val))
			}
			if seen {
				return errors.Errorf("EXECUTE parameter expression returned multiple rows")
			}
			if row.val[0] != nil {
				value = append([]byte(nil), row.val[0]...)
			}
			seen = true
		}
		return nil
	}
	if err = h.doltgresHandler.ComQuery(context.Background(), h.mysqlConn, queries[0].String, queries[0].AST, callback); err != nil {
		return nil, err
	}
	if !seen {
		return nil, errors.Errorf("EXECUTE parameter expression returned no rows")
	}
	return value, nil
}

// handleParse handles a parse message, returning any error that occurs
func (h *ConnectionHandler) handleParse(message *pgproto3.Parse) error {
	h.waitForSync = true

	if query, ok, err := h.convertSQLCursorCommand(message.Query); ok || err != nil {
		if err != nil {
			return err
		}
		h.preparedStatements[message.Name] = PreparedStatementData{
			Query:        query,
			ReturnFields: h.cursorReturnFields(query),
		}
		h.sendBuffered(&pgproto3.ParseComplete{})
		return nil
	}

	// TODO: "Named prepared statements must be explicitly closed before they can be redefined by another Parse message, but this is not required for the unnamed statement"
	queries, err := h.convertQuery(message.Query)
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("Error parsing query: %+v\n", err)
		}
		return err
	}
	if len(queries) != 1 {
		return errors.Errorf("cannot insert multiple commands into a prepared statement")
	}
	query := queries[0]

	if query.AST == nil {
		// special case: empty query
		h.preparedStatements[message.Name] = PreparedStatementData{
			Query: query,
		}
		return nil
	}

	if h.queryHandledOutsideEngine(query) {
		h.preparedStatements[message.Name] = PreparedStatementData{
			Query: query,
		}
		h.sendBuffered(&pgproto3.ParseComplete{})
		return nil
	}
	if isTruncateQuery(query) {
		h.preparedStatements[message.Name] = PreparedStatementData{
			Query: query,
		}
		h.sendBuffered(&pgproto3.ParseComplete{})
		return nil
	}

	ctx, err := h.doltgresHandler.sm.NewContextWithQuery(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	parsedQuery, fields, err := h.doltgresHandler.ComPrepareParsed(ctx, h.mysqlConn, query.String, query.AST)
	if err != nil {
		return err
	}

	analyzedPlan, ok := parsedQuery.(sql.Node)
	if !ok {
		return errors.Errorf("expected a sql.Node, got %T", parsedQuery)
	}

	// A valid Parse message must have ParameterObjectIDs if there are any binding variables.
	bindVarTypes := message.ParameterOIDs

	// Clients can specify an OID of zero to indicate that the type should be inferred. If we
	// see any zero OIDs, we fall back to extracting the bind var types from the plan.
	if len(bindVarTypes) == 0 || slices.Contains(bindVarTypes, 0) {
		// NOTE: This is used for Prepared Statement Tests only.
		bindVarTypes, err = extractBindVarTypes(ctx, analyzedPlan)
		if err != nil {
			return err
		}
	}

	preparedData := PreparedStatementData{
		Query:        query,
		ReturnFields: fields,
		BindVarTypes: bindVarTypes,
	}
	if h.preparedPlanCacheable(query, bindVarTypes) {
		h.cachePreparedPlan(ctx, &preparedData, analyzedPlan)
	}
	h.preparedStatements[message.Name] = preparedData
	h.sendBuffered(&pgproto3.ParseComplete{})
	return nil
}

// handleDescribe handles a Describe message, returning any error that occurs
func (h *ConnectionHandler) handleDescribe(message *pgproto3.Describe) error {
	var fields []pgproto3.FieldDescription
	var bindvarTypes []uint32
	var query ConvertedQuery

	h.waitForSync = true
	if message.ObjectType == 'S' {
		preparedStatementData, ok := h.preparedStatements[message.Name]
		if !ok {
			return errors.Errorf("prepared statement %s does not exist", message.Name)
		}

		fields = preparedStatementData.ReturnFields
		bindvarTypes = preparedStatementData.BindVarTypes
		query = preparedStatementData.Query
	} else {
		portalData, ok := h.portals[message.Name]
		if !ok {
			return errors.Errorf("portal %s does not exist", message.Name)
		}

		fields = portalData.Fields
		query = portalData.Query
	}

	return h.sendDescribeResponse(fields, bindvarTypes, query)
}

// handleBind handles a bind message, returning any error that occurs
func (h *ConnectionHandler) handleBind(message *pgproto3.Bind) error {
	h.waitForSync = true

	// TODO: a named portal object lasts till the end of the current transaction, unless explicitly destroyed
	//  we need to destroy the named portal as a side effect of the transaction ending
	logrus.Tracef("binding portal %q to prepared statement %s", message.DestinationPortal, message.PreparedStatement)
	preparedData, ok := h.preparedStatements[message.PreparedStatement]
	if !ok {
		return errors.Errorf("prepared statement %s does not exist", message.PreparedStatement)
	}

	if preparedData.Query.AST == nil {
		// special case: empty query
		h.portals[message.DestinationPortal] = PortalData{
			Query:        preparedData.Query,
			IsEmptyQuery: true,
		}
		h.sendBuffered(&pgproto3.BindComplete{})
		return nil
	}

	if h.queryHandledOutsideEngine(preparedData.Query) {
		h.portals[message.DestinationPortal] = PortalData{
			Query:  preparedData.Query,
			Fields: preparedData.ReturnFields,
		}
		h.sendBuffered(&pgproto3.BindComplete{})
		return nil
	}
	if isTruncateQuery(preparedData.Query) {
		h.portals[message.DestinationPortal] = PortalData{
			Query: preparedData.Query,
		}
		h.sendBuffered(&pgproto3.BindComplete{})
		return nil
	}

	var fields []pgproto3.FieldDescription
	var boundPlan sql.Node
	if len(message.Parameters) == 0 {
		cacheCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, preparedData.Query.String)
		if err != nil {
			return err
		}
		if cachedPlan, ok := h.cachedPreparedPlan(cacheCtx, preparedData); ok {
			boundPlan = cachedPlan
			fields, err = schemaToFieldDescriptionsWithSource(cacheCtx, boundPlan.Schema(cacheCtx), boundPlan, message.ResultFormatCodes)
			if err != nil {
				return err
			}
		}
	}
	if boundPlan == nil {
		analyzedPlan, bindFields, err := h.doltgresHandler.ComBind(
			context.Background(),
			h.mysqlConn,
			preparedData.Query.String,
			preparedData.Query.AST,
			BindVariables{
				varTypes:    preparedData.BindVarTypes,
				formatCodes: message.ParameterFormatCodes,
				parameters:  message.Parameters,
			},
			message.ResultFormatCodes)
		if err != nil {
			return err
		}
		fields = bindFields
		var ok bool
		boundPlan, ok = analyzedPlan.(sql.Node)
		if !ok {
			return errors.Errorf("expected a sql.Node, got %T", analyzedPlan)
		}
		if len(message.Parameters) == 0 {
			cacheCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, preparedData.Query.String)
			if err != nil {
				return err
			}
			h.cachePreparedPlan(cacheCtx, &preparedData, boundPlan)
			h.preparedStatements[message.PreparedStatement] = preparedData
		}
	}
	if err := validatePreparedStatementResultShape(preparedData.Query, preparedData.ReturnFields, fields); err != nil {
		return err
	}

	resultFormatCodes, err := executionFormatCodes(len(fields), message.ResultFormatCodes)
	if err != nil {
		return err
	}
	var replicationCapture *replicationChangeCapture
	var replicationFields []pgproto3.FieldDescription
	var replicationBoundPlan sql.Node
	var replicationFormatCodes []int16
	var replicationQuery ConvertedQuery
	if replsource.HasSlots() {
		var hasReplicationCapture bool
		replicationCapture, replicationQuery, hasReplicationCapture, err = h.prepareReplicationChangeQuery(preparedData.Query)
		if err != nil {
			return err
		}
		if hasReplicationCapture {
			replicationPlan, bindReplicationFields, err := h.doltgresHandler.ComBind(
				context.Background(),
				h.mysqlConn,
				replicationQuery.String,
				replicationQuery.AST,
				BindVariables{
					varTypes:    preparedData.BindVarTypes,
					formatCodes: message.ParameterFormatCodes,
					parameters:  message.Parameters,
				},
				nil)
			if err != nil {
				return err
			}
			replicationFields = bindReplicationFields
			var ok bool
			replicationBoundPlan, ok = replicationPlan.(sql.Node)
			if !ok {
				return errors.Errorf("expected a sql.Node, got %T", replicationPlan)
			}
			replicationCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, replicationQuery.String)
			if err != nil {
				return err
			}
			replicationBoundPlan, err = wrapReplicationCapturePlan(replicationCtx, replicationBoundPlan, replicationCapture)
			if err != nil {
				return err
			}
			if len(resultFormatCodes) > 0 && replicationCapture != nil && replicationCapture.clientReturnsRows && len(fields) > 0 {
				replicationFormatCodes = make([]int16, len(replicationFields))
				copy(replicationFormatCodes, resultFormatCodes)
			}
		} else {
			replicationCapture = nil
		}
	}
	h.portals[message.DestinationPortal] = PortalData{
		Query:                  preparedData.Query,
		Fields:                 fields,
		BoundPlan:              boundPlan,
		FormatCodes:            resultFormatCodes,
		ReplicationQuery:       replicationQuery,
		ReplicationCapture:     replicationCapture,
		ReplicationFields:      replicationFields,
		ReplicationBoundPlan:   replicationBoundPlan,
		ReplicationFormatCodes: replicationFormatCodes,
	}
	h.sendBuffered(&pgproto3.BindComplete{})
	return nil
}

// handleExecute handles an execute message, returning any error that occurs
func (h *ConnectionHandler) handleExecute(message *pgproto3.Execute) error {
	h.waitForSync = true
	defer h.releaseXactAdvisoryLocksIfOutsideTransaction()

	// TODO: implement the RowMax
	portalData, ok := h.portals[message.Portal]
	if !ok {
		return errors.Errorf("portal %s does not exist", message.Portal)
	}

	logrus.Tracef("executing portal %s with contents %v", message.Portal, portalData)
	query := portalData.Query

	if portalData.IsEmptyQuery {
		h.sendBuffered(&pgproto3.EmptyQueryResponse{})
		return nil
	}

	// Certain statement types get handled directly by the handler instead of being passed to the engine
	if err := h.rejectLockTableOutsideTransaction(query); err != nil {
		return err
	}
	handled, _, err := h.handleQueryOutsideEngine(query)
	if handled {
		return err
	}

	if err = h.rejectConcurrentIndexInTransaction(query); err != nil {
		return err
	}
	if err = h.rejectReadOnlyTransactionWrite(query); err != nil {
		return err
	}
	if err = h.validateReplicationChangeReplicaIdentity(query); err != nil {
		return err
	}

	if isTruncateQuery(query) {
		return h.query(query)
	}

	// |rowsAffected| gets altered by the callback below
	rowsAffected := int32(0)

	executionQuery := query
	executionPlan := portalData.BoundPlan
	executionFormatCodes := portalData.FormatCodes
	executionFields := portalData.Fields
	advanceLSN := false
	replicationCapture := portalData.ReplicationCapture
	if replicationCapture != nil && replsource.HasSlots() {
		executionQuery = portalData.ReplicationQuery
		executionPlan = portalData.ReplicationBoundPlan
		executionFormatCodes = portalData.ReplicationFormatCodes
		executionFields = portalData.ReplicationFields
	} else {
		replicationCapture = nil
		if _, ok := replicationChangeCaptureFromStatement(query.AST); ok {
			advanceLSN = true
		}
	}
	if err = h.executeBoundWithReplication(query, executionQuery, executionPlan, executionFormatCodes, executionFields, replicationCapture, advanceLSN, &rowsAffected, true); err != nil {
		if isReadOnlyTransactionError(err) {
			return readOnlyTransactionError()
		}
		return err
	}
	if err = h.applyXactVarSavepointHook(query); err != nil {
		return err
	}
	h.markTransactionStatement(query)

	// Invalidate cached prepared plans for schema-mutating queries that
	// reach the engine through the extended Parse/Bind/Execute path. The
	// simple-query handler invalidates here on its own, but without this
	// call an ALTER TABLE / DROP / Insert run via Execute leaves stale
	// plans for subsequent SELECTs against the same table.
	h.invalidatePreparedPlanCacheIfNeeded(query)
	h.sendBuffered(makeCommandComplete(query.StatementTag, rowsAffected))
	return h.finishNotifications(query)
}

func (h *ConnectionHandler) applyXactVarSavepointHook(query ConvertedQuery) error {
	var apply func(*sql.Context) error
	switch stmt := query.AST.(type) {
	case *sqlparser.Savepoint:
		name := stmt.Identifier
		h.pushReplicationSavepoint(name)
		apply = func(ctx *sql.Context) error {
			largeobject.PushSavepoint(uint32(ctx.Session.ID()), name)
			return functions.PushSessionXactVarSavepoint(ctx, name)
		}
	case *sqlparser.RollbackSavepoint:
		name := stmt.Identifier
		h.rollbackReplicationToSavepoint(name)
		apply = func(ctx *sql.Context) error {
			if err := largeobject.RollbackToSavepoint(uint32(ctx.Session.ID()), name); err != nil {
				return err
			}
			if err := functions.RollbackSessionXactVarsToSavepoint(ctx, name); err != nil {
				return err
			}
			core.ClearContextValues(ctx)
			return nil
		}
	case *sqlparser.ReleaseSavepoint:
		name := stmt.Identifier
		h.releaseReplicationSavepoint(name)
		apply = func(ctx *sql.Context) error {
			largeobject.ReleaseSavepoint(uint32(ctx.Session.ID()), name)
			functions.ReleaseSessionXactVarSavepoint(ctx, name)
			return nil
		}
	default:
		return nil
	}
	ctx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	return apply(ctx)
}

func (h *ConnectionHandler) executeBoundWithReplication(clientQuery ConvertedQuery, executionQuery ConvertedQuery, boundPlan sql.Node, formatCodes []int16, resultFields []pgproto3.FieldDescription, capture *replicationChangeCapture, advanceLSN bool, rowsAffected *int32, isExecute bool) error {
	suppressRows := capture != nil && !capture.clientReturnsRows
	var captureCtx *sql.Context
	callback := h.spoolRowsCallbackWithRowSuppression(clientQuery, rowsAffected, isExecute, suppressRows)
	if capture != nil {
		clientCallback := callback
		callback = func(ctx *sql.Context, res *Result) error {
			captureCtx = ctx
			clientResult, err := capture.appendResultAndTrimClient(ctx, res)
			if err != nil {
				return err
			}
			return clientCallback(ctx, clientResult)
		}
	}
	if err := h.doltgresHandler.ComExecuteBoundWithFields(context.Background(), h.mysqlConn, executionQuery.String, boundPlan, formatCodes, resultFields, callback); err != nil {
		return err
	}
	if capture != nil {
		return h.publishOrBufferReplicationCapture(captureCtx, capture)
	}
	if advanceLSN && *rowsAffected > 0 {
		h.advanceOrBufferReplicationLSN()
	}
	return nil
}

func (h *ConnectionHandler) publishOrBufferReplicationCapture(ctx *sql.Context, capture *replicationChangeCapture) error {
	if capture == nil {
		return nil
	}
	if h.inTransaction {
		h.pendingReplicationCaptures = append(h.pendingReplicationCaptures, capture)
		return nil
	}
	return capture.publish(ctx)
}

func (h *ConnectionHandler) advanceOrBufferReplicationLSN() {
	if h.inTransaction {
		h.pendingReplicationAdvance = true
		return
	}
	replsource.AdvanceLSN()
}

func (h *ConnectionHandler) flushPendingReplication(ctx *sql.Context) error {
	if len(h.pendingReplicationCaptures) > 0 {
		if err := publishReplicationCaptures(ctx, h.pendingReplicationCaptures); err != nil {
			return err
		}
	}
	if len(h.pendingReplicationCaptures) == 0 && h.pendingReplicationAdvance {
		replsource.AdvanceLSN()
	}
	h.clearPendingReplication()
	return nil
}

func (h *ConnectionHandler) clearPendingReplication() {
	h.pendingReplicationCaptures = nil
	h.pendingReplicationAdvance = false
	h.pendingReplicationSavepoints = nil
}

func (h *ConnectionHandler) pushReplicationSavepoint(name string) {
	if !h.inTransaction {
		return
	}
	h.pendingReplicationSavepoints = append(h.pendingReplicationSavepoints, replicationSavepointState{
		name:       strings.ToLower(name),
		captures:   len(h.pendingReplicationCaptures),
		advanceLSN: h.pendingReplicationAdvance,
	})
}

func (h *ConnectionHandler) rollbackReplicationToSavepoint(name string) {
	idx := h.replicationSavepointIndex(name)
	if idx < 0 {
		return
	}
	savepoint := h.pendingReplicationSavepoints[idx]
	h.pendingReplicationCaptures = h.pendingReplicationCaptures[:savepoint.captures]
	h.pendingReplicationAdvance = savepoint.advanceLSN
	h.pendingReplicationSavepoints = h.pendingReplicationSavepoints[:idx+1]
}

func (h *ConnectionHandler) releaseReplicationSavepoint(name string) {
	idx := h.replicationSavepointIndex(name)
	if idx < 0 {
		return
	}
	h.pendingReplicationSavepoints = h.pendingReplicationSavepoints[:idx]
}

func (h *ConnectionHandler) replicationSavepointIndex(name string) int {
	name = strings.ToLower(name)
	for i := len(h.pendingReplicationSavepoints) - 1; i >= 0; i-- {
		if h.pendingReplicationSavepoints[i].name == name {
			return i
		}
	}
	return -1
}

func makeCommandComplete(tag string, rows int32) *pgproto3.CommandComplete {
	switch tag {
	case "INSERT", "DELETE", "UPDATE", "MERGE", "SELECT", "CREATE TABLE AS", "MOVE", "FETCH", "COPY":
		if tag == "INSERT" {
			tag = "INSERT 0"
		}
		tag = fmt.Sprintf("%s %d", tag, rows)
	}

	return &pgproto3.CommandComplete{
		CommandTag: []byte(tag),
	}
}

// handleCopyData handles the COPY DATA message, by loading the data sent from the client. The |stop| response parameter
// is true if the connection handler should shut down the connection, |endOfMessages| is true if no more COPY DATA
// messages are expected, and the server should tell the client that it is ready for the next query, and |err| contains
// any error that occurred while processing the COPY DATA message.
func (h *ConnectionHandler) handleCopyData(message *pgproto3.CopyData) (stop bool, endOfMessages bool, err error) {
	if h.copyFromStdinState == nil && h.copyFromStdinFailed {
		return false, false, nil
	}
	copyFromData := bytes.NewReader(message.Data)
	stop, endOfMessages, err = h.handleCopyDataHelper(h.copyFromStdinState, copyFromData)
	if err != nil && h.copyFromStdinState != nil {
		h.copyFromStdinState.copyErr = err
		h.copyFromStdinState = nil
		h.copyFromStdinFailed = true
	}
	return stop, endOfMessages, err
}

// copyFromFileQuery handles a COPY FROM message that is reading from a file, returning any error that occurs
func (h *ConnectionHandler) copyFromFileQuery(stmt *node.CopyFrom) error {
	copyState := &copyFromStdinState{
		copyFromStdinNode: stmt,
	}

	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "COPY FROM")
	if err != nil {
		return err
	}
	if stmt.Program != "" {
		if err = requireCopyServerProgramPrivilege(sqlCtx); err != nil {
			return err
		}
		return errors.Errorf("COPY FROM PROGRAM is not supported")
	}

	if !filepath.IsAbs(stmt.File) {
		return errors.Errorf("relative path not allowed for COPY FROM")
	}
	if err = requireCopyFromServerFilePrivilege(sqlCtx); err != nil {
		return err
	}

	f, err := os.Open(stmt.File)
	if err != nil {
		return err
	}
	defer f.Close()

	_, _, err = h.handleCopyDataHelper(copyState, f)
	if err != nil {
		return err
	}

	sqlCtx, err = h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return err
	}

	loadDataResults, err := copyState.dataLoader.Finish(sqlCtx)
	if err != nil {
		return h.abortCopyStatement(sqlCtx, copyState, err)
	}

	if err = h.releaseCopyStatementSavepoint(sqlCtx, copyState); err != nil {
		return err
	}

	if err = h.commitCopyTransactionIfAutocommit(sqlCtx); err != nil {
		return err
	}
	if copyState.replicationCapture != nil {
		if err = h.publishOrBufferReplicationCapture(sqlCtx, copyState.replicationCapture); err != nil {
			return err
		}
	}

	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("COPY %d", loadDataResults.RowsLoaded)),
	})
}

// handleCopyDataHelper is a helper function that should only be invoked by handleCopyData. handleCopyData wraps this
// function so that it can capture any returned error message and store it in the saved state.
func (h *ConnectionHandler) handleCopyDataHelper(copyState *copyFromStdinState, copyFromData io.Reader) (stop bool, endOfMessages bool, err error) {
	if copyState == nil {
		return false, true, errors.Errorf("COPY DATA message received without a COPY FROM STDIN operation in progress")
	}

	// Grab a sql.Context and ensure the session has a transaction started, otherwise the copied data
	// won't get committed correctly.
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "COPY FROM STDIN")
	if err != nil {
		return false, false, err
	}
	if err = startTransactionIfNecessary(sqlCtx); err != nil {
		return false, false, err
	}
	if err = h.ensureCopyStatementSavepoint(sqlCtx, copyState); err != nil {
		return false, false, err
	}

	dataLoader := copyState.dataLoader
	if dataLoader == nil {
		if err = h.initializeCopyFromState(sqlCtx, copyState); err != nil {
			return false, false, h.abortCopyStatement(sqlCtx, copyState, err)
		}
		dataLoader = copyState.dataLoader
	}

	reader := bufio.NewReader(copyFromData)
	if err = dataLoader.SetNextDataChunk(sqlCtx, reader); err != nil {
		return false, false, h.abortCopyStatement(sqlCtx, copyState, err)
	}

	callback := func(ctx *sql.Context, res *Result) error {
		if copyState.replicationCapture == nil {
			return nil
		}
		_, err := copyState.replicationCapture.appendResultAndTrimClient(ctx, res)
		return err
	}
	err = h.doltgresHandler.ComExecuteBound(sqlCtx, h.mysqlConn, "COPY FROM", copyState.insertNode, nil, callback)
	if err != nil {
		return false, false, h.abortCopyStatement(sqlCtx, copyState, err)
	}

	// We expect to see more CopyData messages until we see either a CopyDone or CopyFail message, so
	// return false for endOfMessages
	return false, false, nil
}

func requireCopyFromServerFilePrivilege(ctx *sql.Context) error {
	return requireCopyServerRolePrivilege(ctx, "pg_read_server_files", "COPY from a file")
}

func requireCopyToServerFilePrivilege(ctx *sql.Context) error {
	return requireCopyServerRolePrivilege(ctx, "pg_write_server_files", "COPY to a file")
}

func requireCopyServerProgramPrivilege(ctx *sql.Context) error {
	return requireCopyServerRolePrivilege(ctx, "pg_execute_server_program", "COPY to or from a program")
}

func requireCopyServerRolePrivilege(ctx *sql.Context, roleName string, action string) error {
	var allowed bool
	auth.LockRead(func() {
		role := auth.GetRole(ctx.Client().User)
		allowed = role.IsValid() && (role.IsSuperUser || auth.HasInheritedRole(role.ID(), roleName))
	})
	if !allowed {
		return errors.Errorf("permission denied to %s: must be superuser or have privileges of the %s role", action, roleName)
	}
	return nil
}

func (h *ConnectionHandler) initializeCopyFromState(sqlCtx *sql.Context, copyState *copyFromStdinState) error {
	if copyState == nil {
		return errors.Errorf("COPY DATA message received without a COPY FROM operation in progress")
	}
	if copyState.dataLoader != nil {
		return nil
	}

	copyFromStdinNode := copyState.copyFromStdinNode
	if copyFromStdinNode == nil {
		return errors.Errorf("no COPY FROM STDIN node found")
	}
	authHandler := auth.AuthorizationHandler{}
	if err := authHandler.HandleAuth(sqlCtx, nil, sqlparser.AuthInformation{
		AuthType:    auth.AuthType_INSERT,
		TargetType:  auth.AuthTargetType_TableIdentifiers,
		TargetNames: []string{copyFromStdinNode.DatabaseName, copyFromStdinNode.TableName.Schema, copyFromStdinNode.TableName.Name},
	}); err != nil {
		return err
	}

	// we build an insert node to use for the full insert plan, for which the copy from node will be the row source
	builder := planbuilder.New(sqlCtx, h.doltgresHandler.e.Analyzer.Catalog, nil)
	planNode, flags, err := builder.BindOnly(copyFromStdinNode.InsertStub, "", nil)
	if err != nil {
		return err
	}

	insertNode, ok := planNode.(*plan.InsertInto)
	if !ok {
		return errors.Errorf("expected plan.InsertInto, got %T", planNode)
	}

	// now that we have our insert node, we can build the data loader
	tbl := getInsertableTable(insertNode.Destination)
	if tbl == nil {
		// this should be impossible, enforced by analyzer above
		return errors.Errorf("no insertable table found in %v", insertNode.Destination)
	}
	if capture, ok := replicationChangeCaptureFromStatement(copyFromStdinNode.InsertStub); ok {
		fullRowColumns := make([]string, len(tbl.Schema(sqlCtx)))
		for i, column := range tbl.Schema(sqlCtx) {
			fullRowColumns[i] = column.Name
		}
		ensureReturningFullRow(copyFromStdinNode.InsertStub, fullRowColumns)
		capture.fullRowFieldCount = len(fullRowColumns)
		copyState.replicationCapture = capture

		planNode, flags, err = builder.BindOnly(copyFromStdinNode.InsertStub, "", nil)
		if err != nil {
			return err
		}
		insertNode, ok = planNode.(*plan.InsertInto)
		if !ok {
			return errors.Errorf("expected plan.InsertInto, got %T", planNode)
		}
		tbl = getInsertableTable(insertNode.Destination)
		if tbl == nil {
			return errors.Errorf("no insertable table found in %v", insertNode.Destination)
		}
	}
	if err = validateCopyFromGeneratedColumns(copyFromStdinNode.Columns, tbl.Schema(sqlCtx), copyFromStdinNode.TableName.Name); err != nil {
		return err
	}

	var dataLoader dataloader.DataLoader
	errorPolicy := dataloader.LoadErrorPolicy{
		Ignore:         copyFromStdinNode.CopyOptions.OnError == tree.CopyOnErrorIgnore,
		RejectLimit:    copyFromStdinNode.CopyOptions.RejectLimit,
		RejectLimitSet: copyFromStdinNode.CopyOptions.RejectLimitSet,
	}
	switch copyFromStdinNode.CopyOptions.CopyFormat {
	case tree.CopyFormatText:
		dataLoader, err = dataloader.NewTabularDataLoader(insertNode.ColumnNames, tbl.Schema(sqlCtx), copyFromStdinNode.CopyOptions.Delimiter, "", copyFromStdinNode.CopyOptions.Header, copyFromStdinNode.CopyOptions.Default, copyFromStdinNode.CopyOptions.DefaultSet, errorPolicy)
	case tree.CopyFormatCsv:
		dataLoader, err = dataloader.NewCsvDataLoader(insertNode.ColumnNames, tbl.Schema(sqlCtx), copyFromStdinNode.CopyOptions.Delimiter, copyFromStdinNode.CopyOptions.Header, copyFromStdinNode.CopyOptions.Default, copyFromStdinNode.CopyOptions.DefaultSet, errorPolicy)
	case tree.CopyFormatBinary:
		dataLoader, err = dataloader.NewBinaryDataLoader(insertNode.ColumnNames, tbl.Schema(sqlCtx))
	default:
		err = errors.Errorf("unknown format specified for COPY FROM: %v",
			copyFromStdinNode.CopyOptions.CopyFormat)
	}

	if err != nil {
		return err
	}

	// we have to set the data loader on the copyFrom node before we analyze it, because we need the loader's
	// schema to analyze
	copyState.copyFromStdinNode.DataLoader = dataLoader

	// After building out stub insert node, swap out the source node with the COPY node, then analyze the entire thing
	planNode = insertNode.WithSource(copyFromStdinNode)
	analyzedNode, err := h.doltgresHandler.e.Analyzer.Analyze(sqlCtx, planNode, nil, flags)
	if err != nil {
		return err
	}
	if copyFromStdinNode.CopyOptions.DefaultSet {
		analyzedInsert := getInsertInto(analyzedNode)
		if analyzedInsert == nil {
			return errors.Errorf("no analyzed INSERT node found for COPY FROM STDIN")
		}
		defaultLoader, ok := dataLoader.(dataloader.DefaultSchemaLoader)
		if !ok {
			return errors.Errorf("COPY DEFAULT is unsupported for %s", dataLoader.String())
		}
		if err = defaultLoader.SetSchemaForDefaults(insertNode.ColumnNames, analyzedInsert.Destination.Schema(sqlCtx)); err != nil {
			return err
		}
	}

	copyState.insertNode = analyzedNode
	copyState.dataLoader = dataLoader
	return nil
}

func getInsertInto(node sql.Node) *plan.InsertInto {
	var insert *plan.InsertInto
	transform.Inspect(node, func(node sql.Node) bool {
		if n, ok := node.(*plan.InsertInto); ok {
			insert = n
			return false
		}
		return true
	})
	return insert
}

// Returns the first sql.InsertableTable node found in the tree provided, or nil if none is found.
func getInsertableTable(node sql.Node) sql.InsertableTable {
	var tbl sql.InsertableTable
	transform.Inspect(node, func(node sql.Node) bool {
		if rt, ok := node.(*plan.ResolvedTable); ok {
			if insertable, ok := rt.Table.(sql.InsertableTable); ok {
				tbl = insertable
				return false
			}
		}
		return true
	})

	return tbl
}

func validateCopyFromGeneratedColumns(columns tree.NameList, schema sql.Schema, tableName string) error {
	for _, column := range columns {
		idx := schema.IndexOfColName(core.EncodePhysicalColumnName(string(column)))
		if idx < 0 {
			continue
		}
		col := schema[idx]
		if col.Generated != nil && !col.AutoIncrement {
			return errors.Errorf(`The value specified for generated column "%s" in table "%s" is not allowed.`, core.DecodePhysicalColumnName(col.Name), tableName)
		}
	}
	return nil
}

// handleCopyDone handles a COPY DONE message by finalizing the in-progress COPY DATA operation and committing the
// loaded table data. The |stop| response parameter is true if the connection handler should shut down the connection,
// |endOfMessages| is true if no more COPY DATA messages are expected, and the server should tell the client that it is
// ready for the next query, and |err| contains any error that occurred while processing the COPY DATA message.
func (h *ConnectionHandler) handleCopyDone(_ *pgproto3.CopyDone) (stop bool, endOfMessages bool, err error) {
	if h.copyFromStdinState == nil {
		if h.copyFromStdinFailed {
			h.copyFromStdinFailed = false
			return false, false, nil
		}
		return false, true,
			errors.Errorf("COPY DONE message received without a COPY FROM STDIN operation in progress")
	}

	// If there was a previous error returned from processing a CopyData message, then don't return an error here
	// and don't send endOfMessage=true, since the CopyData error already sent endOfMessage=true. If we do send
	// endOfMessage=true here, then the client gets confused about the unexpected/extra Idle message since the
	// server has already reported it was idle in the last message after the returned error.
	if h.copyFromStdinState.copyErr != nil {
		h.copyFromStdinState = nil
		h.copyFromStdinFailed = false
		return false, false, nil
	}

	dataLoader := h.copyFromStdinState.dataLoader
	if dataLoader == nil {
		return false, true,
			errors.Errorf("no data loader found for COPY FROM STDIN operation")
	}

	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return false, false, err
	}

	loadDataResults, err := dataLoader.Finish(sqlCtx)
	if err != nil {
		return false, false, h.abortCopyStatement(sqlCtx, h.copyFromStdinState, err)
	}

	if err = h.releaseCopyStatementSavepoint(sqlCtx, h.copyFromStdinState); err != nil {
		return false, false, err
	}

	if err = h.commitCopyTransactionIfAutocommit(sqlCtx); err != nil {
		return false, false, err
	}
	if h.copyFromStdinState.replicationCapture != nil {
		if err = h.publishOrBufferReplicationCapture(sqlCtx, h.copyFromStdinState.replicationCapture); err != nil {
			return false, false, err
		}
	}

	h.copyFromStdinState = nil
	// We send back endOfMessage=true, since the COPY DONE message ends the COPY DATA flow and the server is ready
	// to accept the next query now.
	return false, true, h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("COPY %d", loadDataResults.RowsLoaded)),
	})
}

func (h *ConnectionHandler) commitCopyTransactionIfAutocommit(sqlCtx *sql.Context) error {
	if h.inTransaction || sqlCtx.GetTransaction() == nil {
		return nil
	}
	txSession, ok := sqlCtx.Session.(sql.TransactionSession)
	if !ok {
		return errors.Errorf("session does not implement sql.TransactionSession")
	}
	if err := txSession.CommitTransaction(sqlCtx, txSession.GetTransaction()); err != nil {
		return err
	}
	sqlCtx.SetIgnoreAutoCommit(false)
	return nil
}

func (h *ConnectionHandler) ensureCopyStatementSavepoint(sqlCtx *sql.Context, copyState *copyFromStdinState) error {
	if copyState.statementSavepoint != "" {
		return nil
	}
	tx := sqlCtx.GetTransaction()
	if tx == nil {
		return errors.Errorf("COPY FROM requires an active transaction")
	}
	txSession, ok := sqlCtx.Session.(sql.TransactionSession)
	if !ok {
		return errors.Errorf("session does not implement sql.TransactionSession")
	}
	copyState.statementSavepoint = fmt.Sprintf("__doltgresql_copy_%d_%d", h.mysqlConn.ConnectionID, h.cancelSecretKey)
	return txSession.CreateSavepoint(sqlCtx, tx, copyState.statementSavepoint)
}

func (h *ConnectionHandler) releaseCopyStatementSavepoint(sqlCtx *sql.Context, copyState *copyFromStdinState) error {
	if copyState == nil || copyState.statementSavepoint == "" || sqlCtx.GetTransaction() == nil {
		return nil
	}
	txSession, ok := sqlCtx.Session.(sql.TransactionSession)
	if !ok {
		return errors.Errorf("session does not implement sql.TransactionSession")
	}
	savepoint := copyState.statementSavepoint
	copyState.statementSavepoint = ""
	return txSession.ReleaseSavepoint(sqlCtx, sqlCtx.GetTransaction(), savepoint)
}

func (h *ConnectionHandler) abortCopyStatement(sqlCtx *sql.Context, copyState *copyFromStdinState, cause error) error {
	if copyState == nil || copyState.statementSavepoint == "" || sqlCtx.GetTransaction() == nil {
		return cause
	}
	txSession, ok := sqlCtx.Session.(sql.TransactionSession)
	if !ok {
		return errors.Errorf("%w; COPY rollback failed: session does not implement sql.TransactionSession", cause)
	}
	savepoint := copyState.statementSavepoint
	copyState.statementSavepoint = ""
	if err := txSession.RollbackToSavepoint(sqlCtx, sqlCtx.GetTransaction(), savepoint); err != nil {
		return fmt.Errorf("%w; COPY rollback failed: %v", cause, err)
	}
	if err := txSession.ReleaseSavepoint(sqlCtx, sqlCtx.GetTransaction(), savepoint); err != nil {
		return fmt.Errorf("%w; COPY savepoint release failed: %v", cause, err)
	}
	if h.inTransaction {
		return cause
	}
	if err := txSession.Rollback(sqlCtx, sqlCtx.GetTransaction()); err != nil {
		return fmt.Errorf("%w; COPY transaction rollback failed: %v", cause, err)
	}
	sqlCtx.SetIgnoreAutoCommit(false)
	sqlCtx.SetTransaction(nil)
	return cause
}

// handleCopyFail handles a COPY FAIL message by aborting the in-progress COPY DATA operation.  The |stop| response
// parameter is true if the connection handler should shut down the connection, |endOfMessages| is true if no more
// COPY DATA messages are expected, and the server should tell the client that it is ready for the next query, and
// |err| contains any error that occurred while processing the COPY DATA message.
func (h *ConnectionHandler) handleCopyFail(_ *pgproto3.CopyFail) (stop bool, endOfMessages bool, err error) {
	if h.copyFromStdinState == nil {
		if h.copyFromStdinFailed {
			h.copyFromStdinFailed = false
			return false, false, nil
		}
		return false, true,
			errors.Errorf("COPY FAIL message received without a COPY FROM STDIN operation in progress")
	}

	dataLoader := h.copyFromStdinState.dataLoader
	if dataLoader == nil {
		return false, true,
			errors.Errorf("no data loader found for COPY FROM STDIN operation")
	}

	h.copyFromStdinState = nil
	h.copyFromStdinFailed = false
	// We send back endOfMessage=true, since the COPY FAIL message ends the COPY DATA flow and the server is ready
	// to accept the next query now.
	return false, true, nil
}

func (h *ConnectionHandler) handleCopyToStdoutQuery(copyTo *node.CopyTo) error {
	if copyTo == nil {
		return nil
	}
	if !copyTo.Stdout {
		sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "COPY TO")
		if err != nil {
			return err
		}
		if copyTo.Program != "" {
			if err = requireCopyServerProgramPrivilege(sqlCtx); err != nil {
				return err
			}
			return errors.Errorf("COPY TO PROGRAM is not supported")
		}
		if err = requireCopyToServerFilePrivilege(sqlCtx); err != nil {
			return err
		}
		if !filepath.IsAbs(copyTo.File) {
			return errors.Errorf("relative path not allowed for COPY TO")
		}
		return errors.Errorf("COPY TO server files is not supported")
	}
	if copyTo.CopyOptions.CopyFormat == tree.CopyFormatBinary && copyTo.CopyOptions.Header {
		return errors.Errorf("COPY TO cannot use HEADER with BINARY format")
	}

	selectQuery := copyToSelectQuery(copyTo)
	queries, err := h.convertQuery(selectQuery)
	if err != nil {
		return err
	}
	if len(queries) != 1 {
		return errors.Errorf("COPY TO generated multiple queries")
	}
	query := queries[0]

	parsedQuery, fields, err := h.doltgresHandler.ComPrepareParsed(context.Background(), h.mysqlConn, query.String, query.AST)
	if err != nil {
		return err
	}
	boundPlan, ok := parsedQuery.(sql.Node)
	if !ok {
		return errors.Errorf("expected a sql.Node, got %T", parsedQuery)
	}

	formatCodes := make([]int16, len(fields))
	overallFormat := byte(0)
	if copyTo.CopyOptions.CopyFormat == tree.CopyFormatBinary {
		overallFormat = 1
		for i := range formatCodes {
			formatCodes[i] = 1
		}
	}
	columnFormatCodes := make([]uint16, len(fields))
	if overallFormat == 1 {
		for i := range columnFormatCodes {
			columnFormatCodes[i] = 1
		}
	}

	if err = h.send(&pgproto3.CopyOutResponse{
		OverallFormat:     overallFormat,
		ColumnFormatCodes: columnFormatCodes,
	}); err != nil {
		return err
	}

	rowsCopied := int32(0)
	if overallFormat == 1 {
		if err = h.send(&pgproto3.CopyData{Data: binaryCopyHeader()}); err != nil {
			return err
		}
	} else if copyTo.CopyOptions.Header {
		header := make([][]byte, len(fields))
		for i := range fields {
			header[i] = fields[i].Name
		}
		data, err := encodeCopyTextLikeRow(header, copyTo.CopyOptions, false)
		if err != nil {
			return err
		}
		if err = h.send(&pgproto3.CopyData{Data: data}); err != nil {
			return err
		}
	}

	callback := func(ctx *sql.Context, res *Result) error {
		for _, row := range res.Rows {
			var data []byte
			var err error
			if overallFormat == 1 {
				data = encodeCopyBinaryRow(row.val)
			} else {
				data, err = encodeCopyTextLikeRow(row.val, copyTo.CopyOptions, true)
				if err != nil {
					return err
				}
			}
			if err = h.send(&pgproto3.CopyData{Data: data}); err != nil {
				return err
			}
			rowsCopied++
		}
		return nil
	}

	if err = h.doltgresHandler.ComExecuteBound(context.Background(), h.mysqlConn, query.String, boundPlan, formatCodes, callback); err != nil {
		return err
	}

	if overallFormat == 1 {
		if err = h.send(&pgproto3.CopyData{Data: binaryCopyTrailer()}); err != nil {
			return err
		}
	}
	if err = h.send(&pgproto3.CopyDone{}); err != nil {
		return err
	}
	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("COPY %d", rowsCopied)),
	})
}

func copyToSelectQuery(copyTo *node.CopyTo) string {
	if copyTo.Query != "" {
		return copyTo.Query
	}

	var columns string
	if len(copyTo.Columns) == 0 {
		columns = "*"
	} else {
		quotedColumns := make([]string, len(copyTo.Columns))
		for i := range copyTo.Columns {
			quotedColumns[i] = quoteCopyIdentifier(string(copyTo.Columns[i]))
		}
		columns = strings.Join(quotedColumns, ", ")
	}

	tableName := quoteCopyIdentifier(copyTo.TableName.Name)
	if copyTo.TableName.Schema != "" {
		tableName = quoteCopyIdentifier(copyTo.TableName.Schema) + "." + tableName
	}
	return fmt.Sprintf("SELECT %s FROM %s", columns, tableName)
}

func quoteCopyIdentifier(identifier string) string {
	return quoteSQLIdentifier(identifier)
}

func quoteQualifiedIdentifier(schema string, table string) string {
	tableName := quoteSQLIdentifier(table)
	if schema != "" {
		tableName = quoteSQLIdentifier(schema) + "." + tableName
	}
	return tableName
}

func quoteSQLIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func quoteSQLString(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}

func (h *ConnectionHandler) rewriteSimpleUpdatableViewDML(query string) (string, bool, error) {
	if matches := updatableViewInsertPattern.FindStringSubmatch(query); matches != nil {
		baseTable, ok, err := h.simpleUpdatableViewBaseTable(query, matches[1])
		if err != nil || !ok {
			return query, false, err
		}
		return "INSERT INTO " + baseTable + matches[2], true, nil
	}
	if matches := updatableViewUpdatePattern.FindStringSubmatch(query); matches != nil {
		baseTable, ok, err := h.simpleUpdatableViewBaseTable(query, matches[1])
		if err != nil || !ok {
			return query, false, err
		}
		return "UPDATE " + baseTable + matches[2], true, nil
	}
	if matches := updatableViewDeletePattern.FindStringSubmatch(query); matches != nil {
		baseTable, ok, err := h.simpleUpdatableViewBaseTable(query, matches[1])
		if err != nil || !ok {
			return query, false, err
		}
		return "DELETE FROM " + baseTable + matches[2], true, nil
	}
	return query, false, nil
}

func (h *ConnectionHandler) simpleUpdatableViewBaseTable(query string, rawView string) (string, bool, error) {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query)
	if err != nil {
		return "", false, err
	}
	view, viewSchema, ok, err := viewDefinitionForDMLTarget(sqlCtx, rawView)
	if err != nil || !ok {
		return "", false, err
	}
	return simpleViewBaseTable(sqlCtx, view, viewSchema)
}

func viewDefinitionForDMLTarget(ctx *sql.Context, rawView string) (sql.ViewDefinition, string, bool, error) {
	db, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil || db == nil {
		return sql.ViewDefinition{}, "", false, err
	}
	rawSchema, viewName := splitQualifiedCatalogName(rawView)
	schemaName, err := core.GetSchemaName(ctx, db, rawSchema)
	if err != nil {
		return sql.ViewDefinition{}, "", false, err
	}
	viewSource := db
	if schemaDB, ok := db.(sql.SchemaDatabase); ok && schemaDB.SupportsDatabaseSchemas() {
		schema, exists, err := schemaDB.GetSchema(ctx, schemaName)
		if err != nil || !exists {
			return sql.ViewDefinition{}, "", false, err
		}
		viewSource = schema
	}
	viewDB, ok := viewSource.(sql.ViewDatabase)
	if !ok {
		return sql.ViewDefinition{}, "", false, nil
	}
	view, exists, err := viewDB.GetViewDefinition(ctx, viewName)
	if err != nil || !exists {
		return sql.ViewDefinition{}, "", false, err
	}
	return view, schemaName, true, nil
}

func simpleViewBaseTable(ctx *sql.Context, view sql.ViewDefinition, viewSchema string) (string, bool, error) {
	createViewStatement := strings.TrimSpace(view.CreateViewStatement)
	if createViewStatement == "" {
		createViewStatement = "CREATE VIEW " + quoteQualifiedIdentifier(viewSchema, view.Name) + " AS " + view.TextDefinition
	}
	stmts, err := parser.Parse(createViewStatement)
	if err != nil || len(stmts) == 0 {
		return "", false, err
	}
	createView, ok := stmts[0].AST.(*tree.CreateView)
	if !ok {
		return "", false, nil
	}
	baseTable, ok := simpleViewBaseTableName(createView)
	if !ok {
		return "", false, nil
	}
	baseSchema := viewSchema
	if baseTable.ExplicitSchema {
		baseSchema = string(baseTable.SchemaName)
	}
	relation := doltdb.TableName{Name: string(baseTable.ObjectName), Schema: baseSchema}
	sqlTable, err := core.GetSqlTableFromContext(ctx, "", relation)
	if err != nil || sqlTable == nil {
		return "", false, err
	}
	if !simpleViewProjectionMatchesTable(createView, sqlTable.Schema(ctx)) {
		return "", false, nil
	}
	return quoteQualifiedIdentifier(baseSchema, relation.Name), true, nil
}

func simpleViewBaseTableName(createView *tree.CreateView) (tree.TableName, bool) {
	if createView.AsSource == nil ||
		createView.AsSource.With != nil ||
		createView.AsSource.Limit != nil ||
		len(createView.AsSource.OrderBy) > 0 ||
		len(createView.AsSource.Locking) > 0 {
		return tree.TableName{}, false
	}
	selectClause, ok := createView.AsSource.Select.(*tree.SelectClause)
	if !ok {
		return tree.TableName{}, false
	}
	if selectClause.Distinct ||
		len(selectClause.DistinctOn) > 0 ||
		len(selectClause.GroupBy) > 0 ||
		selectClause.Having != nil ||
		len(selectClause.Window) > 0 ||
		selectClause.Where != nil ||
		len(selectClause.From.Tables) != 1 {
		return tree.TableName{}, false
	}
	tableExpr := tree.StripTableParens(selectClause.From.Tables[0])
	if aliased, ok := tableExpr.(*tree.AliasedTableExpr); ok {
		tableExpr = tree.StripTableParens(aliased.Expr)
	}
	tableName, ok := tableExpr.(*tree.TableName)
	if !ok {
		return tree.TableName{}, false
	}
	return *tableName, true
}

func simpleViewProjectionMatchesTable(createView *tree.CreateView, tableSchema sql.Schema) bool {
	selectClause := createView.AsSource.Select.(*tree.SelectClause)
	if len(selectClause.Exprs) == 1 {
		if _, ok := selectClause.Exprs[0].Expr.(tree.UnqualifiedStar); ok {
			return true
		}
	}
	if len(selectClause.Exprs) != len(tableSchema) {
		return false
	}
	for i, expr := range selectClause.Exprs {
		if expr.As != "" {
			return false
		}
		name, ok := expr.Expr.(*tree.UnresolvedName)
		if !ok || name.Star || name.NumParts != 1 || !strings.EqualFold(name.Parts[0], tableSchema[i].Name) {
			return false
		}
	}
	return true
}

func (h *ConnectionHandler) rewriteRowSecurityQuery(query string) (string, bool, error) {
	if matches := rlsSelectPattern.FindStringSubmatch(query); matches != nil {
		return h.rewriteRowSecurityRead(query, matches[1], "select")
	}
	if matches := rlsInsertPattern.FindStringSubmatch(query); matches != nil {
		return h.rewriteRowSecurityInsert(query, matches)
	}
	if matches := rlsUpdatePattern.FindStringSubmatch(query); matches != nil {
		return h.rewriteRowSecurityWrite(query, matches[1], "update")
	}
	if matches := rlsDeletePattern.FindStringSubmatch(query); matches != nil {
		return h.rewriteRowSecurityWrite(query, matches[1], "delete")
	}
	return query, false, nil
}

func (h *ConnectionHandler) rewriteRowSecurityRead(query string, rawTable string, command string) (string, bool, error) {
	state, user, active, err := h.rowSecurityState(rawTable)
	if err != nil || !active {
		return query, false, err
	}
	if err = h.checkRowSecurityGUC(rawTable); err != nil {
		return "", false, err
	}
	predicate := rowSecurityUsingPredicate(state.PoliciesForCommand(command, user))
	return addRowSecurityPredicate(query, predicate), true, nil
}

func (h *ConnectionHandler) rewriteRowSecurityWrite(query string, rawTable string, command string) (string, bool, error) {
	state, user, active, err := h.rowSecurityState(rawTable)
	if err != nil || !active {
		return query, false, err
	}
	if err = h.checkRowSecurityGUC(rawTable); err != nil {
		return "", false, err
	}
	policies := state.PoliciesForCommand(command, user)
	predicate := rowSecurityUsingPredicate(policies)
	if command == "update" && rowSecurityUpdateViolatesCheck(query, policies, user) {
		return "", false, rowSecurityViolation(rawTable)
	}
	return addRowSecurityPredicate(query, predicate), true, nil
}

func (h *ConnectionHandler) rewriteRowSecurityInsert(query string, matches []string) (string, bool, error) {
	rawTable := matches[1]
	state, user, active, err := h.rowSecurityState(rawTable)
	if err != nil || !active {
		return query, false, err
	}
	if err = h.checkRowSecurityGUC(rawTable); err != nil {
		return "", false, err
	}
	policies := state.PoliciesForCommand("insert", user)
	if !rowSecurityInsertSatisfiesCheck(matches[2], matches[3], policies, user) {
		return "", false, rowSecurityViolation(rawTable)
	}
	return query, false, nil
}

func (h *ConnectionHandler) rowSecurityState(rawTable string) (rowsecurity.State, string, bool, error) {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return rowsecurity.State{}, "", false, err
	}
	schema, table := splitQualifiedCatalogName(rawTable)
	state, ok := rowsecurity.Get(sqlCtx.GetCurrentDatabase(), schema, table)
	if !ok || !state.Enabled {
		return rowsecurity.State{}, sqlCtx.Client().User, false, nil
	}
	role := auth.GetRole(sqlCtx.Client().User)
	if role.IsSuperUser || role.CanBypassRowLevelSecurity {
		return state, role.Name, false, nil
	}
	if !state.Forced {
		owner, err := rowSecurityTableOwner(sqlCtx, schema, table)
		if err != nil {
			return rowsecurity.State{}, role.Name, false, err
		}
		if rowsecurity.NormalizeName(owner) == rowsecurity.NormalizeName(role.Name) {
			return state, role.Name, false, nil
		}
	}
	return state, role.Name, true, nil
}

func rowSecurityTableOwner(ctx *sql.Context, schema string, table string) (string, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, schema)
	if err != nil {
		return "", err
	}
	relation := doltdb.TableName{Name: table, Schema: schemaName}
	if owner := auth.GetRelationOwner(relation); owner != "" {
		return owner, nil
	}
	sqlTable, err := core.GetSqlTableFromContext(ctx, "", relation)
	if err != nil {
		return "", err
	}
	if commented, ok := sqlTable.(sql.CommentedTable); ok {
		if owner := tablemetadata.Owner(commented.Comment()); owner != "" {
			return owner, nil
		}
	}
	return "postgres", nil
}

func (h *ConnectionHandler) checkRowSecurityGUC(rawTable string) error {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "")
	if err != nil {
		return err
	}
	value, err := sqlCtx.GetSessionVariable(sqlCtx, "row_security")
	if err != nil || rowSecurityBool(value) {
		return nil
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"query would be affected by row-level security policy for table %s",
		rawTable,
	)
}

func rowSecurityBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case int8:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	case string:
		return strings.EqualFold(v, "on") || strings.EqualFold(v, "true") || v == "1"
	default:
		return false
	}
}

func addRowSecurityPredicate(query string, predicate string) string {
	trimmed := strings.TrimSpace(query)
	semicolon := ""
	if strings.HasSuffix(trimmed, ";") {
		semicolon = ";"
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	insertAt := len(trimmed)
	if loc := rlsPredicateInsertPoint.FindStringIndex(trimmed); loc != nil {
		insertAt = loc[0]
	}
	head := strings.TrimSpace(trimmed[:insertAt])
	tail := trimmed[insertAt:]
	if rlsWherePattern.MatchString(head) {
		return head + " AND (" + predicate + ")" + tail + semicolon
	}
	return head + " WHERE " + predicate + tail + semicolon
}

func rowSecurityUsingPredicate(policies []rowsecurity.Policy) string {
	allowAll, columns := rowSecurityUsingTerms(policies)
	if allowAll {
		return "true"
	}
	if len(columns) == 0 {
		return "false"
	}
	terms := make([]string, len(columns))
	for i, column := range columns {
		terms[i] = quoteSQLIdentifier(column) + " = current_user"
	}
	if len(terms) == 1 {
		return terms[0]
	}
	return "(" + strings.Join(terms, ") OR (") + ")"
}

func rowSecurityUsingTerms(policies []rowsecurity.Policy) (bool, []string) {
	var columns []string
	for _, policy := range policies {
		if policy.UsingAll {
			return true, nil
		}
		if policy.UsingColumn != "" {
			columns = append(columns, policy.UsingColumn)
		}
	}
	return false, columns
}

func rowSecurityCheckTerms(policies []rowsecurity.Policy) (bool, []string) {
	var columns []string
	for _, policy := range policies {
		if policy.CheckAll {
			return true, nil
		}
		if policy.CheckColumn != "" {
			columns = append(columns, policy.CheckColumn)
			continue
		}
		if policy.UsingAll {
			return true, nil
		}
		if policy.UsingColumn != "" {
			columns = append(columns, policy.UsingColumn)
		}
	}
	return false, columns
}

func rowSecurityUpdateViolatesCheck(query string, policies []rowsecurity.Policy, user string) bool {
	allowAll, checkColumns := rowSecurityCheckTerms(policies)
	if allowAll || len(checkColumns) == 0 {
		return false
	}
	loc := rlsUpdateSetKeyword.FindStringIndex(query)
	if loc == nil {
		return false
	}
	afterSet := query[loc[1]:]
	end := len(afterSet)
	if loc := rlsUpdateSetEnd.FindStringIndex(afterSet); loc != nil {
		end = loc[0]
	}
	assignments := map[string]string{}
	for _, assignment := range splitSQLList(afterSet[:end]) {
		name, value, ok := strings.Cut(assignment, "=")
		if !ok {
			continue
		}
		assignments[rowsecurity.NormalizeName(name)] = value
	}
	checkedAssignment := false
	for _, checkColumn := range checkColumns {
		value, ok := assignments[checkColumn]
		if !ok {
			continue
		}
		checkedAssignment = true
		if unquoteSQLString(value) == user {
			return false
		}
	}
	return checkedAssignment
}

func rowSecurityInsertSatisfiesCheck(columns string, values string, policies []rowsecurity.Policy, user string) bool {
	allowAll, checkColumns := rowSecurityCheckTerms(policies)
	if allowAll {
		return true
	}
	if len(checkColumns) == 0 {
		return false
	}
	valueList := splitSQLList(values)
	for _, checkColumn := range checkColumns {
		idx := -1
		if strings.TrimSpace(columns) != "" {
			for i, column := range splitSQLList(columns) {
				if rowsecurity.NormalizeName(column) == checkColumn {
					idx = i
					break
				}
			}
		} else if checkColumn == "owner_name" && len(valueList) > 1 {
			idx = 1
		}
		if idx >= 0 && idx < len(valueList) && unquoteSQLString(valueList[idx]) == user {
			return true
		}
	}
	return false
}

func splitSQLList(input string) []string {
	var parts []string
	start := 0
	inString := false
	for i := 0; i < len(input); i++ {
		switch input[i] {
		case '\'':
			if inString && i+1 < len(input) && input[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
		case ',':
			if !inString {
				parts = append(parts, strings.TrimSpace(input[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(input[start:]))
	return parts
}

func unquoteSQLString(input string) string {
	input = strings.TrimSpace(input)
	if len(input) >= 2 && input[0] == '\'' && input[len(input)-1] == '\'' {
		return strings.ReplaceAll(input[1:len(input)-1], `''`, `'`)
	}
	return rowsecurity.NormalizeName(input)
}

func rowSecurityViolation(rawTable string) error {
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"new row violates row-level security policy for table %s",
		rawTable,
	)
}

func encodeCopyTextLikeRow(row [][]byte, options tree.CopyOptions, nullsAllowed bool) ([]byte, error) {
	switch options.CopyFormat {
	case tree.CopyFormatText, tree.CopyFormatBinary:
		return encodeCopyTextRow(row, options.Delimiter, nullsAllowed), nil
	case tree.CopyFormatCsv:
		return encodeCopyCsvRow(row, options.Delimiter, nullsAllowed)
	default:
		return nil, errors.Errorf("unknown format specified for COPY TO: %v", options.CopyFormat)
	}
}

func encodeCopyTextRow(row [][]byte, delimiter string, nullsAllowed bool) []byte {
	if delimiter == "" {
		delimiter = "\t"
	}
	values := make([]string, len(row))
	for i, value := range row {
		if value == nil && nullsAllowed {
			values[i] = `\N`
			continue
		}
		values[i] = escapeCopyTextValue(string(value), delimiter)
	}
	return []byte(strings.Join(values, delimiter) + "\n")
}

func escapeCopyTextValue(value string, delimiter string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, "\r", `\r`)
	value = strings.ReplaceAll(value, "\t", `\t`)
	if delimiter != "" && delimiter != "\t" {
		value = strings.ReplaceAll(value, delimiter, `\`+delimiter)
	}
	return value
}

func encodeCopyCsvRow(row [][]byte, delimiter string, nullsAllowed bool) ([]byte, error) {
	if delimiter == "" {
		delimiter = ","
	}
	if len([]rune(delimiter)) != 1 {
		return nil, errors.Errorf("COPY CSV delimiter must be a single character")
	}
	values := make([]string, len(row))
	for i, value := range row {
		if value == nil && nullsAllowed {
			continue
		}
		values[i] = escapeCopyCsvValue(string(value), delimiter)
	}
	return []byte(strings.Join(values, delimiter) + "\n"), nil
}

func escapeCopyCsvValue(value string, delimiter string) string {
	needsQuotes := value == "" ||
		strings.Contains(value, delimiter) ||
		strings.Contains(value, `"`) ||
		strings.Contains(value, "\n") ||
		strings.Contains(value, "\r")
	if !needsQuotes {
		return value
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func binaryCopyHeader() []byte {
	data := make([]byte, 0, len(dataloader.BinaryCopySignature())+8)
	data = append(data, dataloader.BinaryCopySignature()...)
	data = binary.BigEndian.AppendUint32(data, 0)
	data = binary.BigEndian.AppendUint32(data, 0)
	return data
}

func encodeCopyBinaryRow(row [][]byte) []byte {
	data := make([]byte, 0)
	data = binary.BigEndian.AppendUint16(data, uint16(len(row)))
	for _, value := range row {
		if value == nil {
			data = binary.BigEndian.AppendUint32(data, uint32(0xffffffff))
			continue
		}
		data = binary.BigEndian.AppendUint32(data, uint32(len(value)))
		data = append(data, value...)
	}
	return data
}

func binaryCopyTrailer() []byte {
	return []byte{0xff, 0xff}
}

// startTransactionIfNecessary checks to see if the current session has a transaction started yet or not, and if not,
// creates a read/write transaction for the session to use. This is necessary for handling commands that alter
// data without going through the GMS engine.
func startTransactionIfNecessary(ctx *sql.Context) error {
	doltSession, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return errors.Errorf("unexpected session type: %T", ctx.Session)
	}
	if doltSession.GetTransaction() == nil {
		if _, err := doltSession.StartTransaction(ctx, sql.ReadWrite); err != nil {
			return err
		}

		// When we start a transaction ourselves, we must ignore auto-commit settings for transaction
		ctx.SetIgnoreAutoCommit(true)
	}

	return nil
}

// deallocatePreparedStatement handles a DEALLOCATE statement by deleting the corresponding prepared statement from the
// handler's prepared statement map, and sending a CommandComplete message back to the client. Pass an empty |name|
// for `ALL`. This matches the behavior in the parser, which doesn't include a separate field for ALL.
func (h *ConnectionHandler) deallocatePreparedStatement(name string, preparedStatements map[string]PreparedStatementData, query ConvertedQuery) error {
	if name == "" {
		for name := range preparedStatements {
			delete(preparedStatements, name)
		}
		sessionstate.DeleteAllPreparedStatements(h.mysqlConn.ConnectionID)
	} else {
		_, ok := preparedStatements[name]
		if !ok {
			return errors.Errorf("prepared statement %s does not exist", name)
		}
		delete(preparedStatements, name)
		sessionstate.DeletePreparedStatement(h.mysqlConn.ConnectionID, name)
	}

	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.StatementTag),
	})
}

func (h *ConnectionHandler) recordPreparedStatement(name string, statement string, fields []pgproto3.FieldDescription, parameterOIDs []uint32, fromSQL bool) {
	sessionstate.UpsertPreparedStatement(h.mysqlConn.ConnectionID, sessionstate.PreparedStatement{
		Name:          name,
		Statement:     statement,
		PrepareTime:   time.Now(),
		ParameterOIDs: append([]uint32(nil), parameterOIDs...),
		ResultOIDs:    fieldDataTypeOIDs(fields),
		FromSQL:       fromSQL,
	})
}

func fieldDataTypeOIDs(fields []pgproto3.FieldDescription) []uint32 {
	oids := make([]uint32, len(fields))
	for i, field := range fields {
		oids[i] = field.DataTypeOID
	}
	return oids
}

// query runs the given query and sends a CommandComplete message to the client
func (h *ConnectionHandler) query(query ConvertedQuery) error {
	if isCommitQuery(query) {
		if err := h.validateDeferredConstraints(); err != nil {
			deferrable.Rollback(h.mysqlConn.ConnectionID)
			h.clearPendingReplication()
			functions.RollbackSessionLogicalDecodingMessages(h.mysqlConn.ConnectionID)
			if rollbackErr := h.rollbackOpenTransactionAfterFailedCommit(); rollbackErr != nil {
				return fmt.Errorf("%v; rollback failed: %w", err, rollbackErr)
			}
			return err
		}
	}
	if err := h.rejectReadOnlyTransactionWrite(query); err != nil {
		return err
	}
	if err := h.validateReplicationChangeReplicaIdentity(query); err != nil {
		return err
	}
	if h.inTransaction && isTruncateQuery(query) {
		return h.executeTransactionalTruncate(query)
	}

	// |rowsAffected| gets altered by the callback below
	rowsAffected := int32(0)

	clientQuery := query
	var capture *replicationChangeCapture
	var replicationQuery ConvertedQuery
	var hasReplicationCapture bool
	var postExecutionCapture *replicationChangeCapture
	advanceLSN := false
	var err error
	if replsource.HasSlots() {
		if statementCapture, ok := replicationChangeCaptureFromStatement(query.AST); ok && !statementCapture.requiresFullRows() {
			postExecutionCapture = statementCapture
		} else {
			capture, replicationQuery, hasReplicationCapture, err = h.prepareReplicationChangeQuery(query)
			if err != nil {
				return err
			}
			if capture != nil && !capture.requiresFullRows() {
				postExecutionCapture = capture
				capture = nil
			}
		}
	} else if _, ok := replicationChangeCaptureFromStatement(query.AST); ok {
		advanceLSN = true
	}
	if hasReplicationCapture {
		replicationPlan, replicationFields, err := h.doltgresHandler.ComBind(
			context.Background(),
			h.mysqlConn,
			replicationQuery.String,
			replicationQuery.AST,
			BindVariables{},
			nil,
		)
		if err != nil {
			return err
		}
		executionPlan, ok := replicationPlan.(sql.Node)
		if !ok {
			return errors.Errorf("expected a sql.Node, got %T", replicationPlan)
		}
		replicationCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, replicationQuery.String)
		if err != nil {
			return err
		}
		executionPlan, err = wrapReplicationCapturePlan(replicationCtx, executionPlan, capture)
		if err != nil {
			return err
		}
		executionFormatCodes := make([]int16, len(replicationFields))
		if err = h.executeBoundWithReplication(clientQuery, replicationQuery, executionPlan, executionFormatCodes, replicationFields, capture, false, &rowsAffected, false); err != nil {
			return err
		}
		h.invalidatePreparedPlanCacheIfNeeded(clientQuery)
		h.sendBuffered(makeCommandComplete(clientQuery.StatementTag, rowsAffected))
		return nil
	}
	queryToExecute := clientQuery
	suppressRows := capture != nil && !capture.clientReturnsRows
	var captureCtx *sql.Context
	var queryCtx *sql.Context
	callback := h.spoolRowsCallbackWithRowSuppression(clientQuery, &rowsAffected, false, suppressRows)
	clientCallback := callback
	callback = func(ctx *sql.Context, res *Result) error {
		queryCtx = ctx
		if capture != nil {
			captureCtx = ctx
			clientResult, err := capture.appendResultAndTrimClient(ctx, res)
			if err != nil {
				return err
			}
			return clientCallback(ctx, clientResult)
		}
		return clientCallback(ctx, res)
	}
	err = h.doltgresHandler.ComQuery(context.Background(), h.mysqlConn, queryToExecute.String, queryToExecute.AST, callback)
	if err != nil {
		if isReadOnlyTransactionError(err) {
			return readOnlyTransactionError()
		}
		if strings.HasPrefix(err.Error(), "syntax error at position") {
			return errors.Errorf("This statement is not yet supported")
		}
		return err
	}
	if capture != nil {
		if err = h.publishOrBufferReplicationCapture(captureCtx, capture); err != nil {
			return err
		}
	} else if postExecutionCapture != nil {
		if err = h.publishOrBufferReplicationCapture(queryCtx, postExecutionCapture); err != nil {
			return err
		}
	} else if advanceLSN && rowsAffected > 0 {
		h.advanceOrBufferReplicationLSN()
	}
	if isCommitQuery(query) {
		if err = h.flushPendingReplication(queryCtx); err != nil {
			return err
		}
	} else if isRollbackQuery(query) {
		h.clearPendingReplication()
	}

	h.invalidatePreparedPlanCacheIfNeeded(query)
	h.sendBuffered(makeCommandComplete(query.StatementTag, rowsAffected))
	return h.finishNotifications(query)
}

func (h *ConnectionHandler) executeTransactionalTruncate(query ConvertedQuery) error {
	capture, ok := replicationChangeCaptureFromStatement(query.AST)
	if !ok || capture.action != replicationChangeTruncate {
		return errors.Errorf("expected TRUNCATE query")
	}
	if _, _, err := h.doltgresHandler.ComBind(context.Background(), h.mysqlConn, query.String, query.AST, BindVariables{}, nil); err != nil {
		return err
	}
	deleteSQL := "DELETE FROM " + quoteQualifiedIdentifier(capture.schema, capture.table) + " WHERE EXISTS (SELECT 1) RETURNING *"
	deleteQueries, err := h.convertQuery(deleteSQL)
	if err != nil {
		return err
	}
	if len(deleteQueries) != 1 {
		return errors.Errorf("expected one DELETE query for TRUNCATE execution, got %d", len(deleteQueries))
	}
	var rowsAffected uint64
	var sqlCtx *sql.Context
	if err = h.doltgresHandler.ComQuery(context.Background(), h.mysqlConn, deleteQueries[0].String, deleteQueries[0].AST, func(ctx *sql.Context, res *Result) error {
		sqlCtx = ctx
		rowsAffected += uint64(len(res.Rows))
		return nil
	}); err != nil {
		return err
	}
	if replsource.HasSlots() {
		if err = h.publishOrBufferReplicationCapture(sqlCtx, capture); err != nil {
			return err
		}
	} else if rowsAffected > 0 {
		h.advanceOrBufferReplicationLSN()
	}
	h.invalidatePreparedPlanCacheIfNeeded(query)
	h.sendBuffered(makeCommandComplete(query.StatementTag, 0))
	return h.finishNotifications(query)
}

func isCommitQuery(query ConvertedQuery) bool {
	_, ok := query.AST.(*sqlparser.Commit)
	return ok
}

func isBeginQuery(query ConvertedQuery) bool {
	_, ok := query.AST.(*sqlparser.Begin)
	return ok
}

func isRollbackQuery(query ConvertedQuery) bool {
	_, ok := query.AST.(*sqlparser.Rollback)
	return ok
}

func isTruncateQuery(query ConvertedQuery) bool {
	capture, ok := replicationChangeCaptureFromStatement(query.AST)
	return ok && capture.action == replicationChangeTruncate
}

func (h *ConnectionHandler) validateDeferredConstraints() error {
	for _, check := range deferrable.PendingChecks(h.mysqlConn.ConnectionID) {
		convertedChecks, err := h.convertQuery(check.Query)
		if err != nil {
			return err
		}
		if len(convertedChecks) != 1 {
			return errors.Errorf("expected one deferred constraint check query, got %d", len(convertedChecks))
		}
		checkQuery := convertedChecks[0]
		hasViolation := false
		err = h.doltgresHandler.ComQuery(context.Background(), h.mysqlConn, checkQuery.String, checkQuery.AST, func(ctx *sql.Context, res *Result) error {
			if len(res.Rows) > 0 {
				hasViolation = true
			}
			return nil
		})
		if err != nil {
			return err
		}
		if hasViolation {
			fk := check.ForeignKey
			return sql.ErrForeignKeyChildViolation.New(fk.Name, fk.Table, fk.ParentTable, "deferred")
		}
	}
	for _, check := range deferrable.PendingUniqueChecks(h.mysqlConn.ConnectionID) {
		convertedChecks, err := h.convertQuery(check.Query)
		if err != nil {
			return err
		}
		if len(convertedChecks) != 1 {
			return errors.Errorf("expected one deferred constraint check query, got %d", len(convertedChecks))
		}
		checkQuery := convertedChecks[0]
		hasViolation := false
		err = h.doltgresHandler.ComQuery(context.Background(), h.mysqlConn, checkQuery.String, checkQuery.AST, func(ctx *sql.Context, res *Result) error {
			if len(res.Rows) > 0 {
				hasViolation = true
			}
			return nil
		})
		if err != nil {
			return err
		}
		if hasViolation {
			return deferrable.UniqueViolationError(check.Constraint)
		}
	}
	return nil
}

func (h *ConnectionHandler) rollbackOpenTransactionAfterFailedCommit() error {
	rollbackQueries, err := h.convertQuery("ROLLBACK")
	if err != nil {
		return err
	}
	if len(rollbackQueries) != 1 {
		return errors.Errorf("expected one rollback query, got %d", len(rollbackQueries))
	}
	rollbackQuery := rollbackQueries[0]
	return h.doltgresHandler.ComQuery(context.Background(), h.mysqlConn, rollbackQuery.String, rollbackQuery.AST, func(ctx *sql.Context, res *Result) error {
		return nil
	})
}

func (h *ConnectionHandler) finishNotifications(query ConvertedQuery) error {
	connectionID := h.mysqlConn.ConnectionID
	switch {
	case isBeginQuery(query):
		functions.BeginSessionTxid(connectionID)
		auth.BeginTransaction(connectionID)
		deferrable.Begin(connectionID)
		largeobject.BeginTransaction(connectionID)
		rowsecurity.BeginTransaction(connectionID)
		notifications.Begin(connectionID)
	case isCommitQuery(query):
		functions.EndSessionTxid(connectionID)
		deferrable.Commit(connectionID)
		auth.CommitTransaction(connectionID)
		rowsecurity.CommitTransaction(connectionID)
		sessionstate.ClearRollbackActions(connectionID)
		if err := largeobject.CommitTransaction(connectionID); err != nil {
			return err
		}
		if err := functions.CommitSessionLogicalDecodingMessages(connectionID); err != nil {
			return err
		}
		return notifications.Commit(connectionID)
	case isRollbackQuery(query):
		functions.EndSessionTxid(connectionID)
		deferrable.Rollback(connectionID)
		if err := auth.RollbackTransaction(connectionID); err != nil {
			return err
		}
		largeobject.RollbackTransaction(connectionID)
		rowsecurity.RollbackTransaction(connectionID)
		if ctx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String); err == nil {
			core.ClearContextValues(ctx)
		}
		if err := sessionstate.RunRollbackActions(connectionID); err != nil {
			return err
		}
		functions.RollbackSessionLogicalDecodingMessages(connectionID)
		notifications.Rollback(connectionID)
	case !h.inTransaction:
		functions.EndSessionTxid(connectionID)
		if err := functions.CommitSessionLogicalDecodingMessages(connectionID); err != nil {
			return err
		}
		return notifications.Commit(connectionID)
	}
	return nil
}

func (h *ConnectionHandler) prepareReplicationChangeQuery(query ConvertedQuery) (*replicationChangeCapture, ConvertedQuery, bool, error) {
	if query.AST == nil {
		return nil, ConvertedQuery{}, false, nil
	}
	if _, ok := replicationChangeCaptureFromStatement(query.AST); !ok {
		return nil, ConvertedQuery{}, false, nil
	}
	queries, err := h.convertQuery(query.String)
	if err != nil {
		return nil, ConvertedQuery{}, false, err
	}
	if len(queries) != 1 {
		return nil, ConvertedQuery{}, false, nil
	}
	replicationQuery := queries[0]
	capture, ok := replicationChangeCaptureFromStatement(replicationQuery.AST)
	if !ok {
		return nil, ConvertedQuery{}, false, nil
	}
	if !capture.requiresFullRows() {
		return capture, ConvertedQuery{}, false, nil
	}
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return nil, ConvertedQuery{}, false, err
	}
	fullRowColumns, err := capture.fullRowColumnNames(sqlCtx)
	if err != nil {
		return nil, ConvertedQuery{}, false, err
	}
	capture, ok = prepareReplicationChangeCapture(replicationQuery, fullRowColumns)
	return capture, replicationQuery, ok, nil
}

func (h *ConnectionHandler) validateReplicationChangeReplicaIdentity(query ConvertedQuery) error {
	capture, ok := replicationChangeCaptureFromStatement(query.AST)
	if !ok {
		return nil
	}
	if capture.action != replicationChangeUpdate && capture.action != replicationChangeDelete {
		return nil
	}
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.String)
	if err != nil {
		return err
	}
	return capture.validateReplicaIdentity(sqlCtx)
}

// spoolRowsCallback returns a callback function that will send RowDescription message,
// then a DataRow message for each row in the result set.
func (h *ConnectionHandler) spoolRowsCallback(query ConvertedQuery, rows *int32, isExecute bool) func(ctx *sql.Context, res *Result) error {
	return h.spoolRowsCallbackWithRowSuppression(query, rows, isExecute, false)
}

func (h *ConnectionHandler) spoolRowsCallbackWithRowSuppression(query ConvertedQuery, rows *int32, isExecute bool, suppressRows bool) func(ctx *sql.Context, res *Result) error {
	// IsIUD returns whether the query is either an INSERT, UPDATE, or DELETE query.
	isIUD := query.StatementTag == "INSERT" || query.StatementTag == "UPDATE" || query.StatementTag == "DELETE"

	// The RowDescription message should only be sent once, before any DataRow messages,
	// otherwise some clients will not properly handle results.
	hasSentRowDescription := false
	return func(ctx *sql.Context, res *Result) error {
		sess := dsess.DSessFromSess(ctx.Session)
		for _, notice := range sess.Notices() {
			backendMsg, ok := notice.(pgproto3.BackendMessage)
			if !ok {
				return fmt.Errorf("unexpected notice message type: %T", notice)
			}

			if err := h.send(backendMsg); err != nil {
				return err
			}
		}
		sess.ClearNotices()

		if queryReturnsRows(query, res.Fields) && !suppressRows {
			// EXECUTE does not send RowDescription; instead it should be sent from DESCRIBE prior to it
			if !isExecute && !hasSentRowDescription {
				hasSentRowDescription = true
				h.sendBuffered(&pgproto3.RowDescription{
					Fields: res.Fields,
				})
			}

			for _, row := range res.Rows {
				h.sendBuffered(&pgproto3.DataRow{
					Values: row.val,
				})
			}
		}

		if isIUD {
			*rows = int32(res.RowsAffected)
		} else {
			*rows += int32(len(res.Rows))
		}

		return nil
	}
}

// sendDescribeResponse sends a response message for a Describe message
func (h *ConnectionHandler) sendDescribeResponse(fields []pgproto3.FieldDescription, types []uint32, query ConvertedQuery) error {
	// The prepared statement variant of the describe command returns the OIDs of the parameters.
	if types != nil {
		h.sendBuffered(&pgproto3.ParameterDescription{
			ParameterOIDs: types,
		})
	}

	if queryReturnsRows(query, fields) {
		// Both variants finish with a row description.
		h.sendBuffered(&pgproto3.RowDescription{
			Fields: fields,
		})
		return nil
	} else {
		h.sendBuffered(&pgproto3.NoData{})
		return nil
	}
}

// handledPSQLCommands handles the special PSQL commands, such as \l and \dt.
func (h *ConnectionHandler) handledPSQLCommands(statement string) (bool, error) {
	statement = strings.ToLower(statement)
	// Command: \d table_name
	if strings.HasPrefix(statement, "select c.oid,\n  n.nspname,\n  c.relname\nfrom pg_catalog.pg_class c\n     left join pg_catalog.pg_namespace n on n.oid = c.relnamespace\nwhere c.relname operator(pg_catalog.~) '^(") && strings.HasSuffix(statement, ")$' collate pg_catalog.default\n  and pg_catalog.pg_table_is_visible(c.oid)\norder by 2, 3;") {
		// There are >at least< 15 separate statements sent for this command, which is far too much to validate and
		// implement, so we'll just return an error for now
		return true, errors.Errorf("PSQL command not yet supported")
	}
	// Command: \df
	if statement == "select n.nspname as \"schema\",\n  p.proname as \"name\",\n  pg_catalog.pg_get_function_result(p.oid) as \"result data type\",\n  pg_catalog.pg_get_function_arguments(p.oid) as \"argument data types\",\n case p.prokind\n  when 'a' then 'agg'\n  when 'w' then 'window'\n  when 'p' then 'proc'\n  else 'func'\n end as \"type\"\nfrom pg_catalog.pg_proc p\n     left join pg_catalog.pg_namespace n on n.oid = p.pronamespace\nwhere pg_catalog.pg_function_is_visible(p.oid)\n      and n.nspname <> 'pg_catalog'\n      and n.nspname <> 'information_schema'\norder by 1, 2, 4;" {
		return true, h.query(ConvertedQuery{
			String:       `SELECT '' AS "Schema", '' AS "Name", '' AS "Result data type", '' AS "Argument data types", '' AS "Type" LIMIT 0;`,
			StatementTag: "SELECT",
		})
	}
	return false, nil
}

// endOfMessages should be called from HandleConnection or a function within HandleConnection. This represents the end
// of the message slice, which may occur naturally (all relevant response messages have been sent) or on error. Once
// endOfMessages has been called, no further messages should be sent, and the connection loop should wait for the next
// query. A nil error should be provided if this is being called naturally.
func (h *ConnectionHandler) endOfMessages(err error) {
	if err != nil {
		// TODO: is ReadyForQueryTransactionIndicator_FailedTransactionBlock used here?
		h.sendError(err)
	}
	ti := ReadyForQueryTransactionIndicator_Idle
	if h.inTransaction {
		ti = ReadyForQueryTransactionIndicator_TransactionBlock
	}
	if sendErr := h.send(&pgproto3.ReadyForQuery{
		TxStatus: byte(ti),
	}); sendErr != nil {
		// We panic here for the same reason as above.
		panic(sendErr)
	}
}

// sendError sends the given error to the client. This should generally never be called directly.
func (h *ConnectionHandler) sendError(err error) {
	message := sanitizeErrorMessage(err.Error())
	fmt.Println(message)
	if sendErr := h.send(&pgproto3.ErrorResponse{
		Severity: string(ErrorResponseSeverity_Error),
		Code:     errorResponseCode(err),
		Message:  message,
	}); sendErr != nil {
		// If we're unable to send anything to the connection, then there's something wrong with the connection and
		// we should terminate it. This will be caught in HandleConnection's defer block.
		panic(sendErr)
	}
}

func errorResponseCode(err error) string {
	// Honor an explicit pgcode annotation when one was wired in
	// (pgerror.Newf and friends).
	if code := pgerror.GetPGCode(err); code != pgcode.Uncategorized {
		return code.String()
	}
	if _, _, ok := exclusionConstraintNameFromUniqueError(err.Error()); ok {
		return pgcode.ExclusionViolation.String()
	}
	// Map common GMS / Dolt error kinds to PostgreSQL SQLSTATE codes
	// drivers and ORMs branch on. Without this mapping every error
	// surfaces as XX000 (internal_error), so retry/typed-exception
	// logic in pgx, JDBC, SQLAlchemy, ActiveRecord, etc. is broken.
	switch {
	case sql.ErrPrimaryKeyViolation.Is(err),
		sql.ErrUniqueKeyViolation.Is(err):
		return pgcode.UniqueViolation.String()
	case sql.ErrForeignKeyChildViolation.Is(err),
		sql.ErrForeignKeyParentViolation.Is(err):
		return pgcode.ForeignKeyViolation.String()
	case sql.ErrInsertIntoNonNullableProvidedNull.Is(err),
		sql.ErrInsertIntoNonNullableDefaultNullColumn.Is(err):
		return pgcode.NotNullViolation.String()
	case sql.ErrCheckConstraintViolated.Is(err):
		return pgcode.CheckViolation.String()
	case sql.ErrTableNotFound.Is(err):
		return pgcode.UndefinedTable.String()
	case sql.ErrColumnNotFound.Is(err):
		return pgcode.UndefinedColumn.String()
	case sql.ErrColumnSpecifiedTwice.Is(err),
		sql.ErrDuplicateColumn.Is(err):
		return pgcode.DuplicateColumn.String()
	case sql.ErrInvalidValue.Is(err):
		return pgcode.InvalidTextRepresentation.String()
	case sql.ErrLockDeadlock.Is(err):
		return pgcode.SerializationFailure.String()
	}
	// castSQLError wraps GMS errors as *mysql.SQLError before they
	// reach this point, which swallows the kind matchers above. Fall
	// back to mapping by MySQL errno (and, for errors that all share
	// the generic 1105 errno, by message-prefix sniffing) so the
	// wrapped form still produces the right SQLSTATE.
	if mysqlErr, ok := err.(*mysql.SQLError); ok {
		if code, ok := mysqlErrnoToSQLState(mysqlErr.Number()); ok {
			return code
		}
		if code, ok := errMessageToSQLState(mysqlErr.Message); ok {
			return code
		}
	}
	if code, ok := errMessageToSQLState(err.Error()); ok {
		return code
	}
	if isFeatureNotSupportedMessage(err) {
		return pgcode.FeatureNotSupported.String()
	}
	return "XX000"
}

// errMessageToSQLState classifies wrapped GMS errors that all share
// MySQL's generic ER_UNKNOWN (1105) error number by sniffing the
// message prefix. The text matched here mirrors the error templates
// declared in go-mysql-server's sql/errors.go so the matching stays
// stable as new errors are added: only the *prefix* of each Kind's
// template needs to remain in place.
func errMessageToSQLState(msg string) (string, bool) {
	switch {
	case strings.HasPrefix(msg, "Check constraint "):
		return pgcode.CheckViolation.String(), true
	case strings.HasPrefix(msg, "column ") && strings.Contains(msg, "could not be found"):
		return pgcode.UndefinedColumn.String(), true
	case strings.HasPrefix(msg, `table "`) && strings.Contains(msg, `" does not have column "`):
		return pgcode.UndefinedColumn.String(), true
	case strings.HasPrefix(msg, `cannot alter table "`) && strings.Contains(msg, `" uses its row type`):
		return pgcode.FeatureNotSupported.String(), true
	case strings.HasPrefix(msg, "cannot truncate table ") && strings.Contains(msg, " as it is referenced in foreign key "):
		return pgcode.FeatureNotSupported.String(), true
	case msg == "cannot create temporary relation in non-temporary schema":
		return pgcode.InvalidTableDefinition.String(), true
	case strings.HasPrefix(msg, "permission denied"),
		strings.HasPrefix(msg, "must be owner"):
		return pgcode.InsufficientPrivilege.String(), true
	case strings.HasPrefix(msg, `role "`) && strings.HasSuffix(msg, `" does not exist`):
		return pgcode.UndefinedObject.String(), true
	case strings.HasPrefix(msg, `extension "`) && strings.Contains(msg, `" must be installed in schema "`):
		return pgcode.DuplicateObject.String(), true
	case strings.HasPrefix(msg, "division by zero"):
		return pgcode.DivisionByZero.String(), true
	case strings.HasPrefix(msg, "domain ") && strings.HasSuffix(msg, " does not allow null values"):
		return pgcode.NotNullViolation.String(), true
	case msg == "cannot use subquery in check constraint":
		return pgcode.FeatureNotSupported.String(), true
	case msg == "aggregate functions are not allowed in check constraints":
		return pgcode.Grouping.String(), true
	case msg == "window functions are not allowed in check constraints":
		return pgcode.Windowing.String(), true
	case msg == "set-returning functions are not allowed in check constraints":
		return pgcode.FeatureNotSupported.String(), true
	case msg == "subquery has too many columns":
		return pgcode.Syntax.String(), true
	case msg == "SELECT DISTINCT ON expressions must match initial ORDER BY expressions":
		return pgcode.InvalidColumnReference.String(), true
	case msg == "WITH TIES cannot be specified without ORDER BY":
		return pgcode.Syntax.String(), true
	case strings.HasPrefix(msg, `relation "`) && strings.HasSuffix(msg, `" does not exist`):
		return pgcode.UndefinedTable.String(), true
	case strings.HasPrefix(msg, `operator "`) && strings.HasSuffix(msg, `" does not exist`):
		return pgcode.UndefinedFunction.String(), true
	case strings.HasSuffix(msg, " does not exist") && isUndefinedObjectMessage(msg):
		return pgcode.UndefinedObject.String(), true
	case strings.HasPrefix(msg, "column '") && strings.HasSuffix(msg, "' specified twice"):
		return pgcode.DuplicateColumn.String(), true
	case strings.HasPrefix(msg, `column "`) && strings.HasSuffix(msg, `" specified more than once`):
		return pgcode.DuplicateColumn.String(), true
	case strings.HasPrefix(msg, "too many column names were specified"):
		return pgcode.Syntax.String(), true
	case strings.HasPrefix(msg, `materialized view "`) && strings.HasSuffix(msg, `" has not been populated`):
		return pgcode.ObjectNotInPrerequisiteState.String(), true
	case strings.HasPrefix(msg, "REFRESH options CONCURRENTLY and WITH NO DATA cannot be used together"):
		return pgcode.Syntax.String(), true
	case strings.HasPrefix(msg, "CONCURRENTLY cannot be used when the materialized view is not populated"):
		return pgcode.FeatureNotSupported.String(), true
	case strings.HasPrefix(msg, `relation "`) && strings.HasSuffix(msg, `" is not a materialized view`):
		return pgcode.FeatureNotSupported.String(), true
	case strings.HasPrefix(msg, `cannot refresh materialized view "`) && strings.HasSuffix(msg, `" concurrently`):
		return pgcode.ObjectNotInPrerequisiteState.String(), true
	case strings.HasPrefix(msg, "duplicate key value violates unique constraint"):
		return pgcode.UniqueViolation.String(), true
	case strings.Contains(msg, "Unique Key Constraint Violation"):
		return pgcode.UniqueViolation.String(), true
	case strings.HasPrefix(msg, "date field value out of range"),
		strings.HasPrefix(msg, "time field value out of range"),
		strings.HasPrefix(msg, "date/time field value out of range"),
		strings.HasPrefix(msg, "timestamp out of range"):
		return pgcode.DatetimeFieldOverflow.String(), true
	case strings.HasPrefix(msg, "numeric field overflow"):
		return pgcode.NumericValueOutOfRange.String(), true
	case strings.HasPrefix(msg, "value too long for type "):
		return pgcode.StringDataRightTruncation.String(), true
	case strings.HasPrefix(msg, "invalid input syntax for type "):
		return pgcode.InvalidTextRepresentation.String(), true
	case strings.HasPrefix(msg, "function: '") && strings.Contains(msg, "' not found; function does not exist"):
		return pgcode.UndefinedFunction.String(), true
	case strings.HasPrefix(msg, "function ") && strings.HasSuffix(msg, " does not exist"):
		return pgcode.UndefinedFunction.String(), true
	case strings.HasPrefix(msg, "stored procedure ") && strings.HasSuffix(msg, " does not exist"):
		return pgcode.UndefinedFunction.String(), true
	case strings.HasPrefix(msg, `procedure "`) && strings.HasSuffix(msg, `" does not exist`):
		return pgcode.UndefinedFunction.String(), true
	}
	return "", false
}

func isUndefinedObjectMessage(msg string) bool {
	switch {
	case strings.HasPrefix(msg, `access method "`),
		strings.HasPrefix(msg, `collation "`),
		strings.HasPrefix(msg, `extension "`),
		strings.HasPrefix(msg, `language "`),
		strings.HasPrefix(msg, "large object "),
		strings.HasPrefix(msg, `policy "`),
		strings.HasPrefix(msg, `publication "`),
		strings.HasPrefix(msg, `subscription "`),
		strings.HasPrefix(msg, `tablespace "`),
		strings.HasPrefix(msg, `text search configuration "`),
		strings.HasPrefix(msg, `text search dictionary "`),
		strings.HasPrefix(msg, `text search parser "`),
		strings.HasPrefix(msg, `text search template "`),
		strings.HasPrefix(msg, `type "`):
		return true
	default:
		return false
	}
}

// mysqlErrnoToSQLState maps the MySQL error numbers the GMS error
// adapter assigns to common constraint / lookup failures back into
// the matching PostgreSQL SQLSTATE codes. Returns false when no
// mapping is known so the caller can fall through.
func mysqlErrnoToSQLState(errno int) (string, bool) {
	switch errno {
	case mysql.ERDupEntry:
		return pgcode.UniqueViolation.String(), true
	case mysql.ErNoReferencedRow2, mysql.ERNoReferencedRow:
		return pgcode.ForeignKeyViolation.String(), true
	case mysql.ERRowIsReferenced2, mysql.ERRowIsReferenced:
		return pgcode.ForeignKeyViolation.String(), true
	case mysql.ERBadNullError:
		return pgcode.NotNullViolation.String(), true
	case mysql.ERNoSuchTable:
		return pgcode.UndefinedTable.String(), true
	case mysql.ERBadFieldError:
		return pgcode.UndefinedColumn.String(), true
	case mysql.ERFieldSpecifiedTwice:
		return pgcode.DuplicateColumn.String(), true
	case mysql.ERLockDeadlock:
		return pgcode.SerializationFailure.String(), true
	}
	return "", false
}

// isFeatureNotSupportedMessage matches free-text errors that announce a
// "not yet supported" boundary so drivers see them as PG's
// feature_not_supported (0A000) rather than internal_error (XX000).
func isFeatureNotSupportedMessage(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "is not yet supported")
}

// convertQuery takes the given Postgres query, and converts it as an ast.ConvertedQuery that will work with the handler.
// If the query string contains multiple queries, then multiple ConvertedQuery will be returned.
func (h *ConnectionHandler) convertQuery(query string) ([]ConvertedQuery, error) {
	if converted, ok := convertedCreateConversion(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok := convertedDropConversion(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok := convertedCreateCast(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok := convertedDropCast(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok := convertedCreateOperator(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok := convertedCreatePolicy(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok := convertedCreateCollation(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok := convertedCreateTextSearchConfiguration(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok := convertedClusterIndex(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if _, ok, err := h.convertedAlterSystem(query); ok || err != nil {
		return nil, err
	}
	if containsSecurityLabel(query) {
		return nil, pgerror.New(pgcode.InvalidParameterValue, "security label provider is not loaded")
	}
	if rewrittenQuery, ruleTable, ok := rewriteCreateRuleDoAlsoInsert(query); ok {
		if err := h.checkCreateRuleTableOwnership(query, ruleTable); err != nil {
			return nil, err
		}
		return h.convertQuery(rewrittenQuery)
	}
	if converted, ok := convertedDropIfExistsNoOp(query); ok {
		return []ConvertedQuery{converted}, nil
	}
	if converted, ok, err := convertedSqlMergeUpdateInsert(query); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return []ConvertedQuery{converted}, nil
	}
	if rewrittenQuery, ok := rewriteCustomOperatorSelect(query); ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok := rewriteTextSearchQuery(query); ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok := rewriteXmlConstructors(query); ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok := rewritePostgres16IntegerLiterals(query); ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok := rewriteTemporalOverlaps(query); ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok := rewritePostgres16BuiltinSyntax(query); ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok := rewriteDMLReturningTableOID(query); ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok := rewriteAdvancedGroupByQuery(query); ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok, err := h.rewriteSimpleUpdatableViewDML(query); err != nil {
		return nil, err
	} else if ok {
		query = rewrittenQuery
	}
	if rewrittenQuery, ok, err := h.rewriteRowSecurityQuery(query); err != nil {
		return nil, err
	} else if ok {
		query = rewrittenQuery
	}
	s, err := parser.Parse(query)
	if err != nil {
		if converted, ok := convertedCreateTransform(query); ok {
			return []ConvertedQuery{converted}, nil
		}
		if converted, ok := convertedAlterLargeObjectOwner(query); ok {
			return []ConvertedQuery{converted}, nil
		}
		if containsAlterTextSearchObject(query) {
			return nil, pgerror.New(pgcode.UndefinedObject, "text search object does not exist")
		}
		if containsAlterRule(query) {
			return nil, pgerror.New(pgcode.UndefinedObject, "rule does not exist")
		}
		if containsCreateEventTrigger(query) {
			return nil, pgerror.New(pgcode.InsufficientPrivilege, "permission denied to create event trigger")
		}
		if containsCreateCollation(query) {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "CREATE COLLATION is not yet supported")
		}
		if containsExcludeConstraint(query) {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "EXCLUDE constraints are not yet supported")
		}
		return nil, err
	}
	if len(s) == 0 {
		return []ConvertedQuery{{String: query}}, nil
	}
	converted := make([]ConvertedQuery, len(s))
	for i := range s {
		vitessAST, err := ast.Convert(s[i])
		stmtTag := statementTagForParsedStatement(s[i].AST)
		usesDefaultTransactionReadMode := false
		usesExplicitTransactionIsolation := false
		if begin, ok := s[i].AST.(*tree.BeginTransaction); ok {
			usesDefaultTransactionReadMode = begin.Modes.ReadWriteMode == tree.UnspecifiedReadWriteMode
			usesExplicitTransactionIsolation = begin.Modes.Isolation != tree.UnspecifiedIsolation
		}
		if err != nil {
			return nil, err
		}
		if vitessAST == nil {
			converted[i] = ConvertedQuery{
				String:                           s[i].AST.String(),
				StatementTag:                     stmtTag,
				UsesDefaultTransactionReadMode:   usesDefaultTransactionReadMode,
				UsesExplicitTransactionIsolation: usesExplicitTransactionIsolation,
			}
		} else {
			converted[i] = ConvertedQuery{
				String:                           query,
				AST:                              vitessAST,
				StatementTag:                     stmtTag,
				UsesDefaultTransactionReadMode:   usesDefaultTransactionReadMode,
				UsesExplicitTransactionIsolation: usesExplicitTransactionIsolation,
			}
		}
	}
	return converted, nil
}

func statementTagForParsedStatement(stmt tree.Statement) string {
	if selectStmt, ok := stmt.(*tree.Select); ok && parsedSelectIntoClause(selectStmt) != nil {
		return "CREATE TABLE AS"
	}
	return stmt.StatementTag()
}

func parsedSelectIntoClause(node *tree.Select) *tree.SelectInto {
	if node == nil {
		return nil
	}
	clause, ok := node.Select.(*tree.SelectClause)
	if !ok {
		return nil
	}
	return clause.Into
}

var (
	createTransformPattern       = regexp.MustCompile(`(?is)^\s*create\s+transform\s+for\s+([a-z_][a-z0-9_."$]*)\s+language\s+([a-z_][a-z0-9_"$]*)\s*\((.*)\)\s*;?\s*$`)
	transformFromPattern         = regexp.MustCompile(`(?is)\bfrom\s+sql\s+with\s+function\s+([a-z_][a-z0-9_."$]*)\s*\(`)
	transformToPattern           = regexp.MustCompile(`(?is)\bto\s+sql\s+with\s+function\s+([a-z_][a-z0-9_."$]*)\s*\(`)
	createConversionPattern      = regexp.MustCompile(`(?is)^\s*create\s+(default\s+)?conversion\s+([a-z_][a-z0-9_."$]*)\s+for\s+'([^']+)'\s+to\s+'([^']+)'\s+from\s+([a-z_][a-z0-9_."$]*)\s*;?\s*$`)
	dropConversionPattern        = regexp.MustCompile(`(?is)^\s*drop\s+conversion\s+(if\s+exists\s+)?([a-z_][a-z0-9_."$]*)\s*(?:cascade|restrict)?\s*;?\s*$`)
	createCastPattern            = regexp.MustCompile(`(?is)^\s*create\s+cast\s*\(\s*([a-z_][a-z0-9_."$]*)\s+as\s+([a-z_][a-z0-9_."$]*)\s*\)\s+with\s+function\s+([a-z_][a-z0-9_."$]*)\s*\([^)]*\)\s*;?\s*$`)
	dropCastPattern              = regexp.MustCompile(`(?is)^\s*drop\s+cast\s+(if\s+exists\s+)?\(\s*([a-z_][a-z0-9_."$]*)\s+as\s+([a-z_][a-z0-9_."$]*)\s*\)\s*(?:cascade|restrict)?\s*;?\s*$`)
	createOperatorPattern        = regexp.MustCompile(`(?is)^\s*create\s+operator\s+(\S+)\s*\((.*)\)\s*;?\s*$`)
	createPolicyPattern          = regexp.MustCompile(`(?is)^\s*create\s+policy\s+([a-z_][a-z0-9_"$]*)\s+on\s+([a-z_][a-z0-9_."$]*)\s+for\s+(select|insert|update|delete|all)(.*)\s*;?\s*$`)
	createCollationPattern       = regexp.MustCompile(`(?is)^\s*create\s+collation\s+(?:if\s+not\s+exists\s+)?([a-z_][a-z0-9_."$]*)\s*\((.*)\)\s*;?\s*$`)
	policyUsingPattern           = regexp.MustCompile(`(?is)\busing\s*\(([^)]*)\)`)
	policyCheckPattern           = regexp.MustCompile(`(?is)\bwith\s+check\s*\(([^)]*)\)`)
	policyRolesPattern           = regexp.MustCompile(`(?is)\bto\s+(.+?)(?:\busing\s*\(|\bwith\s+check\s*\(|$)`)
	policyCurrentUserPattern     = regexp.MustCompile(`(?is)^\s*([a-z_][a-z0-9_"$]*)\s*=\s*current_user\s*$`)
	policyCurrentUserLeftPattern = regexp.MustCompile(`(?is)^\s*current_user\s*=\s*([a-z_][a-z0-9_"$]*)\s*$`)
	createTsConfigPattern        = regexp.MustCompile(`(?is)^\s*create\s+text\s+search\s+configuration\s+([a-z_][a-z0-9_."$]*)\s*\(\s*copy\s*=\s*[a-z_][a-z0-9_."$]*\s*\)\s*;?\s*$`)
	createRuleDoAlsoPattern      = regexp.MustCompile(`(?is)^\s*create\s+rule\s+([a-z_][a-z0-9_"$]*)\s+as\s+on\s+insert\s+to\s+([a-z_][a-z0-9_."$]*)\s+do\s+also\s+(insert\s+into\s+.+?)\s*;?\s*$`)
	alterSystemPattern           = regexp.MustCompile(`(?is)^\s*alter\s+system\s+(?:set|reset)\b.+;?\s*$`)
	clusterIndexPattern          = regexp.MustCompile(`(?is)^\s*cluster\s+((?:"[^"]+"|[a-z_][a-z0-9_$]*)(?:\.(?:"[^"]+"|[a-z_][a-z0-9_$]*))?)\s+on\s+((?:"[^"]+"|[a-z_][a-z0-9_$]*)(?:\.(?:"[^"]+"|[a-z_][a-z0-9_$]*))?)\s*;?\s*$`)
	dropOperatorPattern          = regexp.MustCompile(`(?is)^\s*drop\s+operator\s+if\s+exists\s+(\S+)\s*\(\s*([^,)]*)\s*,\s*([^)]*)\)\s*(?:cascade|restrict)?\s*;?\s*$`)
	dropOperatorClassPattern     = regexp.MustCompile(`(?is)^\s*drop\s+operator\s+class\s+if\s+exists\s+\S+\s+using\s+\S+\s*(?:cascade|restrict)?\s*;?\s*$`)
	dropOperatorFamilyPattern    = regexp.MustCompile(`(?is)^\s*drop\s+operator\s+family\s+if\s+exists\s+\S+\s+using\s+\S+\s*(?:cascade|restrict)?\s*;?\s*$`)
	dropPolicyIfExistsPattern    = regexp.MustCompile(`(?is)^\s*drop\s+policy\s+if\s+exists\s+([a-z_][a-z0-9_"$]*)\s+on\s+([a-z_][a-z0-9_."$]*)\s*(?:cascade|restrict)?\s*;?\s*$`)
	dropRuleIfExistsPattern      = regexp.MustCompile(`(?is)^\s*drop\s+rule\s+if\s+exists\s+([a-z_][a-z0-9_"$]*)\s+on\s+([a-z_][a-z0-9_."$]*)\s*(?:cascade|restrict)?\s*;?\s*$`)
	dropCollationIfExistsPattern = regexp.MustCompile(`(?is)^\s*drop\s+collation\s+if\s+exists\s+[a-z_][a-z0-9_."$]*\s*(?:cascade|restrict)?\s*;?\s*$`)
	dropTextSearchPattern        = regexp.MustCompile(`(?is)^\s*drop\s+text\s+search\s+(configuration|dictionary|parser|template)\s+if\s+exists\s+([a-z_][a-z0-9_."$]*)\s*(?:cascade|restrict)?\s*;?\s*$`)
	securityLabelPattern         = regexp.MustCompile(`(?is)^\s*security\s+label\b`)
	selectStatementPattern       = regexp.MustCompile(`(?is)^\s*select\s+(.+?)\s*;?\s*$`)
	insertReturningPattern       = regexp.MustCompile(`(?is)^\s*insert\s+into\s+([a-z_][a-z0-9_."$]*)(?:\s*\([^)]*\))?\s+.+\breturning\b`)
	updateReturningPattern       = regexp.MustCompile(`(?is)^\s*update\s+([a-z_][a-z0-9_."$]*)\s+.+\breturning\b`)
	deleteReturningPattern       = regexp.MustCompile(`(?is)^\s*delete\s+from\s+([a-z_][a-z0-9_."$]*)\s+.+\breturning\b`)
	rlsIdentifier                = `(?:"[^"]+"|[a-z_][a-z0-9_$]*)(?:\.(?:"[^"]+"|[a-z_][a-z0-9_$]*))?`
	rlsSelectPattern             = regexp.MustCompile(`(?is)^\s*select\s+.+?\s+from\s+(` + rlsIdentifier + `)(.*)$`)
	rlsInsertPattern             = regexp.MustCompile(`(?is)^\s*insert\s+into\s+(` + rlsIdentifier + `)(?:\s*\(([^)]*)\))?\s+values\s*\(([^)]*)\)(.*)$`)
	rlsUpdatePattern             = regexp.MustCompile(`(?is)^\s*update\s+(` + rlsIdentifier + `)\s+set\s+(.+?)(\s+where\s+.+?)?(\s+returning\s+.*)?;?\s*$`)
	rlsDeletePattern             = regexp.MustCompile(`(?is)^\s*delete\s+from\s+(` + rlsIdentifier + `)(.*)$`)
	updatableViewInsertPattern   = regexp.MustCompile(`(?is)^\s*insert\s+into\s+(` + rlsIdentifier + `)(.*)$`)
	updatableViewUpdatePattern   = regexp.MustCompile(`(?is)^\s*update\s+(` + rlsIdentifier + `)(.*)$`)
	updatableViewDeletePattern   = regexp.MustCompile(`(?is)^\s*delete\s+from\s+(` + rlsIdentifier + `)(.*)$`)
	rlsPredicateInsertPoint      = regexp.MustCompile(`(?is)\s(returning|order\s+by|group\s+by|limit|offset)\s`)
	rlsUpdateSetKeyword          = regexp.MustCompile(`(?is)\sset\s`)
	rlsUpdateSetEnd              = regexp.MustCompile(`(?is)\s(where|returning)\s`)
	rlsWherePattern              = regexp.MustCompile(`(?is)\swhere\s`)
	returningTableoidPattern     = regexp.MustCompile(`(?is)\btableoid\b(?:\s*::\s*regclass)?`)
	textSearchMatchPattern       = regexp.MustCompile(`(?is)(to_tsvector\s*\([^)]*\))\s*@@\s*(to_tsquery\s*\([^)]*\))`)
	xmlElementNamePattern        = regexp.MustCompile(`(?is)xmlelement\s*\(\s*name\s+([a-z_][a-z0-9_$]*)\s*\)`)
	xmlForestCallPattern         = regexp.MustCompile(`(?is)xmlforest\s*\((.+?)\)`)
	xmlForestArgPattern          = regexp.MustCompile(`(?is)^\s*(.+?)\s+as\s+([a-z_][a-z0-9_$]*)\s*$`)
	temporalOverlapsPattern      = regexp.MustCompile(`(?is)\(\s*(date\s+'[^']+')\s*,\s*((?:date|interval)\s+'[^']+')\s*\)\s+overlaps\s+\(\s*(date\s+'[^']+')\s*,\s*((?:date|interval)\s+'[^']+')\s*\)`)
	pgInputErrorInfoPattern      = regexp.MustCompile(`(?is)\(\s*pg_input_error_info\s*\(([^)]*)\)\s*\)\s*\.\s*sql_error_code`)
	systemUserPattern            = regexp.MustCompile(`(?i)\bsystem_user\b`)
	anyValuePattern              = regexp.MustCompile(`(?i)\bany_value\s*\(`)
	advancedGroupByPattern       = regexp.MustCompile(`(?is)^\s*select\s+coalesce\s*\(\s*([a-z_][a-z0-9_]*)\s*,\s*'([^']*)'\s*\)\s+as\s+([a-z_][a-z0-9_]*)\s*,\s*coalesce\s*\(\s*([a-z_][a-z0-9_]*)\s*,\s*'([^']*)'\s*\)\s+as\s+([a-z_][a-z0-9_]*)\s*,\s*sum\s*\(\s*([a-z_][a-z0-9_]*)\s*\)::text\s+as\s+([a-z_][a-z0-9_]*)\s+from\s+([a-z_][a-z0-9_]*)\s+group\s+by\s+(.+?)\s+order\s+by\s+.+?;?\s*$`)
)

var createIndexConcurrentlyPattern = regexp.MustCompile(`(?is)^\s*create\s+(?:unique\s+)?index\s+concurrently\b`)
var dropIndexConcurrentlyPattern = regexp.MustCompile(`(?is)^\s*drop\s+index\s+concurrently\b`)
var readOnlyTemporaryCreatePattern = regexp.MustCompile(`(?is)^\s*create\s+(?:temp|temporary)\s+(?:table|sequence)\b`)

func (h *ConnectionHandler) convertedAlterSystem(query string) (ConvertedQuery, bool, error) {
	if !alterSystemPattern.MatchString(query) {
		return ConvertedQuery{}, false, nil
	}
	if h.inTransaction {
		return ConvertedQuery{}, true, pgerror.Newf(pgcode.ActiveSQLTransaction,
			"ALTER SYSTEM cannot run inside a transaction block")
	}
	return ConvertedQuery{}, true, pgerror.New(pgcode.FeatureNotSupported, "ALTER SYSTEM is not yet supported")
}

func convertedClusterIndex(query string) (ConvertedQuery, bool) {
	matches := clusterIndexPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	indexSchema, indexName := splitQualifiedCatalogName(matches[1])
	tableSchema, tableName := splitQualifiedCatalogName(matches[2])
	schemaName := tableSchema
	if schemaName == "" {
		schemaName = indexSchema
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewClusterIndex(schemaName, tableName, indexName),
		},
		StatementTag: "CLUSTER",
	}, true
}

func convertedCreateTransform(query string) (ConvertedQuery, bool) {
	matches := createTransformPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	body := matches[3]
	fromSQL := ""
	if fromMatches := transformFromPattern.FindStringSubmatch(body); fromMatches != nil {
		fromSQL = normalizeTransformFunctionName(fromMatches[1])
	}
	toSQL := ""
	if toMatches := transformToPattern.FindStringSubmatch(body); toMatches != nil {
		toSQL = normalizeTransformFunctionName(toMatches[1])
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewCreateTransform(matches[1], matches[2], fromSQL, toSQL),
		},
		StatementTag: "CREATE TRANSFORM",
	}, true
}

func normalizeTransformFunctionName(name string) string {
	name = strings.TrimSpace(name)
	if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
		return strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
	}
	return strings.ToLower(name)
}

func convertedCreateConversion(query string) (ConvertedQuery, bool) {
	matches := createConversionPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	namespace, name := splitQualifiedCatalogName(matches[2])
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewCreateConversion(
				name,
				namespace,
				conversionEncodingCode(matches[3]),
				conversionEncodingCode(matches[4]),
				normalizeTransformFunctionName(matches[5]),
				strings.TrimSpace(matches[1]) != "",
			),
		},
		StatementTag: "CREATE CONVERSION",
	}, true
}

func convertedDropConversion(query string) (ConvertedQuery, bool) {
	matches := dropConversionPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	namespace, name := splitQualifiedCatalogName(matches[2])
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewDropConversion(name, namespace, strings.TrimSpace(matches[1]) != ""),
		},
		StatementTag: "DROP CONVERSION",
	}, true
}

func splitQualifiedCatalogName(raw string) (namespace string, name string) {
	parts := strings.Split(raw, ".")
	if len(parts) == 2 {
		return normalizeTransformFunctionName(parts[0]), normalizeTransformFunctionName(parts[1])
	}
	return "", normalizeTransformFunctionName(raw)
}

func conversionEncodingCode(encoding string) int32 {
	switch strings.ToUpper(strings.TrimSpace(encoding)) {
	case "UTF8", "UTF-8":
		return 6
	case "LATIN1", "ISO88591", "ISO-8859-1":
		return 8
	default:
		return 0
	}
}

func convertedCreateCast(query string) (ConvertedQuery, bool) {
	matches := createCastPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewCreateCast(matches[1], matches[2], normalizeTransformFunctionName(matches[3])),
		},
		StatementTag: "CREATE CAST",
	}, true
}

func convertedDropCast(query string) (ConvertedQuery, bool) {
	matches := dropCastPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewDropCast(matches[2], matches[3], strings.TrimSpace(matches[1]) != ""),
		},
		StatementTag: "DROP CAST",
	}, true
}

func convertedCreateOperator(query string) (ConvertedQuery, bool) {
	matches := createOperatorPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	namespace, name := splitQualifiedCatalogName(matches[1])
	options := parseCreateOperatorOptions(matches[2])
	leftType, hasLeft := options["leftarg"]
	rightType, hasRight := options["rightarg"]
	function, hasFunction := options["procedure"]
	if !hasLeft || !hasRight || !hasFunction {
		return ConvertedQuery{}, false
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewCreateOperator(namespace, name, leftType, rightType, function),
		},
		StatementTag: "CREATE OPERATOR",
	}, true
}

func convertedCreatePolicy(query string) (ConvertedQuery, bool) {
	matches := createPolicyPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	schema, table := splitQualifiedCatalogName(matches[2])
	policy := rowsecurity.Policy{
		Name:    matches[1],
		Command: strings.ToLower(matches[3]),
	}
	body := matches[4]
	policy.Roles = rowSecurityPolicyRoles(body)
	if usingMatches := policyUsingPattern.FindStringSubmatch(body); usingMatches != nil {
		policy.UsingAll, policy.UsingColumn = rowSecurityPolicyExpression(usingMatches[1])
	}
	if checkMatches := policyCheckPattern.FindStringSubmatch(body); checkMatches != nil {
		policy.CheckAll, policy.CheckColumn = rowSecurityPolicyExpression(checkMatches[1])
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewCreatePolicy(
				doltdb.TableName{Name: table, Schema: schema},
				policy,
			),
		},
		StatementTag: "CREATE POLICY",
	}, true
}

func convertedCreateCollation(query string) (ConvertedQuery, bool) {
	matches := createCollationPattern.FindStringSubmatch(query)
	if matches == nil || !isSupportedCreateCollationBody(matches[2]) {
		return ConvertedQuery{}, false
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NoOp{},
		},
		StatementTag: "CREATE COLLATION",
	}, true
}

func isSupportedCreateCollationBody(body string) bool {
	options := map[string]string{}
	for _, option := range splitTopLevelComma(body) {
		key, value, ok := strings.Cut(option, "=")
		if !ok {
			continue
		}
		options[strings.ToLower(strings.TrimSpace(key))] = strings.Trim(strings.TrimSpace(value), "'")
	}
	return strings.EqualFold(options["provider"], "icu") && strings.TrimSpace(options["locale"]) != ""
}

func rowSecurityPolicyRoles(body string) []string {
	matches := policyRolesPattern.FindStringSubmatch(body)
	if matches == nil {
		return nil
	}
	rawRoles := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(matches[1]), ";"))
	var roles []string
	for _, role := range splitSQLList(rawRoles) {
		role = rowsecurity.NormalizeName(role)
		if role != "" {
			roles = append(roles, role)
		}
	}
	return roles
}

func rowSecurityPolicyExpression(expr string) (bool, string) {
	if strings.EqualFold(strings.TrimSpace(expr), "true") {
		return true, ""
	}
	return false, rowSecurityCurrentUserColumn(expr)
}

func rowSecurityCurrentUserColumn(expr string) string {
	matches := policyCurrentUserPattern.FindStringSubmatch(expr)
	if matches == nil {
		matches = policyCurrentUserLeftPattern.FindStringSubmatch(expr)
	}
	if matches == nil {
		return ""
	}
	return rowsecurity.NormalizeName(matches[1])
}

func parseCreateOperatorOptions(body string) map[string]string {
	options := map[string]string{}
	for _, part := range strings.Split(body, ",") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		options[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return options
}

func convertedCreateTextSearchConfiguration(query string) (ConvertedQuery, bool) {
	matches := createTsConfigPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false
	}
	namespace, name := splitQualifiedCatalogName(matches[1])
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewCreateTextSearchConfiguration(namespace, name),
		},
		StatementTag: "CREATE TEXT SEARCH CONFIGURATION",
	}, true
}

func rewriteCreateRuleDoAlsoInsert(query string) (string, string, bool) {
	matches := createRuleDoAlsoPattern.FindStringSubmatch(query)
	if matches == nil {
		return "", "", false
	}
	ruleName := normalizeTransformFunctionName(matches[1])
	tableName := matches[2]
	insertStatement := strings.TrimSuffix(strings.TrimSpace(matches[3]), ";")
	functionName := quoteSQLIdentifier(ruleBackingFunctionName(ruleName))
	triggerName := quoteSQLIdentifier(ruleName)
	return fmt.Sprintf(
		"CREATE FUNCTION %s() RETURNS trigger AS $$ BEGIN %s; RETURN NEW; END; $$ LANGUAGE plpgsql; CREATE TRIGGER %s AFTER INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()",
		functionName,
		insertStatement,
		triggerName,
		tableName,
		functionName,
	), tableName, true
}

func ruleBackingFunctionName(ruleName string) string {
	return "__dolt_rule_" + strings.ReplaceAll(ruleName, `"`, "_")
}

func (h *ConnectionHandler) checkCreateRuleTableOwnership(query string, rawTableName string) error {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query)
	if err != nil {
		return err
	}
	rawSchema, tableName := splitQualifiedCatalogName(rawTableName)
	schemaName, err := core.GetSchemaName(sqlCtx, nil, rawSchema)
	if err != nil {
		return err
	}
	table, err := core.GetSqlTableFromContext(sqlCtx, "", doltdb.TableName{Name: tableName, Schema: schemaName})
	if err != nil {
		return err
	}
	owner := ""
	if commented, ok := table.(sql.CommentedTable); ok {
		owner = tablemetadata.Owner(commented.Comment())
	}
	if owner == "" {
		owner = "postgres"
	}
	if owner == sqlCtx.Client().User {
		return nil
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(sqlCtx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege, "must be owner of table %s", tableName)
}

func convertedDropIfExistsNoOp(query string) (ConvertedQuery, bool) {
	statementTag := ""
	switch {
	case dropOperatorPattern.MatchString(query):
		matches := dropOperatorPattern.FindStringSubmatch(query)
		namespace, name := splitQualifiedCatalogName(matches[1])
		return ConvertedQuery{
			String: query,
			AST: sqlparser.InjectedStatement{
				Statement: node.NewDropOperator(namespace, name, matches[2], matches[3], true),
			},
			StatementTag: "DROP OPERATOR",
		}, true
	case dropOperatorClassPattern.MatchString(query):
		statementTag = "DROP OPERATOR CLASS"
	case dropOperatorFamilyPattern.MatchString(query):
		statementTag = "DROP OPERATOR FAMILY"
	case dropTextSearchPattern.MatchString(query):
		matches := dropTextSearchPattern.FindStringSubmatch(query)
		statementTag = "DROP TEXT SEARCH " + strings.ToUpper(matches[1])
		if strings.EqualFold(matches[1], "configuration") {
			namespace, name := splitQualifiedCatalogName(matches[2])
			return ConvertedQuery{
				String: query,
				AST: sqlparser.InjectedStatement{
					Statement: node.NewDropTextSearchConfiguration(namespace, name, true),
				},
				StatementTag: statementTag,
			}, true
		}
	case dropPolicyIfExistsPattern.MatchString(query):
		matches := dropPolicyIfExistsPattern.FindStringSubmatch(query)
		tableSchema, tableName := splitQualifiedCatalogName(matches[2])
		return ConvertedQuery{
			String: query,
			AST: sqlparser.InjectedStatement{
				Statement: node.NewDropPolicy(
					doltdb.TableName{Schema: tableSchema, Name: tableName},
					matches[1],
					true,
				),
			},
			StatementTag: "DROP POLICY",
		}, true
	case dropRuleIfExistsPattern.MatchString(query):
		matches := dropRuleIfExistsPattern.FindStringSubmatch(query)
		tableSchema, tableName := splitQualifiedCatalogName(matches[2])
		ruleName := normalizeTransformFunctionName(matches[1])
		return ConvertedQuery{
			String: query,
			AST: sqlparser.InjectedStatement{
				Statement: node.NewDropRule(tableSchema, tableName, ruleName, ruleBackingFunctionName(ruleName), true),
			},
			StatementTag: "DROP RULE",
		}, true
	case dropCollationIfExistsPattern.MatchString(query):
		statementTag = "DROP COLLATION"
	default:
		return ConvertedQuery{}, false
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NoOp{},
		},
		StatementTag: statementTag,
	}, true
}

func containsSecurityLabel(query string) bool {
	return securityLabelPattern.MatchString(query)
}

func rewriteCustomOperatorSelect(query string) (string, bool) {
	matches := selectStatementPattern.FindStringSubmatch(query)
	if matches == nil {
		return "", false
	}
	var operators []auth.Operator
	auth.LockRead(func() {
		operators = auth.GetAllOperators()
	})
	if len(operators) == 0 {
		return "", false
	}
	projections := splitTopLevelComma(matches[1])
	rewritten := make([]string, len(projections))
	changed := false
	for i, projection := range projections {
		rewrittenProjection := projection
		for _, operator := range operators {
			if next, ok := rewriteCustomOperatorProjection(projection, operator); ok {
				rewrittenProjection = next
				changed = true
				break
			}
		}
		rewritten[i] = rewrittenProjection
	}
	if !changed {
		return "", false
	}
	return "SELECT " + strings.Join(rewritten, ", "), true
}

func rewriteTextSearchQuery(query string) (string, bool) {
	rewritten := strings.ReplaceAll(query, "::regconfig", "")
	rewritten = textSearchMatchPattern.ReplaceAllString(rewritten, "ts_match_vq($1, $2)")
	return rewritten, rewritten != query
}

func rewriteXmlConstructors(query string) (string, bool) {
	rewritten := xmlElementNamePattern.ReplaceAllString(query, "xmlelement('$1')")
	rewritten = xmlForestCallPattern.ReplaceAllStringFunc(rewritten, func(call string) string {
		matches := xmlForestCallPattern.FindStringSubmatch(call)
		if matches == nil {
			return call
		}
		args := splitTopLevelComma(matches[1])
		rewrittenArgs := make([]string, 0, len(args)*2)
		for _, arg := range args {
			argMatches := xmlForestArgPattern.FindStringSubmatch(arg)
			if argMatches == nil {
				return call
			}
			rewrittenArgs = append(rewrittenArgs, quoteSQLString(argMatches[2]), strings.TrimSpace(argMatches[1]))
		}
		return "xmlforest(" + strings.Join(rewrittenArgs, ", ") + ")"
	})
	return rewritten, rewritten != query
}

func rewritePostgres16IntegerLiterals(query string) (string, bool) {
	var rewritten strings.Builder
	changed := false
	for i := 0; i < len(query); {
		if query[i] == '\'' {
			start := i
			i++
			for i < len(query) {
				if query[i] == '\'' {
					i++
					if i < len(query) && query[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			rewritten.WriteString(query[start:i])
			continue
		}
		if isSQLDigit(query[i]) && (i == 0 || !isSQLIdentifierPart(query[i-1])) {
			if next, ok := rewritePostgres16IntegerLiteralAt(query, i); ok {
				rewritten.WriteString(next.literal)
				i = next.end
				changed = true
				continue
			}
		}
		rewritten.WriteByte(query[i])
		i++
	}
	if !changed {
		return "", false
	}
	return rewritten.String(), true
}

type rewrittenIntegerLiteral struct {
	literal string
	end     int
}

func rewritePostgres16IntegerLiteralAt(query string, start int) (rewrittenIntegerLiteral, bool) {
	if start+2 < len(query) && query[start] == '0' {
		base := 0
		switch query[start+1] {
		case 'x', 'X':
			base = 16
		case 'o', 'O':
			base = 8
		case 'b', 'B':
			base = 2
		}
		if base != 0 {
			end := start + 2
			for end < len(query) && (query[end] == '_' || digitValue(query[end]) >= 0 && digitValue(query[end]) < base) {
				end++
			}
			if end == start+2 || end < len(query) && isSQLIdentifierPart(query[end]) {
				return rewrittenIntegerLiteral{}, false
			}
			value, err := strconv.ParseUint(strings.ReplaceAll(query[start+2:end], "_", ""), base, 64)
			if err != nil {
				return rewrittenIntegerLiteral{}, false
			}
			return rewrittenIntegerLiteral{literal: strconv.FormatUint(value, 10), end: end}, true
		}
	}
	end := start
	hasUnderscore := false
	for end < len(query) && (isSQLDigit(query[end]) || query[end] == '_') {
		if query[end] == '_' {
			hasUnderscore = true
		}
		end++
	}
	if !hasUnderscore || end < len(query) && isSQLIdentifierPart(query[end]) {
		return rewrittenIntegerLiteral{}, false
	}
	return rewrittenIntegerLiteral{literal: strings.ReplaceAll(query[start:end], "_", ""), end: end}, true
}

func isSQLDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isSQLIdentifierPart(ch byte) bool {
	return ch == '_' || ch == '$' || ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func digitValue(ch byte) int {
	switch {
	case ch >= '0' && ch <= '9':
		return int(ch - '0')
	case ch >= 'a' && ch <= 'f':
		return int(ch-'a') + 10
	case ch >= 'A' && ch <= 'F':
		return int(ch-'A') + 10
	default:
		return -1
	}
}

func rewriteTemporalOverlaps(query string) (string, bool) {
	rewritten := temporalOverlapsPattern.ReplaceAllString(query, "__doltgres_overlaps($1, $2, $3, $4)")
	return rewritten, rewritten != query
}

func rewritePostgres16BuiltinSyntax(query string) (string, bool) {
	rewritten := pgInputErrorInfoPattern.ReplaceAllString(query, "pg_input_error_info_sql_error_code($1)")
	rewritten = systemUserPattern.ReplaceAllString(rewritten, "system_user()")
	rewritten = anyValuePattern.ReplaceAllString(rewritten, "min(")
	return rewritten, rewritten != query
}

func rewriteDMLReturningTableOID(query string) (string, bool) {
	tableName := ""
	for _, pattern := range []*regexp.Regexp{insertReturningPattern, updateReturningPattern, deleteReturningPattern} {
		if matches := pattern.FindStringSubmatch(query); matches != nil {
			tableName = regclassLiteralTableName(matches[1])
			break
		}
	}
	if tableName == "" || !returningTableoidPattern.MatchString(query) {
		return "", false
	}
	rewritten := returningTableoidPattern.ReplaceAllString(query, quoteSQLString(tableName)+"::regclass")
	return rewritten, rewritten != query
}

func regclassLiteralTableName(raw string) string {
	raw = strings.TrimSpace(raw)
	parts := strings.Split(raw, ".")
	for i, part := range parts {
		parts[i] = normalizeTransformFunctionName(part)
	}
	return strings.Join(parts, ".")
}

var sqlMergeUpdateInsertPattern = regexp.MustCompile(`(?is)^\s*MERGE\s+INTO\s+(.+?)\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_$]*)\s+USING\s+(.+?)\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_$]*)\s+ON\s+(.+?)\s+WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+(.+?)\s+WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*\((.+?)\)\s+VALUES\s*\((.+?)\)\s*;?\s*$`)

func convertedSqlMergeUpdateInsert(query string) (ConvertedQuery, bool, error) {
	matches := sqlMergeUpdateInsertPattern.FindStringSubmatch(query)
	if matches == nil {
		return ConvertedQuery{}, false, nil
	}
	targetTable := strings.TrimSpace(matches[1])
	targetAlias := strings.TrimSpace(matches[2])
	sourceTable := strings.TrimSpace(matches[3])
	sourceAlias := strings.TrimSpace(matches[4])
	onExpr := strings.TrimSpace(matches[5])
	updateSet := strings.TrimSpace(matches[6])
	insertColumns := strings.TrimSpace(matches[7])
	insertValues := strings.TrimSpace(matches[8])
	if _, ok := sqlMergeConflictColumn(onExpr, targetAlias, sourceAlias); !ok {
		return ConvertedQuery{}, false, nil
	}
	updateSet = sqlMergeOnDuplicateUpdateSet(updateSet, targetAlias)
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s AS %s WHERE true ON DUPLICATE KEY UPDATE %s",
		targetTable, insertColumns, insertValues, sourceTable, sourceAlias, updateSet)
	vitessAST, err := sqlparser.Parse(insertQuery)
	if err != nil {
		return ConvertedQuery{}, true, err
	}
	return ConvertedQuery{
		String:       insertQuery,
		AST:          vitessAST,
		StatementTag: "MERGE",
	}, true, nil
}

func sqlMergeOnDuplicateUpdateSet(updateSet string, targetAlias string) string {
	targetColumnPattern := regexp.MustCompile(regexp.QuoteMeta(targetAlias) + `\.([A-Za-z_][A-Za-z0-9_$]*)`)
	return targetColumnPattern.ReplaceAllString(updateSet, "$1")
}

func sqlMergeConflictColumn(onExpr string, targetAlias string, sourceAlias string) (string, bool) {
	parts := strings.Split(onExpr, "=")
	if len(parts) != 2 {
		return "", false
	}
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])
	targetPrefix := targetAlias + "."
	sourcePrefix := sourceAlias + "."
	if strings.HasPrefix(left, targetPrefix) && strings.HasPrefix(right, sourcePrefix) {
		return strings.TrimSpace(strings.TrimPrefix(left, targetPrefix)), true
	}
	if strings.HasPrefix(left, sourcePrefix) && strings.HasPrefix(right, targetPrefix) {
		return strings.TrimSpace(strings.TrimPrefix(right, targetPrefix)), true
	}
	return "", false
}

func rewriteAdvancedGroupByQuery(query string) (string, bool) {
	matches := advancedGroupByPattern.FindStringSubmatch(query)
	if matches == nil {
		return "", false
	}
	groupByClause := strings.ToLower(strings.Join(strings.Fields(matches[10]), " "))
	var groupingSets [][]string
	switch {
	case strings.HasPrefix(groupByClause, "grouping sets"):
		groupingSets = [][]string{{matches[1], matches[4]}, {matches[1]}, {}}
	case strings.HasPrefix(groupByClause, "rollup"):
		groupingSets = [][]string{{matches[1], matches[4]}, {matches[1]}, {}}
	case strings.HasPrefix(groupByClause, "cube"):
		groupingSets = [][]string{{matches[1], matches[4]}, {matches[1]}, {matches[4]}, {}}
	default:
		return "", false
	}
	firstCol, firstAll, firstAlias := matches[1], matches[2], matches[3]
	secondCol, secondAll, secondAlias := matches[4], matches[5], matches[6]
	amountCol, totalAlias, tableName := matches[7], matches[8], matches[9]
	queries := make([]string, 0, len(groupingSets))
	for _, groupingSet := range groupingSets {
		queries = append(queries, advancedGroupBySelect(tableName, firstCol, secondCol, amountCol, groupingSet))
	}
	return fmt.Sprintf(
		"SELECT COALESCE(%s_key, '%s') AS %s, COALESCE(%s_key, '%s') AS %s, %s FROM (%s) AS grouping_rewrite ORDER BY 1, 2",
		firstCol, firstAll, firstAlias,
		secondCol, secondAll, secondAlias,
		totalAlias,
		strings.Join(queries, " UNION ALL "),
	), true
}

func advancedGroupBySelect(tableName string, firstCol string, secondCol string, amountCol string, groupingSet []string) string {
	firstExpr := "NULL"
	secondExpr := "NULL"
	groupBy := make([]string, 0, len(groupingSet))
	for _, col := range groupingSet {
		switch col {
		case firstCol:
			firstExpr = firstCol
			groupBy = append(groupBy, firstCol)
		case secondCol:
			secondExpr = secondCol
			groupBy = append(groupBy, secondCol)
		}
	}
	query := fmt.Sprintf("SELECT %s AS %s_key, %s AS %s_key, sum(%s)::text AS total FROM %s",
		advancedGroupByKeyExpr(firstExpr), firstCol, advancedGroupByKeyExpr(secondExpr), secondCol, amountCol, tableName)
	if len(groupBy) > 0 {
		query += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	return query
}

func advancedGroupByKeyExpr(expr string) string {
	if expr == "NULL" {
		return "NULL::text"
	}
	return expr
}

func rewriteCustomOperatorProjection(projection string, operator auth.Operator) (string, bool) {
	pattern := regexp.MustCompile(`(?is)^\s*(.+?)\s+` + regexp.QuoteMeta(operator.Name) + `\s+(.+?)\s*$`)
	matches := pattern.FindStringSubmatch(projection)
	if matches == nil {
		return "", false
	}
	functionName := operator.Function
	if operator.FunctionSchema != "" {
		functionName = operator.FunctionSchema + "." + functionName
	}
	return functionName + "(" + strings.TrimSpace(matches[1]) + ", " + strings.TrimSpace(matches[2]) + ")", true
}

func splitTopLevelComma(s string) []string {
	var parts []string
	start := 0
	depth := 0
	inString := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString && depth > 0 {
				depth--
			}
		case ',':
			if !inString && depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

func convertedAlterLargeObjectOwner(query string) (ConvertedQuery, bool) {
	fields := strings.Fields(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	if len(fields) != 7 ||
		!strings.EqualFold(fields[0], "alter") ||
		!strings.EqualFold(fields[1], "large") ||
		!strings.EqualFold(fields[2], "object") ||
		!strings.EqualFold(fields[4], "owner") ||
		!strings.EqualFold(fields[5], "to") {
		return ConvertedQuery{}, false
	}
	oid, err := strconv.ParseUint(fields[3], 10, 32)
	if err != nil {
		return ConvertedQuery{}, false
	}
	return ConvertedQuery{
		String: query,
		AST: sqlparser.InjectedStatement{
			Statement: node.NewAlterLargeObjectOwner(uint32(oid), fields[6]),
		},
		StatementTag: "ALTER LARGE OBJECT",
	}, true
}

func containsCreateEventTrigger(query string) bool {
	return containsKeywordSequence(query, "create", "event", "trigger")
}

func containsAlterTextSearchObject(query string) bool {
	return containsKeywordSequence(query, "alter", "text", "search")
}

func containsAlterRule(query string) bool {
	return containsKeywordSequence(query, "alter", "rule")
}

func containsCreateCollation(query string) bool {
	return containsKeywordSequence(query, "create", "collation")
}

func containsExcludeConstraint(query string) bool {
	return containsKeywordSequence(query, "exclude", "using")
}

func containsKeywordSequence(query string, sequence ...string) bool {
	words := strings.Fields(strings.ToLower(query))
	for i := 0; i+len(sequence) <= len(words); i++ {
		matches := true
		for j := range sequence {
			if words[i+j] != sequence[j] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

// discardAll handles the DISCARD ALL command
func (h *ConnectionHandler) discardAll(query ConvertedQuery) error {
	if h.inTransaction {
		// PostgreSQL refuses DISCARD ALL inside an explicit transaction block
		// because the statement resets session state (prepared statements,
		// temporary tables, sequences, plans) that cannot be safely rolled back
		// if the surrounding transaction aborts.
		return pgerror.Newf(pgcode.ActiveSQLTransaction,
			"DISCARD ALL cannot run inside a transaction block")
	}

	for name := range h.preparedStatements {
		delete(h.preparedStatements, name)
	}
	sessionstate.DeleteAllPreparedStatements(h.mysqlConn.ConnectionID)
	functionstats.DeleteAll(h.mysqlConn.ConnectionID)
	notifications.UnlistenAll(h.mysqlConn.ConnectionID)

	err := h.doltgresHandler.ComResetConnection(h.mysqlConn)
	if err != nil {
		return err
	}

	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.StatementTag),
	})
}

// discardTemp handles DISCARD TEMP by dropping all temporary tables visible to
// the current session in the current database.
func (h *ConnectionHandler) discardTemp(query ConvertedQuery) error {
	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, query.StatementTag)
	if err != nil {
		return err
	}
	dbName := sqlCtx.GetCurrentDatabase()
	db, err := core.GetSqlDatabaseFromContext(sqlCtx, dbName)
	if err != nil {
		return err
	}
	tempDB, ok := db.(sql.TemporaryTableDatabase)
	if ok {
		tempTables, err := tempDB.GetAllTemporaryTables(sqlCtx)
		if err != nil {
			return err
		}
		session := dsess.DSessFromSess(sqlCtx.Session)
		for _, table := range tempTables {
			session.DropTemporaryTable(sqlCtx, dbName, table.Name())
		}
	}
	return h.send(&pgproto3.CommandComplete{
		CommandTag: []byte(query.StatementTag),
	})
}

// handleCopyFromStdinQuery handles the COPY FROM STDIN query at the Doltgres layer, without passing it to the engine.
// COPY FROM STDIN can't be handled directly by the GMS engine, since COPY FROM STDIN relies on multiple messages sent
// over the wire.
func (h *ConnectionHandler) handleCopyFromStdinQuery(copyFrom *node.CopyFrom, conn net.Conn) (err error) {
	h.copyFromStdinFailed = false
	h.copyFromStdinState = &copyFromStdinState{
		copyFromStdinNode: copyFrom,
	}
	defer func() {
		if err != nil {
			h.copyFromStdinState = nil
			h.copyFromStdinFailed = true
		}
	}()

	sqlCtx, err := h.doltgresHandler.NewContext(context.Background(), h.mysqlConn, "COPY FROM STDIN")
	if err != nil {
		return err
	}
	if err = h.checkCopyFromRowSecurity(copyFrom); err != nil {
		return err
	}
	if err = startTransactionIfNecessary(sqlCtx); err != nil {
		return err
	}
	if err = h.initializeCopyFromState(sqlCtx, h.copyFromStdinState); err != nil {
		return err
	}

	overallFormat := byte(0)
	if copyFrom.CopyOptions.CopyFormat == tree.CopyFormatBinary {
		overallFormat = 1
	}
	columnFormatCodes := make([]uint16, len(h.copyFromStdinState.dataLoader.Schema(sqlCtx)))
	if overallFormat == 1 {
		for i := range columnFormatCodes {
			columnFormatCodes[i] = 1
		}
	}

	return h.send(&pgproto3.CopyInResponse{
		OverallFormat:     overallFormat,
		ColumnFormatCodes: columnFormatCodes,
	})
}

func (h *ConnectionHandler) checkCopyFromRowSecurity(copyFrom *node.CopyFrom) error {
	if copyFrom == nil {
		return nil
	}
	rawTable := copyFrom.TableName.Name
	if copyFrom.TableName.Schema != "" {
		rawTable = copyFrom.TableName.Schema + "." + rawTable
	}
	_, _, active, err := h.rowSecurityState(rawTable)
	if err != nil || !active {
		return err
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"COPY FROM not supported with row-level security for table %s",
		rawTable,
	)
}

// DiscardToSync discards all messages in the buffer until a Sync has been reached. If a Sync was never sent, then this
// may cause the connection to lock until the client send a Sync, as their request structure was malformed.
func (h *ConnectionHandler) discardToSync() error {
	for {
		message, err := h.backend.Receive()
		if err != nil {
			return err
		}

		if _, ok := message.(*pgproto3.Sync); ok {
			return nil
		}
	}
}

// Send sends the given message over the connection.
func (h *ConnectionHandler) send(message pgproto3.BackendMessage) error {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	h.backend.Send(message)
	return h.backend.Flush()
}

// flush sends any backend messages queued by sendBuffered. PostgreSQL's
// extended query protocol lets the client batch Parse/Bind/Execute messages
// until Sync, but a Flush message must make pending responses visible earlier.
func (h *ConnectionHandler) flush() error {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	return h.backend.Flush()
}

// sendBuffered queues a backend message without forcing a socket flush. Query
// execution uses this for responses that are immediately followed by Sync,
// Flush, or ReadyForQuery, which flushes the full response batch.
func (h *ConnectionHandler) sendBuffered(message pgproto3.BackendMessage) {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	h.backend.Send(message)
}

// returnsRow returns whether the query returns set of rows such as SELECT and FETCH statements.
func returnsRow(query ConvertedQuery) bool {
	switch query.StatementTag {
	case "SELECT", "SHOW", "FETCH", "EXPLAIN", "SHOW TABLES", "SHOW CREATE", "SHOW INDEXES FROM TABLE", "SHOW DATABASES", "SHOW SCHEMAS":
		return true
	case "INSERT", "UPDATE", "DELETE":
		return hasReturningClause(query.AST)
	default:
		return false
	}
}

func queryReturnsRows(query ConvertedQuery, fields []pgproto3.FieldDescription) bool {
	if returnsRow(query) {
		return true
	}
	return query.StatementTag == "CALL" && len(fields) > 0
}

// hasReturningClause return true if |statement| has a RETURNING clause defined.
func hasReturningClause(statement sqlparser.Statement) bool {
	hasReturningClause := false
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.Update:
			if len(node.Returning) > 0 {
				hasReturningClause = true
			}
			return false, nil
		case *sqlparser.Insert:
			if len(node.Returning) > 0 {
				hasReturningClause = true
			}
			return false, nil
		case *sqlparser.Delete:
			if len(node.Returning) > 0 {
				hasReturningClause = true
			}
		}
		return true, nil
	}, statement)

	return hasReturningClause
}

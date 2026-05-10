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

package _go

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestGORMClientSmoke runs the real GORM PostgreSQL dialect against Doltgres.
// The raw Go driver harnesses pin database/sql adapters; this covers GORM's
// AutoMigrate catalog path, association loading, JSONB/text[] values, raw
// parameter binding, pooled reads, and transaction boundaries.
func TestGORMClientSmoke(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH; install Go to enable this harness")
	}
	if testing.Short() {
		t.Skip("GORM harness downloads module dependencies; skipped under -short")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	work := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(work, "go.mod"), []byte(`module doltgres-gorm-harness

go 1.23

require (
	github.com/lib/pq v1.10.9
	gorm.io/datatypes v1.2.7
	gorm.io/driver/postgres v1.5.11
	gorm.io/gorm v1.25.12
)
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(work, "main.go"), []byte(`package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Account struct {
	ID     int            `+"`gorm:\"primaryKey\"`"+`
	Email  string         `+"`gorm:\"type:text;not null;uniqueIndex\"`"+`
	Active bool           `+"`gorm:\"not null;default:true\"`"+`
	Meta   datatypes.JSON `+"`gorm:\"type:jsonb;not null\"`"+`
	Items  []Item         `+"`gorm:\"foreignKey:AccountID\"`"+`
}

func (Account) TableName() string {
	return "gorm_accounts"
}

type Item struct {
	ID        int            `+"`gorm:\"primaryKey\"`"+`
	AccountID int           `+"`gorm:\"not null;index\"`"+`
	Account   Account       `+"`gorm:\"constraint:OnUpdate:CASCADE,OnDelete:RESTRICT;\"`"+`
	Amount    float64      `+"`gorm:\"type:numeric(10,2);not null\"`"+`
	Tags      pq.StringArray `+"`gorm:\"type:text[];not null\"`"+`
	Payload   datatypes.JSON `+"`gorm:\"type:jsonb;not null\"`"+`
}

func (Item) TableName() string {
	return "gorm_items"
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	dsn := os.Args[1]
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	check(err)
	sqlDB, err := db.DB()
	check(err)
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(2)
	sqlDB.SetMaxIdleConns(2)

	var appName string
	check(db.Raw("SELECT current_setting('application_name')").Scan(&appName).Error)
	if appName != "gorm-harness" {
		panic(fmt.Sprintf("unexpected application_name: %s", appName))
	}

	check(db.AutoMigrate(&Account{}, &Item{}))

	acmeMeta, _ := json.Marshal(map[string]string{"tier": "pro"})
	betaMeta, _ := json.Marshal(map[string]string{"tier": "free"})
	check(db.Create(&[]Account{
		{ID: 1, Email: "acme@example.com", Active: true, Meta: datatypes.JSON(acmeMeta)},
		{ID: 2, Email: "beta@example.com", Active: false, Meta: datatypes.JSON(betaMeta)},
	}).Error)

	payload, _ := json.Marshal(map[string]any{"kind": "invoice", "lines": []int{1, 2}})
	item := Item{
		ID:        10,
		AccountID: 1,
		Amount:    12.34,
		Tags:      pq.StringArray{"red", "blue"},
		Payload:   datatypes.JSON(payload),
	}
	check(db.Create(&item).Error)

	var selected Item
	check(db.Preload("Account").First(&selected, "id = ?", 10).Error)
	if selected.Account.Email != "acme@example.com" || fmt.Sprintf("%.2f", selected.Amount) != "12.34" || selected.Tags[1] != "blue" {
		panic(fmt.Sprintf("unexpected selected item: %#v", selected))
	}

	var raw []struct {
		Email  string
		Amount string
		Tag    string
		Kind   string
	}
	check(db.Raw(
		"SELECT a.email, i.amount::text AS amount, i.tags[2] AS tag, i.payload #>> '{kind}' AS kind "+
			"FROM gorm_items i JOIN gorm_accounts a ON a.id = i.account_id "+
			"WHERE i.account_id = @account_id AND @tag = ANY(i.tags)",
		sql.Named("account_id", 1),
		sql.Named("tag", "blue"),
	).Scan(&raw).Error)
	if len(raw) != 1 || raw[0].Email != "acme@example.com" || raw[0].Amount != "12.34" || raw[0].Tag != "blue" || raw[0].Kind != "invoice" {
		panic(fmt.Sprintf("unexpected raw result: %#v", raw))
	}

	var wg sync.WaitGroup
	names := make([]string, 2)
	errs := make([]error, 2)
	for i, accountID := range []int{1, 2} {
		wg.Add(1)
		go func(i int, accountID int) {
			defer wg.Done()
			var account Account
			errs[i] = db.First(&account, "id = ?", accountID).Error
			names[i] = account.Email
		}(i, accountID)
	}
	wg.Wait()
	for _, err := range errs {
		check(err)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "acme@example.com,beta@example.com" {
		panic(fmt.Sprintf("unexpected concurrent names: %v", names))
	}

	gammaMeta, _ := json.Marshal(map[string]string{"tier": "trial"})
	check(db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&Account{ID: 3, Email: "gamma@example.com", Active: true, Meta: datatypes.JSON(gammaMeta)}).Error
	}))

	rollbackErr := db.Transaction(func(tx *gorm.DB) error {
		check(tx.Create(&Account{ID: 4, Email: "rolled-back@example.com", Active: true, Meta: datatypes.JSON(gammaMeta)}).Error)
		return fmt.Errorf("force rollback")
	})
	if rollbackErr == nil || rollbackErr.Error() != "force rollback" {
		panic(fmt.Sprintf("unexpected rollback error: %v", rollbackErr))
	}

	var summary string
	check(db.Raw("SELECT array_to_string(array_agg(email ORDER BY id), ',') FROM gorm_accounts").Scan(&summary).Error)
	if summary != "acme@example.com,beta@example.com,gamma@example.com" {
		panic(fmt.Sprintf("unexpected summary: %s", summary))
	}
	fmt.Printf(`+"`"+`{"ok":true,"emails":%q}`+"`"+`+"\n", summary)
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	tidy := exec.CommandContext(cmdCtx, goBin, "mod", "tidy")
	tidy.Dir = work
	tidy.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := tidy.CombinedOutput(); err != nil {
		t.Fatalf("go mod tidy for GORM probe failed: %v\n%s", err, string(out))
	}

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=gorm-harness", port)
	cmd := exec.CommandContext(cmdCtx, goBin, "run", ".")
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	cmd.Args = append(cmd.Args, url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "GORM probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"emails":"acme@example.com,beta@example.com,gamma@example.com"`)
}

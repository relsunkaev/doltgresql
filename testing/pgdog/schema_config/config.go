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

package schema_config

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

type Options struct {
	LogicalDatabase    string
	BackendHost        string
	SourceDatabase     string
	DoltgresDatabase   string
	User               string
	Password           string
	SharedSchema       string
	CustomerSchema     string
	CustomerTable      string
	ShardColumn        string
	ShardDataType      string
	SourcePort         int
	DoltgresShardPorts []int
	CustomerIDs        []int64
	CrossShardDisabled bool
}

type Generated struct {
	PgDogTOML        string
	UsersTOML        string
	CustomerToShard  map[int64]int
	ShardToCustomers map[int][]int64
}

func DefaultOptions() Options {
	return Options{
		LogicalDatabase:    "pgdog",
		BackendHost:        "host.docker.internal",
		SourceDatabase:     "pgdog",
		DoltgresDatabase:   "postgres",
		User:               "postgres",
		Password:           "password",
		SharedSchema:       "shared",
		CustomerSchema:     "customer",
		CustomerTable:      "orders",
		ShardColumn:        "customer_id",
		ShardDataType:      "bigint",
		SourcePort:         15437,
		CrossShardDisabled: true,
	}
}

func Generate(opts Options) (Generated, error) {
	opts = fillDefaults(opts)
	if err := validate(opts); err != nil {
		return Generated{}, err
	}

	customerIDs := append([]int64(nil), opts.CustomerIDs...)
	sort.Slice(customerIDs, func(i, j int) bool {
		return customerIDs[i] < customerIDs[j]
	})

	customerToShard := make(map[int64]int, len(customerIDs))
	shardToCustomers := map[int][]int64{}
	for i, customerID := range customerIDs {
		shardID := 1 + (i % len(opts.DoltgresShardPorts))
		customerToShard[customerID] = shardID
		shardToCustomers[shardID] = append(shardToCustomers[shardID], customerID)
	}

	return Generated{
		PgDogTOML:        renderPgDogTOML(opts, shardToCustomers),
		UsersTOML:        renderUsersTOML(opts),
		CustomerToShard:  customerToShard,
		ShardToCustomers: shardToCustomers,
	}, nil
}

func fillDefaults(opts Options) Options {
	defaults := DefaultOptions()
	if opts.LogicalDatabase == "" {
		opts.LogicalDatabase = defaults.LogicalDatabase
	}
	if opts.BackendHost == "" {
		opts.BackendHost = defaults.BackendHost
	}
	if opts.SourceDatabase == "" {
		opts.SourceDatabase = defaults.SourceDatabase
	}
	if opts.DoltgresDatabase == "" {
		opts.DoltgresDatabase = defaults.DoltgresDatabase
	}
	if opts.User == "" {
		opts.User = defaults.User
	}
	if opts.Password == "" {
		opts.Password = defaults.Password
	}
	if opts.SharedSchema == "" {
		opts.SharedSchema = defaults.SharedSchema
	}
	if opts.CustomerSchema == "" {
		opts.CustomerSchema = defaults.CustomerSchema
	}
	if opts.CustomerTable == "" {
		opts.CustomerTable = defaults.CustomerTable
	}
	if opts.ShardColumn == "" {
		opts.ShardColumn = defaults.ShardColumn
	}
	if opts.ShardDataType == "" {
		opts.ShardDataType = defaults.ShardDataType
	}
	if opts.SourcePort == 0 {
		opts.SourcePort = defaults.SourcePort
	}
	return opts
}

func validate(opts Options) error {
	if len(opts.DoltgresShardPorts) == 0 {
		return fmt.Errorf("at least one Doltgres shard port is required")
	}
	if len(opts.CustomerIDs) == 0 {
		return fmt.Errorf("at least one customer ID is required")
	}

	seenPorts := map[int]struct{}{opts.SourcePort: {}}
	for _, port := range opts.DoltgresShardPorts {
		if port <= 0 {
			return fmt.Errorf("invalid Doltgres shard port %d", port)
		}
		if _, ok := seenPorts[port]; ok {
			return fmt.Errorf("duplicate backend port %d", port)
		}
		seenPorts[port] = struct{}{}
	}

	seenCustomers := map[int64]struct{}{}
	for _, customerID := range opts.CustomerIDs {
		if _, ok := seenCustomers[customerID]; ok {
			return fmt.Errorf("duplicate customer ID %d", customerID)
		}
		seenCustomers[customerID] = struct{}{}
	}
	return nil
}

func renderPgDogTOML(opts Options, shardToCustomers map[int][]int64) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "[general]\n")
	fmt.Fprintf(&buf, "host = \"0.0.0.0\"\n")
	fmt.Fprintf(&buf, "port = 6432\n")
	fmt.Fprintf(&buf, "prepared_statements = \"extended\"\n")
	fmt.Fprintf(&buf, "read_write_split = \"include_primary\"\n")
	fmt.Fprintf(&buf, "load_schema = \"on\"\n")
	fmt.Fprintf(&buf, "cross_shard_disabled = %t\n\n", opts.CrossShardDisabled)

	writeDatabase(&buf, opts, 0, opts.SourcePort, opts.SourceDatabase)
	for i, port := range opts.DoltgresShardPorts {
		writeDatabase(&buf, opts, i+1, port, opts.DoltgresDatabase)
	}

	fmt.Fprintf(&buf, "[[sharded_schemas]]\n")
	fmt.Fprintf(&buf, "database = %s\n", tomlString(opts.LogicalDatabase))
	fmt.Fprintf(&buf, "name = %s\n", tomlString(opts.SharedSchema))
	fmt.Fprintf(&buf, "shard = 0\n\n")

	fmt.Fprintf(&buf, "[[sharded_tables]]\n")
	fmt.Fprintf(&buf, "database = %s\n", tomlString(opts.LogicalDatabase))
	fmt.Fprintf(&buf, "schema = %s\n", tomlString(opts.CustomerSchema))
	fmt.Fprintf(&buf, "name = %s\n", tomlString(opts.CustomerTable))
	fmt.Fprintf(&buf, "column = %s\n", tomlString(opts.ShardColumn))
	fmt.Fprintf(&buf, "data_type = %s\n\n", tomlString(opts.ShardDataType))

	shardIDs := make([]int, 0, len(shardToCustomers))
	for shardID := range shardToCustomers {
		shardIDs = append(shardIDs, shardID)
	}
	sort.Ints(shardIDs)
	for _, shardID := range shardIDs {
		writeMapping(&buf, opts, "list", shardID, shardToCustomers[shardID])
	}
	writeMapping(&buf, opts, "default", 0, nil)

	return buf.String()
}

func writeDatabase(buf *bytes.Buffer, opts Options, shardID int, port int, database string) {
	fmt.Fprintf(buf, "[[databases]]\n")
	fmt.Fprintf(buf, "name = %s\n", tomlString(opts.LogicalDatabase))
	fmt.Fprintf(buf, "host = %s\n", tomlString(opts.BackendHost))
	fmt.Fprintf(buf, "port = %d\n", port)
	fmt.Fprintf(buf, "database_name = %s\n", tomlString(database))
	fmt.Fprintf(buf, "user = %s\n", tomlString(opts.User))
	fmt.Fprintf(buf, "password = %s\n", tomlString(opts.Password))
	fmt.Fprintf(buf, "role = \"primary\"\n")
	fmt.Fprintf(buf, "shard = %d\n\n", shardID)
}

func writeMapping(buf *bytes.Buffer, opts Options, kind string, shardID int, customerIDs []int64) {
	fmt.Fprintf(buf, "[[sharded_mappings]]\n")
	fmt.Fprintf(buf, "database = %s\n", tomlString(opts.LogicalDatabase))
	fmt.Fprintf(buf, "schema = %s\n", tomlString(opts.CustomerSchema))
	fmt.Fprintf(buf, "table = %s\n", tomlString(opts.CustomerTable))
	fmt.Fprintf(buf, "column = %s\n", tomlString(opts.ShardColumn))
	fmt.Fprintf(buf, "kind = %s\n", tomlString(kind))
	if kind == "list" {
		fmt.Fprintf(buf, "values = [%s]\n", joinInt64s(customerIDs))
	}
	fmt.Fprintf(buf, "shard = %d\n\n", shardID)
}

func renderUsersTOML(opts Options) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "[[users]]\n")
	fmt.Fprintf(&buf, "name = %s\n", tomlString(opts.User))
	fmt.Fprintf(&buf, "password = %s\n", tomlString(opts.Password))
	fmt.Fprintf(&buf, "database = %s\n", tomlString(opts.LogicalDatabase))
	fmt.Fprintf(&buf, "server_user = %s\n", tomlString(opts.User))
	fmt.Fprintf(&buf, "server_password = %s\n", tomlString(opts.Password))
	return buf.String()
}

func joinInt64s(values []int64) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = fmt.Sprintf("%d", value)
	}
	return strings.Join(parts, ", ")
}

func tomlString(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value)
	return `"` + escaped + `"`
}

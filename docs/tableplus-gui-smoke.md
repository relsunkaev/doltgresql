# TablePlus GUI Smoke

This records the local TablePlus GUI workflow used for the app-compatibility
checklist. It is intentionally a local proof, not a CI gate, because it drives
`/Applications/TablePlus.app`.

## Environment

- Date: 2026-05-10
- TablePlus: `/Applications/TablePlus.app`, `CFBundleShortVersionString` 6.9.0
- Doltgres: local checkout via `go run ./cmd/doltgres`
- Listener: `127.0.0.1:55432`
- Database/user/password: `postgres` / `postgres` / `password`

## Fixture

The temporary Doltgres config used a throwaway data directory under
`/tmp/doltgres-tableplus.*` and this listener:

```yaml
listener:
  host: 127.0.0.1
  port: 55432
  allow_cleartext_passwords: true
```

The schema created for the GUI run:

```sql
CREATE TABLE gui_accounts (
  id integer PRIMARY KEY,
  name text NOT NULL UNIQUE,
  active boolean NOT NULL DEFAULT true,
  payload jsonb NOT NULL
);

CREATE TABLE gui_items (
  id integer PRIMARY KEY,
  account_id integer NOT NULL REFERENCES gui_accounts(id),
  amount numeric(10,2) NOT NULL,
  tags text[] NOT NULL,
  created_at timestamptz NOT NULL DEFAULT current_timestamp
);

CREATE VIEW gui_account_totals AS
  SELECT a.id, a.name, count(i.id) AS item_count,
         coalesce(sum(i.amount), 0)::numeric(10,2) AS total_amount
  FROM gui_accounts a
  LEFT JOIN gui_items i ON i.account_id = a.id
  GROUP BY a.id, a.name;
```

## Workflow

1. Started Doltgres with the temporary config.
2. Loaded the fixture with `psql`.
3. Opened the real TablePlus app through its PostgreSQL URL handler:

   ```sh
   open 'postgresql://postgres:password@127.0.0.1:55432/postgres?sslmode=disable'
   ```

4. Opened `gui_accounts` from the TablePlus workspace using the real GUI
   `Navigate > Open Anything` path.

## Evidence

`lsof -nP -iTCP:55432` showed the real TablePlus process connected to the live
Doltgres listener:

```text
TablePlus ... TCP 127.0.0.1:<ephemeral>->127.0.0.1:55432 (ESTABLISHED)
```

The macOS accessibility tree for the TablePlus workspace showed:

```text
PostgreSQL 15.5 : New Connection : postgres : public.gui_accounts
gui_account_totals
gui_accounts
gui_items
```

This verifies the real TablePlus GUI can connect to Doltgres, inspect the
`public` schema, list tables/views, and open a base table workspace backed by
the RowDescription table OID / attribute metadata used for editable table
surfaces.

## 0.7.0 - 2025-11-02

### 🐛 Fix

- Fixed support for double-quoted column names in SELECT and RETURNING clauses (e.g., `SELECT "user-id", "order"` now works correctly).
- Fixed an issue with next token handling in paginated queries that could cause incomplete result sets
- Fixed DynamoDB client lifecycle management to prevent resource leaks and connection issues
- Fixed support for optional whitespace before commas in column lists (e.g., `SELECT id , name` now works correctly)

### ⚠️ Deprecations

- `pqxdDriver.Open` has been deprecated and is now a no-op.  
  This deprecation has no impact on existing functionality.
  pqxd now implements `driver.DriverContext`.
  If the driver implements `driver.DriverContext`, the `database/sql` package will automatically connect using the `driver.Connector` obtained by `driver.DriverContext#OpenConnector`, rather than the traditional `Open` method.
  Therefore, `pqxdDriver.Open` is no longer called by `database/sql`.

## 0.6.0 - 2025-09-23

### 💥 Breaking Changes

- `db.Ping` now calls ListTable API instead of DescribeEndpoints API

### 🐛 Fix

- Fixed an issue where SELECT statements without a selection list would fail to scan.  
  However, we still recommend explicitly selecting columns as before.

## 0.5.0 - 2024-11-02

### ✨ New Features

- Added support for [ListTables API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html) with `!pqxd_list_tables`, the meta-table.

## 0.4.0 - 2024-11-01

### ✨ New Features

- Added support for [DescribeTable API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html) with `!pqxd_describe_table`, the meta-table.

### ⚡️ Performance

- Fixed a problem where Scanner.Scan was running with `rows.Next`.  
  This is expected to improve performance.

### 📚 Documentation

- Few fixes in the example codes

## 0.3.0 - 2024-10-29

### ✨ New Features

- Added support for `sql.Scanner`

### 📚 Documentation

- Few fixes in the example codes

## 0.2.0 - 2024-10-28

### ✨ New Features

- Added `RETURNING` support

### 🐛 Fix

- Fixed a problem that may cause channel close to be performed multiple times.

### 📚 Documentation

- Few fixes in the example codes

## 0.1.1 - 2024-10-27

### ♻️ Refactor

- `NewConnector` are now returns `driver.Connector` instead of `*pqxd.connector`

### 📚Documentation

- few fixes in the example codes

## 0.1.0 - 2024-10-27

### 🎉Initial Release



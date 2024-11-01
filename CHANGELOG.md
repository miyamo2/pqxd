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



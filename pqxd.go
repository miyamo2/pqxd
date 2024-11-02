package pqxd

// DriverName is the name that should be used in sql.Open to use this driver
//
// Example:
//
//	db, err := sql.Open(pqxd.DriverName, "AWS_REGION=ap-northeast-1;AWS_ACCESS_KEY_ID=AKIA...;AWS_SECRET_ACCESS_KEY=...;")
const DriverName = "pqxd"

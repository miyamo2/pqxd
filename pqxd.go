package pqxd

// DriverName is the name that should be used in sql.Open to use this driver
//
// Example:
//
//	db, err := sql.Open(pqxd.DriverName, "REGION=ap-northeast-1;ACCESSKEY=dummy;SECRETKEY=dummy")
const DriverName = "pqxd"

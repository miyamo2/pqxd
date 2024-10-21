package integration

type TestTables struct {
	PK    string  `dynamodbav:"pk"`
	SK    float64 `dynamodbav:"sk"`
	GSIPK string  `dynamodbav:"gsi_pk"`
	GSISK string  `dynamodbav:"gsi_sk"`
}

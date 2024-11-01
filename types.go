package pqxd

import (
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"time"
)

// compatibility check
var (
	_ sql.Scanner = (*ArchivalSummary)(nil)
	_ sql.Scanner = (*AttributeDefinitions)(nil)
	_ sql.Scanner = (*BillingModeSummary)(nil)
	_ sql.Scanner = (*CreationDateTime)(nil)
	_ sql.Scanner = (*DeletionProtectionEnabled)(nil)
	_ sql.Scanner = (*KeySchema)(nil)
	_ sql.Scanner = (*GlobalSecondaryIndexes)(nil)
	_ sql.Scanner = (*GlobalTableVersion)(nil)
	_ sql.Scanner = (*ItemCount)(nil)
	_ sql.Scanner = (*LocalSecondaryIndexes)(nil)
	_ sql.Scanner = (*OnDemandThroughput)(nil)
	_ sql.Scanner = (*ProvisionedThroughput)(nil)
	_ sql.Scanner = (*Replicas)(nil)
	_ sql.Scanner = (*RestoreSummary)(nil)
	_ sql.Scanner = (*SSEDescription)(nil)
	_ sql.Scanner = (*StreamSpecification)(nil)
	_ sql.Scanner = (*TableClassSummary)(nil)
	_ sql.Scanner = (*TableStatus)(nil)
)

// ArchivalSummary See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#ArchivalSummary
type ArchivalSummary sql.Null[types.ArchivalSummary]

// Scan implements the sql.Scanner interface.
func (a *ArchivalSummary) Scan(src any) error {
	switch v := src.(type) {
	case *types.ArchivalSummary:
		if v == nil {
			return nil
		}
		a.V = *v
		a.Valid = true
		return nil
	}
	return nil
}

// AttributeDefinitions See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#AttributeDefinitions
type AttributeDefinitions []types.AttributeDefinition

// Scan implements the sql.Scanner interface.
func (a *AttributeDefinitions) Scan(src any) error {
	switch v := src.(type) {
	case []types.AttributeDefinition:
		*a = v
		return nil
	}
	return nil
}

// BillingModeSummary See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#BillingModeSummary
type BillingModeSummary sql.Null[types.BillingModeSummary]

// Scan implements the sql.Scanner interface.
func (b *BillingModeSummary) Scan(src any) error {
	switch v := src.(type) {
	case *types.BillingModeSummary:
		if v == nil {
			return nil
		}
		b.V = *v
		b.Valid = true
		return nil
	}
	return nil
}

// CreationDateTime See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#TableDescription
type CreationDateTime struct {
	sql.NullTime
}

// Scan implements the sql.Scanner interface.
func (c *CreationDateTime) Scan(src any) error {
	switch v := src.(type) {
	case *time.Time:
		if v == nil {
			return nil
		}
		c.Time = *v
		c.Valid = true
	}
	return nil
}

// DeletionProtectionEnabled See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#TableDescription
type DeletionProtectionEnabled struct {
	sql.NullBool
}

// Scan implements the sql.Scanner interface.
func (d *DeletionProtectionEnabled) Scan(src any) error {
	switch v := src.(type) {
	case *bool:
		if v == nil {
			return nil
		}
		d.Bool = *v
		d.Valid = true
		return nil
	}
	return nil
}

// KeySchema See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#KeySchemaElement
type KeySchema []types.KeySchemaElement

// Scan implements the sql.Scanner interface.
func (k *KeySchema) Scan(src any) error {
	switch v := src.(type) {
	case []types.KeySchemaElement:
		*k = v
		return nil
	}
	return nil
}

// GlobalSecondaryIndexes See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#GlobalSecondaryIndex
type GlobalSecondaryIndexes []types.GlobalSecondaryIndexDescription

func (g *GlobalSecondaryIndexes) Scan(src any) error {
	switch v := src.(type) {
	case []types.GlobalSecondaryIndexDescription:
		*g = v
		return nil
	}
	return nil
}

// GlobalTableVersion See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#TableDescription
type GlobalTableVersion struct {
	sql.NullString
}

// Scan implements the sql.Scanner interface.
func (g *GlobalTableVersion) Scan(src any) error {
	switch v := src.(type) {
	case *string:
		if v == nil {
			return nil
		}
		g.String = *v
		g.Valid = true
		return nil
	}
	return nil
}

// ItemCount See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#TableDescription
type ItemCount struct {
	sql.NullInt64
}

// Scan implements the sql.Scanner interface.
func (i *ItemCount) Scan(src any) error {
	switch v := src.(type) {
	case *int64:
		if v == nil {
			return nil
		}
		i.Int64 = *v
		i.Valid = true
		return nil
	}
	return nil
}

// LocalSecondaryIndexes See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#LocalSecondaryIndex
type LocalSecondaryIndexes []types.LocalSecondaryIndex

// Scan implements the sql.Scanner interface.
func (l *LocalSecondaryIndexes) Scan(src any) error {
	switch v := src.(type) {
	case []types.LocalSecondaryIndex:
		*l = v
		return nil
	}
	return nil
}

// OnDemandThroughput See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#OnDemandThroughput
type OnDemandThroughput sql.Null[types.OnDemandThroughput]

// Scan implements the sql.Scanner interface.
func (o *OnDemandThroughput) Scan(src any) error {
	switch v := src.(type) {
	case *types.OnDemandThroughput:
		if v == nil {
			return nil
		}
		o.V = *v
		o.Valid = true
		return nil
	}
	return nil
}

// ProvisionedThroughput See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#ProvisionedThroughputDescription
type ProvisionedThroughput sql.Null[types.ProvisionedThroughputDescription]

// Scan implements the sql.Scanner interface.
func (p *ProvisionedThroughput) Scan(src any) error {
	switch v := src.(type) {
	case *types.ProvisionedThroughputDescription:
		if v == nil {
			return nil
		}
		p.V = *v
		p.Valid = true
		return nil
	}
	return nil
}

// Replicas See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#ReplicaDescription
type Replicas sql.Null[types.ReplicaDescription]

// Scan implements the sql.Scanner interface.
func (r *Replicas) Scan(src any) error {
	switch v := src.(type) {
	case *types.ReplicaDescription:
		if v == nil {
			return nil
		}
		r.V = *v
		r.Valid = true
		return nil
	}
	return nil
}

// RestoreSummary See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#RestoreSummary
type RestoreSummary sql.Null[types.RestoreSummary]

// Scan implements the sql.Scanner interface.
func (r *RestoreSummary) Scan(src any) error {
	switch v := src.(type) {
	case *types.RestoreSummary:
		if v == nil {
			return nil
		}
		r.V = *v
		r.Valid = true
		return nil
	}
	return nil
}

// SSEDescription See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#SSEDescription
type SSEDescription sql.Null[types.SSEDescription]

// Scan implements the sql.Scanner interface.
func (s *SSEDescription) Scan(src any) error {
	switch v := src.(type) {
	case *types.SSEDescription:
		if v == nil {
			return nil
		}
		s.V = *v
		s.Valid = true
		return nil
	}
	return nil
}

// StreamSpecification See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#StreamSpecification
type StreamSpecification sql.Null[types.StreamSpecification]

// Scan implements the sql.Scanner interface.
func (s *StreamSpecification) Scan(src any) error {
	switch v := src.(type) {
	case *types.StreamSpecification:
		if v == nil {
			return nil
		}
		s.V = *v
		s.Valid = true
		return nil
	}
	return nil
}

// TableClassSummary See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#TableClassSummary
type TableClassSummary sql.Null[types.TableClassSummary]

// Scan implements the sql.Scanner interface.
func (t *TableClassSummary) Scan(src any) error {
	switch v := src.(type) {
	case *types.TableClassSummary:
		if v == nil {
			return nil
		}
		t.V = *v
		t.Valid = true
		return nil
	}
	return nil
}

// TableStatus See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#TableDescription
type TableStatus struct {
	types.TableStatus
}

// Scan implements the sql.Scanner interface.
func (t *TableStatus) Scan(src any) error {
	switch v := src.(type) {
	case types.TableStatus:
		t.TableStatus = v
		return nil
	}
	return nil
}

// Values See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#TableStatus
func (t *TableStatus) Values() []types.TableStatus {
	return t.TableStatus.Values()
}

// String returns the string representation of the TableStatus.
func (t *TableStatus) String() string {
	return string(t.TableStatus)
}

package sql

import (
    "testing"
    "time"

    "github.com/jhump/protoreflect/desc"
    "github.com/jhump/protoreflect/dynamic"
    pbRelations "github.com/streamingfast/substreams-sink-sql/pb/test/relations"
    pbSchema "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
    "github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
    "go.uber.org/zap"
    "google.golang.org/protobuf/reflect/protodesc"
    "google.golang.org/protobuf/types/descriptorpb"
    "google.golang.org/protobuf/types/known/timestamppb"
)

// testDialect is a minimal implementation sufficient for BaseDatabase tests.
type testDialect struct{ *BaseDialect }

func (d *testDialect) SchemaHash() string                  { return "" }
func (d *testDialect) FullTableName(t *schema.Table) string { return t.Name }
func (d *testDialect) UseVersionField() bool               { return false }
func (d *testDialect) UseDeletedField() bool               { return false }

type captureInserter struct {
    rows map[string][][]any
}

func (ci *captureInserter) Insert(table string, values []any) error {
    if ci.rows == nil {
        ci.rows = map[string][][]any{}
    }
    // Copy slice to avoid later mutation surprises
    copied := make([]any, len(values))
    copy(copied, values)
    ci.rows[table] = append(ci.rows[table], copied)
    return nil
}

// buildMessageDescriptor creates a jhump desc for test.relations using generated protos.
func buildMessageDescriptor(t *testing.T, fqMsg string) *desc.MessageDescriptor {
    t.Helper()
    // Dependencies: google.protobuf.descriptor, google.protobuf.timestamp, schema.proto
    fdDescriptor := protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto)
    fdTimestamp := protodesc.ToFileDescriptorProto(timestamppb.File_google_protobuf_timestamp_proto)
    fdSchema := protodesc.ToFileDescriptorProto(pbSchema.File_sf_substreams_sink_sql_schema_v1_schema_proto)
    fdRelations := protodesc.ToFileDescriptorProto(pbRelations.File_test_relations_relations_proto)

    dDescriptor, err := desc.CreateFileDescriptor(fdDescriptor)
    if err != nil { t.Fatalf("create descriptor.proto fd: %v", err) }
    dTimestamp, err := desc.CreateFileDescriptor(fdTimestamp)
    if err != nil { t.Fatalf("create timestamp.proto fd: %v", err) }
    dSchema, err := desc.CreateFileDescriptor(fdSchema, dDescriptor)
    if err != nil { t.Fatalf("create schema.proto fd: %v", err) }
    dRelations, err := desc.CreateFileDescriptor(fdRelations, dDescriptor, dTimestamp, dSchema)
    if err != nil { t.Fatalf("create relations.proto fd: %v", err) }

    md := dRelations.FindMessage(fqMsg)
    if md == nil {
        t.Fatalf("message %s not found", fqMsg)
    }
    return md
}

func TestWalkMessageDescriptor_ParentIDFromPK_UsesProtoFieldName(t *testing.T) {
    // Use test.relations.Order as a parent with OrderItem children
    orderMD := buildMessageDescriptor(t, "test.relations.Order")

    // Build schema with proto options enabled
    zlog := zap.NewNop()
    sch, err := schema.NewSchema("test", orderMD, true, zlog)
    if err != nil { t.Fatalf("new schema: %v", err) }

    // Simulate a column rename scenario: change the PK column name while keeping its descriptor
    ordersTbl := sch.TableRegistry["orders"]
    if ordersTbl == nil || ordersTbl.PrimaryKey == nil { t.Fatalf("orders table or pk missing") }
    ordersTbl.PrimaryKey.Name = "order_pk_renamed"

    // Dialect wired on the schema registry
    d := &testDialect{BaseDialect: NewBaseDialect(sch.TableRegistry, zlog)}

    // Base database under test
    baseDB, err := NewBaseDatabase("ignored", orderMD, true /* useProtoOptions */, zlog)
    if err != nil { t.Fatalf("new base db: %v", err) }

    // Craft dynamic message: Order with two OrderItem children
    dm := dynamic.NewMessage(orderMD)
    if err := dm.TrySetFieldByName("order_id", "ORD-001"); err != nil { t.Fatalf("set order_id: %v", err) }
    if err := dm.TrySetFieldByName("customer_ref_id", "CUST-9"); err != nil { t.Fatalf("set customer_ref_id: %v", err) }

    // Build OrderItem messages
    orderItemMD := orderMD.GetFile().FindMessage("test.relations.OrderItem")
    if orderItemMD == nil { t.Fatalf("order item md not found") }
    item1 := dynamic.NewMessage(orderItemMD)
    if err := item1.TrySetFieldByName("item_id", "SKU-1"); err != nil { t.Fatalf("set item1.item_id: %v", err) }
    if err := item1.TrySetFieldByName("quantity", int64(2)); err != nil { t.Fatalf("set item1.quantity: %v", err) }
    item2 := dynamic.NewMessage(orderItemMD)
    if err := item2.TrySetFieldByName("item_id", "SKU-2"); err != nil { t.Fatalf("set item2.item_id: %v", err) }
    if err := item2.TrySetFieldByName("quantity", int64(1)); err != nil { t.Fatalf("set item2.quantity: %v", err) }

    // Set repeated field
    if err := dm.TrySetFieldByName("items", []any{item1, item2}); err != nil { t.Fatalf("set items: %v", err) }

    cap := &captureInserter{}

    // Execute
    _, err = baseDB.WalkMessageDescriptorAndInsertWithDialect(dm, 100, time.Unix(0, 0), nil, d, cap)
    if err != nil { t.Fatalf("walk insert: %v", err) }

    // Validate parent row captured
    parentRows := cap.rows["orders"]
    if len(parentRows) != 1 {
        t.Fatalf("expected 1 parent row, got %d", len(parentRows))
    }
    parent := parentRows[0]
    if got := parent[2]; got != "ORD-001" {
        t.Fatalf("expected parent pk at index 2 to be 'ORD-001', got %#v", got)
    }

    // Validate children received the parent id (third value: block_number, block_timestamp, parent_id, ...)
    childRows := cap.rows["order_items"]
    if len(childRows) != 2 {
        t.Fatalf("expected 2 child rows, got %d", len(childRows))
    }
    for i, r := range childRows {
        if len(r) < 3 {
            t.Fatalf("child row %d has too few columns: %v", i, r)
        }
        if r[2] != "ORD-001" {
            t.Fatalf("child row %d expected parent id 'ORD-001' at index 2, got %#v", i, r[2])
        }
    }
}


package sql

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"github.com/streamingfast/substreams-sink-sql/internal/timefmt"
	pbSchema "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
	pbRelations "github.com/streamingfast/substreams-sink-sql/pb/test/relations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testDialect is a minimal implementation sufficient for BaseDatabase tests.
type testDialect struct{ *BaseDialect }

func (d *testDialect) SchemaHash() string                   { return "" }
func (d *testDialect) FullTableName(t *schema.Table) string { return t.Name }
func (d *testDialect) UseVersionField() bool                { return false }
func (d *testDialect) UseDeletedField() bool                { return false }

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
	if err != nil {
		t.Fatalf("create descriptor.proto fd: %v", err)
	}
	dTimestamp, err := desc.CreateFileDescriptor(fdTimestamp)
	if err != nil {
		t.Fatalf("create timestamp.proto fd: %v", err)
	}
	dSchema, err := desc.CreateFileDescriptor(fdSchema, dDescriptor)
	if err != nil {
		t.Fatalf("create schema.proto fd: %v", err)
	}
	dRelations, err := desc.CreateFileDescriptor(fdRelations, dDescriptor, dTimestamp, dSchema)
	if err != nil {
		t.Fatalf("create relations.proto fd: %v", err)
	}

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
	if err != nil {
		t.Fatalf("new schema: %v", err)
	}

	// Simulate a column rename scenario: change the PK column name while keeping its descriptor
	ordersTbl := sch.TableRegistry["orders"]
	if ordersTbl == nil || ordersTbl.PrimaryKey == nil {
		t.Fatalf("orders table or pk missing")
	}
	ordersTbl.PrimaryKey.Name = "order_pk_renamed"

	// Dialect wired on the schema registry
	d := &testDialect{BaseDialect: NewBaseDialect(sch.TableRegistry, zlog)}

	// Base database under test
	baseDB, err := NewBaseDatabase("ignored", orderMD, true /* useProtoOptions */, zlog)
	if err != nil {
		t.Fatalf("new base db: %v", err)
	}

	// Craft dynamic message: Order with two OrderItem children
	dm := dynamic.NewMessage(orderMD)
	if err := dm.TrySetFieldByName("order_id", "ORD-001"); err != nil {
		t.Fatalf("set order_id: %v", err)
	}
	if err := dm.TrySetFieldByName("customer_ref_id", "CUST-9"); err != nil {
		t.Fatalf("set customer_ref_id: %v", err)
	}

	// Build OrderItem messages
	orderItemMD := orderMD.GetFile().FindMessage("test.relations.OrderItem")
	if orderItemMD == nil {
		t.Fatalf("order item md not found")
	}
	item1 := dynamic.NewMessage(orderItemMD)
	if err := item1.TrySetFieldByName("item_id", "SKU-1"); err != nil {
		t.Fatalf("set item1.item_id: %v", err)
	}
	if err := item1.TrySetFieldByName("quantity", int64(2)); err != nil {
		t.Fatalf("set item1.quantity: %v", err)
	}
	item2 := dynamic.NewMessage(orderItemMD)
	if err := item2.TrySetFieldByName("item_id", "SKU-2"); err != nil {
		t.Fatalf("set item2.item_id: %v", err)
	}
	if err := item2.TrySetFieldByName("quantity", int64(1)); err != nil {
		t.Fatalf("set item2.quantity: %v", err)
	}

	// Set repeated field
	if err := dm.TrySetFieldByName("items", []any{item1, item2}); err != nil {
		t.Fatalf("set items: %v", err)
	}

	cap := &captureInserter{}

	// Execute
	_, err = baseDB.WalkMessageDescriptorAndInsertWithDialect(dm, 100, time.Unix(0, 0), nil, d, cap)
	if err != nil {
		t.Fatalf("walk insert: %v", err)
	}

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

func TestWalkMessageDescriptor_NormalizesSemanticTimestamp(t *testing.T) {
	// Build descriptor with a string field annotated as block_timestamp
	columnOpts := &pbSchema.Column{SemanticType: proto.String(string(SemanticBlockTimestamp))}
	fieldOpts := &descriptorpb.FieldOptions{}
	if err := proto.SetExtension(fieldOpts, pbSchema.E_Field, columnOpts); err != nil {
		t.Fatalf("set field extension: %v", err)
	}
	field := &descriptorpb.FieldDescriptorProto{
		Name:    proto.String("event_time"),
		Number:  proto.Int32(1),
		Type:    descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		Options: fieldOpts,
	}

	tableOpts := &pbSchema.Table{Name: "events"}
	msgOpts := &descriptorpb.MessageOptions{}
	if err := proto.SetExtension(msgOpts, pbSchema.E_Table, tableOpts); err != nil {
		t.Fatalf("set message extension: %v", err)
	}
	msgProto := &descriptorpb.DescriptorProto{
		Name:    proto.String("Event"),
		Field:   []*descriptorpb.FieldDescriptorProto{field},
		Options: msgOpts,
	}

	fileProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("semantic_event.proto"),
		Package:     proto.String("test.semantic"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
	}

	fd, err := desc.CreateFileDescriptor(fileProto)
	if err != nil {
		t.Fatalf("create file descriptor: %v", err)
	}
	md := fd.FindMessage("test.semantic.Event")
	if md == nil {
		t.Fatalf("message descriptor not found")
	}

	zlog := zap.NewNop()
	sch, err := schema.NewSchema("test_schema", md, true, zlog)
	if err != nil {
		t.Fatalf("new schema: %v", err)
	}

	d := &testDialect{BaseDialect: NewBaseDialect(sch.TableRegistry, zlog)}
	baseDB, err := NewBaseDatabase("test.semantic.Event", md, true, zlog)
	if err != nil {
		t.Fatalf("new base db: %v", err)
	}

	dm := dynamic.NewMessage(md)
	const rawTimestamp = "2025-09-18T18:24:17.806811415Z"
	if err := dm.TrySetFieldByName("event_time", rawTimestamp); err != nil {
		t.Fatalf("set event_time: %v", err)
	}

	cap := &captureInserter{}
	blockTime := time.Unix(100, 0)
	_, err = baseDB.WalkMessageDescriptorAndInsertWithDialect(dm, 12345, blockTime, nil, d, cap)
	if err != nil {
		t.Fatalf("walk insert: %v", err)
	}

	rows := cap.rows["events"]
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	values := rows[0]
	t.Logf("captured b_value values: %#v", values)
	if len(values) < 3 {
		t.Fatalf("expected at least 3 columns, got %d", len(values))
	}
	gotTime, ok := values[2].(time.Time)
	if !ok {
		t.Fatalf("expected normalized time.Time, got %T", values[2])
	}
	wantTime, err := timefmt.ParseTimestamp(rawTimestamp)
	if err != nil {
		t.Fatalf("parse expected timestamp: %v", err)
	}
	if !gotTime.Equal(wantTime) {
		t.Fatalf("normalized time mismatch: got %v want %v", gotTime, wantTime)
	}
}

func TestWalkMessageDescriptor_ChildOfAncestorOneofHasCompleteArguments(t *testing.T) {
	// Build descriptors mirroring the stability pool snippet
	fieldID := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String("id"),
		Number: proto.Int32(1),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		Options: func() *descriptorpb.FieldOptions {
			opts := &descriptorpb.FieldOptions{}
			if err := proto.SetExtension(opts, pbSchema.E_Field, &pbSchema.Column{PrimaryKey: true}); err != nil {
				t.Fatalf("set field option: %v", err)
			}
			return opts
		}(),
	}

	bValueField := func(name string, number int32) *descriptorpb.FieldDescriptorProto {
		opts := &descriptorpb.FieldOptions{}
		if err := proto.SetExtension(opts, pbSchema.E_Field, &pbSchema.Column{SemanticType: proto.String("uint256")}); err != nil {
			t.Fatalf("set semantic option: %v", err)
		}
		return &descriptorpb.FieldDescriptorProto{
			Name:    proto.String(name),
			Number:  proto.Int32(number),
			Type:    descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			Options: opts,
		}
	}

	bValueUpdate := &descriptorpb.DescriptorProto{
		Name: proto.String("BValueUpdate"),
		Field: []*descriptorpb.FieldDescriptorProto{
			bValueField("b_value", 1),
			bValueField("scale", 2),
		},
		Options: func() *descriptorpb.MessageOptions {
			opts := &descriptorpb.MessageOptions{}
			if err := proto.SetExtension(opts, pbSchema.E_Table, &pbSchema.Table{
				Name:    "stability_pool_b_value_updates",
				ChildOf: proto.String("stability_pool_operations on id"),
			}); err != nil {
				t.Fatalf("set table option: %v", err)
			}
			return opts
		}(),
	}

	sValueUpdate := &descriptorpb.DescriptorProto{
		Name: proto.String("SValueUpdate"),
		Field: []*descriptorpb.FieldDescriptorProto{
			bValueField("s_value", 1),
			bValueField("scale", 2),
		},
		// no table option required for this test
	}

	stabilityUpdate := &descriptorpb.DescriptorProto{
		Name: proto.String("StabilityPoolUpdate"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:       proto.String("p_value"),
				Number:     proto.Int32(1),
				Type:       descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				OneofIndex: proto.Int32(0),
			},
			{
				Name:       proto.String("s_value"),
				Number:     proto.Int32(2),
				Type:       descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName:   proto.String(".test.contract.SValueUpdate"),
				OneofIndex: proto.Int32(0),
			},
			{
				Name:       proto.String("b_value"),
				Number:     proto.Int32(3),
				Type:       descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName:   proto.String(".test.contract.BValueUpdate"),
				OneofIndex: proto.Int32(0),
			},
			{
				Name:       proto.String("scale"),
				Number:     proto.Int32(4),
				Type:       descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				OneofIndex: proto.Int32(0),
			},
		},
		Options: func() *descriptorpb.MessageOptions {
			opts := &descriptorpb.MessageOptions{}
			if err := proto.SetExtension(opts, pbSchema.E_Table, &pbSchema.Table{
				Name:    "stability_pool_updates",
				ChildOf: proto.String("stability_pool_operations on id"),
			}); err != nil {
				t.Fatalf("set table option: %v", err)
			}
			return opts
		}(),
		OneofDecl: []*descriptorpb.OneofDescriptorProto{
			{Name: proto.String("update_type")},
		},
	}

	operation := &descriptorpb.DescriptorProto{
		Name: proto.String("StabilityPoolOperation"),
		Field: []*descriptorpb.FieldDescriptorProto{
			fieldID,
			{
				Name:     proto.String("updates"),
				Number:   proto.Int32(2),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".test.contract.StabilityPoolUpdate"),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
			},
		},
		Options: func() *descriptorpb.MessageOptions {
			opts := &descriptorpb.MessageOptions{}
			if err := proto.SetExtension(opts, pbSchema.E_Table, &pbSchema.Table{Name: "stability_pool_operations"}); err != nil {
				t.Fatalf("set table option: %v", err)
			}
			return opts
		}(),
	}

	fileProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("stability_pool.proto"),
		Package: proto.String("test.contract"),
		MessageType: []*descriptorpb.DescriptorProto{
			operation,
			stabilityUpdate,
			sValueUpdate,
			bValueUpdate,
		},
	}

	fd, err := desc.CreateFileDescriptor(fileProto)
	if err != nil {
		t.Fatalf("create file descriptor: %v", err)
	}
	opMD := fd.FindMessage("test.contract.StabilityPoolOperation")
	if opMD == nil {
		t.Fatalf("operation descriptor not found")
	}

	zlog := zap.NewNop()
	sch, err := schema.NewSchema("test_schema", opMD, true, zlog)
	if err != nil {
		t.Fatalf("new schema: %v", err)
	}
	if _, ok := sch.TableRegistry["stability_pool_b_value_updates"]; !ok {
		t.Fatalf("expected stability_pool_b_value_updates table in registry")
	}

	dialect := &testDialect{BaseDialect: NewBaseDialect(sch.TableRegistry, zlog)}

	baseDB, err := NewBaseDatabase("test.contract.StabilityPoolOperation", opMD, true, zlog)
	if err != nil {
		t.Fatalf("new base db: %v", err)
	}

	operationMsg := dynamic.NewMessage(opMD)
	require.NoError(t, operationMsg.TrySetFieldByName("id", "op-1"))

	bValueMD := fd.FindMessage("test.contract.BValueUpdate")
	require.NotNil(t, bValueMD)
	bValueMsg := dynamic.NewMessage(bValueMD)
	require.NoError(t, bValueMsg.TrySetFieldByName("b_value", "123"))
	// Deliberately leave "scale" unset to ensure NULL handling still produces full argument list

	updateMD := fd.FindMessage("test.contract.StabilityPoolUpdate")
	require.NotNil(t, updateMD)
	updateMsg := dynamic.NewMessage(updateMD)
	require.NoError(t, updateMsg.TrySetFieldByName("b_value", bValueMsg))

	require.NoError(t, operationMsg.TrySetFieldByName("updates", []any{updateMsg}))

	cap := &captureInserter{}

	bValueTable := sch.TableRegistry["stability_pool_b_value_updates"]
	require.NotNil(t, bValueTable)
	require.NotNil(t, bValueTable.ChildOf)

	_, err = baseDB.WalkMessageDescriptorAndInsertWithDialect(operationMsg, 42, time.Unix(100, 0), nil, dialect, cap)
	require.NoError(t, err)
	rows := cap.rows["stability_pool_b_value_updates"]
	require.Len(t, rows, 1, "expected one b_value update row")

	values := rows[0]
	require.Len(t, values, 5, "expected values for all columns including parent id")
	assert.Equal(t, uint64(42), values[0])
	assert.Equal(t, time.Unix(100, 0), values[1])
	assert.Equal(t, "op-1", values[2], "parent id should propagate")
	assert.Equal(t, "123", values[3])
	assert.Equal(t, "", values[4], "scale should default to empty string when unset")
}

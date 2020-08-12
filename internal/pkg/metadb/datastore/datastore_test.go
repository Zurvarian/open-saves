// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/googleforgames/triton/api"
	m "github.com/googleforgames/triton/internal/pkg/metadb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newDriver(ctx context.Context, t *testing.T) *Driver {
	driver, err := NewDriver(ctx, "triton-for-games-dev")
	if err != nil {
		t.Fatalf("Initializing Datastore driver: %v", err)
	}
	driver.Namespace = "datastore-unittests"
	if err := driver.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to Datastore: %v", err)
	}
	return driver
}

func newStoreKey() string {
	return "unittest_store_" + uuid.New().String()
}

func newRecordKey() string {
	return "unittest_record_" + uuid.New().String()
}

func assertEqualStore(t *testing.T, expected, actual *m.Store, msgAndArgs ...interface{}) {
	assert.Equal(t, expected.Key, actual.Key, msgAndArgs...)
	assert.Equal(t, expected.Name, actual.Name, msgAndArgs...)
	assert.Equal(t, expected.OwnerID, actual.OwnerID, msgAndArgs...)
	assert.ElementsMatch(t, expected.Tags, actual.Tags, msgAndArgs...)
	assert.True(t, expected.Timestamps.CreatedAt.Equal(actual.Timestamps.CreatedAt), msgAndArgs...)
	assert.True(t, expected.Timestamps.UpdatedAt.Equal(actual.Timestamps.UpdatedAt), msgAndArgs...)
	assert.Equal(t, expected.Timestamps.Signature, actual.Timestamps.Signature, msgAndArgs...)
}

func assertEqualRecord(t *testing.T, expected, actual *m.Record, msgAndArgs ...interface{}) {
	assert.Equal(t, expected.Key, actual.Key, msgAndArgs...)
	assert.Equal(t, expected.Blob, actual.Blob, msgAndArgs...)
	assert.Equal(t, expected.BlobSize, actual.BlobSize, msgAndArgs...)
	assert.Equal(t, expected.Properties, actual.Properties, msgAndArgs...)
	assert.ElementsMatch(t, expected.Tags, actual.Tags, msgAndArgs...)
	assert.Equal(t, expected.OwnerID, actual.OwnerID, msgAndArgs...)
	assert.True(t, expected.Timestamps.CreatedAt.Equal(actual.Timestamps.CreatedAt), msgAndArgs...)
	assert.True(t, expected.Timestamps.UpdatedAt.Equal(actual.Timestamps.UpdatedAt), msgAndArgs...)
	assert.Equal(t, expected.Timestamps.Signature, actual.Timestamps.Signature, msgAndArgs...)
}

func TestDriver_ConnectDisconnect(t *testing.T) {
	ctx := context.Background()
	// Connect() is tested inside newDriver().
	driver := newDriver(ctx, t)
	if err := driver.Disconnect(ctx); err != nil {
		t.Fatalf("Failed to disconnect: %v", err)
	}
}

func TestDriver_SimpleCreateGetDeleteStore(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := newStoreKey()
	storeName := "SimpleCreateGetDeleteStore" + uuid.New().String()
	createdAt := time.Date(1988, 4, 16, 8, 6, 5, int(1234*time.Microsecond), time.UTC)
	store := &m.Store{
		Key:     storeKey,
		Name:    storeName,
		OwnerID: "triton",
		Tags:    []string{"abc", "def"},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.MustParse("db94be80-e036-4ca8-a9c0-2259b8a67acc"),
		},
	}
	if err := driver.CreateStore(ctx, store); err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}

	store2, err := driver.GetStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Could not get store (%s): %v", storeKey, err)
	}
	assertEqualStore(t, store, store2, "GetStore should return the exact same store.")

	store3, err := driver.FindStoreByName(ctx, storeName)
	if err != nil {
		t.Fatalf("Could not fetch store by name (%s): %v", storeName, err)
	}
	assertEqualStore(t, store, store3, "FindStoreByName should return the exact same store.")

	err = driver.DeleteStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Failed to delete a store (%s): %v", storeKey, err)
	}
	_, err = driver.GetStore(ctx, storeKey)
	if err == nil {
		t.Fatalf("GetStore didn't return an error after deleting a store: %v", err)
	}
}

func TestDriver_SimpleCreateGetDeleteRecord(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := newStoreKey()
	store := &m.Store{
		Key:     storeKey,
		Name:    "SimpleCreateGetDeleteRecord",
		OwnerID: "triton",
	}
	if err := driver.CreateStore(ctx, store); err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	recordKey := newRecordKey()
	blob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	createdAt := time.Date(1988, 4, 16, 8, 6, 5, int(1234*time.Microsecond), time.UTC)
	record := &m.Record{
		Key:      recordKey,
		Blob:     blob,
		BlobSize: int64(len(blob)),
		OwnerID:  "Triton",
		Tags:     []string{"abc", "def"},
		Properties: m.PropertyMap{
			"BoolTP":   {Type: pb.Property_BOOLEAN, BooleanValue: false},
			"IntTP":    {Type: pb.Property_INTEGER, IntegerValue: 42},
			"StringTP": {Type: pb.Property_STRING, StringValue: "a string value"},
		},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.MustParse("89223949-0414-438e-8f5e-3fd9e2d11c1e"),
		},
	}
	if err := driver.InsertRecord(ctx, storeKey, record); err != nil {
		t.Fatalf("Failed to create a new record (%s) in store (%s): %v", recordKey, storeKey, err)
	}

	record2, err := driver.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	assertEqualRecord(t, record, record2, "GetRecord should return the exact same record.")

	err = driver.InsertRecord(ctx, storeKey, record)
	if err == nil {
		t.Fatal("Insert should fail if a record with the same already exists.")
	}

	err = driver.DeleteRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to delete a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	_, err = driver.GetRecord(ctx, storeKey, recordKey)
	if err == nil {
		t.Fatalf("GetRecord didn't return an error after deleting a record (%s)", recordKey)
	}

	err = driver.DeleteStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Failed to delete a store (%s): %v", storeKey, err)
	}
}

func TestDriver_InsertRecordShouldFailWithNonExistentStore(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := newStoreKey()
	record := &m.Record{
		Key: newRecordKey(),
	}
	err := driver.InsertRecord(ctx, storeKey, record)
	if err == nil {
		t.Error("InsertRecord should fail if the store doesn't exist.")
		driver.DeleteRecord(ctx, storeKey, record.Key)
	} else {
		assert.Equalf(t, codes.FailedPrecondition, status.Code(err),
			"InsertRecord should return FailedPrecondition if the store doesn't exist: %v", err)
	}
}

func TestDriver_DeleteStoreShouldFailWhenNotEmpty(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	store := &m.Store{Key: newStoreKey()}
	record := &m.Record{Key: newRecordKey(), Properties: make(m.PropertyMap)}

	assert.NoError(t, driver.CreateStore(ctx, store))
	defer driver.DeleteStore(ctx, store.Key)
	assert.NoError(t, driver.InsertRecord(ctx, store.Key, record))
	defer driver.DeleteRecord(ctx, store.Key, record.Key)

	err := driver.DeleteStore(ctx, store.Key)
	if err == nil {
		t.Error("DeleteStore should fail if the store is not empty.")
	} else {
		assert.Equalf(t, codes.FailedPrecondition, status.Code(err),
			"DeleteStore should return FailedPrecondition if the store is not empty: %v", err)
	}
}

func TestDriver_UpdateRecord(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := newStoreKey()
	store := &m.Store{
		Key:     storeKey,
		Name:    "UpdateRecord",
		OwnerID: "triton",
	}
	if err := driver.CreateStore(ctx, store); err != nil {
		t.Fatalf("Could not create a new store: %v", err)
	}
	recordKey := newRecordKey()
	blob := []byte{0x54, 0x72, 0x69, 0x74, 0x6f, 0x6e}
	createdAt := time.Date(1988, 4, 16, 8, 6, 5, int(1234*time.Microsecond), time.UTC)
	record := &m.Record{
		Key:        recordKey,
		Blob:       blob,
		BlobSize:   int64(len(blob)),
		Properties: make(m.PropertyMap),
		OwnerID:    "Triton",
		Tags:       []string{"abc", "def"},
		Timestamps: m.Timestamps{
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Signature: uuid.MustParse("e4a677f6-8f1c-4765-be45-11b6400cc43b"),
		},
	}

	if err := driver.UpdateRecord(ctx, storeKey, record); err == nil {
		t.Error("UpdateRecord should return an error if the specified record doesn't exist.")
	}

	if err := driver.InsertRecord(ctx, storeKey, record); err != nil {
		t.Fatalf("Failed to create a new record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	record.Tags = append(record.Tags, "ghi")
	record.OwnerID = "NewOwner"
	record.Timestamps.UpdatedAt = time.Date(1988, 5, 17, 5, 6, 8, int(4321*time.Microsecond), time.UTC)
	record.Timestamps.Signature = uuid.MustParse("3c1dc762-8f22-4d85-b729-b90393f45ca6")
	if err := driver.UpdateRecord(ctx, storeKey, record); err != nil {
		t.Fatalf("Failed to update a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}

	record2, err := driver.GetRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to get a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	assertEqualRecord(t, record, record2, "GetRecord should fetch the updated record.")

	err = driver.DeleteRecord(ctx, storeKey, recordKey)
	if err != nil {
		t.Fatalf("Failed to delete a record (%s) in store (%s): %v", recordKey, storeKey, err)
	}
	err = driver.DeleteStore(ctx, storeKey)
	if err != nil {
		t.Fatalf("Failed to delete a store (%s): %v", storeKey, err)
	}
}

func TestDriver_DeleteShouldNotFailWithNonExistentKey(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	storeKey := newStoreKey()
	if err := driver.DeleteStore(ctx, storeKey); err != nil {
		t.Fatalf("DeleteStore failed with a non-existent key: %v", err)
	}
	recordKey := newRecordKey()
	if err := driver.DeleteRecord(ctx, storeKey, recordKey); err != nil {
		t.Fatalf("DeleteRecord failed with a non-existent key: %v", err)
	}
}

func TestDriver_TimestampPrecision(t *testing.T) {
	ctx := context.Background()
	driver := newDriver(ctx, t)
	defer driver.Disconnect(ctx)
	assert.Equal(t, timestampPrecision, driver.TimestampPrecision())
}
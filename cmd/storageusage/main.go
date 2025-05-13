package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/datastore"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/api/iterator"
)

const (
	maxConcurrency     = 100
	storeKind          = "store"
	recordKind         = "record"
	storeSize          = 262
	recordBaseSize     = 125
	blobBaseSize       = 120
	chunkSize          = 200
	propertiesBaseSize = 50
	tagsBaseSize       = 40
	// projectID          = "dev-triton"
	// environment        = "dev"
	projectID   = "kgct-clouddata"
	environment = "prod"
)

type productUsage struct {
	productID      string
	recordsDSSize int64
	blobsDSSize   int64
	blobsGCSSize  int64
	chunksDSSize  int64
}

func main() {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Enabling Metrics exporter")
	meterProvider, err := metrics.InitMetrics(true, false)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := meterProvider.Shutdown(ctx); err != nil {
			log.Fatalf("Error shutting down meter provider: %v", err)
		}
	}()

	cldDatastoreMeter := otel.Meter("cld-datastore")
	recordDSSizeGauge, _ := cldDatastoreMeter.Int64Gauge("dna.cld.datastore.record-size-bytes")
	blobDSSizeGauge, _ := cldDatastoreMeter.Int64Gauge("dna.cld.datastore.blob-size-bytes")
	chunkDSSizeGauge, _ := cldDatastoreMeter.Int64Gauge("dna.cld.datastore.chunk-size-bytes")
	cldGCSMeter := otel.Meter("cld-gcs")
	blobGCSMeter, _ := cldGCSMeter.Int64Gauge("dna.cld.gcs.blob-size-bytes")

	// Start fetching Stores asynchronously.
	// Each store will be emitted in the storeChannel, once all are emitted the channel will be closed.
	storeChannel := fetchStoresAsync(ctx, client, maxConcurrency)

	// Calculate storage usage asynchronously, obtaining the stores to calculate from the store channel.
	// Calculations will be emitted in the usageChannel, once all are emitted the channel will be closed.
	// As we depend on the storeChannel, the usageChannel won't be closed until storeChannel is closed too.
	usageChannel := calculateStoreUsageAsync(ctx, client, storeChannel, maxConcurrency)

	// Accumulate each store productUsage coming from the usageChannel.
	productStorageUsage := map[string]productUsage{}
	for storageUsage := range usageChannel {
		totalStorageUsage, ok := productStorageUsage[storageUsage.productID]
		if ok {
			totalStorageUsage.recordsDSSize += storageUsage.recordsDSSize
			totalStorageUsage.blobsDSSize += storageUsage.blobsDSSize
			totalStorageUsage.chunksDSSize += storageUsage.chunksDSSize
			totalStorageUsage.blobsGCSSize += storageUsage.blobsGCSSize
		} else {
			totalStorageUsage = *storageUsage
		}
		productStorageUsage[storageUsage.productID] = totalStorageUsage
	}

	// Finally emit the metrics per product.
	for productID, storageUsage := range productStorageUsage {
		log.Printf("Emitting metric for product %s = %#v", productID, storageUsage)

		attributes := attribute.NewSet(attribute.String("product_id", productID), attribute.String("env", environment),
			attribute.String("project_id", projectID), attribute.String("service", "storage-usage-calculator"),
			attribute.String("version", "2"))

		recordDSSizeGauge.Record(ctx, storageUsage.recordsDSSize, metric.WithAttributeSet(attributes))
		blobDSSizeGauge.Record(ctx, storageUsage.blobsDSSize, metric.WithAttributeSet(attributes))
		chunkDSSizeGauge.Record(ctx, storageUsage.chunksDSSize, metric.WithAttributeSet(attributes))
		blobGCSMeter.Record(ctx, storageUsage.blobsGCSSize, metric.WithAttributeSet(attributes))
	}
}

func fetchStoresAsync(ctx context.Context, client *datastore.Client, concurrency int) <-chan *datastore.Key {
	output := make(chan *datastore.Key, concurrency)

	go func() {
		defer close(output)

		query := datastore.NewQuery(storeKind).KeysOnly().
			FilterField("Tags", "in", []interface{}{"store:user-product", "store:game"}).Limit(100)
		storesIter := client.Run(ctx, query)

		for {
			storeKey, err := storesIter.Next(nil)
			if err == iterator.Done {
				log.Println("All stores read")
				break
			} else if err != nil {
				log.Fatal(err)
			}

			output <- storeKey
		}
	}()

	return output
}

func calculateStoreUsageAsync(ctx context.Context, client *datastore.Client, storeChannel <-chan *datastore.Key, concurrency int) <-chan *productUsage {
	output := make(chan *productUsage, concurrency)

	go func() {
		// As we are reading the records async too, we need to spark as much Go routines as the maximum concurrency.
		// To do so, we benefit of the blocking nature of channels.
		// Each time a store is captured, we emit an event into the concurrencyLimiter channel, once the number of events matches with maxConcurrency,
		// the channel will block and will stop reading from the storeChannel, which in turn will block producing new Stores (Backpressure).
		concurrencyLimiter := make(chan any, maxConcurrency)
		// To prevent a race condition when the last stores are read BUT have not yet been processed, we need to keep track of the
		// stores we've received, so we can wait for all of them being processed.
		storesCounter := sync.WaitGroup{}

		defer func() {
			close(output)
			close(concurrencyLimiter)
		}()

		// sync.Mutex is required to update the shared productStorageUsage in a thread safe way.
		for storeKey := range storeChannel {
			storesCounter.Add(1)
			concurrencyLimiter <- 1
			go func(storeKey *datastore.Key) {
				defer func() {
					<-concurrencyLimiter
					storesCounter.Done()
				}()

				storeStorageUsage := calculateRecordsUsage(ctx, client, storeKey)

				output <- &storeStorageUsage

				log.Printf("Storage calculated for store %s = %#v", storeKey.Name, storeStorageUsage)
			}(storeKey)
		}

		storesCounter.Wait()
	}()

	return output
}

func calculateRecordsUsage(ctx context.Context, client *datastore.Client, storeKey *datastore.Key) productUsage {
	query := datastore.NewQuery(recordKind).Ancestor(storeKey).Limit(100)

	productID := storeKey.Name
	if strings.Contains(storeKey.Name, ":") {
		productID = strings.Split(storeKey.Name, ":")[1]
	}

	productUsage := productUsage{
		productID: productID,
	}

	recordsIter := client.Run(ctx, query)
	for {
		record := new(record.Record)
		_, err := recordsIter.Next(record)
		if err == iterator.Done {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		storeKeyLen := len(record.StoreKey)
		recordKeyLen := len(record.Key)
		externalBlobLen := len(record.ExternalBlob)
		blobSizeLen := len(strconv.FormatInt(record.BlobSize, 10))
		chunkCountLen := len(strconv.FormatInt(record.ChunkCount, 10))
		inlineBlobLen := len(record.Blob)
		inlineBlobSizeLen := len(strconv.FormatInt(record.BlobSize, 10))
		inlineBlobMD5Len := len(record.MD5)
		ownerIDLen := len(record.OwnerID)

		opaqueStringLen := len(record.OpaqueString)
		propertiesLen := propertiesBaseSize * len(record.Properties)
		tagsLen := tagsBaseSize * len(record.Tags)

		productUsage.recordsDSSize += int64(recordBaseSize + storeKeyLen + recordKeyLen + inlineBlobLen + inlineBlobSizeLen +
			opaqueStringLen + propertiesLen + tagsLen + chunkCountLen + inlineBlobMD5Len + ownerIDLen)
		if record.Chunked {
			productUsage.blobsDSSize += int64(blobBaseSize + externalBlobLen + storeKeyLen + recordKeyLen + blobSizeLen + chunkCountLen)
			productUsage.blobsGCSSize += record.BlobSize
			productUsage.chunksDSSize += chunkSize * record.ChunkCount
		}
	}

	return productUsage
}

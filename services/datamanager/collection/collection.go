// Deals with parsing Collection responses from API Server.

package collection

import (
	"flag"
	"fmt"
	"git.curoverse.com/arvados.git/sdk/go/arvadosclient"
	"git.curoverse.com/arvados.git/sdk/go/blockdigest"
	"git.curoverse.com/arvados.git/sdk/go/logger"
	"git.curoverse.com/arvados.git/sdk/go/manifest"
	"git.curoverse.com/arvados.git/sdk/go/util"
	"git.curoverse.com/arvados.git/services/datamanager/loggerutil"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

var (
	heapProfileFilename string
	// globals for debugging
	totalManifestSize uint64
	maxManifestSize   uint64
)

type Collection struct {
	Uuid              string
	OwnerUuid         string
	ReplicationLevel  int
	BlockDigestToSize map[blockdigest.BlockDigest]int
	TotalSize         int
}

type ReadCollections struct {
	ReadAllCollections        bool
	UuidToCollection          map[string]Collection
	OwnerToCollectionSize     map[string]int
	BlockToDesiredReplication map[blockdigest.DigestWithSize]int
	CollectionUuidToIndex     map[string]int
	CollectionIndexToUuid     []string
	BlockToCollectionIndices  map[blockdigest.DigestWithSize][]int
}

type GetCollectionsParams struct {
	Client    arvadosclient.ArvadosClient
	Logger    *logger.Logger
	BatchSize int
}

type SdkCollectionInfo struct {
	Uuid         string    `json:"uuid"`
	OwnerUuid    string    `json:"owner_uuid"`
	Redundancy   int       `json:"redundancy"`
	ModifiedAt   time.Time `json:"modified_at"`
	ManifestText string    `json:"manifest_text"`
}

type SdkCollectionList struct {
	ItemsAvailable int                 `json:"items_available"`
	Items          []SdkCollectionInfo `json:"items"`
}

func init() {
	flag.StringVar(&heapProfileFilename,
		"heap-profile",
		"",
		"File to write the heap profiles to. Leave blank to skip profiling.")
}

// Write the heap profile to a file for later review.
// Since a file is expected to only contain a single heap profile this
// function overwrites the previously written profile, so it is safe
// to call multiple times in a single run.
// Otherwise we would see cumulative numbers as explained here:
// https://groups.google.com/d/msg/golang-nuts/ZyHciRglQYc/2nh4Ndu2fZcJ
func WriteHeapProfile() {
	if heapProfileFilename != "" {

		heap_profile, err := os.Create(heapProfileFilename)
		if err != nil {
			log.Fatal(err)
		}

		defer heap_profile.Close()

		err = pprof.WriteHeapProfile(heap_profile)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func GetCollectionsAndSummarize(params GetCollectionsParams) (results ReadCollections) {
	results = GetCollections(params)
	results.Summarize(params.Logger)

	log.Printf("Uuid to Size used: %v", results.OwnerToCollectionSize)
	log.Printf("Read and processed %d collections",
		len(results.UuidToCollection))

	// TODO(misha): Add a "readonly" flag. If we're in readonly mode,
	// lots of behaviors can become warnings (and obviously we can't
	// write anything).
	// if !readCollections.ReadAllCollections {
	// 	log.Fatalf("Did not read all collections")
	// }

	return
}

func GetCollections(params GetCollectionsParams) (results ReadCollections) {
	if &params.Client == nil {
		log.Fatalf("params.Client passed to GetCollections() should " +
			"contain a valid ArvadosClient, but instead it is nil.")
	}

	fieldsWanted := []string{"manifest_text",
		"owner_uuid",
		"uuid",
		"redundancy",
		"modified_at"}

	sdkParams := arvadosclient.Dict{
		"select":  fieldsWanted,
		"order":   []string{"modified_at ASC"},
		"filters": [][]string{[]string{"modified_at", ">=", "1900-01-01T00:00:00Z"}}}

	if params.BatchSize > 0 {
		sdkParams["limit"] = params.BatchSize
	}

	var defaultReplicationLevel int
	{
		value, err := params.Client.Discovery("defaultCollectionReplication")
		if err != nil {
			loggerutil.FatalWithMessage(params.Logger,
				fmt.Sprintf("Error querying default collection replication: %v", err))
		}

		defaultReplicationLevel = int(value.(float64))
		if defaultReplicationLevel <= 0 {
			loggerutil.FatalWithMessage(params.Logger,
				fmt.Sprintf("Default collection replication returned by arvados SDK "+
					"should be a positive integer but instead it was %d.",
					defaultReplicationLevel))
		}
	}

	initialNumberOfCollectionsAvailable, err :=
		util.NumberItemsAvailable(params.Client, "collections")
	if err != nil {
		loggerutil.FatalWithMessage(params.Logger,
			fmt.Sprintf("Error querying collection count: %v", err))
	}
	// Include a 1% margin for collections added while we're reading so
	// that we don't have to grow the map in most cases.
	maxExpectedCollections := int(
		float64(initialNumberOfCollectionsAvailable) * 1.01)
	results.UuidToCollection = make(map[string]Collection, maxExpectedCollections)

	if params.Logger != nil {
		params.Logger.Update(func(p map[string]interface{}, e map[string]interface{}) {
			collectionInfo := logger.GetOrCreateMap(p, "collection_info")
			collectionInfo["num_collections_at_start"] = initialNumberOfCollectionsAvailable
			collectionInfo["batch_size"] = params.BatchSize
			collectionInfo["default_replication_level"] = defaultReplicationLevel
		})
	}

	// These values are just for getting the loop to run the first time,
	// afterwards they'll be set to real values.
	previousTotalCollections := -1
	totalCollections := 0
	for totalCollections > previousTotalCollections {
		// We're still finding new collections

		// Write the heap profile for examining memory usage
		WriteHeapProfile()

		// Get next batch of collections.
		var collections SdkCollectionList
		err := params.Client.List("collections", sdkParams, &collections)
		if err != nil {
			loggerutil.FatalWithMessage(params.Logger,
				fmt.Sprintf("Error querying collections: %v", err))
		}

		// Process collection and update our date filter.
		sdkParams["filters"].([][]string)[0][2] =
			ProcessCollections(params.Logger,
				collections.Items,
				defaultReplicationLevel,
				results.UuidToCollection).Format(time.RFC3339)

		// update counts
		previousTotalCollections = totalCollections
		totalCollections = len(results.UuidToCollection)

		log.Printf("%d collections read, %d new in last batch, "+
			"%s latest modified date, %.0f %d %d avg,max,total manifest size",
			totalCollections,
			totalCollections-previousTotalCollections,
			sdkParams["filters"].([][]string)[0][2],
			float32(totalManifestSize)/float32(totalCollections),
			maxManifestSize, totalManifestSize)

		if params.Logger != nil {
			params.Logger.Update(func(p map[string]interface{}, e map[string]interface{}) {
				collectionInfo := logger.GetOrCreateMap(p, "collection_info")
				collectionInfo["collections_read"] = totalCollections
				collectionInfo["latest_modified_date_seen"] = sdkParams["filters"].([][]string)[0][2]
				collectionInfo["total_manifest_size"] = totalManifestSize
				collectionInfo["max_manifest_size"] = maxManifestSize
			})
		}
	}

	// Write the heap profile for examining memory usage
	WriteHeapProfile()

	return
}

// StrCopy returns a newly allocated string.
// It is useful to copy slices so that the garbage collector can reuse
// the memory of the longer strings they came from.
func StrCopy(s string) string {
	return string([]byte(s))
}

func ProcessCollections(arvLogger *logger.Logger,
	receivedCollections []SdkCollectionInfo,
	defaultReplicationLevel int,
	uuidToCollection map[string]Collection) (latestModificationDate time.Time) {
	for _, sdkCollection := range receivedCollections {
		collection := Collection{Uuid: StrCopy(sdkCollection.Uuid),
			OwnerUuid:         StrCopy(sdkCollection.OwnerUuid),
			ReplicationLevel:  sdkCollection.Redundancy,
			BlockDigestToSize: make(map[blockdigest.BlockDigest]int)}

		if sdkCollection.ModifiedAt.IsZero() {
			loggerutil.FatalWithMessage(arvLogger,
				fmt.Sprintf(
					"Arvados SDK collection returned with unexpected zero "+
						"modification date. This probably means that either we failed to "+
						"parse the modification date or the API server has changed how "+
						"it returns modification dates: %+v",
					collection))
		}

		if sdkCollection.ModifiedAt.After(latestModificationDate) {
			latestModificationDate = sdkCollection.ModifiedAt
		}

		if collection.ReplicationLevel == 0 {
			collection.ReplicationLevel = defaultReplicationLevel
		}

		manifest := manifest.Manifest{sdkCollection.ManifestText}
		manifestSize := uint64(len(sdkCollection.ManifestText))

		if _, alreadySeen := uuidToCollection[collection.Uuid]; !alreadySeen {
			totalManifestSize += manifestSize
		}
		if manifestSize > maxManifestSize {
			maxManifestSize = manifestSize
		}

		blockChannel := manifest.BlockIterWithDuplicates()
		for block := range blockChannel {
			if stored_size, stored := collection.BlockDigestToSize[block.Digest]; stored && stored_size != block.Size {
				message := fmt.Sprintf(
					"Collection %s contains multiple sizes (%d and %d) for block %s",
					collection.Uuid,
					stored_size,
					block.Size,
					block.Digest)
				loggerutil.FatalWithMessage(arvLogger, message)
			}
			collection.BlockDigestToSize[block.Digest] = block.Size
		}
		collection.TotalSize = 0
		for _, size := range collection.BlockDigestToSize {
			collection.TotalSize += size
		}
		uuidToCollection[collection.Uuid] = collection

		// Clear out all the manifest strings that we don't need anymore.
		// These hopefully form the bulk of our memory usage.
		manifest.Text = ""
		sdkCollection.ManifestText = ""
	}

	return
}

func (readCollections *ReadCollections) Summarize(arvLogger *logger.Logger) {
	readCollections.OwnerToCollectionSize = make(map[string]int)
	readCollections.BlockToDesiredReplication = make(map[blockdigest.DigestWithSize]int)
	numCollections := len(readCollections.UuidToCollection)
	readCollections.CollectionUuidToIndex = make(map[string]int, numCollections)
	readCollections.CollectionIndexToUuid = make([]string, 0, numCollections)
	readCollections.BlockToCollectionIndices = make(map[blockdigest.DigestWithSize][]int)

	for _, coll := range readCollections.UuidToCollection {
		collectionIndex := len(readCollections.CollectionIndexToUuid)
		readCollections.CollectionIndexToUuid =
			append(readCollections.CollectionIndexToUuid, coll.Uuid)
		readCollections.CollectionUuidToIndex[coll.Uuid] = collectionIndex

		readCollections.OwnerToCollectionSize[coll.OwnerUuid] =
			readCollections.OwnerToCollectionSize[coll.OwnerUuid] + coll.TotalSize

		for block, size := range coll.BlockDigestToSize {
			locator := blockdigest.DigestWithSize{Digest: block, Size: uint32(size)}
			readCollections.BlockToCollectionIndices[locator] =
				append(readCollections.BlockToCollectionIndices[locator],
					collectionIndex)
			storedReplication := readCollections.BlockToDesiredReplication[locator]
			if coll.ReplicationLevel > storedReplication {
				readCollections.BlockToDesiredReplication[locator] =
					coll.ReplicationLevel
			}
		}
	}

	if arvLogger != nil {
		arvLogger.Update(func(p map[string]interface{}, e map[string]interface{}) {
			collectionInfo := logger.GetOrCreateMap(p, "collection_info")
			// Since maps are shallow copied, we run a risk of concurrent
			// updates here. By copying results.OwnerToCollectionSize into
			// the log, we're assuming that it won't be updated.
			collectionInfo["owner_to_collection_size"] =
				readCollections.OwnerToCollectionSize
			collectionInfo["distinct_blocks_named"] =
				len(readCollections.BlockToDesiredReplication)
		})
	}

	return
}

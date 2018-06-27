/*******************************************************************************
 * Copyright (c) 2018 Genome Research Ltd.
 *
 * Author: Joshua C. Randall <jcrandall@alum.mit.edu>
 *
 * This file is part of Arvados.
 *
 * Arvados is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.curoverse.com/arvados.git/sdk/go/arvados"
	"github.com/ceph/go-ceph/rados"
)

var (
	radosPool        string
	radosKeyRingFile string
	radosMonHost     string
	radosCluster     string
	radosUser        string
	radosReplication int
)

var (
	zeroTime time.Time
)

type radosVolumeAdder struct {
	*Config
}

// String implements flag.Value
func (s *radosVolumeAdder) String() string {
	return "-"
}

func (s *radosVolumeAdder) Set(poolName string) error {
	if poolName == "" {
		return fmt.Errorf("no rados pool name given")
	}
	if radosKeyRingFile == "" {
		return fmt.Errorf("-rados-keyring-file argument must given before -rados-pool-volume")
	}
	if deprecated.flagSerializeIO {
		log.Print("Notice: -serialize is not supported by rados-pool volumes.")
	}
	s.Config.Volumes = append(s.Config.Volumes, &RadosVolume{
		Pool:             poolName,
		KeyringFile:      radosKeyringFile,
		MonHost:          radosMonHost,
		Cluster:          radosCluster,
		User:             radosUser,
		RadosReplication: radosReplication,
	})
	return nil
}

func init() {
	VolumeTypes = append(VolumeTypes, func() VolumeWithExamples { return &RadosVolume{} })

	flag.Var(&radosVolumeAdder{theConfig},
		"rados-pool-volume",
		"Use the given Ceph pool as a storage volume. Can be given multiple times.")
	flag.StringVar(
		&radosCluster,
		"rados-cluster",
		"ceph",
		"The name of the Ceph cluster to use.")
	flag.StringVar(
		&radosKeyringFile,
		"rados-keyring-file",
		"",
		"`File` containing the ceph keyring used for subsequent -rados-pool-volume arguments.")
	flag.StringVar(
		&radosMonHost,
		"rados-mon-host",
		"",
		"The mon host IP address (or addresses, separated by commas) used for subsequent -rados-pool-volume arguments.")
	flag.StringVar(
		&radosUser,
		"rados-user",
		"",
		"The ceph client username (usually in the form `client.username`) used for subsequent -rados-pool-volume arguments.")
	flag.IntVar(
		&radosReplication,
		"rados-replication",
		3,
		"Replication level reported to clients for subsequent -rados-pool-volume arguments.")
}

// RadosVolume implements Volume using an Rados pool.
type RadosVolume struct {
	Pool             string
	KeyringFile      string
	MonHost          string
	Cluster          string
	User             string
	Debug            bool
	RadosReplication int

	ReadOnly       bool
	StorageClasses []string

	radosPool *radospool

	startOnce sync.Once
}

// Volume type as specified in config file. Examples: "S3",
// "Directory".
func (*RadosVolume) Type() string {
	return "Rados"
}

// Do whatever private setup tasks and configuration checks
// are needed. Return non-nil if the volume is unusable (e.g.,
// invalid config).
func (v *RadosVolume) Start() error {
	var err error

	if v.Pool == "" {
		return fmt.Errorf("-rados-pool is required for Rados volumes")
	}

	if v.MonHost == "" {
		return fmt.Errorf("-rados-mon-host is required for Rados volumes")
	}

	if v.Cluster == "" {
		v.Cluster = "ceph"
	}

	if v.User == "" {
		v.Cluster = "client.admin"
	}

	rv_major, rv_minor, rv_patch := rados.Version()
	theConfig.debugLogf("rados: using librados version %d.%d.%d", rv_major, rv_minor, rv_patch)

	conn, err := rados.NewConnWithClusterAndUser(v.Cluster, v.User)
	if err != nil {
		return fmt.Errorf("rados: error creating rados connection to ceph cluster '%s' for user '%s': %v", v.Cluster, v.User, err)
	}

	err = conn.SetConfigOption("mon_host", v.MonHost)
	if err != nil {
		return fmt.Errorf("rados: error setting mon_host for rados to '%s': %v", v.MonHost, err)
	}

	if v.Debug {
		err = conn.SetConfigOption("log_to_stderr", "1")
		if err != nil {
			return fmt.Errorf("rados: error setting log_to_stderr rados config for debugging: %v", err)
		}
		err = conn.SetConfigOption("err_to_stderr", "1")
		if err != nil {
			return fmt.Errorf("rados: error setting err_to_stderr rados config for debugging: %v", err)
		}
	}

	if v.KeyringFile != "" {
		err = conn.SetConfigOption("keyring", v.KeyringFile)
		if err != nil {
			return fmt.Errorf("rados: error setting keyring for rados to '%s': %v", v.KeyringFile, err)
		}
	}

	err = conn.Connect()
	if err != nil {
		return fmt.Errorf("rados: error connecting to rados cluster: %v", err)
	}
	theConfig.debugLogf("rados: connected to cluster '%s' as user '%s'")

	fsid, err := conn.GetFSID()
	if err != nil {
		return fmt.Errorf("rados: error getting rados cluster FSID: %v", err)
	}
	theConfig.debugLogf("rados: cluster FSID is '%s'", fsid)

	cs, err := conn.GetClusterStats()
	if err != nil {
		return fmt.Errorf("rados: error getting rados cluster stats: %v", err)
	}
	theConfig.debugLogf("rados: ceph cluster %s has %.1f GiB with %.1f GiB used in %d objects and %.1f GiB available", v.Cluster, cs.Kb/1024/1024, cs.Kb_used/1024/1024, cs.Kb_avail/1024/1024, cs.Num_objects)

	pools, err := conn.ListPools()
	if err != nil {
		return fmt.Errorf("rados: error listing pools: %v", err)
	}
	if !stringInSlice(v.Pool, pools) {
		return fmt.Error("rados: pool '%s' not present in ceph cluster. available pools in this cluster are: %v", v.Pool, pools)
	}

	ioctx, err := conn.OpenIOContext(v.Pool)
	if err != nil {
		return fmt.Errorf("rados: error opening IO context for pool '%s': %v", v.Pool, err)
	}

	ioctx.SetNamespace("keep")

	v.radosPool = &radospool{
		IOContext: ioctx,
	}

	return nil
}

// Get a block: copy the block data into buf, and return the
// number of bytes copied.
//
// loc is guaranteed to consist of 32 or more lowercase hex
// digits.
//
// Get should not verify the integrity of the data: it should
// just return whatever was found in its backing
// store. (Integrity checking is the caller's responsibility.)
//
// If an error is encountered that prevents it from
// retrieving the data, that error should be returned so the
// caller can log (and send to the client) a more useful
// message.
//
// If the error is "not found", and there's no particular
// reason to expect the block to be found (other than that a
// caller is asking for it), the returned error should satisfy
// os.IsNotExist(err): this is a normal condition and will not
// be logged as an error (except that a 404 will appear in the
// access log if the block is not found on any other volumes
// either).
//
// If the data in the backing store is bigger than len(buf),
// then Get is permitted to return an error without reading
// any of the data.
//
// len(buf) will not exceed BlockSize.
func (v *RadosVolume) Get(ctx context.Context, loc string, buf []byte) (int, error) {
	rdr, err := v.getReaderWithContext(ctx, loc)
	if err != nil {
		return 0, err
	}

	var n int
	ready := make(chan bool)
	go func() {
		defer close(ready)

		defer rdr.Close()
		n, err = io.ReadFull(rdr, buf)

		switch err {
		case nil, io.EOF, io.ErrUnexpectedEOF:
			err = nil
		default:
			err = v.translateError(err)
		}
	}()
	select {
	case <-ctx.Done():
		theConfig.debugLogf("rados: interrupting ReadFull() with Close() because %s", ctx.Err())
		rdr.Close()
		// Must wait for ReadFull to return, to ensure it
		// doesn't write to buf after we return.
		theConfig.debugLogf("rados: waiting for ReadFull() to fail")
		<-ready
		return 0, ctx.Err()
	case <-ready:
		return n, err
	}
}

// Compare the given data with the stored data (i.e., what Get
// would return). If equal, return nil. If not, return
// CollisionError or DiskHashError (depending on whether the
// data on disk matches the expected hash), or whatever error
// was encountered opening/reading the stored data.
func (v *RadosVolume) Compare(ctx context.Context, loc string, expect []byte) error {
	rdr, err := v.getReaderWithContext(ctx, loc)
	if err != nil {
		return err
	}
	defer rdr.Close()
	return v.translateError(compareReaderWithBuf(ctx, rdr, expect, loc[:32]))
}

// Put writes a block to an underlying storage device.
//
// loc is as described in Get.
//
// len(block) is guaranteed to be between 0 and BlockSize.
//
// If a block is already stored under the same name (loc) with
// different content, Put must either overwrite the existing
// data with the new data or return a non-nil error. When
// overwriting existing data, it must never leave the storage
// device in an inconsistent state: a subsequent call to Get
// must return either the entire old block, the entire new
// block, or an error. (An implementation that cannot peform
// atomic updates must leave the old data alone and return an
// error.)
//
// Put also sets the timestamp for the given locator to the
// current time.
//
// Put must return a non-nil error unless it can guarantee
// that the entire block has been written and flushed to
// persistent storage, and that its timestamp is current. Of
// course, this guarantee is only as good as the underlying
// storage device, but it is Put's responsibility to at least
// get whatever guarantee is offered by the storage device.
//
// Put should not verify that loc==hash(block): this is the
// caller's responsibility.
func (v *RadosVolume) Put(ctx context.Context, loc string, block []byte) error {
	if v.ReadOnly {
		return MethodDisabledError
	}

	err := v.radosPool.IOContext.WriteFull(loc, block)
	v.radosPool.stats.Tick(&v.radosPool.stats.Ops, &v.radosPool.stats.PutOps)
	v.radosPool.stats.TickErr(err)
	if err != nil {
		return err
	}
	v.radosPool.stats.TickOutBytes(len(block))

	// must also touch the object to set keep_mtime
	v.radosPool.Touch(loc)

	return v.translateError(err)
}

// Touch sets the timestamp for the given locator to the
// current time.
//
// loc is as described in Get.
//
// If invoked at time t0, Touch must guarantee that a
// subsequent call to Mtime will return a timestamp no older
// than {t0 minus one second}. For example, if Touch is called
// at 2015-07-07T01:23:45.67890123Z, it is acceptable for a
// subsequent Mtime to return any of the following:
//
//   - 2015-07-07T01:23:45.00000000Z
//   - 2015-07-07T01:23:45.67890123Z
//   - 2015-07-07T01:23:46.67890123Z
//   - 2015-07-08T00:00:00.00000000Z
//
// It is not acceptable for a subsequente Mtime to return
// either of the following:
//
//   - 2015-07-07T00:00:00.00000000Z -- ERROR
//   - 2015-07-07T01:23:44.00000000Z -- ERROR
//
// Touch must return a non-nil error if the timestamp cannot
// be updated.
func (v *RadosVolume) Touch(loc string) error {
	if v.ReadOnly {
		return MethodDisabledError
	}
	_, err := v.radosPool.Head(loc, nil)
	err = v.translateError(err)
	if os.IsNotExist(err) && v.fixRace(loc) {
		// The data object got trashed in a race, but fixRace
		// rescued it.
	} else if err != nil {
		return err
	}
	err = v.radosPool.PutReader("recent/"+loc, nil, 0, "application/octet-stream", radosACL, rados.Options{})
	return v.translateError(err)
}

// Mtime returns the stored timestamp for the given locator.
//
// loc is as described in Get.
//
// Mtime must return a non-nil error if the given block is not
// found or the timestamp could not be retrieved.
func (v *RadosVolume) Mtime(loc string) (time.Time, error) {
	_, err := v.radosPool.Head(loc, nil)
	if err != nil {
		return zeroTime, v.translateError(err)
	}
	resp, err := v.radosPool.Head("recent/"+loc, nil)
	err = v.translateError(err)
	if os.IsNotExist(err) {
		// The data object X exists, but recent/X is missing.
		err = v.radosPool.PutReader("recent/"+loc, nil, 0, "application/octet-stream", radosACL, rados.Options{})
		if err != nil {
			log.Printf("error: creating %q: %s", "recent/"+loc, err)
			return zeroTime, v.translateError(err)
		}
		log.Printf("info: created %q to migrate existing block to new storage scheme", "recent/"+loc)
		resp, err = v.radosPool.Head("recent/"+loc, nil)
		if err != nil {
			log.Printf("error: created %q but HEAD failed: %s", "recent/"+loc, err)
			return zeroTime, v.translateError(err)
		}
	} else if err != nil {
		// HEAD recent/X failed for some other reason.
		return zeroTime, err
	}
	return v.lastModified(resp)
}

// IndexTo writes a complete list of locators with the given
// prefix for which Get() can retrieve data.
//
// prefix consists of zero or more lowercase hexadecimal
// digits.
//
// Each locator must be written to the given writer using the
// following format:
//
//   loc "+" size " " timestamp "\n"
//
// where:
//
//   - size is the number of bytes of content, given as a
//     decimal number with one or more digits
//
//   - timestamp is the timestamp stored for the locator,
//     given as a decimal number of seconds after January 1,
//     1970 UTC.
//
// IndexTo must not write any other data to writer: for
// example, it must not write any blank lines.
//
// If an error makes it impossible to provide a complete
// index, IndexTo must return a non-nil error. It is
// acceptable to return a non-nil error after writing a
// partial index to writer.
//
// The resulting index is not expected to be sorted in any
// particular order.
func (v *RadosVolume) IndexTo(prefix string, writer io.Writer) error {
	// Use a merge sort to find matching sets of X and recent/X.
	dataL := radosLister{
		Pool:   v.radosPool.Pool,
		Prefix: prefix,
	}
	recentL := radosLister{
		Pool:   v.radosPool.Pool,
		Prefix: "recent/" + prefix,
	}
	v.radosPool.stats.Tick(&v.radosPool.stats.Ops, &v.radosPool.stats.ListOps)
	v.radosPool.stats.Tick(&v.radosPool.stats.Ops, &v.radosPool.stats.ListOps)
	for data, recent := dataL.First(), recentL.First(); data != nil; data = dataL.Next() {
		v.radosPool.stats.Tick(&v.radosPool.stats.Ops, &v.radosPool.stats.ListOps)
		if data.Key >= "g" {
			// Conveniently, "recent/*" and "trash/*" are
			// lexically greater than all hex-encoded data
			// hashes, so stopping here avoids iterating
			// over all of them needlessly with dataL.
			break
		}
		if !v.isKeepBlock(data.Key) {
			continue
		}

		// stamp is the list entry we should use to report the
		// last-modified time for this data block: it will be
		// the recent/X entry if one exists, otherwise the
		// entry for the data block itself.
		stamp := data

		// Advance to the corresponding recent/X marker, if any
		for recent != nil {
			if cmp := strings.Compare(recent.Key[7:], data.Key); cmp < 0 {
				recent = recentL.Next()
				v.radosPool.stats.Tick(&v.radosPool.stats.Ops, &v.radosPool.stats.ListOps)
				continue
			} else if cmp == 0 {
				stamp = recent
				recent = recentL.Next()
				v.radosPool.stats.Tick(&v.radosPool.stats.Ops, &v.radosPool.stats.ListOps)
				break
			} else {
				// recent/X marker is missing: we'll
				// use the timestamp on the data
				// object.
				break
			}
		}
		t, err := time.Parse(time.RFC3339, stamp.LastModified)
		if err != nil {
			return err
		}
		fmt.Fprintf(writer, "%s+%d %d\n", data.Key, data.Size, t.UnixNano())
	}
	return nil
}

// Trash moves the block data from the underlying storage
// device to trash area. The block then stays in trash for
// -trash-lifetime interval before it is actually deleted.
//
// loc is as described in Get.
//
// If the timestamp for the given locator is newer than
// BlobSignatureTTL, Trash must not trash the data.
//
// If a Trash operation overlaps with any Touch or Put
// operations on the same locator, the implementation must
// ensure one of the following outcomes:
//
//   - Touch and Put return a non-nil error, or
//   - Trash does not trash the block, or
//   - Both of the above.
//
// If it is possible for the storage device to be accessed by
// a different process or host, the synchronization mechanism
// should also guard against races with other processes and
// hosts. If such a mechanism is not available, there must be
// a mechanism for detecting unsafe configurations, alerting
// the operator, and aborting or falling back to a read-only
// state. In other words, running multiple keepstore processes
// with the same underlying storage device must either work
// reliably or fail outright.
//
// Corollary: A successful Touch or Put guarantees a block
// will not be trashed for at least BlobSignatureTTL
// seconds.
func (v *RadosVolume) Trash(loc string) error {
	if v.ReadOnly {
		return MethodDisabledError
	}
	if t, err := v.Mtime(loc); err != nil {
		return err
	} else if time.Since(t) < theConfig.BlobSignatureTTL.Duration() {
		return nil
	}
	if theConfig.TrashLifetime == 0 {
		if !radosUnsafeDelete {
			return ErrRadosTrashDisabled
		}
		return v.translateError(v.radosPool.Del(loc))
	}
	err := v.checkRaceWindow(loc)
	if err != nil {
		return err
	}
	err = v.safeCopy("trash/"+loc, loc)
	if err != nil {
		return err
	}
	return v.translateError(v.radosPool.Del(loc))
}

// Untrash moves block from trash back into store
func (v *RadosVolume) Untrash(loc string) error {
	err := v.safeCopy(loc, "trash/"+loc)
	if err != nil {
		return err
	}
	err = v.radosPool.PutReader("recent/"+loc, nil, 0, "application/octet-stream", radosACL, rados.Options{})
	return v.translateError(err)
}

// Status returns a *VolumeStatus representing the current
// in-use and available storage capacity and an
// implementation-specific volume identifier (e.g., "mount
// point" for a UnixVolume).
func (v *RadosVolume) Status() *VolumeStatus {
	return &VolumeStatus{
		DeviceNum: 1,
		BytesFree: BlockSize * 1000,
		BytesUsed: 1,
	}
}

// String returns an identifying label for this volume,
// suitable for including in log messages. It should contain
// enough information to uniquely identify the underlying
// storage device, but should not contain any credentials or
// secrets.
func (v *RadosVolume) String() string {
	return fmt.Sprintf("rados-pool:%+q", v.Pool)
}

// Writable returns false if all future Put, Mtime, and Delete
// calls are expected to fail.
//
// If the volume is only temporarily unwritable -- or if Put
// will fail because it is full, but Mtime or Delete can
// succeed -- then Writable should return false.
func (v *RadosVolume) Writable() bool {
	return !v.ReadOnly
}

// Replication returns the storage redundancy of the
// underlying device. It will be passed on to clients in
// responses to PUT requests.
func (v *RadosVolume) Replication() int {
	return v.RadosReplication
}

// EmptyTrash looks for trashed blocks that exceeded TrashLifetime
// and deletes them from the volume.
func (v *RadosVolume) EmptyTrash() {
	var bytesInTrash, blocksInTrash, bytesDeleted, blocksDeleted int64

	// Define "ready to delete" as "...when EmptyTrash started".
	startT := time.Now()

	emptyOneKey := func(trash *rados.Key) {
		loc := trash.Key[6:]
		if !v.isKeepBlock(loc) {
			return
		}
		atomic.AddInt64(&bytesInTrash, trash.Size)
		atomic.AddInt64(&blocksInTrash, 1)

		trashT, err := time.Parse(time.RFC3339, trash.LastModified)
		if err != nil {
			log.Printf("warning: %s: EmptyTrash: %q: parse %q: %s", v, trash.Key, trash.LastModified, err)
			return
		}
		recent, err := v.radosPool.Head("recent/"+loc, nil)
		if err != nil && os.IsNotExist(v.translateError(err)) {
			log.Printf("warning: %s: EmptyTrash: found trash marker %q but no %q (%s); calling Untrash", v, trash.Key, "recent/"+loc, err)
			err = v.Untrash(loc)
			if err != nil {
				log.Printf("error: %s: EmptyTrash: Untrash(%q): %s", v, loc, err)
			}
			return
		} else if err != nil {
			log.Printf("warning: %s: EmptyTrash: HEAD %q: %s", v, "recent/"+loc, err)
			return
		}
		recentT, err := v.lastModified(recent)
		if err != nil {
			log.Printf("warning: %s: EmptyTrash: %q: parse %q: %s", v, "recent/"+loc, recent.Header.Get("Last-Modified"), err)
			return
		}
		if trashT.Sub(recentT) < theConfig.BlobSignatureTTL.Duration() {
			if age := startT.Sub(recentT); age >= theConfig.BlobSignatureTTL.Duration()-time.Duration(v.RaceWindow) {
				// recent/loc is too old to protect
				// loc from being Trashed again during
				// the raceWindow that starts if we
				// delete trash/X now.
				//
				// Note this means (TrashCheckInterval
				// < BlobSignatureTTL - raceWindow) is
				// necessary to avoid starvation.
				log.Printf("notice: %s: EmptyTrash: detected old race for %q, calling fixRace + Touch", v, loc)
				v.fixRace(loc)
				v.Touch(loc)
				return
			}
			_, err := v.radosPool.Head(loc, nil)
			if os.IsNotExist(err) {
				log.Printf("notice: %s: EmptyTrash: detected recent race for %q, calling fixRace", v, loc)
				v.fixRace(loc)
				return
			} else if err != nil {
				log.Printf("warning: %s: EmptyTrash: HEAD %q: %s", v, loc, err)
				return
			}
		}
		if startT.Sub(trashT) < theConfig.TrashLifetime.Duration() {
			return
		}
		err = v.radosPool.Del(trash.Key)
		if err != nil {
			log.Printf("warning: %s: EmptyTrash: deleting %q: %s", v, trash.Key, err)
			return
		}
		atomic.AddInt64(&bytesDeleted, trash.Size)
		atomic.AddInt64(&blocksDeleted, 1)

		_, err = v.radosPool.Head(loc, nil)
		if err == nil {
			log.Printf("warning: %s: EmptyTrash: HEAD %q succeeded immediately after deleting %q", v, loc, loc)
			return
		}
		if !os.IsNotExist(v.translateError(err)) {
			log.Printf("warning: %s: EmptyTrash: HEAD %q: %s", v, loc, err)
			return
		}
		err = v.radosPool.Del("recent/" + loc)
		if err != nil {
			log.Printf("warning: %s: EmptyTrash: deleting %q: %s", v, "recent/"+loc, err)
		}
	}

	var wg sync.WaitGroup
	todo := make(chan *rados.Key, theConfig.EmptyTrashWorkers)
	for i := 0; i < 1 || i < theConfig.EmptyTrashWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range todo {
				emptyOneKey(key)
			}
		}()
	}

	trashL := radosLister{
		Pool:   v.radosPool.Pool,
		Prefix: "trash/",
	}
	for trash := trashL.First(); trash != nil; trash = trashL.Next() {
		todo <- trash
	}
	close(todo)
	wg.Wait()

	if err := trashL.Error(); err != nil {
		log.Printf("error: %s: EmptyTrash: lister: %s", v, err)
	}
	log.Printf("EmptyTrash stats for %v: Deleted %v bytes in %v blocks. Remaining in trash: %v bytes in %v blocks.", v.String(), bytesDeleted, blocksDeleted, bytesInTrash-bytesDeleted, blocksInTrash-blocksDeleted)
}

// Return a globally unique ID of the underlying storage
// device if possible, otherwise "".
func (v *RadosVolume) DeviceID() string {
	return "rados://" + v.MonHost + "/" + v.Cluster + "/" + v.Pool
}

// Get the storage classes associated with this volume
func (v *RadosVolume) GetStorageClasses() []string {
	return v.StorageClasses
}

// Examples implements VolumeWithExamples.
func (*RadosVolume) Examples() []Volume {
	return []Volume{
		&RadosVolume{
			Pool:             "keep01",
			KeyringFile:      "/etc/ceph/client.keep01.keyring",
			MonHost:          "1.2.3.4,5.6.7.8,9.10.11.12",
			Cluster:          "ceph",
			User:             "client.keep01",
			RadosReplication: 3,
		},
		&RadosVolume{
			Pool:             "keep02",
			KeyringFile:      "/etc/ceph/client.keep02.keyring",
			MonHost:          "1.2.3.4,5.6.7.8,9.10.11.12",
			Cluster:          "ceph",
			User:             "client.keep02",
			RadosReplication: 3,
		},
	}
}

// Return true if string s is an element of slice l
func stringInSlice(s string, l []string) bool {
	for _, li := range l {
		if li == s {
			return true
		}
	}
	return false
}

// TODO
func (v *RadosVolume) getReaderWithContext(ctx context.Context, loc string) (rdr io.ReadCloser, err error) {
	ready := make(chan bool)
	go func() {
		rdr, err = v.getReader(loc)
		close(ready)
	}()
	select {
	case <-ready:
		return
	case <-ctx.Done():
		theConfig.debugLogf("rados: abandoning getReader(): %s", ctx.Err())
		go func() {
			<-ready
			if err == nil {
				rdr.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// getReader wraps radosPool.GetReader
func (v *RadosVolume) getReader(loc string) (rdr io.ReadCloser, err error) {
	rdr, err = v.radosPool.GetReader(loc)
	if err != nil {
		err = v.translateError(err)
	}
	return
}

// checkRaceWindow returns a non-nil error if trash/loc is, or might
// be, in the race window (i.e., it's not safe to trash loc).
func (v *RadosVolume) checkRaceWindow(loc string) error {
	resp, err := v.radosPool.Head("trash/"+loc, nil)
	err = v.translateError(err)
	if os.IsNotExist(err) {
		// OK, trash/X doesn't exist so we're not in the race
		// window
		return nil
	} else if err != nil {
		// Error looking up trash/X. We don't know whether
		// we're in the race window
		return err
	}
	t, err := v.lastModified(resp)
	if err != nil {
		// Can't parse timestamp
		return err
	}
	safeWindow := t.Add(theConfig.TrashLifetime.Duration()).Sub(time.Now().Add(time.Duration(v.RaceWindow)))
	if safeWindow <= 0 {
		// We can't count on "touch trash/X" to prolong
		// trash/X's lifetime. The new timestamp might not
		// become visible until now+raceWindow, and EmptyTrash
		// is allowed to delete trash/X before then.
		return fmt.Errorf("same block is already in trash, and safe window ended %s ago", -safeWindow)
	}
	// trash/X exists, but it won't be eligible for deletion until
	// after now+raceWindow, so it's safe to overwrite it.
	return nil
}

// safeCopy calls PutCopy, and checks the response to make sure the
// copy succeeded and updated the timestamp on the destination object
// (PutCopy returns 200 OK if the request was received, even if the
// copy failed).
func (v *RadosVolume) safeCopy(dst, src string) error {
	resp, err := v.radosPool.PutCopy(dst, radosACL, rados.CopyOptions{
		ContentType:       "application/octet-stream",
		MetadataDirective: "REPLACE",
	}, v.radosPool.Name+"/"+src)
	err = v.translateError(err)
	if err != nil {
		return err
	}
	if t, err := time.Parse(time.RFC3339Nano, resp.LastModified); err != nil {
		return fmt.Errorf("PutCopy succeeded but did not return a timestamp: %q: %s", resp.LastModified, err)
	} else if time.Now().Sub(t) > maxClockSkew {
		return fmt.Errorf("PutCopy succeeded but returned an old timestamp: %q: %s", resp.LastModified, t)
	}
	return nil
}

// Get the LastModified header from resp, and parse it as RFC1123 or
// -- if it isn't valid RFC1123 -- as Amazon's variant of RFC1123.
func (v *RadosVolume) lastModified(resp *http.Response) (t time.Time, err error) {
	s := resp.Header.Get("Last-Modified")
	t, err = time.Parse(time.RFC1123, s)
	if err != nil && s != "" {
		// AWS example is "Sun, 1 Jan 2006 12:00:00 GMT",
		// which isn't quite "Sun, 01 Jan 2006 12:00:00 GMT"
		// as required by HTTP spec. If it's not a valid HTTP
		// header value, it's probably AWS (or radostest) giving
		// us a nearly-RFC1123 timestamp.
		t, err = time.Parse(nearlyRFC1123, s)
	}
	return
}

// InternalStats returns pool I/O and API call counters.
func (v *RadosVolume) InternalStats() interface{} {
	return &v.radosPool.stats
}

// String implements fmt.Stringer.

// Writable returns false if all future Put, Mtime, and Delete calls
// are expected to fail.

// Replication returns the storage redundancy of the underlying
// device. Configured via command line flag.

// GetStorageClasses implements Volume

var radosKeepBlockRegexp = regexp.MustCompile(`^[0-9a-f]{32}$`)

func (v *RadosVolume) isKeepBlock(s string) bool {
	return radosKeepBlockRegexp.MatchString(s)
}

// fixRace(X) is called when "recent/X" exists but "X" doesn't
// exist. If the timestamps on "recent/"+loc and "trash/"+loc indicate
// there was a race between Put and Trash, fixRace recovers from the
// race by Untrashing the block.
func (v *RadosVolume) fixRace(loc string) bool {
	trash, err := v.radosPool.Head("trash/"+loc, nil)
	if err != nil {
		if !os.IsNotExist(v.translateError(err)) {
			log.Printf("error: fixRace: HEAD %q: %s", "trash/"+loc, err)
		}
		return false
	}
	trashTime, err := v.lastModified(trash)
	if err != nil {
		log.Printf("error: fixRace: parse %q: %s", trash.Header.Get("Last-Modified"), err)
		return false
	}

	recent, err := v.radosPool.Head("recent/"+loc, nil)
	if err != nil {
		log.Printf("error: fixRace: HEAD %q: %s", "recent/"+loc, err)
		return false
	}
	recentTime, err := v.lastModified(recent)
	if err != nil {
		log.Printf("error: fixRace: parse %q: %s", recent.Header.Get("Last-Modified"), err)
		return false
	}

	ageWhenTrashed := trashTime.Sub(recentTime)
	if ageWhenTrashed >= theConfig.BlobSignatureTTL.Duration() {
		// No evidence of a race: block hasn't been written
		// since it became eligible for Trash. No fix needed.
		return false
	}

	log.Printf("notice: fixRace: %q: trashed at %s but touched at %s (age when trashed = %s < %s)", loc, trashTime, recentTime, ageWhenTrashed, theConfig.BlobSignatureTTL)
	log.Printf("notice: fixRace: copying %q to %q to recover from race between Put/Touch and Trash", "recent/"+loc, loc)
	err = v.safeCopy(loc, "trash/"+loc)
	if err != nil {
		log.Printf("error: fixRace: %s", err)
		return false
	}
	return true
}

func (v *RadosVolume) translateError(err error) error {
	switch err := err.(type) {
	case *rados.RadosError:
		if err == rados.RadosErrorNotFound {
			return os.ErrNotExist
		}
	}
	return err
}

// EmptyTrash looks for trashed blocks that exceeded TrashLifetime
// and deletes them from the volume.

type radosLister struct {
	Pool       *rados.Pool
	Prefix     string
	PageSize   int
	nextMarker string
	buf        []rados.Key
	err        error
}

// First fetches the first page and returns the first item. It returns
// nil if the response is the empty set or an error occurs.
func (lister *radosLister) First() *rados.Key {
	lister.getPage()
	return lister.pop()
}

// Next returns the next item, fetching the next page if necessary. It
// returns nil if the last available item has already been fetched, or
// an error occurs.
func (lister *radosLister) Next() *rados.Key {
	if len(lister.buf) == 0 && lister.nextMarker != "" {
		lister.getPage()
	}
	return lister.pop()
}

// Return the most recent error encountered by First or Next.
func (lister *radosLister) Error() error {
	return lister.err
}

func (lister *radosLister) getPage() {
	resp, err := lister.Pool.List(lister.Prefix, "", lister.nextMarker, lister.PageSize)
	lister.nextMarker = ""
	if err != nil {
		lister.err = err
		return
	}
	if resp.IsTruncated {
		lister.nextMarker = resp.NextMarker
	}
	lister.buf = make([]rados.Key, 0, len(resp.Contents))
	for _, key := range resp.Contents {
		if !strings.HasPrefix(key.Key, lister.Prefix) {
			log.Printf("warning: radosLister: Rados Pool.List(prefix=%q) returned key %q", lister.Prefix, key.Key)
			continue
		}
		lister.buf = append(lister.buf, key)
	}
}

func (lister *radosLister) pop() (k *rados.Key) {
	if len(lister.buf) > 0 {
		k = &lister.buf[0]
		lister.buf = lister.buf[1:]
	}
	return
}

type radospoolreaderat struct {
	*radospool
	oid string
}

func (r *radospoolreaderat) ReadAt(p []byte, off int64) (n int, err error) {
	return r.radospool.IOContext.Read(r.oid, p, off)
}

// radospool wraps rados.pool and counts I/O and API usage stats.
type radospool struct {
	*rados.IOContext
	stats radospoolStats
}

func (p *radospool) Touch(oid string) error {
	mtime := time.Now().UnixNano()
	mtime_bytes := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(mtime_bytes, mtime)
	err = p.IOContext.SetXattr(oid, "keep_mtime", mtime_bytes)
	p.stats.Tick(&p.stats.Ops, &p.stats.XattrOps)
	p.stats.TickErr(err)
	if err != nil {
		err = fmt.Errorf("Failed to update mtime xattr for oid '%s': %v", oid, err)
	}
	return err
}

func (p *radospool) GetReader(oid string) (io.ReadCloser, error) {
	rdrat := &radospoolreaderat{
		radospool: p,
		oid:       oid,
	}
	rdr := io.NewSectionReader(rdrat, 0, BlockSize)
	p.stats.Tick(&p.stats.Ops, &p.stats.GetOps)
	return NewCountingReader(rdr, p.stats.TickInBytes), nil
}

func (p *radospool) Head(oid string, headers map[string][]string) (*rados.ObjectStat, error) {
	stat, err := p.IOContext.Stat(oid, headers)
	p.stats.Tick(&p.stats.Ops, &p.stats.StatOps)
	p.stats.TickErr(err)
	return stat, err
}

func (p *radospool) PutReader(oid string, r io.Reader, length int64) error {
	if length == 0 {
		// Ceph (or at least go-ceph) cannot store 0-length
		// objects, so we treat them as a special case.
		r = nil
	} else {
		r = NewCountingReader(r, p.stats.TickOutBytes)
	}

	buf := make([]byte, length)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return err
	}

	return err
}

func (p *radospool) Del(path string) error {
	err := p.IOContext.Del(path)
	p.stats.Tick(&p.stats.Ops, &p.stats.DelOps)
	p.stats.TickErr(err)
	return err
}

type radospoolStats struct {
	statsTicker
	Ops      uint64
	GetOps   uint64
	PutOps   uint64
	StatOps  uint64
	XattrOps uint64
	DelOps   uint64
	ListOps  uint64
}

func (s *radospoolStats) TickErr(err error) {
	if err == nil {
		return
	}
	errType := fmt.Sprintf("%T", err)
	if err, ok := err.(*rados.RadosError); ok {
		errType = errType + fmt.Sprintf(" %d %s", err.StatusCode, err.Code)
	}
	s.statsTicker.TickErr(err, errType)
}

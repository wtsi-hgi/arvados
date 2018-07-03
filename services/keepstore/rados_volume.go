// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0
//
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
	"github.com/satori/go.uuid"
)

var (
	radosPool        string
	radosKeyRingFile string
	radosMonHost     string
	radosCluster     string
	radosUser        string
	radosReplication int
	radosIndexWorkers int
)

var (
	zeroTime time.Time
)

const (
	RFC3339NanoMaxLen = 36
	RadosLockBusy = -16
	RadosLockExist = -17
	RadosLockLocked = 0
	RadosLockUnlocked = 0
	RadosLockData = "keep_lock_data"
	RadosLockMtime = "keep_lock_mtime"
	RadosLockTrash = "keep_lock_trash"
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
		return fmt.Errorf("rados: no pool name given")
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
	flag.IntVar(
		&radosIndexWorkers,
		"rados-index-workers",
		64,
		"Number of worker goroutines to use for gathering object size/mtime for index")

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

	ReadTimeout        arvados.Duration
	WriteTimeout       arvados.Duration
	MetadataTimeout       arvados.Duration
	ReadOnly       bool
	StorageClasses []string

	ioctx *rados.IOContext
	stats radospoolStats

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

	v.ioctx = ioctx

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
func (v *RadosVolume) Get(ctx context.Context, loc string, buf []byte) (n int, err error) {
	if isEmptyBlock(loc) {
		buf = buf[:0]
		return 0, nil
	}

	// Obtain a shared read lock so that Get does not happen at the same time as Put
	cookie, err = v.LockShared(ctx, loc, RadosLockData, "Get", v.ReadTimeout)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.Unlock(loc, RadosLockData, cookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()

	n, err = v.ioctx.Read(loc, buf, 0)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.GetOps)
	v.stats.TickErr(err)
	if err != nil {
		return
	}
	v.stats.TickInBytes(n)
	return
}

// Compare the given data with the stored data (i.e., what Get
// would return). If equal, return nil. If not, return
// CollisionError or DiskHashError (depending on whether the
// data on disk matches the expected hash), or whatever error
// was encountered opening/reading the stored data.
func (v *RadosVolume) Compare(ctx context.Context, loc string, expect []byte) (err error) {
	buf := make([]byte, len(expect))
	n, err := v.Get(ctx, loc, buf)
	if err != nil {
		return
	}
	if n != len(expect) {
		err = fmt.Errorf("rados: Get returned %d bytes but we were expecting %d", n, len(expect))
		return
	}
	expectHash = loc[:32]
	hash := md5.Sum(buf)
	if hash != expectHash {
		err = fmt.Errorf("rados: Get returned object with md5 hash '%s' but we were expecting '%s'", hash, expectHash)
		return
	}
	return
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
func (v *RadosVolume) Put(ctx context.Context, loc string, block []byte) (err error) {
	if v.ReadOnly {
		return MethodDisabledError
	}	

	cookie, err = v.LockExclusive(ctx, loc, RadosLockData, "Put", v.ReadTimeout)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.Unlock(loc, RadosLockData, cookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()

	// only store non-empty blocks
	if !isEmptyBlock(loc) {
		err = v.ioctx.WriteFull(loc, block)
		err = v.translateError(err)
		v.stats.Tick(&v.stats.Ops, &v.stats.PutOps)
		v.stats.TickErr(err)
		if err != nil {
			return
		}
		v.stats.TickOutBytes(len(block))
		if lockErr != nil {
			err = lockErr
			return
		}
	}

	// Touch object to set Mtime on the object to the current time
	// note that this will create an empty object as a side effect
	// if it does not already exist.
	err = v.Touch(loc)

	return
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
func (v *RadosVolume) Touch(loc string) (err error) {
	if v.ReadOnly {
		return MethodDisabledError
	}
	cookie, err = v.LockExclusive(ctx, loc, RadosLockMtime, "Touch", v.ReadTimeout)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.Unlock(loc, RadosLockMtime, cookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()

	err = v.TouchNoLock(loc)
	return
}

// TouchNoLock performs the functions of Touch without obtaining
// a lock on mtime, so that it can be called by methods that
// already hold such a lock.
//
// Not part of the *Volume interface
func (v *RadosVolume) ToucnNoLock(loc string) (err error) {
	mtime := time.Now()
	err = v.SetMtime(mtime)
	return
}

// Mtime returns the stored timestamp for the given locator.
//
// loc is as described in Get.
//
// Mtime must return a non-nil error if the given block is not
// found or the timestamp could not be retrieved.
func (v *RadosVolume) Mtime(loc string) (mtime time.Time, err error) {
	cookie, err = v.LockShared(ctx, loc, RadosLockMtime, "Mtime", v.ReadTimeout)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.Unlock(loc, RadosLockMtime, cookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()

	mtime, err = v.MtimeNoLock(loc)
	return
}

// MtimeNoLock performs the functions of Mtime without obtaining
// a lock on mtime, so that it can be called by methods that
// already hold such a lock.
//
// Not part of the *Volume interface
func (v *RadosVolume) MtimeNoLock(loc string) (mtime time.Time, err error) {
	mtime_bytes := make([]byte, RFC3339NanoMaxLen)
	n, err := ioctx.GetXattr(loc, "keep_mtime", mtime_bytes)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.GetXattrOps)
	if err != nil {
                err = fmt.Errorf("rados: failed to get keep_mtime xattr object %v: %v", loc, err)
		v.stats.TickErr(err)
		return
	}
	if n != RFC3339NanoMaxLen {
                err = fmt.Errorf("rados: GetXattr read %d bytes for keep_mtime xattr but we were expecting %d", n, RFC3339NanoMaxLen)
		v.stats.TickErr(err)
		return
	}
	mtime, err = time.Parse(time.RFC3339Nano, string(mtime_bytes[:RFC3339NanoMaxLen]))
	if err != nil {
		mtime = zeroTime
		v.stats.TickErr(err)
		return
	}
	v.stats.TickErr(err)
	return
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
func (v *RadosVolume) IndexTo(prefix string, writer io.Writer) (err error) {
	err = v.listObjects(prefix, writer)
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
func (v *RadosVolume) Trash(loc string) (err error) {
	if v.ReadOnly {
		return MethodDisabledError
	}

	// Must take the data lock before the mtime lock to ensure that
	// we cannot get into a deadlock with Put, which takes the two
	// locks in the same order. We obtain a trash lock as well to
	// prevent races between multiple Trash invocations or between
	// Trash and Untrash
	dataCookie, err = v.LockExclusive(ctx, loc, RadosLockData, "Trash", v.ReadTimeout)
	if err != nil {
		return
	}
	mtimeCookie, err = v.LockExclusive(ctx, loc, RadosLockMtime, "Trash", v.ReadTimeout)
	if err != nil {
		return
	}
	trashCookie, err = v.LockExclusive(ctx, loc, RadosLockTrash, "Trash", v.ReadTimeout)
	if err != nil {
		return
	}
	defer func() {
		dataLockErr := v.Unlock(loc, RadosLockData, dataCookie)
		if err == nil && dataLockErr != nil {
			err = dataLockErr
		}
		mtimeLockErr := v.Unlock(loc, RadosLockMtime, mtimeCookie)
		if err == nil && mtimeLockErr != nil {
			err = mtimeLockErr
		}
		trashLockErr := v.Unlock(loc, RadosLockTrash, trashCookie)
		if err == nil && trashLockErr != nil {
			err = trashLockErr
		}
		return
	}()

	// check if this object is already trash
	isTrash, err := v.IsTrash(loc)
	// foo

	// check mtime and mark as trash if needed, all while holding data and mtime locks
	// call v.MtimeNoLock instead of v.Mtime as we already have an exclusive lock on mtime
	// which would conflict with the shared lock that v.Mtime obtains
	if t, err := v.MtimeNoLock(loc); err != nil {
		return
	} else if time.Since(t) < theConfig.BlobSignatureTTL.Duration() {
		err = nil
		return
	}
	if theConfig.TrashLifetime == 0 {
		err = v.DeleteNoLock(loc)
		return
	}
	
	// mark as trash using MarkTrashNoLock as we already have obtained the trash lock
	err = v.MarkTrashNoLock(loc)
	if err != nil {
		return
	}

	// update mtime so that the time it was trashed is recorded
	// call v.TouchNoLock instead of v.Touch as we already hold an
	// exclusive lock on mtime
	err = v.TouchNoLock(loc)

	return
}

// Untrash moves block from trash back into store
func (v *RadosVolume) Untrash(loc string) error {
	// mark as not trash (MarkNotTrash handles locking itself)
	err = v.MarkNotTrash(loc)
	return
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
			ReadTimeout:    arvados.Duration(5 * time.Minute),
			WriteTimeout:    arvados.Duration(5 * time.Minute),
			MetadataTimeout:    arvados.Duration(10 * time.Second),
		},
		&RadosVolume{
			Pool:             "keep02",
			KeyringFile:      "/etc/ceph/client.keep02.keyring",
			MonHost:          "1.2.3.4,5.6.7.8,9.10.11.12",
			Cluster:          "ceph",
			User:             "client.keep02",
			RadosReplication: 3,
			ReadTimeout:    arvados.Duration(5 * time.Minute),
			WriteTimeout:    arvados.Duration(5 * time.Minute),
			MetadataTimeout:    arvados.Duration(10 * time.Second),
		},
	}
}


// Size returns the size in bytes for the given locator.
//
// loc is as described in Get.
//
// Size must return a non-nil error if the given block is not
// found or the size could not be retrieved.
//
// Not part of the *Volume interface
func (v *RadosVolume) Size(loc string) (size int64, err error) {
	stat, err := v.ioctx.Stat(loc)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.StatOps)
	v.stats.TickErr(err)
	if err != nil {
		return
	}
	size = stat.Size
	return
}

// Obtains an exclusive lock on the object for writing, waiting
// if necessary. 
//
// Returns the lock cookie which must be passed to Unlock()
// to release the lock.
//
// Not part of the *Volume interface
func (v *RadosVolume) LockExclusive(ctx context.Context, loc string, name string, desc string, timeout time.Duration) (cookie string, err error) {
	lock_acquired := make(chan bool)
	cookie, err = uuid.NewV4()
	if err != nil {
		return
	}
	// attempt to obtain lock repeatedly, unless ctx.Done() occurs, in which case abandon attempt to lock
	go func() {
		locked := false
		for !locked {
			select {
			case <-ctx.Done():
				close(lock_acquired)
				return
			default:
				res, err = v.ioctx.LockExclusive(loc, name, cookie, desc, timeout, nil)
				if err != nil {
					return
				}
				switch res {
				case RadosLockBusy:
					theConfig.debugLogf("rados: attempting to get exclusive %s lock for %s on object %s: lock busy", name, desc, oid)
					time.Sleep(100 * time.Millisecond)
				case RadosLockExist:
					theConfig.debugLogf("rados: attempting to get exclusive %s lock for %s on object %s: lock exists", name, desc, oid)
					time.Sleep(100 * time.Millisecond)
				case RadosLockLocked:
					locked = true
				default:
					err = fmt.Errorf("rados: attempting to get exclusive %s lock for %s on object %s: unexpected non-error return value %d from LockExclusive", name, desc, oid, res)
					return
				}
			}
		}
		close(lock_acquired)
	}()
	select {
	case <-lock_acquired:
		return
	case <-ctx.Done():
		theConfig.debugLogf("rados: abandoning attempt to obtain exclusive %s lock for %s on object %s: %s", name, desc, oid, ctx.Err())
		go func() {
			<-ready
			if err == nil {
				rdr.Close()
			}
		}()
		return nil, ctx.Err()
	}
	
	return
}

// Obtains a shared lock on the object, waiting if necessary.
//
// Used to prevent read operations such as Get and Compare from
// occurring during a Put.
//
// Returns the lock cookie which must be passed to Unlock()
// to release the lock.
//
// Not part of the *Volume interface
func (v *RadosVolume) LockShared(ctx context.Context, loc string, name string, desc string, timeout time.Duration) (cookie string, err error) {
	lock_acquired := make(chan bool)
	cookie, err = uuid.NewV4()
	if err != nil {
		return
	}
	// attempt to obtain lock repeatedly, unless ctx.Done() occurs, in which case abandon attempt to lock
	go func() {
		locked := false
		for !locked {
			select {
			case <-ctx.Done():
				close(lock_acquired)
				return
			default:
				res, err = v.ioctx.LockShared(loc, name, cookie, "", desc, timeout, nil)
				if err != nil {
					return
				}
				switch res {
				case RadosLockBusy:
					theConfig.debugLogf("rados: attempting to get shared %s lock for %s on object %s: lock busy", name, desc, oid)
					time.Sleep(100 * time.Millisecond)
				case RadosLockExist:
					theConfig.debugLogf("rados: attempting to get shared %s lock for %s on object %s: lock exists", name, desc, oid)
					time.Sleep(100 * time.Millisecond)
				case RadosLockLocked:
					locked = true
				default:
					err = fmt.Errorf("rados: attempting to get shared %s lock for %s on object %s: unexpected non-error return value %d from LockShared", name, desc, oid, res)
					return
				}
			}
		}
		close(lock_acquired)
	}()
	select {
	case <-lock_acquired:
		return
	case <-ctx.Done():
		theConfig.debugLogf("rados: abandoning attempt to obtain shared %s lock for %s on object %s: %s", name, desc, oid, ctx.Err())
		go func() {
			<-ready
			if err == nil {
				rdr.Close()
			}
		}()
		return nil, ctx.Err()
	}
	
	return
}

// Unlock previously obtained data lock
//
// Not part of the *Volume interface
func (v *RadosVolume) Unlock(loc string, name string, cookie string) (err error) {
	res, err = v.ioctx.Unlock(loc, name, cookie)
	if err != nil {
		return
	}
	if res == rados.RadosErrorNotFound {
		err = fmt.Errorf("rados: attempting to unlock %s lock for %s on object %s, lock was not held for cookie '%s'", name, desc, loc, cookie)
	}
	return
}

// DeleteNoLock deletes the block now, regardless of trash state
//
// Not part of the *Volume interface
func (v *RadosVolume) DeleteNoLock(loc string) (err error) {
	err = v.ioctx.Delete(loc)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.DelOps)
	v.stats.TickErr(err)
	return
}

// IsTrash returns true is the object is marked as trash
// otherwise it returns false
//
// Not part of the *Volume interface
func (v *RadosVolume) IsTrash(oid string) (trash bool, err error) {
	cookie, err = v.LockShared(ctx, loc, RadosLockTrash, "IsTrash", v.ReadTimeout)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.Unlock(loc, RadosLockTrash, cookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()
	trash_bytes := make([]byte, 1)
	n, err := ioctx.GetXattr(oid, "keep_trash", trash_bytes)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.GetXattrOps)
	v.stats.TickErr(err)
	if err != nil {
                err = fmt.Errorf("rados: failed to get keep_trash xattr object %v: %v", loc, err)
		return
	}
	if n != 1 {
                err = fmt.Errorf("rados: GetXattr read %d bytes for keep_trash xattr but we were expecting %d", n, RFC3339NanoMaxLen)
		return
	}
	switch trash_bytes[0] {
	case 0:
		trash = false
	case 1:
		trash = true
	default:
		err = fmt.Errorf("rados: got unexpected keep_trash xattr value %d when we were expecting either 0 or 1", trash_bytes[0])
	}
	return
}

// MarkTrash marks an object as trash.
//
// Not part of the *Volume interface
func (v *RadosVolume) MarkTrash(oid string) (err error) {
	cookie, err = v.LockExclusive(ctx, loc, RadosLockTrash, "MarkTrash", v.ReadTimeout)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.Unlock(loc, RadosLockTrash, cookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()
	err = v.MarkTrashNoLock(oid)
	return
}

// MarkTrashNoLock does the same as MarkTrash but without
// first obtaining a lock on trash, as the caller is expected
// to already hold one.
//
// Not part of the *Volume interface
func (v *RadosVolume) MarkTrashNoLock(oid string) (err error) {
	err = v.ioctx.SetXattr(oid, "keep_trash", []byte(1))
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.SetXattrOps)
	v.stats.TickErr(err)
	if err != nil {
		err = fmt.Errorf("rados: MarkTrashNoLock failed to update keep_trash xattr for oid '%s': %v", oid, err)
	}
	return
}

// MarkNotTrash ensures that the object is not marked as trash
//
// Not part of the *Volume interface
func (v *RadosVolume) MarkNotTrash(oid string) (err error) {
	cookie, err = v.LockExclusive(ctx, loc, RadosLockTrash, "MarkNotTrash", v.ReadTimeout)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.Unlock(loc, RadosLockTrash, cookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()
	err = v.MarkNotTrashNoLock(oid)
	return
}

// MarkNotTrashNoLock does the same thing as MarkNotTrash, but
// without obtaining a lock on trash, as the caller is expected
// to already hold one. 
//
// Not part of the *Volume interface
func (v *RadosVolume) MarkNotTrashNoLock(oid string) (err error) {
	err = v.ioctx.SetXattr(oid, "keep_trash", []byte(0))
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.SetXattrOps)
	v.stats.TickErr(err)
	if err != nil {
		err = fmt.Errorf("rados: MarkNotTrashNoLock failed to update keep_trash xattr for oid '%s': %v", oid, err)
	}
	return
}

// SetMtime sets the keep_mtime xattr to the given time
//
// Not part of the *Volume interface
func (v  *RadosVolume) SetMtime(mtime time.Time) (err error) {
	mtime_string := mtime.Format(time.RFC3339Nano)
	mtime_bytes := 	[]byte(fmt.Sprintf("%[1]*[2]s", RFC3339NanoMaxLen, mtime_string))
	err = v.ioctx.SetXattr(oid, "keep_mtime", mtime_bytes)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.SetXattrOps)
	if err != nil {
		err = fmt.Errorf("rados: failed to update keep_mtime xattr for oid '%s': %v", oid, err)
	}
	v.stats.TickErr(err)
	return
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

// InternalStats returns pool I/O and API call counters.
func (v *RadosVolume) InternalStats() interface{} {
	return &v.stats
}

var radosKeepBlockRegexp = regexp.MustCompile(`^[0-9a-f]{32}$`)

func (v *RadosVolume) isEmptyBlock(loc string) bool {
	return loc[:32] == EmptyHash
}

func (v *RadosVolume) isKeepBlock(s string) bool {
	return radosKeepBlockRegexp.MatchString(s)
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

type listEntry struct {
	oid string
	size int64
	mtime time.Time
	err error
}

func (v *RadosVolume) listObjects(prefix string, writer io.Writer) (err error) {
	// open object list iterator
	iter, err := v.ioctx.Iter()
	v.stats.Tick(&v.stats.Ops, &v.stats.ListOps)
	v.stats.TickErr(err)
	if err != nil {
		return
	}
	defer iter.Close()

	// channels to enqueue oids and receive list entries (or errors) from concurrent workers
	listOidChan := make(chan[string], radosIndexWorkers)
	listEntryChan := make(chan[listEntry], radosIndexWorkers)

	// asynchronously put objects to list on listOidChan
	go func(){ 
		for iter.Next() {
			v.stats.Tick(&v.stats.Ops, &v.stats.ListOps)
			oid := iter.Value()
			if !v.isKeepBlock(oid) || !strings.HasPrefix(oid, prefix) {
				continue
			}
			listOidChan <- oid
		}
		close(listOidChan)
	}()

	// start radosIndexWorkers to process listOids and output to listEntryChan
	for i := 0; i < radosIndexWorkers; i++ {
		go func() {
			for oid := range listOidChan {
				le := listEntry{
					oid: oid
				}
				le.size, err := v.Size(oid)
				if err != nil {
					le.err = err
					listEntryChan <- le
					return
				}
				le.mtime, err := v.Mtime(oid)
				if err != nil {
					le.err = err
					listEntryChan <- le
					return
				}
				listEntryChan <- le
			}
		}
	}

	// process listEntry entries from listEntryChan and output them
	for le := range listEntryChan {
		if le.err != nil {
			// TODO could signal async work to stop here as we don't need the results anymore
			err = le.err
			continue
		}
		fmt.Fprintf(writer, "%s+%d %d\n", le.oid, le.size, le.mtime.UnixNano())
	}
	return
}

type radospoolStats struct {
	statsTicker
	Ops      uint64
	GetOps   uint64
	PutOps   uint64
	StatOps  uint64
	GetXattrOps uint64
	SetXattrOps uint64
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


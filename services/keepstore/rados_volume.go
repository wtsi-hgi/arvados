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
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"math"
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
	radosPool         string
	radosKeyringFile  string
	radosMonHost      string
	radosCluster      string
	radosUser         string
	radosReplication  int
	radosIndexWorkers int
)

var (
	RadosZeroTime time.Time
)

const (
	RFC3339NanoMaxLen  = 36
	RadosLockNotFound  = -2
	RadosLockBusy      = -16
	RadosLockExist     = -17
	RadosLockLocked    = 0
	RadosLockUnlocked  = 0
	RadosLockData      = "keep_lock_data"
	RadosLockTouch     = "keep_lock_touch"
	RadosXattrMtime    = "keep_mtime"
	RadosXattrTrash    = "keep_trash"
	RadosKeepNamespace = "keep"
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
	if radosKeyringFile == "" {
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

// Define our own minimal interface types for rados so we can mock them
type radosImplementation interface {
	Version() (int, int, int)
	NewConnWithClusterAndUser(clusterName string, userName string) (radosConn, error)
}

type radosConn interface {
	SetConfigOption(option, value string) error
	Connect() error
	GetFSID() (fsid string, err error)
	GetClusterStats() (stat rados.ClusterStat, err error)
	ListPools() (names []string, err error)
	OpenIOContext(pool string) (radosIOContext, error)
}

type radosIOContext interface {
	Delete(oid string) error
	GetPoolStats() (stat rados.PoolStat, err error)
	GetXattr(object string, name string, data []byte) (int, error)
	Iter() (radosIter, error)
	LockExclusive(oid, name, cookie, desc string, duration time.Duration, flags *byte) (int, error)
	LockShared(oid, name, cookie, tag, desc string, duration time.Duration, flags *byte) (int, error)
	Read(oid string, data []byte, offset uint64) (int, error)
	SetNamespace(namespace string)
	SetXattr(object string, name string, data []byte) error
	Stat(object string) (stat rados.ObjectStat, err error)
	Unlock(oid, name, cookie string) (int, error)
	WriteFull(oid string, data []byte) error
}

type radosIter interface {
	Next() bool
	Value() string
	Close()
}

// RadosVolume implements Volume using an Rados pool.
type RadosVolume struct {
	Pool             string
	KeyringFile      string
	MonHost          string
	Cluster          string
	User             string
	FSID             string
	Debug            bool
	RadosReplication int

	ReadTimeout     arvados.Duration
	WriteTimeout    arvados.Duration
	MetadataTimeout arvados.Duration
	ReadOnly        bool
	StorageClasses  []string

	rados radosImplementation
	conn  radosConn
	ioctx radosIOContext
	stats radospoolStats

	startOnce sync.Once
}

// Volume type as specified in config file.
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
		v.User = "client.admin"
	}

	if v.rados == nil {
		v.rados = &radosRealImpl{}
	}

	rv_major, rv_minor, rv_patch := v.rados.Version()
	theConfig.debugLogf("rados: using librados version %d.%d.%d", rv_major, rv_minor, rv_patch)

	conn, err := v.rados.NewConnWithClusterAndUser(v.Cluster, v.User)
	if err != nil {
		return fmt.Errorf("rados: error creating rados connection to ceph cluster '%s' for user '%s': %v", v.Cluster, v.User, err)
	}
	v.conn = conn

	err = v.conn.SetConfigOption("mon_host", v.MonHost)
	if err != nil {
		return fmt.Errorf("rados: error setting mon_host for rados to '%s': %v", v.MonHost, err)
	}

	if v.Debug {
		err = v.conn.SetConfigOption("log_to_stderr", "1")
		if err != nil {
			return fmt.Errorf("rados: error setting log_to_stderr rados config for debugging: %v", err)
		}
		err = v.conn.SetConfigOption("err_to_stderr", "1")
		if err != nil {
			return fmt.Errorf("rados: error setting err_to_stderr rados config for debugging: %v", err)
		}
	}

	if v.KeyringFile != "" {
		err = v.conn.SetConfigOption("keyring", v.KeyringFile)
		if err != nil {
			return fmt.Errorf("rados: error setting keyring for rados to '%s': %v", v.KeyringFile, err)
		}
	}

	err = v.conn.Connect()
	if err != nil {
		return fmt.Errorf("rados: error connecting to rados cluster: %v", err)
	}
	theConfig.debugLogf("rados: connected to cluster '%s' as user '%s'", v.Cluster, v.User)

	fsid, err := v.conn.GetFSID()
	if err != nil {
		return fmt.Errorf("rados: error getting rados cluster FSID: %v", err)
	}
	v.FSID = fsid
	theConfig.debugLogf("rados: cluster FSID is '%s'", v.FSID)

	cs, err := v.conn.GetClusterStats()
	if err != nil {
		return fmt.Errorf("rados: error getting rados cluster stats: %v", err)
	}
	theConfig.debugLogf("rados: cluster %s has %.1f GiB with %.1f GiB used in %d objects and %.1f GiB available", v.Cluster, float64(cs.Kb)/1024/1024, float64(cs.Kb_used)/1024/1024, cs.Num_objects, float64(cs.Kb_avail)/1024/1024)

	pools, err := v.conn.ListPools()
	if err != nil {
		return fmt.Errorf("rados: error listing pools: %v", err)
	}
	if !stringInSlice(v.Pool, pools) {
		return fmt.Errorf("rados: pool '%s' not present in cluster %s. available pools in this cluster are: %v", v.Pool, v.Cluster, pools)
	}
	theConfig.debugLogf("rados: pool '%s' was found", v.Pool)

	ioctx, err := v.conn.OpenIOContext(v.Pool)
	if err != nil {
		return fmt.Errorf("rados: error opening IO context for pool '%s': %v", v.Pool, err)
	}
	theConfig.debugLogf("rados: pool '%s' was opened", v.Pool)

	ioctx.SetNamespace(RadosKeepNamespace)

	v.ioctx = ioctx
	if v.RadosReplication == 0 {
		// RadosReplication was not set or was explicitly set to 0 - determine it from the PoolStats if we can
		v.RadosReplication = 1
		ps, err := v.ioctx.GetPoolStats()
		if err != nil {
			theConfig.debugLogf("rados: failed to get pool stats, set RadosReplication to 1: %v", err)
		} else {
			if ps.Num_objects > 0 {
				actualReplication := float64(ps.Num_object_clones) / float64(ps.Num_objects)
				v.RadosReplication = int(math.Ceil(actualReplication))
				theConfig.debugLogf("rados: pool has %d objects and %d object clones for an actual replication of %.2f, set RadosReplication to %d", ps.Num_objects, ps.Num_object_clones, actualReplication, v.RadosReplication)
			}
		}
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
func (v *RadosVolume) Get(ctx context.Context, loc string, buf []byte) (n int, err error) {
	if v.isEmptyBlock(loc) {
		buf = buf[:0]
		return 0, nil
	}

	size, err := v.size(loc)
	if err != nil {
		return
	}
	if size > uint64(len(buf)) {
		err = fmt.Errorf("rados: Get has %d bytes for '%s' but supplied buffer was only %d bytes", size, loc, len(buf))
		return
	}

	// Obtain a shared read lock to prevent a Get from occuring while a Put is
	// still in progress
	lockCookie, err := v.lockShared(ctx, loc, RadosLockData, "Get", v.ReadTimeout, false)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.unlock(loc, RadosLockData, lockCookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()

	// It is not entirely clear whether a call to ioctx.Read might
	// result in a short read or if it always reads the full object,
	// so wrap it in a loop that can handle short reads and keep reading
	// until we get size bytes, just in case.
	n = 0
	off := uint64(0)
	for {
		read_bytes := 0
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
			read_bytes, err = v.ioctx.Read(loc, buf[n:], off)
			err = v.translateError(err)
			v.stats.Tick(&v.stats.Ops, &v.stats.GetOps)
			v.stats.TickErr(err)
			if err != nil {
				return
			}
			v.stats.TickInBytes(uint64(read_bytes))
			n += read_bytes
			off += uint64(read_bytes)
			if uint64(n) >= size {
				break
			}
			if read_bytes == 0 {
				log.Printf("rados warning: %s: Get read 0 bytes from %s at offset %d after reading %d bytes out of %d", v, loc, off, n, size)
				time.Sleep(1 * time.Second)
			}
		}
	}
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
	expectHash := loc[:32]
	hash := fmt.Sprintf("%x", md5.Sum(buf))
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

	// get a lock with create = true so that we get the lock even if the
	// object does not yet exist (N.B. in this case an empty object will be created
	// to facilitate the lock, but that is ok as we are about to write to it)
	lockCookie, err := v.lockExclusive(ctx, loc, RadosLockData, "Put", v.WriteTimeout, true)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.unlock(loc, RadosLockData, lockCookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()

	// only store non-empty blocks
	if !v.isEmptyBlock(loc) {
		err = v.ioctx.WriteFull(loc, block)
		err = v.translateError(err)
		v.stats.Tick(&v.stats.Ops, &v.stats.PutOps)
		v.stats.TickErr(err)
		if err != nil {
			return
		}
		v.stats.TickOutBytes(uint64(len(block)))
	}

	// Since we are about to put this object, it is no longer trash.
	// Do this before Touch so that v.exists() will be true when
	// Touch is invoked.
	err = v.markNotTrash(loc)
	if err != nil {
		return
	}

	// Touch object to set Mtime on the object to the current time.
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

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(v.MetadataTimeout))
	lockCookie, err := v.lockExclusive(ctx, loc, RadosLockTouch, "Touch", v.MetadataTimeout, false)
	if err != nil {
		return
	}
	defer func() {
		lockErr := v.unlock(loc, RadosLockTouch, lockCookie)
		if err == nil && lockErr != nil {
			err = lockErr
		}
		return
	}()

	isTrash, err := v.isTrash(loc)
	if err != nil {
		return
	}
	if isTrash {
		err = os.ErrNotExist
		return
	}

	mtime := time.Now()
	err = v.setMtime(loc, mtime)
	return
}

// exists returns true if the keep object exists (even if it is trash)
// otherwise returns false.
//
// exists returns a non-nil error only if the existence of the object
// could not be determined
func (v *RadosVolume) exists(loc string) (exists bool, err error) {
	exists = false
	_, err = v.isTrash(loc)
	if os.IsNotExist(err) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	exists = true
	return
}

// Mtime returns the stored timestamp for the given locator.
//
// loc is as described in Get.
//
// Mtime must return a non-nil error if the given block is not
// found or the timestamp could not be retrieved.
func (v *RadosVolume) Mtime(loc string) (mtime time.Time, err error) {
	isTrash, err := v.isTrash(loc)
	if err != nil {
		return
	}
	if isTrash {
		err = os.ErrNotExist
		return
	}

	mtime_bytes := make([]byte, RFC3339NanoMaxLen)
	n, err := v.ioctx.GetXattr(loc, RadosXattrMtime, mtime_bytes)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.GetXattrOps)
	if err != nil {
		err = fmt.Errorf("rados: failed to get %s xattr object %v: %v", RadosXattrMtime, loc, err)
		v.stats.TickErr(err)
		return
	}
	if n != RFC3339NanoMaxLen {
		err = fmt.Errorf("rados: Mtime read %d bytes for %s xattr but we were expecting %d", n, RadosXattrMtime, RFC3339NanoMaxLen)
		v.stats.TickErr(err)
		return
	}
	mtime, err = time.Parse(time.RFC3339Nano, string(mtime_bytes[:RFC3339NanoMaxLen]))
	if err != nil {
		mtime = RadosZeroTime
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

	// filter to include only non-trash objects within the given prefix
	filterFunc := func(loc string) (bool, error) {
		if !strings.HasPrefix(loc, prefix) {
			return false, nil
		}
		isTrash, err := v.isTrash(loc)
		if err != nil {
			return false, err
		}
		if isTrash {
			return false, nil
		}
		return true, nil
	}

	mapFunc := func(loc string) listEntry {
		return v.newIndexListEntry(loc)
	}

	reduceFunc := func(le listEntry) {
		fmt.Fprintf(writer, "%s\n", le)
	}
	err = v.listObjects(filterFunc, mapFunc, reduceFunc, radosIndexWorkers)
	return
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

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(v.MetadataTimeout))

	// Must take the data lock before the touch lock to ensure that
	// we cannot get into a deadlock with Put, which takes the two
	// locks in the same order.
	dataLockCookie, err := v.lockExclusive(ctx, loc, RadosLockData, "Trash", v.MetadataTimeout, false)
	if err != nil {
		return
	}
	mtimeLockCookie, err := v.lockExclusive(ctx, loc, RadosLockTouch, "Trash", v.MetadataTimeout, false)
	if err != nil {
		return
	}
	defer func() {
		dataLockErr := v.unlock(loc, RadosLockData, dataLockCookie)
		if err == nil && dataLockErr != nil {
			err = dataLockErr
		}
		mtimeLockErr := v.unlock(loc, RadosLockTouch, mtimeLockCookie)
		if err == nil && mtimeLockErr != nil {
			err = mtimeLockErr
		}
		return
	}()

	// check if this object is already trash
	isTrash, err := v.isTrash(loc)
	if err != nil {
		return
	}
	if isTrash {
		err = fmt.Errorf("rados: attempt to Trash object '%s' that is already trash", loc)
		return
	}

	// check mtime and mark as trash if needed
	t, err := v.Mtime(loc)
	if err != nil {
		return
	}
	if time.Since(t) < theConfig.BlobSignatureTTL.Duration() {
		err = nil
		return
	}
	if theConfig.TrashLifetime == 0 {
		// trash lifetime is zero, just immediately delete the block
		err = v.delete(loc)
		return
	}

	// mark as trash
	err = v.markTrash(loc)
	if err != nil {
		return
	}

	// update mtime so that the time it was trashed is recorded
	// N.B. don't call Touch because Touch doesn't touch trash
	ttime := time.Now()
	err = v.setMtime(loc, ttime)

	return
}

// Untrash moves block from trash back into store
func (v *RadosVolume) Untrash(loc string) (err error) {
	// check if this object is, in fact, trash
	isTrash, err := v.isTrash(loc)
	if err != nil {
		return
	}
	if !isTrash {
		err = fmt.Errorf("rados: attempt to Untrash object '%s' that is not trash", loc)
		return
	}

	// mark as not trash
	err = v.markNotTrash(loc)
	return
}

// Status returns a *VolumeStatus representing the current
// in-use and available storage capacity and an
// implementation-specific volume identifier (e.g., "mount
// point" for a UnixVolume).
func (v *RadosVolume) Status() (vs *VolumeStatus) {
	vs = &VolumeStatus{
		MountPoint: fmt.Sprintf("%s", v),
		DeviceNum:  1,
		BytesFree:  BlockSize * 1000,
		BytesUsed:  1,
	}
	cs, err := v.conn.GetClusterStats()
	if err != nil {
		log.Printf("rados: %s: failed to get cluster stats, Status will not report BytesFree correctly: %v", v, err)
	} else {
		vs.BytesFree = cs.Kb_avail * 1024
	}
	ps, err := v.ioctx.GetPoolStats()
	if err != nil {
		log.Printf("rados: %s: failed to get pool stats, Status will not report BytesUsed correctly: %v", v, err)
	} else {
		vs.BytesUsed = ps.Num_bytes
	}
	return
}

// String returns an identifying label for this volume,
// suitable for including in log messages. It should contain
// enough information to uniquely identify the underlying
// storage device, but should not contain any credentials or
// secrets.
func (v *RadosVolume) String() string {
	return fmt.Sprintf("rados://%s/%s/%s", v.MonHost, v.Cluster, v.Pool)
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

	emptyOneLoc := func(loc string) {
		if !v.isKeepBlock(loc) {
			return
		}
		size, err := v.size(loc)
		if err != nil {
			log.Printf("rados warning: %s: EmptyTrash failed to get size for %s: %v", v, loc, err)
			return
		}
		atomic.AddInt64(&blocksInTrash, 1)
		atomic.AddInt64(&bytesInTrash, int64(size))

		trashT, err := v.Mtime(loc)
		if err != nil {
			log.Printf("rados warning: %s: EmptyTrash failed to get mtime for %s: %v", v, loc, err)
			return
		}

		// bail out if the trash time is within the trash lifetime
		if startT.Sub(trashT) < theConfig.TrashLifetime.Duration() {
			return
		}

		// one final paranoid check to make sure this is actually trash we are deleting
		isTrash, err := v.isTrash(loc)
		if err != nil {
			log.Printf("rados warning: %s: EmptyTrash failed to verify trash status for %s: %v", v, loc, err)
			return
		}
		if !isTrash {
			log.Printf("rados warning: %s: EmptyTrash was about to delete an object '%s' that is no longer trash: %v", v, loc, err)
			return
		}

		// actually delete the object
		err = v.delete(loc)
		if err != nil {
			log.Printf("rados warning: %s: EmptyTrash failed to delete %s: %v", v, loc, err)
			return
		}
		atomic.AddInt64(&bytesDeleted, int64(size))
		atomic.AddInt64(&blocksDeleted, 1)
	}

	// filter to include only trash objects
	filterFunc := func(loc string) (isTrash bool, err error) {
		isTrash, err = v.isTrash(loc)
		return
	}

	// map to just call emptyOneKey for each loc and return empty listEntry
	mapFunc := func(loc string) listEntry {
		emptyOneLoc(loc)
		return &trashListEntry{}
	}

	// empty reduce function
	reduceFunc := func(le listEntry) {}

	err := v.listObjects(filterFunc, mapFunc, reduceFunc, theConfig.EmptyTrashWorkers)
	if err != nil {
		log.Printf("rados error: %s: EmptyTrash: listObjects failed: %s", v, err)
	}

	log.Printf("EmptyTrash stats for %v: Deleted %v bytes in %v blocks. Remaining in trash: %v bytes in %v blocks.", v.String(), bytesDeleted, blocksDeleted, bytesInTrash-bytesDeleted, blocksInTrash-blocksDeleted)
}

// Return a globally unique ID of the underlying storage
// device if possible, otherwise "".
func (v *RadosVolume) DeviceID() string {
	return "rados:" + v.FSID + "/" + v.Pool
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
			ReadTimeout:      arvados.Duration(5 * time.Minute),
			WriteTimeout:     arvados.Duration(5 * time.Minute),
			MetadataTimeout:  arvados.Duration(10 * time.Second),
		},
		&RadosVolume{
			Pool:             "keep02",
			KeyringFile:      "/etc/ceph/client.keep02.keyring",
			MonHost:          "1.2.3.4,5.6.7.8,9.10.11.12",
			Cluster:          "ceph",
			User:             "client.keep02",
			RadosReplication: 3,
			ReadTimeout:      arvados.Duration(5 * time.Minute),
			WriteTimeout:     arvados.Duration(5 * time.Minute),
			MetadataTimeout:  arvados.Duration(10 * time.Second),
		},
	}
}

// size returns the size in bytes for the given locator.
//
// loc is as described in Get.
//
// size must return a non-nil error if the given block is not
// found or the size could not be retrieved.
func (v *RadosVolume) size(loc string) (size uint64, err error) {
	stat, err := v.ioctx.Stat(loc)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.StatOps)
	v.stats.TickErr(err)
	if err != nil {
		return
	}
	size = uint64(stat.Size)
	return
}

// lockExclusive obtains an exclusive lock on the object,
// waiting if necessary.
//
// Returns the lock cookie which must be passed to unlock()
// to release the lock.
func (v *RadosVolume) lockExclusive(ctx context.Context, loc string, name string, desc string, timeout arvados.Duration, create bool) (lockCookie string, err error) {
	lockCookie, err = v.lock(ctx, loc, name, desc, timeout, create, true)
	return
}

// lockShared obtains a shared lock on the object, waiting
// if necessary.
//
// Used to prevent read operations such as Get and Compare from
// occurring during a Put.
//
// Returns the lock cookie which must be passed to unlock()
// to release the lock.
func (v *RadosVolume) lockShared(ctx context.Context, loc string, name string, desc string, timeout arvados.Duration, create bool) (lockCookie string, err error) {
	lockCookie, err = v.lock(ctx, loc, name, desc, timeout, create, false)
	return
}

// lock obtains a lock (either shared or exclusive) on the
// object, waiting to obtain it if necessary until the ctx
// is Done.
//
// Returns the lock cookie which must be passed to unlock()
// to release the lock.
//
// If the object being locked does not exist, the Lock attempt
// will fail and return an error unless the create argument
// is set to true.
func (v *RadosVolume) lock(ctx context.Context, loc string, name string, desc string, timeout arvados.Duration, create bool, exclusive bool) (lockCookie string, err error) {
	locking_finished := make(chan bool)
	lockCookie = uuid.Must(uuid.NewV4()).String()

	// attempt to obtain lock repeatedly, unless ctx.Done() occurs, in which case abandon attempt to lock
	go func() {
		locked := false
	LoopUntilLocked:
		for !locked {
			select {
			case <-ctx.Done():
				close(locking_finished)
				return
			default:
				res := 0
				if exclusive {
					res, err = v.ioctx.LockExclusive(loc, name, lockCookie, desc, time.Duration(timeout), nil)
				} else {
					res, err = v.ioctx.LockShared(loc, name, lockCookie, "", desc, time.Duration(timeout), nil)
				}
				v.stats.Tick(&v.stats.Ops, &v.stats.LockOps)
				v.stats.TickErr(err)
				if err != nil {
					return
				}
				switch res {
				case RadosLockBusy:
					theConfig.debugLogf("rados: attempting to get %s lock for %s on object %s: lock busy", name, desc, loc)
					time.Sleep(100 * time.Millisecond)
				case RadosLockExist:
					theConfig.debugLogf("rados: attempting to get %s lock for %s on object %s: lock exists", name, desc, loc)
					time.Sleep(100 * time.Millisecond)
				case RadosLockLocked:
					if !create {
						// we got the lock, but it is possible that the object may have been created by our attempt
						// to lock it, and we were told not to create objects that don't exist.
						///
						// we use exists(loc) to check if the keep object is supposed to exist, based on the existence
						// of xattrs which would not exist if the object was just created by our attempt to lock it.
						exists, err := v.exists(loc)
						if err != nil {
							theConfig.debugLogf("rados: lock failed to determine if keep object '%s' exists: %v", loc, err)
							exists = false
						}
						if !exists {
							// simply delete the object we have apparently created - no need to unlock afterward
							// as the delete also deletes the lock
							err = v.delete(loc)
							if err != nil {
								theConfig.debugLogf("rados: failed to delete object '%s' created by attempt to lock: %v", loc, err)
								break LoopUntilLocked
							}
							err = os.ErrNotExist
							break LoopUntilLocked
						}
					}
					locked = true
				default:
					err = fmt.Errorf("rados: attempting to get exclusive %s lock for %s on object %s: unexpected non-error return value %d from lockExclusive", name, desc, loc, res)
					return
				}
			}
		}
		close(locking_finished)
	}()

	// block on either locking_finished or ctx.Done()
	select {
	case <-locking_finished:
	case <-ctx.Done():
		theConfig.debugLogf("rados: abandoning attempt to obtain exclusive %s lock for %s on object %s: %s", name, desc, loc, ctx.Err())
		err = ctx.Err()
	}

	return
}

// unlock previously obtained data lock
func (v *RadosVolume) unlock(loc string, name string, lockCookie string) (err error) {
	res := 0
	res, err = v.ioctx.Unlock(loc, name, lockCookie)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.UnlockOps)
	v.stats.TickErr(err)
	if err != nil {
		return
	}
	if res == RadosLockNotFound {
		err = fmt.Errorf("rados: attempting to unlock %s lock on object %s, lock was not held for cookie '%s'", name, loc, lockCookie)
	}
	return
}

// delete deletes the block now, regardless of trash state
//
// Note that this also deletes any xattrs and/or locks on
// the object.
func (v *RadosVolume) delete(loc string) (err error) {
	err = v.ioctx.Delete(loc)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.DelOps)
	v.stats.TickErr(err)
	return
}

// isTrash returns true is the object is marked as trash
// otherwise it returns false. If the object does not exist,
// it returns an error that fulfills os.IsNotExist()
func (v *RadosVolume) isTrash(loc string) (trash bool, err error) {
	trash_bytes := make([]byte, 1)
	n, err := v.ioctx.GetXattr(loc, RadosXattrTrash, trash_bytes)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.GetXattrOps)
	v.stats.TickErr(err)
	if err != nil {
		return
	}
	if n != 1 {
		err = fmt.Errorf("rados: isTrash read %d bytes for %s xattr but we were expecting %d", n, RadosXattrTrash, RFC3339NanoMaxLen)
		return
	}
	switch uint8(trash_bytes[0]) {
	case 0:
		trash = false
	case 1:
		trash = true
	default:
		err = fmt.Errorf("rados: got unexpected %s xattr value %d when we were expecting either 0 or 1", RadosXattrTrash, trash_bytes[0])
	}
	return
}

// markTrash marks an object as trash.
func (v *RadosVolume) markTrash(loc string) (err error) {
	err = v.ioctx.SetXattr(loc, RadosXattrTrash, []byte{uint8(1)})

	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.SetXattrOps)
	v.stats.TickErr(err)
	if err != nil {
		err = fmt.Errorf("rados: markTrash failed to update %s xattr for loc '%s': %v", RadosXattrTrash, loc, err)
	}
	return
}

// markNotTrash ensures that the object is not marked as trash
func (v *RadosVolume) markNotTrash(loc string) (err error) {
	err = v.ioctx.SetXattr(loc, RadosXattrTrash, []byte{uint8(0)})
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.SetXattrOps)
	v.stats.TickErr(err)
	if err != nil {
		err = fmt.Errorf("rados: markNotTrash failed to update %s xattr for loc '%s': %v", RadosXattrTrash, loc, err)
	}
	return
}

// setMtime sets the mtime xattr to the given time
func (v *RadosVolume) setMtime(loc string, mtime time.Time) (err error) {
	mtime_string := mtime.Format(time.RFC3339Nano)
	mtime_bytes := []byte(fmt.Sprintf("%[1]*[2]s", RFC3339NanoMaxLen, mtime_string))
	err = v.ioctx.SetXattr(loc, RadosXattrMtime, mtime_bytes)
	err = v.translateError(err)
	v.stats.Tick(&v.stats.Ops, &v.stats.SetXattrOps)
	if err != nil {
		err = fmt.Errorf("rados: failed to update %s xattr for loc '%s': %v", RadosXattrMtime, loc, err)
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

func (v *RadosVolume) isKeepBlock(s string) bool {
	return radosKeepBlockRegexp.MatchString(s)
}

func (v *RadosVolume) isEmptyBlock(loc string) bool {
	return loc[:32] == EmptyHash
}

func (v *RadosVolume) translateError(err error) error {
	switch err := err.(type) {
	case rados.RadosError:
		if err == rados.RadosErrorNotFound {
			return os.ErrNotExist
		}
	}
	return err
}

type listEntry interface {
	String() string
	Err() error
}

type trashListEntry struct{}

func (tle *trashListEntry) String() string {
	return ""
}

func (tle *trashListEntry) Err() error {
	return nil
}

type indexListEntry struct {
	loc   string
	size  uint64
	mtime time.Time
	err   error
}

func (ile *indexListEntry) String() string {
	return fmt.Sprintf("%s+%d %d", ile.loc, ile.size, ile.mtime.UnixNano())
}

func (ile *indexListEntry) Err() error {
	return ile.err
}

func (v *RadosVolume) newIndexListEntry(loc string) (ile *indexListEntry) {
	ile = &indexListEntry{
		loc: loc,
	}
	size, err := v.size(loc)
	if err != nil {
		ile.err = err
		return
	}
	ile.size = size

	mtime, err := v.Mtime(loc)
	if err != nil {
		ile.err = err
		return
	}
	ile.mtime = mtime

	return
}

func (v *RadosVolume) listObjects(filterFunc func(string) (bool, error), mapFunc func(string) listEntry, reduceFunc func(listEntry), workers int) (err error) {
	// open object list iterator
	iter, err := v.ioctx.Iter()
	v.stats.Tick(&v.stats.Ops, &v.stats.ListOps)
	v.stats.TickErr(err)
	if err != nil {
		return
	}
	defer iter.Close()

	// channels to enqueue locs and receive list entries (or errors) from concurrent workers
	listLocChan := make(chan string, workers)
	listEntryChan := make(chan listEntry, workers)

	// channel to get error status back from async put
	listLocErrChan := make(chan error, 1)

	// asynchronously put objects to list on listLocChan
	go func() {
		var listErr error
		for iter.Next() {
			v.stats.Tick(&v.stats.Ops, &v.stats.ListOps)
			loc := iter.Value()
			if !v.isKeepBlock(loc) {
				continue
			}
			include, listErr := filterFunc(loc)
			if listErr != nil {
				break
			}
			if include {
				listLocChan <- loc
			}
		}
		listLocErrChan <- listErr
		close(listLocChan)
		return
	}()

	// start workers to process listLocs using mapFunc
	// and put the results on listEntryChan
	for i := 0; i < workers; i++ {
		go func() {
			for loc := range listLocChan {
				le := mapFunc(loc)
				listEntryChan <- le
			}
		}()
	}

	// process listEntry entries from listEntryChan and pass them to reduceFunc
	// checking for errors along the way
	for le := range listEntryChan {
		if le.Err() != nil {
			// TODO could signal async work to stop here as we no longer
			// need the results.
			err = le.Err()
			// Absent such a signal, we must not return from this
			// error until we have processed the remaining data from listEntryChan
			// or we may leak goroutines.
			continue
		}
		reduceFunc(le)
	}
	if err != nil {
		// at least one of the list entries had an error
		return
	}

	// check if there was an error from the loc listing function and, if so, return it
	err = <-listLocErrChan
	return
}

type radospoolStats struct {
	statsTicker
	Ops         uint64
	GetOps      uint64
	PutOps      uint64
	StatOps     uint64
	GetXattrOps uint64
	SetXattrOps uint64
	LockOps     uint64
	UnlockOps   uint64
	DelOps      uint64
	ListOps     uint64
}

func (s *radospoolStats) TickErr(err error) {
	if err == nil {
		return
	}
	errType := fmt.Sprintf("%T", err)
	if err, ok := err.(*rados.RadosError); ok {
		errType = errType + fmt.Sprintf(" %s", err.Error())
	}
	s.statsTicker.TickErr(err, errType)
}

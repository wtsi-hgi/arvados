// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0
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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ghodss/yaml"
	check "gopkg.in/check.v1"
)

const (
	RadosMockPool      = "mocktestpool"
	RadosMockMonHost   = "mocktestmonhost"
	RadosMockTotalSize = 1 * 1024 * 1024 * 1024
	RadosMockFSID      = "mockmock-mock-mock-mock-mockmockmock"
)

var radosTestPool string
var RadosMockPools []string

func init() {
	flag.StringVar(
		&radosTestPool,
		"test.rados-pool-volume",
		"",
		"Rados pool to use for testing (i.e. to test against a 'real' Ceph cluster such as a ceph/demo docker container). Do not use a pool with real data for testing! Use normal rados volume arguments (e.g. -rados-mon-host, -rados-user, -rados-keyring-file) to supply required parameters to access the pool.")

	RadosMockPools = []string{"mocktestpool0", "mocktestpool1", "mocktestpool2", "mocktestpool3", "mocktestpool4", RadosMockPool}
}

type radosStubObj struct {
	data           []byte
	xattrs         map[string][]byte
	exclusiveLocks map[string]string
	sharedLocks    map[string]map[string]bool
}

func newRadosStubObj(data []byte) *radosStubObj {
	return &radosStubObj{
		data:           data,
		xattrs:         make(map[string][]byte),
		exclusiveLocks: make(map[string]string),
		sharedLocks:    make(map[string]map[string]bool),
	}
}

type radosStubNamespace struct {
	objects map[string]*radosStubObj
}

func newRadosStubNamespace() *radosStubNamespace {
	return &radosStubNamespace{
		objects: make(map[string]*radosStubObj),
	}
}

type radosStubPool struct {
	namespaces map[string]*radosStubNamespace
}

func newRadosStubPool() *radosStubPool {
	return &radosStubPool{
		namespaces: make(map[string]*radosStubNamespace),
	}
}

type radosStubBackend struct {
	sync.Mutex
	config      map[string]string
	totalSize   uint64
	numReplicas uint64
	fsid        string
	pools       map[string]*radosStubPool
	race        chan chan struct{}
}

func newRadosStubBackend(numReplicas uint64) *radosStubBackend {
	return &radosStubBackend{
		config:      make(map[string]string),
		totalSize:   RadosMockTotalSize,
		numReplicas: numReplicas,
		fsid:        "00000000-0000-0000-0000-000000000000",
		pools:       make(map[string]*radosStubPool),
	}
}

func (h *radosStubBackend) PutRaw(pool string, namespace string, oid string, data []byte) {
	h.Lock()
	defer h.Unlock()
	_, ok := h.pools[pool]
	if !ok {
		h.pools[pool] = newRadosStubPool()
	}
	_, ok = h.pools[pool].namespaces[namespace]
	if !ok {
		h.pools[pool].namespaces[namespace] = newRadosStubNamespace()
	}

	_, ok = h.pools[pool].namespaces[namespace].objects[oid]
	if !ok {
		h.pools[pool].namespaces[namespace].objects[oid] = newRadosStubObj(data)
	}
}

func (h *radosStubBackend) unlockAndRace() {
	if h.race == nil {
		return
	}
	h.Unlock()
	// Signal caller that race is starting by reading from
	// h.race. If we get a channel, block until that channel is
	// ready to receive. If we get nil (or h.race is closed) just
	// proceed.
	if c := <-h.race; c != nil {
		c <- struct{}{}
	}
	h.Lock()
}

type TestableRadosVolume struct {
	*RadosVolume
	radosStubBackend *radosStubBackend
	t                TB
}

func NewTestableRadosVolume(t TB, readonly bool, replication int) *TestableRadosVolume {
	var v *RadosVolume
	radosStubBackend := newRadosStubBackend(uint64(replication))
	pool := radosTestPool
	if pool == "" {
		// Connect using mock radosImplementation instead of real Ceph
		t.Log("rados: using mock radosImplementation")
		radosMock := &radosMockImpl{
			b: radosStubBackend,
		}
		v = &RadosVolume{
			Pool:             RadosMockPool,
			MonHost:          RadosMockMonHost,
			ReadOnly:         readonly,
			RadosReplication: replication,
			rados:            radosMock,
		}
	} else {
		// Connect to real Ceph using the real radosImplementation
		t.Log("rados: using real radosImplementation")
		v = &RadosVolume{
			Pool:             pool,
			KeyringFile:      radosKeyringFile,
			MonHost:          radosMonHost,
			Cluster:          radosCluster,
			User:             radosUser,
			ReadOnly:         readonly,
			RadosReplication: replication,
		}
	}

	tv := &TestableRadosVolume{
		RadosVolume:      v,
		radosStubBackend: radosStubBackend,
		t:                t,
	}

	err := tv.Start()
	if err != nil {
		t.Error(err)
	}
	return tv
}

var _ = check.Suite(&StubbedRadosSuite{})

type StubbedRadosSuite struct {
	volume *TestableRadosVolume
}

func (s *StubbedRadosSuite) SetUpTest(c *check.C) {
	s.volume = NewTestableRadosVolume(c, false, 3)
}

func (s *StubbedRadosSuite) TearDownTest(c *check.C) {
	s.volume.Teardown()
}

func TestRadosVolumeWithGeneric(t *testing.T) {
	DoGenericVolumeTests(t, func(t TB) TestableVolume {
		return NewTestableRadosVolume(t, false, radosReplication)
	})
}

func TestRadosReadonlyVolumeWithGeneric(t *testing.T) {
	DoGenericVolumeTests(t, func(t TB) TestableVolume {
		return NewTestableRadosVolume(t, true, radosReplication)
	})
}

func TestRadosVolumeReplication(t *testing.T) {
	for r := 1; r <= 4; r++ {
		v := NewTestableRadosVolume(t, false, r)
		defer v.Teardown()
		if n := v.Replication(); n != r {
			t.Errorf("Got replication %d, expected %d", n, r)
		}
	}
}

func TestRadosVolumeCreateBlobRace(t *testing.T) {
	v := NewTestableRadosVolume(t, false, 3)
	defer v.Teardown()

	var wg sync.WaitGroup

	v.radosStubBackend.race = make(chan chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := v.Put(context.Background(), TestHash, TestBlock)
		if err != nil {
			t.Error(err)
		}
	}()
	continuePut := make(chan struct{})
	// Wait for the stub's Put to create the empty blob
	v.radosStubBackend.race <- continuePut
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, len(TestBlock))
		_, err := v.Get(context.Background(), TestHash, buf)
		if err != nil {
			t.Error(err)
		}
	}()
	// Wait for the stub's Get to get the empty blob
	close(v.radosStubBackend.race)
	// Allow stub's Put to continue, so the real data is ready
	// when the volume's Get retries
	<-continuePut
	// Wait for Get() and Put() to finish
	wg.Wait()
}

func TestRadosVolumeCreateBlobRaceDeadline(t *testing.T) {
	v := NewTestableRadosVolume(t, false, 3)
	defer v.Teardown()

	v.PutRaw(TestHash, nil)

	buf := new(bytes.Buffer)
	v.IndexTo("", buf)
	if buf.Len() != 0 {
		t.Errorf("Index %+q should be empty", buf.Bytes())
	}

	v.TouchWithDate(TestHash, time.Now().Add(-1982*time.Millisecond))

	allDone := make(chan struct{})
	go func() {
		defer close(allDone)
		buf := make([]byte, BlockSize)
		n, err := v.Get(context.Background(), TestHash, buf)
		if err != nil {
			t.Error(err)
			return
		}
		if n != 0 {
			t.Errorf("Got %+q, expected empty buf", buf[:n])
		}
	}()
	select {
	case <-allDone:
	case <-time.After(time.Second):
		t.Error("Get should have stopped waiting for race when block was 2s old")
	}

	buf.Reset()
	v.IndexTo("", buf)
	if !bytes.HasPrefix(buf.Bytes(), []byte(TestHash+"+0")) {
		t.Errorf("Index %+q should have %+q", buf.Bytes(), TestHash+"+0")
	}
}

func TestRadosVolumeContextCancelGet(t *testing.T) {
	testRadosVolumeContextCancel(t, func(ctx context.Context, v *TestableRadosVolume) error {
		v.PutRaw(TestHash, TestBlock)
		_, err := v.Get(ctx, TestHash, make([]byte, BlockSize))
		return err
	})
}

func TestRadosVolumeContextCancelPut(t *testing.T) {
	testRadosVolumeContextCancel(t, func(ctx context.Context, v *TestableRadosVolume) error {
		return v.Put(ctx, TestHash, make([]byte, BlockSize))
	})
}

func TestRadosVolumeContextCancelCompare(t *testing.T) {
	testRadosVolumeContextCancel(t, func(ctx context.Context, v *TestableRadosVolume) error {
		v.PutRaw(TestHash, TestBlock)
		return v.Compare(ctx, TestHash, TestBlock2)
	})
}

func testRadosVolumeContextCancel(t *testing.T, testFunc func(context.Context, *TestableRadosVolume) error) {
	v := NewTestableRadosVolume(t, false, 3)
	defer v.Teardown()
	v.radosStubBackend.race = make(chan chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	allDone := make(chan struct{})
	go func() {
		defer close(allDone)
		err := testFunc(ctx, v)
		if err != context.Canceled {
			t.Errorf("got %T %q, expected %q", err, err, context.Canceled)
		}
	}()
	releaseHandler := make(chan struct{})
	select {
	case <-allDone:
		t.Error("testFunc finished without waiting for v.radosStubBackend.race")
	case <-time.After(10 * time.Second):
		t.Error("timed out waiting to enter handler")
	case v.radosStubBackend.race <- releaseHandler:
	}

	cancel()

	select {
	case <-time.After(10 * time.Second):
		t.Error("timed out waiting to cancel")
	case <-allDone:
	}

	go func() {
		<-releaseHandler
	}()
}

func (s *StubbedRadosSuite) TestStats(c *check.C) {
	stats := func() string {
		buf, err := json.Marshal(s.volume.InternalStats())
		c.Check(err, check.IsNil)
		return string(buf)
	}

	c.Check(stats(), check.Matches, `.*"Ops":0,.*`)
	c.Check(stats(), check.Matches, `.*"Errors":0,.*`)

	loc := "acbd18db4cc2f85cedef654fccc4a4d8"
	_, err := s.volume.Get(context.Background(), loc, make([]byte, 3))
	c.Check(err, check.NotNil)
	c.Check(stats(), check.Matches, `.*"Ops":[^0],.*`)
	c.Check(stats(), check.Matches, `.*"Errors":[^0],.*`)
	c.Check(stats(), check.Matches, `.*"rados\.RadosErrorNotFound.*?":[^0].*`)
	c.Check(stats(), check.Matches, `.*"InBytes":0,.*`)

	err = s.volume.Put(context.Background(), loc, []byte("foo"))
	c.Check(err, check.IsNil)
	c.Check(stats(), check.Matches, `.*"OutBytes":3,.*`)
	c.Check(stats(), check.Matches, `.*"CreateOps":1,.*`)

	_, err = s.volume.Get(context.Background(), loc, make([]byte, 3))
	c.Check(err, check.IsNil)
	_, err = s.volume.Get(context.Background(), loc, make([]byte, 3))
	c.Check(err, check.IsNil)
	c.Check(stats(), check.Matches, `.*"InBytes":6,.*`)
}

func (s *StubbedRadosSuite) TestConfig(c *check.C) {
	var cfg Config
	err := yaml.Unmarshal([]byte(`
Volumes:
  - Type: Rados
    StorageClasses: ["class_a", "class_b"]
`), &cfg)

	c.Check(err, check.IsNil)
	c.Check(cfg.Volumes[0].GetStorageClasses(), check.DeepEquals, []string{"class_a", "class_b"})
}

func (v *TestableRadosVolume) PutRaw(locator string, data []byte) {
	v.radosStubBackend.PutRaw(v.Pool, RadosKeepNamespace, locator, data)
}

func (v *TestableRadosVolume) TouchWithDate(locator string, lastPut time.Time) {
	v.setMtime(locator, lastPut)
}

func (v *TestableRadosVolume) Teardown() {}

// radosMockImpl implements the radosImplemetation interface for testing purposes
type radosMockImpl struct {
	b *radosStubBackend
}

func (r *radosMockImpl) Version() (major int, minor int, patch int) {
	// might as well pass this along to the actual librados client
	return rados.Version()
}

func (r *radosMockImpl) NewConnWithClusterAndUser(clusterName string, userName string) (conn radosConn, err error) {
	conn = &radosMockConn{
		radosMockImpl: r,
		cluster:       clusterName,
		user:          userName,
	}
	return
}

type radosMockConn struct {
	*radosMockImpl
	cluster   string
	user      string
	connected bool
}

func (conn *radosMockConn) SetConfigOption(option, value string) (err error) {
	conn.b.Lock()
	defer conn.b.Unlock()

	conn.b.config[option] = value
	return
}

func (conn *radosMockConn) Connect() (err error) {
	conn.b.Lock()
	defer conn.b.Unlock()

	conn.connected = true
	conn.b.fsid = RadosMockFSID
	for _, pool := range RadosMockPools {
		conn.b.pools[pool] = newRadosStubPool()
	}
	return
}

func (conn *radosMockConn) GetFSID() (fsid string, err error) {
	conn.b.Lock()
	defer conn.b.Unlock()

	fsid = conn.b.fsid
	if !conn.connected {
		err = fmt.Errorf("radosmock: GetFSID called before Connect")
	}
	return
}

func (conn *radosMockConn) GetClusterStats() (stat rados.ClusterStat, err error) {
	conn.b.Lock()
	defer conn.b.Unlock()

	if !conn.connected {
		panic("radosmock: GetClusterStats called before Connect")
	}
	stat.Kb = conn.b.totalSize
	for _, pool := range conn.b.pools {
		for _, namespace := range pool.namespaces {
			for _, obj := range namespace.objects {
				size := len(obj.data)
				stat.Kb_used += uint64(size)
				stat.Num_objects++
			}
		}
	}
	stat.Kb_avail = stat.Kb - stat.Kb_used
	return
}

func (conn *radosMockConn) ListPools() (names []string, err error) {
	conn.b.Lock()
	defer conn.b.Unlock()

	names = make([]string, len(conn.b.pools))
	i := 0
	for k := range conn.b.pools {
		names[i] = k
		i++
	}
	if !conn.connected {
		err = fmt.Errorf("radosmock: ListPools called before Connect")
	}
	return
}

func (conn *radosMockConn) OpenIOContext(pool string) (ioctx radosIOContext, err error) {
	ioctx = &radosMockIoctx{
		radosMockConn: conn,
		pool:          pool,
	}
	ioctx.SetNamespace("")

	return
}

type radosMockIoctx struct {
	*radosMockConn
	pool      string
	namespace string
	objects   map[string]*radosStubObj
}

func (ioctx *radosMockIoctx) Delete(oid string) (err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	_, ok := ioctx.objects[oid]
	if !ok {
		err = rados.RadosErrorNotFound
		return
	}
	delete(ioctx.objects, oid)
	return
}

func (ioctx *radosMockIoctx) GetPoolStats() (stat rados.PoolStat, err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	pool := ioctx.b.pools[ioctx.pool]
	for _, namespace := range pool.namespaces {
		for _, obj := range namespace.objects {
			size := len(obj.data)
			stat.Num_bytes += uint64(size)
			stat.Num_objects++
		}
	}
	stat.Num_kb = stat.Num_bytes / 1024
	stat.Num_object_clones = stat.Num_objects * ioctx.b.numReplicas

	return
}

func (ioctx *radosMockIoctx) GetXattr(oid string, name string, data []byte) (n int, err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	obj, ok := ioctx.objects[oid]
	if !ok {
		err = rados.RadosErrorNotFound
		return
	}
	xv, ok := obj.xattrs[RadosXattrTrash]
	if !ok {
		err = rados.RadosErrorNotFound
	}
	n = copy(data, xv)
	return
}

func (ioctx *radosMockIoctx) Iter() (iter radosIter, err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	oids := make([]string, len(ioctx.objects))
	i := 0
	for oid := range ioctx.objects {
		oids[i] = oid
		i++
	}
	iter = &radosMockIter{
		radosMockIoctx: ioctx,
		oids:           oids,
		current:        -1,
	}
	return
}

func (ioctx *radosMockIoctx) LockExclusive(oid, name, cookie, desc string, duration time.Duration, flags *byte) (res int, err error) {
	return ioctx.lock(oid, name, cookie, true)
}

func (ioctx *radosMockIoctx) lock(oid, name, cookie string, exclusive bool) (res int, err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	_, ok := ioctx.objects[oid]
	if !ok {
		// locking a nonexistant object creates an empty object
		ioctx.objects[oid] = newRadosStubObj([]byte{})
	}
	obj, ok := ioctx.objects[oid]
	if !ok {
		err = fmt.Errorf("radosmock: failed to create nonexistant object for lock")
		return
	}

	existingCookie, exclusiveLockHeld := obj.exclusiveLocks[name]
	if exclusiveLockHeld {
		if exclusive && existingCookie == cookie {
			res = RadosLockExist
		} else {
			res = RadosLockBusy
		}
		return
	}

	existingCookieMap, sharedLockHeld := obj.sharedLocks[name]
	if sharedLockHeld {
		if exclusive {
			// want an exclusive lock but shared locks exist
			res = RadosLockBusy
		} else {
			// want a shared lock
			_, sharedLockExist := existingCookieMap[cookie]
			if sharedLockExist {
				res = RadosLockExist
			} else {
				// want a shared lock and some exist but not ours, add our cookie to the map
				existingCookieMap[cookie] = true
				res = RadosLockLocked
			}
		}
		return
	}

	// there is no existing lock by this name on this object, take the lock
	if exclusive {
		obj.exclusiveLocks[name] = cookie
	} else {
		obj.sharedLocks[name] = make(map[string]bool)
		obj.sharedLocks[name][cookie] = true
	}
	res = RadosLockLocked
	return
}

func (ioctx *radosMockIoctx) LockShared(oid, name, cookie, tag, desc string, duration time.Duration, flags *byte) (res int, err error) {
	return ioctx.lock(oid, name, cookie, false)
}

func (ioctx *radosMockIoctx) Read(oid string, data []byte, offset uint64) (n int, err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	obj, ok := ioctx.objects[oid]
	if !ok {
		err = os.ErrNotExist
		return
	}
	n = copy(data, obj.data)
	return
}

func (ioctx *radosMockIoctx) SetNamespace(namespace string) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	ioctx.namespace = namespace
	_, ok := ioctx.b.pools[ioctx.pool].namespaces[ioctx.namespace]
	if !ok {
		ioctx.b.pools[ioctx.pool].namespaces[ioctx.namespace] = newRadosStubNamespace()
	}
	ns, _ := ioctx.b.pools[ioctx.pool].namespaces[ioctx.namespace]
	ioctx.objects = ns.objects
	return
}

func (ioctx *radosMockIoctx) SetXattr(oid string, name string, data []byte) (err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	obj, ok := ioctx.objects[oid]
	if !ok {
		err = rados.RadosErrorNotFound
		return
	}
	obj.xattrs[name] = data
	return
}

func (ioctx *radosMockIoctx) Stat(oid string) (stat rados.ObjectStat, err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	obj, ok := ioctx.objects[oid]
	if !ok {
		err = os.ErrNotExist
		return
	}
	stat.Size = uint64(len(obj.data))
	// don't bother implementing stat.ModTime as we do not use it

	return
}

func (ioctx *radosMockIoctx) Unlock(oid, name, cookie string) (res int, err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	obj, ok := ioctx.objects[oid]
	if !ok {
		res = RadosLockNotFound
		return
	}

	existingCookie, exclusiveLockHeld := obj.exclusiveLocks[name]
	if exclusiveLockHeld {
		if existingCookie == cookie {
			// this is our lock, delete it
			delete(obj.exclusiveLocks, name)
			res = RadosLockUnlocked
		} else {
			res = RadosLockNotFound
		}
		return
	}

	existingCookieMap, sharedLockHeld := obj.sharedLocks[name]
	if sharedLockHeld {
		_, sharedLockExist := existingCookieMap[cookie]
		if sharedLockExist {
			// this is our cookie, delete it from the cookie map
			delete(existingCookieMap, cookie)
			if len(existingCookieMap) == 0 {
				// this was the last shared cookie, delete the sharedLocks entry as well
				delete(obj.sharedLocks, name)
			}
			res = RadosLockUnlocked
		} else {
			res = RadosLockNotFound
		}
		return
	}

	res = RadosLockNotFound
	return
}

func (ioctx *radosMockIoctx) WriteFull(oid string, data []byte) (err error) {
	ioctx.b.Lock()
	defer ioctx.b.Unlock()

	obj, ok := ioctx.objects[oid]
	if !ok {
		ioctx.objects[oid] = newRadosStubObj([]byte{})
	}
	copy(obj.data, data)
	return
}

type radosMockIter struct {
	*radosMockIoctx
	oids    []string
	current int
}

func (iter *radosMockIter) Next() bool {
	iter.current++
	if iter.current >= len(iter.oids) {
		return false
	}
	return true
}

func (iter *radosMockIter) Value() string {
	if iter.current >= 0 && iter.current < len(iter.oids) {
		return iter.oids[iter.current]
	} else {
		return ""
	}
}

func (iter *radosMockIter) Close() {
	return
}

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
	"sync"
	"testing"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ghodss/yaml"
	check "gopkg.in/check.v1"
)

const (
	RadosMockTestPool    = "mocktestpool"
	RadosMockTestMonHost = "mocktestmonhost"
)

var radosTestPool string

func init() {
	flag.StringVar(
		&radosTestPool,
		"test.rados-pool-volume",
		"",
		"Rados pool to use for testing (i.e. to test against a 'real' Ceph cluster such as a ceph/demo docker container). Do not use a pool with real data for testing! Use normal rados volume arguments (e.g. -rados-mon-host, -rados-user, -rados-keyring-file) to supply required parameters to access the pool.")
}

type radosStubObj struct {
	Data   []byte
	Xattrs map[string][]byte
}

type radosStubBackend struct {
	sync.Mutex
	objects map[string]*radosStubObj
	race    chan chan struct{}
}

func newRadosStubBackend() *radosStubBackend {
	return &radosStubBackend{
		objects: make(map[string]*radosStubObj),
	}
}

func (h *radosStubBackend) PutRaw(oid string, data []byte) {
	h.Lock()
	defer h.Unlock()
	h.objects[oid] = &radosStubObj{
		Data:   data,
		Xattrs: make(map[string][]byte),
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
	radosStubBackend := newRadosStubBackend()
	pool := radosTestPool
	if pool == "" {
		// Connect using mock radosImplementation instead of real Ceph
		radosMock := &radosMockImpl{
			b: radosStubBackend,
		}
		v = &RadosVolume{
			Pool:             RadosMockTestPool,
			MonHost:          RadosMockTestMonHost,
			ReadOnly:         readonly,
			RadosReplication: replication,
			rados:            radosMock,
		}
	} else {
		// Connect to real Ceph using the real radosImplementation
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

	return &TestableRadosVolume{
		RadosVolume:      v,
		radosStubBackend: radosStubBackend,
		t:                t,
	}
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
	v.radosStubBackend.PutRaw(locator, data)
}

func (v *TestableRadosVolume) TouchWithDate(locator string, lastPut time.Time) {
	v.setMtime(locator, lastPut)
}

func (v *TestableRadosVolume) Teardown() {}

// radosMockImpl implements the radosImplemetation interface for testing purposes
type radosMockImpl struct {
	b *radosStubBackend
}

func (rados *radosMockImpl) Version() (major int, minor int, patch int) {
	major = -1
	minor = -1
	patch = -1
	return
}

func (rados *radosMockImpl) NewConnWithClusterAndUser(clusterName string, userName string) (conn radosConn, err error) {
	conn = &radosMockConn{
		radosMockImpl: rados,
		cluster:       clusterName,
		user:          userName,
	}
	return
}

type radosMockConn struct {
	*radosMockImpl
	cluster string
	user    string
	config  map[string]string
}

func (conn *radosMockConn) SetConfigOption(option, value string) (err error) {
	conn.config[option] = value
	return
}

func (conn *radosMockConn) Connect() (err error) {
	return
}

func (conn *radosMockConn) GetFSID() (fsid string, err error) {
	return
}

func (conn *radosMockConn) GetClusterStats() (stat rados.ClusterStat, err error) {
	return
}

func (conn *radosMockConn) ListPools() (names []string, err error) {
	return
}

func (conn *radosMockConn) OpenIOContext(pool string) (ioctx radosIOContext, err error) {
	ioctx = &radosMockIoctx{
		conn,
	}
	return
}

type radosMockIoctx struct {
	*radosMockConn
}

func (ioctx *radosMockIoctx) Delete(oid string) (err error) {
	return
}

func (ioctx *radosMockIoctx) GetPoolStats() (stat rados.PoolStat, err error) {
	return
}

func (ioctx *radosMockIoctx) GetXattr(object string, name string, data []byte) (n int, err error) {
	return
}

func (ioctx *radosMockIoctx) Iter() (iter radosIter, err error) {
	iter = &radosMockIter{
		radosMockIoctx: ioctx,
	}
	return
}

func (ioctx *radosMockIoctx) LockExclusive(oid, name, cookie, desc string, duration time.Duration, flags *byte) (res int, err error) {
	return
}

func (ioctx *radosMockIoctx) LockShared(oid, name, cookie, tag, desc string, duration time.Duration, flags *byte) (res int, err error) {
	return
}

func (ioctx *radosMockIoctx) Read(oid string, data []byte, offset uint64) (n int, err error) {
	return
}

func (ioctx *radosMockIoctx) SetNamespace(namespace string) {
	return
}

func (ioctx *radosMockIoctx) SetXattr(object string, name string, data []byte) (err error) {
	return
}

func (ioctx *radosMockIoctx) Stat(object string) (stat rados.ObjectStat, err error) {
	return
}

func (ioctx *radosMockIoctx) Unlock(oid, name, cookie string) (res int, err error) {
	return
}

func (ioctx *radosMockIoctx) WriteFull(oid string, data []byte) (err error) {
	return
}

type radosMockIter struct {
	*radosMockIoctx
}

func (iter *radosMockIter) Next() bool {
	return false
}

func (iter *radosMockIter) Value() string {
	return "itervalue"
}

func (iter *radosMockIter) Close() {
	return
}

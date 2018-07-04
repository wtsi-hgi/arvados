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
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ghodss/yaml"
	check "gopkg.in/check.v1"
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
	Size   uint64
	Xattrs map[string][]byte
}

type radosStubHandler struct {
	sync.Mutex
	objects map[string]*radosStubObj
	race    chan chan struct{}
}

func newRadosStubHandler() *radosStubHandler {
	return &radosStubHandler{
		objects: make(map[string]*radosStubObj),
	}
}

func (h *radosStubHandler) TouchWithDate(hash string, t time.Time) {
	obj, ok := h.objects[hash]
	if !ok {
		return
	}
	obj.Mtime = t
}

func (h *radosStubHandler) PutRaw(hash string, data []byte) {
	h.Lock()
	defer h.Unlock()
	h.blobs[oid+"|"+hash] = &azBlob{
		Data:        data,
		Mtime:       time.Now(),
		Metadata:    make(map[string]string),
		Uncommitted: make(map[string][]byte),
	}
}

func (h *radosStubHandler) unlockAndRace() {
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
	radosHandler *radosStubHandler
	radosStub    *httptest.Server
	t            TB
}

func NewTestableRadosVolume(t TB, readonly bool, replication int) *TestableRadosVolume {
	radosHandler := newRadosStubHandler()
	radosStub := httptest.NewServer(radosHandler)

	var azClient storage.Client

	pool := radosTestPool
	if pool == "" {
		// Connect to stub instead of real Azure storage service
		stubURLBase := strings.Split(radosStub.URL, "://")[1]
		var err error
		if azClient, err = storage.NewClient(fakeAccountName, fakeAccountKey, stubURLBase, storage.DefaultAPIVersion, false); err != nil {
			t.Fatal(err)
		}
		oid = "fakeoidname"
	} else {
		// Connect to real Azure storage service
		accountKey, err := readKeyFromFile(azureStorageAccountKeyFile)
		if err != nil {
			t.Fatal(err)
		}
		azClient, err = storage.NewBasicClient(azureStorageAccountName, accountKey)
		if err != nil {
			t.Fatal(err)
		}
	}

	bs := azClient.GetBlobService()
	v := &RadosVolume{
		ContainerName:    container,
		ReadOnly:         readonly,
		AzureReplication: replication,
		azClient:         azClient,
		container:        &azureContainer{ctr: bs.GetContainerReference(container)},
	}

	return &TestableRadosVolume{
		RadosVolume:  v,
		radosHandler: radosHandler,
		radosStub:    radosStub,
		t:            t,
	}
}

var _ = check.Suite(&StubbedRadosSuite{})

type StubbedRadosSuite struct {
	volume            *TestableRadosVolume
	origHTTPTransport http.RoundTripper
}

func (s *StubbedRadosSuite) SetUpTest(c *check.C) {
	s.origHTTPTransport = http.DefaultTransport
	http.DefaultTransport = &http.Transport{
		Dial: (&radosStubDialer{}).Dial,
	}
	azureWriteRaceInterval = time.Millisecond
	azureWriteRacePollTime = time.Nanosecond

	s.volume = NewTestableRadosVolume(c, false, 3)
}

func (s *StubbedRadosSuite) TearDownTest(c *check.C) {
	s.volume.Teardown()
	http.DefaultTransport = s.origHTTPTransport
}

func TestRadosVolumeWithGeneric(t *testing.T) {
	defer func(t http.RoundTripper) {
		http.DefaultTransport = t
	}(http.DefaultTransport)
	http.DefaultTransport = &http.Transport{
		Dial: (&radosStubDialer{}).Dial,
	}
	azureWriteRaceInterval = time.Millisecond
	azureWriteRacePollTime = time.Nanosecond
	DoGenericVolumeTests(t, func(t TB) TestableVolume {
		return NewTestableRadosVolume(t, false, azureStorageReplication)
	})
}

func TestRadosVolumeConcurrentRanges(t *testing.T) {
	defer func(b int) {
		azureMaxGetBytes = b
	}(azureMaxGetBytes)

	defer func(t http.RoundTripper) {
		http.DefaultTransport = t
	}(http.DefaultTransport)
	http.DefaultTransport = &http.Transport{
		Dial: (&radosStubDialer{}).Dial,
	}
	azureWriteRaceInterval = time.Millisecond
	azureWriteRacePollTime = time.Nanosecond
	// Test (BlockSize mod azureMaxGetBytes)==0 and !=0 cases
	for _, azureMaxGetBytes = range []int{2 << 22, 2<<22 - 1} {
		DoGenericVolumeTests(t, func(t TB) TestableVolume {
			return NewTestableRadosVolume(t, false, azureStorageReplication)
		})
	}
}

func TestReadonlyRadosVolumeWithGeneric(t *testing.T) {
	defer func(t http.RoundTripper) {
		http.DefaultTransport = t
	}(http.DefaultTransport)
	http.DefaultTransport = &http.Transport{
		Dial: (&radosStubDialer{}).Dial,
	}
	azureWriteRaceInterval = time.Millisecond
	azureWriteRacePollTime = time.Nanosecond
	DoGenericVolumeTests(t, func(t TB) TestableVolume {
		return NewTestableRadosVolume(t, true, azureStorageReplication)
	})
}

func TestRadosVolumeRangeFenceposts(t *testing.T) {
	defer func(t http.RoundTripper) {
		http.DefaultTransport = t
	}(http.DefaultTransport)
	http.DefaultTransport = &http.Transport{
		Dial: (&radosStubDialer{}).Dial,
	}

	v := NewTestableRadosVolume(t, false, 3)
	defer v.Teardown()

	for _, size := range []int{
		2<<22 - 1, // one <max read
		2 << 22,   // one =max read
		2<<22 + 1, // one =max read, one <max
		2 << 23,   // two =max reads
		BlockSize - 1,
		BlockSize,
	} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte((i + 7) & 0xff)
		}
		hash := fmt.Sprintf("%x", md5.Sum(data))
		err := v.Put(context.Background(), hash, data)
		if err != nil {
			t.Error(err)
		}
		gotData := make([]byte, len(data))
		gotLen, err := v.Get(context.Background(), hash, gotData)
		if err != nil {
			t.Error(err)
		}
		gotHash := fmt.Sprintf("%x", md5.Sum(gotData))
		if gotLen != size {
			t.Errorf("length mismatch: got %d != %d", gotLen, size)
		}
		if gotHash != hash {
			t.Errorf("hash mismatch: got %s != %s", gotHash, hash)
		}
	}
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
	defer func(t http.RoundTripper) {
		http.DefaultTransport = t
	}(http.DefaultTransport)
	http.DefaultTransport = &http.Transport{
		Dial: (&radosStubDialer{}).Dial,
	}

	v := NewTestableRadosVolume(t, false, 3)
	defer v.Teardown()

	azureWriteRaceInterval = time.Second
	azureWriteRacePollTime = time.Millisecond

	var wg sync.WaitGroup

	v.radosHandler.race = make(chan chan struct{})

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
	v.radosHandler.race <- continuePut
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
	close(v.radosHandler.race)
	// Allow stub's Put to continue, so the real data is ready
	// when the volume's Get retries
	<-continuePut
	// Wait for Get() and Put() to finish
	wg.Wait()
}

func TestRadosVolumeCreateBlobRaceDeadline(t *testing.T) {
	defer func(t http.RoundTripper) {
		http.DefaultTransport = t
	}(http.DefaultTransport)
	http.DefaultTransport = &http.Transport{
		Dial: (&radosStubDialer{}).Dial,
	}

	v := NewTestableRadosVolume(t, false, 3)
	defer v.Teardown()

	azureWriteRaceInterval = 2 * time.Second
	azureWriteRacePollTime = 5 * time.Millisecond

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
	defer func(t http.RoundTripper) {
		http.DefaultTransport = t
	}(http.DefaultTransport)
	http.DefaultTransport = &http.Transport{
		Dial: (&radosStubDialer{}).Dial,
	}

	v := NewTestableRadosVolume(t, false, 3)
	defer v.Teardown()
	v.radosHandler.race = make(chan chan struct{})

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
		t.Error("testFunc finished without waiting for v.radosHandler.race")
	case <-time.After(10 * time.Second):
		t.Error("timed out waiting to enter handler")
	case v.radosHandler.race <- releaseHandler:
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
	c.Check(stats(), check.Matches, `.*"storage\.AzureStorageServiceError 404 \(404 Not Found\)":[^0].*`)
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
  - Type: Azure
    StorageClasses: ["class_a", "class_b"]
`), &cfg)

	c.Check(err, check.IsNil)
	c.Check(cfg.Volumes[0].GetStorageClasses(), check.DeepEquals, []string{"class_a", "class_b"})
}

func (v *TestableRadosVolume) PutRaw(locator string, data []byte) {
	v.radosHandler.PutRaw(v.ContainerName, locator, data)
}

func (v *TestableRadosVolume) TouchWithDate(locator string, lastPut time.Time) {
	v.radosHandler.TouchWithDate(v.ContainerName, locator, lastPut)
}

func (v *TestableRadosVolume) Teardown() {
	v.radosStub.Close()
}

// radosMockImpl implements the radosImplemetation interface for testing purposes
type radosMockImpl struct {
	clock   *radosFakeClock
	actions chan string
	unblock chan struct{}
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

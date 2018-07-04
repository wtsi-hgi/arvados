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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"git.curoverse.com/arvados.git/sdk/go/arvados"
	"github.com/ghodss/yaml"
	check "gopkg.in/check.v1"
)

const (
	TestPool = "testpool"
	TestMonHost = "testmonhost"
)

type fakeClock struct {
	now *time.Time
}

func (c *fakeClock) Now() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return *c.now
}

var _ = check.Suite(&StubbedRadosSuite{})

type StubbedRadosSuite struct {
	volumes []*TestableRadosVolume
}

func (s *StubbedRadosSuite) TestGeneric(c *check.C) {
	DoGenericVolumeTests(c, func(t TB) TestableVolume {
		return s.newTestableVolume(c, false, 2)
	})
}

func (s *StubbedRadosSuite) TestGenericReadOnly(c *check.C) {
	DoGenericVolumeTests(c, func(t TB) TestableVolume {
		return s.newTestableVolume(c, true, 2)
	})
}

func (s *StubbedRadosSuite) TestIndex(c *check.C) {
	v := s.newTestableVolume(c, false, 2)
	for i := 0; i < 256; i++ {
		v.PutRaw(fmt.Sprintf("%02x%030x", i, i), []byte{102, 111, 111})
	}
	for _, spec := range []struct {
		prefix      string
		expectMatch int
	}{
		{"", 256},
		{"c", 16},
		{"bc", 1},
		{"abc", 0},
	} {
		buf := new(bytes.Buffer)
		err := v.IndexTo(spec.prefix, buf)
		c.Check(err, check.IsNil)

		idx := bytes.SplitAfter(buf.Bytes(), []byte{10})
		c.Check(len(idx), check.Equals, spec.expectMatch+1)
		c.Check(len(idx[len(idx)-1]), check.Equals, 0)
	}
}

func (s *StubbedRadosSuite) TestStats(c *check.C) {
	v := s.newTestableVolume(c, false, 2)
	stats := func() string {
		buf, err := json.Marshal(v.InternalStats())
		c.Check(err, check.IsNil)
		return string(buf)
	}

	c.Check(stats(), check.Matches, `.*"Ops":0,.*`)

	loc := "acbd18db4cc2f85cedef654fccc4a4d8"
	_, err := v.Get(context.Background(), loc, make([]byte, 3))
	c.Check(err, check.NotNil)
	c.Check(stats(), check.Matches, `.*"Ops":[^0],.*`)
	c.Check(stats(), check.Matches, `.*"\*s3.Error 404 [^"]*":[^0].*`)
	c.Check(stats(), check.Matches, `.*"InBytes":0,.*`)

	err = v.Put(context.Background(), loc, []byte("foo"))
	c.Check(err, check.IsNil)
	c.Check(stats(), check.Matches, `.*"OutBytes":3,.*`)
	c.Check(stats(), check.Matches, `.*"PutOps":2,.*`)

	_, err = v.Get(context.Background(), loc, make([]byte, 3))
	c.Check(err, check.IsNil)
	_, err = v.Get(context.Background(), loc, make([]byte, 3))
	c.Check(err, check.IsNil)
	c.Check(stats(), check.Matches, `.*"InBytes":6,.*`)
}

func (s *StubbedRadosSuite) TestGetContextCancel(c *check.C) {
	loc := "acbd18db4cc2f85cedef654fccc4a4d8"
	buf := make([]byte, 3)

	s.testContextCancel(c, func(ctx context.Context, v *TestableRadosVolume) error {
		_, err := v.Get(ctx, loc, buf)
		return err
	})
}

func (s *StubbedRadosSuite) TestCompareContextCancel(c *check.C) {
	loc := "acbd18db4cc2f85cedef654fccc4a4d8"
	buf := []byte("bar")

	s.testContextCancel(c, func(ctx context.Context, v *TestableRadosVolume) error {
		return v.Compare(ctx, loc, buf)
	})
}

func (s *StubbedRadosSuite) TestPutContextCancel(c *check.C) {
	loc := "acbd18db4cc2f85cedef654fccc4a4d8"
	buf := []byte("foo")

	s.testContextCancel(c, func(ctx context.Context, v *TestableRadosVolume) error {
		return v.Put(ctx, loc, buf)
	})
}

func (s *StubbedRadosSuite) testContextCancel(c *check.C, testFunc func(context.Context, *TestableRadosVolume) error) {
	v := s.newTestableVolume(c, false, 2)
	v.Start()

	ctx, cancel := context.WithCancel(context.Background())

	v.radosMock.actions = make(chan string)
	v.radosMock.unblock = make(chan struct{})
	defer close(radosMock.unblock)

	doneFunc := make(chan struct{})
	go func() {
		err := testFunc(ctx, v)
		c.Check(err, check.Equals, context.Canceled)
		close(doneFunc)
	}()

	timeout := time.After(10 * time.Second)

	// Wait for radosMock to receive an action
	select {
	case <-timeout:
		c.Fatal("timed out waiting for test func to call radosMock")
	case <-doneFunc:
		c.Fatal("test func finished without even calling radosMock!")
	case <-v.radosMock.actions:
	}

	cancel()

	select {
	case <-timeout:
		c.Fatal("timed out")
	case <-doneFunc:
	}
}

type TestableRadosVolume struct {
	*RadosVolume
	c           *check.C
	serverClock *fakeClock
}

func (s *StubbedRadosSuite) newTestableVolume(c *check.C, readonly bool, replication int) *TestableRadosVolume {
	clock := &fakeClock{}
	radosMock := &radosMockImpl{
		clock: clock
	}

	v := &TestableRadosVolume{
		RadosVolume: &RadosVolume{
			Pool:             TestPool,
			MonHost:          TestMonHost,
			rados: radosMock,
		},
		c:           c,
		serverClock: clock,
	}
	v.Start()
	err = v.bucket.PutBucket(s3.ACL("private"))
	c.Assert(err, check.IsNil)
	return v
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

func (v *TestableRadosVolume) Start() error {
	tmp, err := ioutil.TempFile("", "keepstore")
	v.c.Assert(err, check.IsNil)
	defer os.Remove(tmp.Name())
	_, err = tmp.Write([]byte("xxx\n"))
	v.c.Assert(err, check.IsNil)
	v.c.Assert(tmp.Close(), check.IsNil)

	v.RadosVolume.AccessKeyFile = tmp.Name()
	v.RadosVolume.SecretKeyFile = tmp.Name()

	v.c.Assert(v.RadosVolume.Start(), check.IsNil)
	return nil
}

func (v *TestableRadosVolume) PutRaw(loc string, block []byte) {
	err := v.Put(context.Background, loc, block)
	if err != nil {
		log.Printf("PutRaw: %s: %+v", loc, err)
	}
}

// TouchWithDate turns back the clock while doing a Touch().
func (v *TestableRadosVolume) TouchWithDate(locator string, lastPut time.Time) {
	v.serverClock.now = &lastPut
	err := v.Touch(locator)
	if err != nil {
		panic(err)
	}
	v.serverClock.now = nil
}

func (v *TestableRadosVolume) Teardown() {
}


type radosMockImpl struct {
	clock *fakeClock
	actions chan string
	unblock chan struct{}
}

func (rados *radosMockImpl) Version() (major int, minor int, patch int) {
	major = -1
	minor = -1
	patch = -1
	return
}

func (rados *radosMockImpl) NewConnWithClusterAndUser(clusterName string, userName string) (conn *radosConn, err error) {
	conn = &radosMockConn{
		rados,
		cluster: clusterName,
		user: userName,
	}
	return
}

type radosMockConn struct {
	*radosMockImpl
	cluster string
	user string
	config map[string]string
}

func (conn *radosMockConn)	SetConfigOption(option, value string) (err error) {
	c.config[option] = value
	return
}

func (conn *radosMockConn)	Connect() (err error) {
	return
}

func (conn *radosMockConn)	GetFSID() (fsid string, err error) {
	return
}

func (conn *radosMockConn)	GetClusterStats() (stat rados.ClusterStat, err error) {
	return
}

func (conn *radosMockConn)	ListPools() (names []string, err error) {
	return
}

func (conn *radosMockConn)	OpenIOContext(pool string) (ioctx *radosIOContext, err error) {
	ioctx = &radosMockIoctx{
		conn,
	}
	return
}

type radosMockIoctx struct {
	*radosMockConn
}

func (ioctx *radosMockIoctx)	Delete(oid string) (err error) {
	return
}

func (ioctx *radosMockIoctx)	GetPoolStats() (stat rados.PoolStat, err error) {
	return
}

func (ioctx *radosMockIoctx)	GetXattr(object string, name string, data []byte) (n int, err error) {
	return
}

func (ioctx *radosMockIoctx)	Iter() (iter *radosIter, err error) {
	iter = &radosMockIter{
		ioctx,
	}
	return
}

func (ioctx *radosMockIoctx)	LockExclusive(oid, name, cookie, desc string, duration time.Duration, flags *byte) (res int, err error) {
	return
}

func (ioctx *radosMockIoctx)	LockShared(oid, name, cookie, tag, desc string, duration time.Duration, flags *byte) (res int, err error) {
	return
}

func (ioctx *radosMockIoctx)	Read(oid string, data []byte, offset uint64) (n int, err error) {
	return
}

func (ioctx *radosMockIoctx)	SetNamespace(namespace string) {
	return
}

func (ioctx *radosMockIoctx)	SetXattr(object string, name string, data []byte) (err error) {
	return
}

func (ioctx *radosMockIoctx)	Stat(object string) (stat ObjectStat, err error) {
	return
}

func (ioctx *radosMockIoctx)	Unlock(oid, name, cookie string) (res int, err error) {
	return
}

func (ioctx *radosMockIoctx)	WriteFull(oid string, data []byte) (err error) {
	return
}

type radosMockIter struct {
	*radosMockIoctx
}

func (iter *radosMockIter)	Next() bool {
	return
}

func (iter *radosMockIter)	Value() string {
	return
}

func (iter *radosMockIter)	Close() {
	return
}


/*
说明：etcd api接口的封装
创建人 jettchen
创建时间 2023/06/15
*/
package etcd_op

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go_app/pkg/log"
	"go_app/pkg/utils"

	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdlog "go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	ResetCd int64 = 60
)

// DBConf 定义了数据库配置
type DBConf struct {
	Url           string
	ConnTimeoutMs int
	OpWaitMs      int
	Username      string
	Password      string
	CertFile      string
	KeyFile       string
	CaFile        string
	LogPath       string
}

// DB etcd 数据库
type DB struct {
	c             *clientv3.Client
	s             *concurrency.Session
	parentCtx     context.Context
	maxWaitMs     time.Duration
	lastResetTime int64
}

// DBError 数据库操作失败的信息
type DBError struct {
	Reason int
	Desc   string
}

// Error 序列化返回DBError
func (err DBError) Error() string {
	return fmt.Sprintf("DBErr:%d(%s)", err.Reason, err.Desc)
}

const (
	ErrWrongVersion int = 1
	ErrNotFound         = 2

	EvtTypeDel = clientv3.EventTypeDelete
	EvtTypeAdd = clientv3.EventTypePut
)

type WatchChan = clientv3.WatchChan
type KeepAliveChan = <-chan *clientv3.LeaseKeepAliveResponse
type DbKV = mvccpb.KeyValue
type WatchResponse = clientv3.WatchResponse

// NewDB 新建数据库对象
func NewDB(conf *DBConf, ctx context.Context) (*DB, error) {
	tls, err := utils.LoadClientTLS(conf.CertFile, conf.KeyFile, conf.CaFile)
	if err != nil && err != utils.ErrNonTLSConfig {
		return nil, err
	}

	tmOut := time.Duration(conf.ConnTimeoutMs) * time.Millisecond
	eps := strings.Split(conf.Url, "|")
	cfg := clientv3.Config{
		Endpoints:   eps,
		DialTimeout: tmOut,
		Username:    conf.Username,
		Password:    conf.Password,
		TLS:         tls,
	}

	if conf.LogPath != "" {
		etcdlog.DefaultZapLoggerConfig.OutputPaths = []string{conf.LogPath}
		etcdlog.DefaultZapLoggerConfig.ErrorOutputPaths = []string{conf.LogPath}
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	s, err := concurrency.NewSession(c)
	if err != nil {
		c.Close()
		return nil, err
	}

	// https://github.com/etcd-io/etcd/issues/9877
	timeoutCtx, cancel := context.WithTimeout(context.Background(), tmOut)
	defer cancel()

	_, err = c.Status(timeoutCtx, cfg.Endpoints[0])
	if err != nil {
		c.Close()
		s.Close()
		return nil, err
	}

	maxWaitMs := 1000 * time.Millisecond
	if conf.OpWaitMs > 0 {
		maxWaitMs = time.Duration(conf.OpWaitMs) * time.Millisecond
	}

	db := &DB{
		c:         c,
		s:         s,
		parentCtx: ctx,
		maxWaitMs: maxWaitMs,
	}

	return db, nil
}

func OpenDB(url, user, passwd string, timeout int) *DB {
	dbConf := &DBConf{
		Url:           url,
		Username:      user,
		Password:      passwd,
		ConnTimeoutMs: timeout,
	}
	db, err := NewDB(dbConf, context.Background())
	if err != nil {
		log.Error("open db failed, err=%s", err.Error())
		return nil
	}

	return db
}

// Close 关闭数据库连接
func (db *DB) Close() {
	s := db.s
	c := db.c
	db.s = nil
	db.c = nil
	if s != nil {
		s.Close()
	}
	if c != nil {
		c.Close()
	}
}

// Put 存放kv对
func (db *DB) Put(key string, val string) error {
	_, err := db.c.Put(db.getRWContext(), key, val)
	if err != nil {
		return err
	}

	log.Debug("put ok:%s,(%s)", key, val)
	return nil
}

// PutWithVersion 存放kv对，并获取当前版本
func (db *DB) PutWithVersion(key string, val string) (dbVersion int64, err error) {
	res, err := db.c.Put(db.getRWContext(), key, val, clientv3.WithPrevKV())
	if err != nil {
		return 0, err
	}

	log.Debug("put with version ok:%s,(%s)", key, val)
	if res.PrevKv != nil {
		return res.PrevKv.Version + 1, nil
	}

	return 1, nil
}

// PutWithLease 带租约号存放kv对
func (db *DB) PutWithLease(key string, val string, leaseId int64) error {
	_, err := db.c.Put(db.getRWContext(), key, val, clientv3.WithLease(clientv3.LeaseID(leaseId)))
	if err == nil {
		log.Debug("put_with_lease ok:%s,%d,(%s)", key, leaseId, val)
		return nil
	}

	return err
}

// Get 查询val
func (db *DB) Get(key string) (string, error) {
	val, _, err := db.GetWithVersion(key)
	return val, err
}

// GetWithVersion 查询val，并获取版本
func (db *DB) GetWithVersion(key string) (string, int64, error) {
	res, err := db.c.Get(db.getRWContext(), key)
	if err != nil {
		return "", 0, err
	}

	if len(res.Kvs) == 0 {
		return "", 0, DBError{Reason: ErrNotFound, Desc: key}
	}

	val := string(res.Kvs[0].Value)
	ver := res.Kvs[0].Version
	log.Debug("get with version ok:%s,%d,(%s)", key, ver, val)
	return val, ver, nil
}

// GetKeyVersion 获取键值的版本
func (db *DB) GetKeyVersion(key string) (int64, error) {
	res, err := db.c.Get(db.getRWContext(), key, clientv3.WithKeysOnly())
	if err != nil {
		return 0, err
	}

	if res.Count == 0 {
		return 0, DBError{Reason: ErrNotFound, Desc: key}
	}

	return res.Kvs[0].Version, nil
}

// Del 删除kv对
func (db *DB) Del(key string) error {
	_, err := db.c.Delete(db.getRWContext(), key)
	if err == nil {
		log.Debug("del ok:%s", key)
		return nil
	}

	return err
}

// DelExclVersion 当满足指定版本时，删除键值
func (db *DB) DelExclVersion(key string, expectVer int64) error {
	ver, err := db.GetKeyVersion(key)
	if err != nil {
		if dbErr, ok := err.(DBError); ok && dbErr.Reason == ErrNotFound {
			return nil
		}

		return err
	}

	if ver != expectVer {
		return DBError{Reason: ErrWrongVersion,
			Desc: fmt.Sprintf("version mismatch:%s,expect=%d", key, expectVer)}
	}

	compare := clientv3.Compare(clientv3.Version(key), "=", expectVer)
	op := clientv3.OpDelete(key)
	res, err := db.c.Txn(db.getRWContext()).If(compare).Then(op).Commit()
	if err != nil {
		return err
	}

	if res.Succeeded {
		log.Debug("del_excl ok:%s,ver=%d", key, expectVer)
		return nil
	}

	return DBError{Reason: ErrWrongVersion,
		Desc: fmt.Sprintf("version mismatch:%s,expect=%d", key, expectVer)}
}

// PutExcl 当键值版本号为0时存入
func (db *DB) PutExcl(key string, val string) error {
	return db.doPutExcl(key, val, 0, 0)
}

// PutExclWithLease 当键值版本号为0时存入
func (db *DB) PutExclWithLease(key string, val string, leaseId int64) error {
	return db.doPutExcl(key, val, 0, leaseId)
}

// PutExclVersion 当键值版本号为curVersion时存入
func (db *DB) PutExclVersion(key string, val string, curVersion int64) error {
	return db.doPutExcl(key, val, curVersion, 0)
}

func (db *DB) doPutExcl(key string, val string, curVersion int64, leaseId int64) error {
	kv := clientv3.NewKV(db.c)
	compare := clientv3.Compare(clientv3.Version(key), "=", curVersion)

	var opPut clientv3.Op
	if leaseId != 0 {
		opPut = clientv3.OpPut(key, val, clientv3.WithLease(clientv3.LeaseID(leaseId)))
	} else {
		opPut = clientv3.OpPut(key, val)
	}

	res, err := kv.Txn(db.getRWContext()).If(compare).Then(opPut).Commit()
	if err != nil {
		return err
	}

	if res.Succeeded {
		log.Debug("put_excl ok:%s,v=%d,l=%d,v=(%s)", key, curVersion, leaseId, val)
		return nil
	}

	log.Error("put_excl key %s value %s failed with wrong version %d", key, val, curVersion)
	return DBError{Reason: ErrWrongVersion, Desc: key}
}

// PutExclAndGet key不存在时，存放kv对，并返回当前val
func (db *DB) PutExclAndGet(key string, val string) (string, error) {
	val, _, err := db.PutExclAndGetWithLease(key, val, 0)
	return val, err
}

// PutExclAndGetWithLease key不存在时存入，返回当前val，租约，err
func (db *DB) PutExclAndGetWithLease(key string, val string, leaseId int64) (string, int64, error) {
	kv := clientv3.NewKV(db.c)
	compare := clientv3util.KeyMissing(key)
	var opPut clientv3.Op
	if leaseId != 0 {
		opPut = clientv3.OpPut(key, val, clientv3.WithLease(clientv3.LeaseID(leaseId)))
	} else {
		opPut = clientv3.OpPut(key, val)
	}

	opGet := clientv3.OpGet(key)
	res, err := kv.Txn(db.getRWContext()).If(compare).Then(opPut).Else(opGet).Commit()
	if err != nil {
		return "", 0, err
	}

	if res.Succeeded {
		log.Debug("put_excl_get ok1:%s,%d,(%s)", key, leaseId, val)
		return val, leaseId, nil
	}

	getRes := res.Responses[0].GetResponseRange()
	curKv := getRes.Kvs[0]

	curVal := string(curKv.Value)
	log.Debug("put_excl_get ok2:%s,(%s)", key, curVal)

	return curVal, curKv.Lease, nil
}

// GrantLease ttl 表示租约有效期
func (db *DB) GrantLease(ttl int64) (int64, error) {
	res, err := db.c.Grant(db.getRWContext(), ttl)
	if err != nil {
		return 0, err
	}

	log.Debug("grant lease ok:%d", res.ID)
	return int64(res.ID), nil
}

// RevokeLease 撤销租约
func (db *DB) RevokeLease(leaseId int64) error {
	_, err := db.c.Revoke(db.getRWContext(), clientv3.LeaseID(leaseId))
	if err == nil {
		log.Debug("revoke lease ok:%d", leaseId)
		return nil
	}

	return err
}

// KeepAliveOnce 更新租约一次
func (db *DB) KeepAliveOnce(leaseId int64) error {
	_, err := db.c.KeepAliveOnce(db.parentCtx, clientv3.LeaseID(leaseId))
	return err
}

// KeepAlive 会试图使租约永远有效
func (db *DB) KeepAlive(leaseId int64) (KeepAliveChan, error) {
	return db.c.KeepAlive(db.parentCtx, clientv3.LeaseID(leaseId))
}

// GetLeaseTTL 获取租约有效期
func (db *DB) GetLeaseTTL(leaseId int64) (int64, error) {
	res, err := db.c.TimeToLive(db.getRWContext(), clientv3.LeaseID(leaseId))
	if err != nil {
		return 0, err
	}

	return res.TTL, nil
}

// WatchKey 监听key变化
func (db *DB) WatchKey(key string) WatchChan {
	return db.c.Watch(db.parentCtx, key)
}

// WatchPrefix 监听以prefix为前缀的所有key
func (db *DB) WatchPrefix(prefix string) WatchChan {
	return db.c.Watch(db.parentCtx, prefix, clientv3.WithPrefix())
}

func (db *DB) getRWContext() context.Context {
	ctx, _ := context.WithTimeout(db.parentCtx, db.maxWaitMs)
	return ctx
}

// GetPrefixKVs 获取键值以prefix为前缀的所有kv对
func (db *DB) GetPrefixKVs(prefix string) ([]*DbKV, error) {
	res, err := db.c.Get(db.getRWContext(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	return res.Kvs, nil
}

// GetPrefixKeys 获取所有以prefix为前缀的键值
func (db *DB) GetPrefixKeys(prefix string) ([]string, error) {
	res, err := db.c.Get(db.getRWContext(), prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(res.Kvs))
	for i, item := range res.Kvs {
		keys[i] = string(item.Key)
	}

	return keys, nil
}

// GetPrefixCount 获取以prefix为前缀的键值总数
func (db *DB) GetPrefixCount(prefix string) (int, error) {
	res, err := db.c.Get(db.getRWContext(), prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return 0, err
	}

	return int(res.Count), nil
}

// DelPrefix 删除key以prefix为前缀的所有kv对
func (db *DB) DelPrefix(prefix string) error {
	_, err := db.c.Delete(db.getRWContext(), prefix, clientv3.WithPrefix())
	if err == nil {
		log.Debug("del_prefix ok:%s", prefix)
		return nil
	}

	return err
}

// 删除老会话，新建1个会话。一般是因为老会话已经过期了
func (db *DB) ResetSession() int {
	now := time.Now().UTC().Unix()
	if db.lastResetTime+ResetCd > now {
		log.Error("reset is too often, lastResetTime=%s", db.lastResetTime)
		return -1
	}
	c := db.c
	new_s, err := concurrency.NewSession(c)
	if err != nil {
		log.Error("reset session err=%v", err)
		return -2
	}
	old_s := db.s
	db.s = new_s
	if old_s != nil {
		err = old_s.Close()
		if err != nil {
			log.Error("old session close err=%v", err)
		}
	}
	return 0
}

// 获取分布式锁，包含一次重试机会
func (db *DB) Lock(prefix string) (*concurrency.Mutex, error) {
	m, err := db.LockOnce(prefix, false)
	if err != nil {
		// 网络或者机器卡顿一段时间，可能是因为之前的session已经被过期删除了。这里尝试重新申请一个session
		errcode := db.ResetSession()
		if errcode != 0 {
			return m, err
		}
		m, err = db.LockOnce(prefix, true)
	}
	return m, err
}

// 获取分布式锁，不包含重试
func (db *DB) LockOnce(prefix string, isRetry bool) (*concurrency.Mutex, error) {
	m := concurrency.NewMutex(db.s, prefix)
	// acquire lock
	if err := m.Lock(context.TODO()); err != nil {
		log.Error("lock once fail, isRetry=%t, err=%v", isRetry, err)
		return m, err
	}
	return m, nil
}

// 竞选leader身份，包含一次重试机会
func (db *DB) BecomeLeader(prefix, value string) error {
	err := db.BecomeLeaderOnce(prefix, value, false)
	if err != nil {
		// 网络或者机器卡顿一段时间，可能是因为之前的session已经被过期删除了。这里尝试重新申请一个session
		errcode := db.ResetSession()
		if errcode != 0 {
			return err
		}
		err = db.BecomeLeaderOnce(prefix, value, true)
	}
	return err
}

// 竞选leader身份，不包含重试
func (db *DB) BecomeLeaderOnce(prefix, value string, isRetry bool) error {
	e := concurrency.NewElection(db.s, prefix)
	if err := e.Campaign(context.TODO(), value); err != nil {
		log.Error("become leader once fail, isRetry=%t, err=%v", isRetry, err)
		return err
	}
	return nil
}

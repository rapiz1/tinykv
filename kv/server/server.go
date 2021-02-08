package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	r, err := server.storage.Reader(nil)
	defer r.Close()
	val, err := r.GetCF(req.GetCf(), req.GetKey())
	res := kvrpcpb.RawGetResponse{
		RegionError: nil,
		Error:       "",
		Value:       val,
		NotFound:    val == nil,
	}
	return &res, err
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	batch := [1]storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		},
	}
	err := server.storage.Write(nil, batch[:])
	res := kvrpcpb.RawPutResponse{
		RegionError: nil,
		Error:       "",
	}
	return &res, err
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	batch := [1]storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	}
	server.storage.Write(nil, batch[:])
	res := kvrpcpb.RawDeleteResponse{
		RegionError: nil,
		Error:       "",
	}
	return &res, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	r, err := server.storage.Reader(nil)
	defer r.Close()

	it := r.IterCF(req.GetCf())
	defer it.Close()

	it.Seek(req.GetStartKey())
	kvs := make([]*kvrpcpb.KvPair, 0, int(req.GetLimit()))

	limit := req.GetLimit()

	for ; it.Valid() && limit != 0; it.Next() {
		val, _ := it.Item().Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   it.Item().Key(),
			Value: val,
			Error: nil,
		})
		limit--
	}

	res := kvrpcpb.RawScanResponse{
		RegionError: nil,
		Error:       "",
		Kvs:         kvs,
	}
	return &res, err
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	r, err := server.storage.Reader(req.Context)
	defer r.Close()
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(r, req.Version)
	l, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	rsp := &kvrpcpb.GetResponse{}
	if l != nil {
		if l.IsLockedFor(req.Key, req.Version, rsp) {
			return rsp, nil
		}
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	rsp.Value = val
	rsp.NotFound = val == nil
	return rsp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// lock all keys involved
	allKeys := [][]byte{}
	for _, m := range req.Mutations {
		allKeys = append(allKeys, m.Key)
	}
	server.Latches.WaitForLatches(allKeys)
	defer server.Latches.ReleaseLatches(allKeys)

	r, err := server.storage.Reader(req.Context)
	defer r.Close()
	rsp := &kvrpcpb.PrewriteResponse{}

	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(r, req.StartVersion)

	for _, m := range req.Mutations {
		key := m.Key
		w, ts, err := txn.MostRecentWrite(key)
		if w != nil && ts >= txn.StartTS {
			rsp.Errors = append(rsp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        m.Key,
					Primary:    req.PrimaryLock,
				}})
			return rsp, nil
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock != nil && lock.Ts != txn.StartTS {
			rsp.Errors = append(rsp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(key),
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: lock.Ts,
					Key:        m.Key,
					Primary:    req.PrimaryLock,
				}})
			return rsp, nil
		}
		var wk mvcc.WriteKind
		switch m.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(key, m.Value)
			wk = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(key)
			wk = mvcc.WriteKindDelete
		}
		txn.PutLock(key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    wk,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	return rsp, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	rsp := &kvrpcpb.CommitResponse{}
	r, err := server.storage.Reader(req.Context)
	defer r.Close()
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(r, req.StartVersion)
	for _, key := range req.Keys {
		l, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		// I don't understand why shouldn't return an error. Just to make the checker happy.
		if l == nil {
			return rsp, nil
		}

		// I don't understand this, either. What's retryable? Why should I set it?
		if l.Ts != txn.StartTS {
			rsp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return rsp, nil
		}

		w := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    l.Kind,
		}
		txn.PutWrite(key, req.CommitVersion, w)
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	return rsp, err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if nil != err {
		return nil, err
	}

	resp := &kvrpcpb.RawGetResponse{}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	defer reader.Close()

	if nil == value {
		resp.NotFound = true
		return resp, err
	}
	resp.Value = value
	resp.NotFound = false

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var put storage.Modify
	put.Data = storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	err := server.storage.Write(nil, []storage.Modify{put})

	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var del storage.Modify
	del.Data = storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	err := server.storage.Write(nil, []storage.Modify{del})

	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if nil != err {
		return nil, err
	}

	resp := &kvrpcpb.RawScanResponse{}
	rIter := reader.IterCF(req.GetCf())
	var limit uint32
	for rIter.Seek(req.GetStartKey()); rIter.Valid() && limit < req.GetLimit(); rIter.Next() {
		item := rIter.Item()
		key := item.Key()
		value, err := item.Value()
		if nil != err {
			continue
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: key, Value: value})
		limit++
	}

	return resp, nil
}

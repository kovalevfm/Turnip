#include "ldb.h"
#include <leveldb/write_batch.h>

void converStatus(leveldb::Status& ldb_status, Status* status){
    if (ldb_status.ok()){
        status->status.code = Status::OK;
        return;
    }
    if (ldb_status.IsNotFound()){
        status->status.code = Status::NotFound;
        return;
    }
//    if (ldb_status.IsCorruption()){
//        status->status.code = Status::Corruption;
//        status->reason = ldb_status.ToString();
//        return;
//    }
//    if (ldb_status.IsIOError()){
//       status->status.code = Status::IOError;
//       status->reason = ldb_status.ToString();
//        return;
//   }
}

LDB::LDB(Options& options_) : options(options_)
// cache(options.ldb_options.block_cache)/*,  filter_policy(options.ldb_options.filter_policy)*/
{
    leveldb::Status status = leveldb::DB::Open(options.ldb_options, options.db_path.c_str(), &db);
}

LDB::~LDB(){
    delete db;
    if (options.ldb_options.block_cache){
        delete options.ldb_options.block_cache;
    }
    if (options.ldb_options.info_log){
        delete options.ldb_options.info_log;
    }

//    if (filter_policy){
//        delete filter_policy;
//    }
//    if (cache){
//        delete cache;
//    }
}

void LDB::Get(const GetRequest& request, GetResponse* response){
    leveldb::ReadOptions read_options;
    read_options.fill_cache = request.fill_cache;
    read_options.verify_checksums = request.verify_checksums;
    leveldb::Status status = db->Get(read_options, request.key, &(response->value));
    converStatus(status, &(response->status));
}

void LDB::Write(const WriteRequest& request, WriteResponse* response){
    leveldb::WriteOptions write_options;
    write_options.sync = request.sync;
    leveldb::WriteBatch batch;
    for (auto i = request.operations.begin(); i != request.operations.end() ; ++i ){
        if (i->do_delete){
            batch.Delete(i->key);
        } else {
            batch.Put(i->key, i->value);
        }
    }
    leveldb::Status status = db->Write(write_options, &batch);
    converStatus(status, &(response->status));
}

leveldb::Logger* LDB::getLogger(){
    return options.ldb_options.info_log;
}

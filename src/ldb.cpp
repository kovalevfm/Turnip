#include "ldb.h"
#include <leveldb/write_batch.h>


LDB::LDB(Options& options_)
{
    leveldb::Status status = leveldb::DB::Open(options_.get_ldb_options(), options_.get_db_path(), &db);
}

LDB::~LDB(){
    delete db;
}

void LDB::Get(Transport* t){
    Message m = t->recv_next();
    if (m.zone.get() == NULL) {throw format_error("get empty message");}
    ReadOptions ro = m.messsage.as<ReadOptions>();
    m = t->recv_next();
    if (m.zone.get() == NULL) {throw format_error("get empty key");}
    std::string result;
    Status status = db->Get(ro.get_leveldb_options(), m.messsage.as<std::string>(), &result);
    m = t->recv_next();
    if (m.zone.get() != NULL) {throw format_error("get too long request");}
    t->send_next(status);
    if (status.code == (int)StatusCode::OK){
        t->send_next(result);
    }
}

void LDB::Write(Transport* t){
    leveldb::WriteBatch batch;
    Message m = t->recv_next();
    if (m.zone.get() == NULL) {throw format_error("write empty message");}
    WriteOptions wo = m.messsage.as<WriteOptions>();
    WriteOperation o;
    while(true){
        m = t->recv_next();
        if (m.zone.get() == NULL){
            break;
        }
        m.messsage.convert(&o);
        if (o.do_delete){
            batch.Delete(o.key);
        } else{
            batch.Put(o.key, o.value);
        }
    }
    Status status = db->Write(wo.get_leveldb_options(), &batch);
    t->send_next(status);
}



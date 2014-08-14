#include "ldb.h"
#include <leveldb/write_batch.h>
#include <leveldb/iterator.h>
#include <leveldb/comparator.h>


LDB::LDB(Options& options_) : logger(options_.get_logger())
{
    leveldb::Status status = leveldb::DB::Open(options_.get_ldb_options(), options_.get_db_path(), &db);
}

LDB::~LDB(){
    delete db;
}

void LDB::Get(Transport* t){
    Message m;
    if (!t->recv_next(&m)) {throw format_error("get empty message");}
    ReadOptions ro = m.messsage.as<ReadOptions>();
    if (!t->recv_next(&m)) {throw format_error("get empty key");}
    std::string result;
    Status status = db->Get(ro.get_leveldb_options(), m.messsage.as<std::string>(), &result);
    if (t->recv_next(&m)) {throw format_error("get too long request");}
    t->send_next(status);
    if (status.code == (int)StatusCode::OK){
        t->send_next(result);
    }
}

void LDB::Write(Transport* t){
    leveldb::WriteBatch batch;
    Message m;
    if (!t->recv_next(&m)) {throw format_error("get empty message");}
    WriteOptions wo = m.messsage.as<WriteOptions>();
    WriteOperation o;
    while(t->recv_next(&m)){
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

void LDB::Range(Transport* t){
    Message m;
    if (!t->recv_next(&m)) {throw format_error("get empty message");}
    ReadOptions ro = m.messsage.as<ReadOptions>();
    if (!t->recv_next(&m)) {throw format_error("range empty key 1");}
    std::string begin_key = m.messsage.as<std::string>();
    if (!t->recv_next(&m)) {throw format_error("range empty key 2");}
    std::string end_key = m.messsage.as<std::string>();
    if (t->recv_next(&m)) {throw format_error("range too long request");}
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(ro.get_leveldb_options()));
    it->Seek(begin_key);
    const leveldb::Comparator* cmp = leveldb::BytewiseComparator();
    while (it->Valid() && (end_key.size() == 0 || cmp->Compare(it->key(), leveldb::Slice(end_key)) <= 0) ){
        leveldb::Log(logger, "send %s %s", std::string(it->key().data(), it->key().size()).c_str(), std::string(it->value().data(), it->value().size()).c_str());
        t->send_next(RangeValue(it.get()));
        it->Next();
    }
}

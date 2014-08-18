#include "ldb.h"
#include <leveldb/write_batch.h>
#include <leveldb/iterator.h>
#include <leveldb/comparator.h>

#include <sstream>
#include <iostream>


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
    t->send_part(status);
    if (status.code == (int)StatusCode::OK){
        t->send_part(result);
    }
    t->commit_message();
}

void LDB::Write(Transport* t){
    leveldb::WriteBatch batch;
    Message m;
    if (!t->recv_next(&m)) {throw format_error("get empty message");}
    WriteOptions wo = m.messsage.as<WriteOptions>();
    WriteOperation o;
    while(t->recv_next(&m)){
//        std::ostringstream oss;
//        oss << m.messsage;
//        leveldb::Log(logger, "write to batch %s", oss.str().c_str());
        m.messsage.convert(&o);
        if (o.do_delete){
            batch.Delete(o.key);
        } else{
            batch.Put(o.key, o.value);
        }
    }
//    leveldb::Log(logger, "done");
    Status status = db->Write(wo.get_leveldb_options(), &batch);
    t->send_message(status);
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
//        std::ostringstream oss;
//        oss << it->key().ToString() << " " << it->value().ToString();
//        leveldb::Log(logger, "write to batch %s", oss.str().c_str());
        t->send_message(RangeValue(it.get()));
        it->Next();
    }
    t->send_message((int)Command::END);
    leveldb::Log(logger, "send end");
}

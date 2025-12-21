#include "KVStore.hpp"
#include <mutex>

void KVStore::set(const std::string& key, const std::string& value) {
    // TODO: acquire unique writer lock on mtx_
    // TODO: insert or update a key-value pair in data_
}

std::string KVStore::get(const std::string& key) {
    // TODO: acquire shared reader lock on mtx_
    // TODO: check if key exists. Return value if found
    return "";
}

bool KVStore::remove(const std::string& key) {
    // TODO: Acquire a unique writer lock on mtx_
    // TODO: Erase key. Return true if found/deleted, false otherwise
    return false; // placeholder
}
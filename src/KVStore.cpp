#include "KVStore.hpp"
#include <mutex>
#include <shared_mutex>

/*
    Insert or update a key-value pair in the store.
    Args: 
        key: the key to insert or update
        value: the value to insert or update
    Returns:
        void
*/
void KVStore::set(const std::string& key, const std::string& value) {
    std::unique_lock<std::shared_timed_mutex> lock(mtx_);
    data_[key] = value;
}

/*
    Get the value for a key in the store if it exists.
    Args: 
        key: the key to get the value for
    Returns:
        the value for the key (empty string if the key is not found)
*/
std::string KVStore::get(const std::string& key) {
    std::shared_lock<std::shared_timed_mutex> lock( mtx_);
    auto it = data_.find(key);
    if (it != data_.end()) {
        return it->second;
    }
    return "";
}

/*
    Remove a key-value pair from the store if it exists.
    Args: 
        key: the key to remove
    Returns:
        true if the key was found and removed, false otherwise
*/
bool KVStore::remove(const std::string& key) {
    std::unique_lock<std::shared_timed_mutex> lock(mtx_);
    if (data_.find(key) != data_.end()) {
        data_.erase(key);
        return true;
    }
    return false;
}
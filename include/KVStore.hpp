#pragma once
#include <unordered_map>
#include <string>
#include <shared_mutex>

class KVStore {
private:
    std::unordered_map<std::string, std::string> data_;
    mutable std::shared_timed_mutex mtx_; 
public:
    void set(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    bool remove(const std::string& key);
};
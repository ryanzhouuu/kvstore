#include <iostream>
#include "KVStore.hpp"
#include "server.hpp"

int main() {
    KVStore store;

    Server server(store, 8080);

    try {
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Server error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
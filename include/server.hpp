#pragma once

#include <string>
#include <netinet/in.h>
#include <KVStore.hpp>

class Server {
public:
    // constructor - takes reference to store
    explicit Server(KVStore& store, int port = 8080);

    ~Server(); // destructor to clean up socket descriptors

    // prevent copying the server
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // main loop: accept connections and spawn threads
    void start();

private:
    KVStore& store_;
    int port_;
    int server_fd_; // file descriptor for the server socket

    // helper to handle single client connection
    void handle_client(int client_socket);
};
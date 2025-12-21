#include "server.hpp"
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <thread>
#include <stdexcept>

/*
    Constructor method for Server class.
    Args:
        store: reference to the KVStore instance
        port: port number to listen on
    Returns:
        void
*/
Server::Server(KVStore& store, int port) : store_(store), port_(port), server_fd_(-1) {
    // create socket
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0); // IPv4, TCP
    if (server_fd_ < 0) { // socket creation error
        throw std::runtime_error("Failed to create socket");
    }

    // set socket options
    int opt = 1;
    // SOL_SOCKET: socket level option
    // SO_REUSEADDR: allow reuse of local address
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(server_fd_);
        throw std::runtime_error("Failed to set socket options");
    }

    // bind socket to address
    struct sockaddr_in address; 
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // bind to all interfaces
    address.sin_port = htons(port_); // network byte order
    if (bind(server_fd_, (struct sockaddr*) &address, sizeof(address)) < 0) {
        close(server_fd_);
        throw std::runtime_error("Failed to bind socket");
    }

    // start listening
    if (listen(server_fd_, SOMAXCONN) < 0) { // SOMAXCONN: maximum pending connections
        close(server_fd_);
        throw std::runtime_error("Failed to listen on socket");
    }
}

/*
    Destructor method for Server class.
*/
Server::~Server() {
    if (server_fd_ != -1) {
        close(server_fd_);
        std::cout << "Server shutting down" << std::endl;
    }
}

/*
    Start the server to accept incoming connections. Runs a listening loop.
*/
void Server::start() {
    std::cout << "Server starting on port " << port_ << std::endl;

    struct sockaddr_in client_address; // stores client IP address and port
    socklen_t client_address_len = sizeof(client_address); // needed for accept()

    while (true) {
        // call accept() to get a client socket
        int client_fd = accept(server_fd_, (struct sockaddr*) &client_address, &client_address_len);
        if (client_fd < 0) { // accept() error
            std::cerr << "Failed to accept client connection" << std::endl;
            continue; // skip to next iteration
        }

        /*
            Spawn a new thread to handle this client
            This: current Server object
            Client_fd: client socket passed to handle_client()
            Detach: thread runs independently of the main thread
        */
        std::thread(&Server::handle_client, this, client_fd).detach();
    }
}

void Server::handle_client(int client_socket) {
    // TODO: create a buffer array
    // TODO: read data from socket in loop
    // TODO: parse command (SET, GET, DEL)
    // TODO: interact with store_
    // TODO: send response back to client using write() or send()

    close(client_socket);
}
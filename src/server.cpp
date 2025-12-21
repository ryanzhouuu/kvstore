#include "server.hpp"
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <thread>

/*
    Constructor method for Server class.
*/
Server::Server(KVStore& store, int port) : store_(store), port_(port), server_fd_(-1) {
    // TODO: initialize socket, set options, bind, and listen
    // use socket(), setsockopt(), bind(), and listen()
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

    while (true) {
        // TODO: call accept() to get a client socket
        // int client_fd = accept(...)

        // TODO: spawn a new thread to handle this client
        // std::thread(&Server::handle_client, this, client_fd).detach();
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
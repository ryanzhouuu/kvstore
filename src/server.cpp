#include "server.hpp"
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <thread>
#include <stdexcept>
#include <cstring>  // strcmp, strtok
#include <sstream>  // parsing strings
#include <vector>   // splitting commands
#include <arpa/inet.h> // htons
#include <netinet/in.h> // sockaddr_in

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
    Args:
        none
    Returns:
        void
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

/*
    Handle a single client connection.
    Args:
        client_socket: the socket file descriptor for the client
    Returns:
        void
*/
void Server::handle_client(int client_socket) {
    // create a buffer
    std::string buffer;
    char chunk[1024] = {0}; // 1024 bytes buffer for data, initialized to 0

    while (true) {
        memset(chunk, 0, sizeof(chunk)); // clear chunk

        // read data from socket
        ssize_t bytes_read = read(client_socket, chunk, sizeof(chunk) - 1);
        if (bytes_read <= 0) { // connection closed or error
            break; // exit loop
        }
        chunk[bytes_read] = '\0'; // null-terminate the string
        buffer += chunk; // append chunk to buffer

        size_t pos;
        while ((pos = buffer.find('\n')) != std::string::npos) {
            std::string line = buffer.substr(0, pos); // extract one line
            buffer.erase(0, pos + 1); // remove the line from the buffer

            if (line.empty()) {
                continue; // skip empty lines
            }

            // parse command (SET, GET, DEL)
            std::istringstream iss(line);
            std::string command;
            iss >> command; // extract command

            std::string response;

            // based on command, call the appropriate KVStore function
            if (command == "SET") { // handle SET command
                std::string key, value;
                iss >> key; // extract key
                std::getline(iss, value); // extract value from rest of line

                if (!value.empty() && value[0] == ' ') {
                    value.erase(0, 1); // remove leading spaces
                }

                if (!key.empty() && !value.empty()) {
                    store_.set(key, value);
                    response = "OK\n";
                } else { // invalid command
                    response = "ERROR: SET requires key and value\n";
                }
            } else if (command == "GET") { // handle GET command
                std::string key;
                iss >> key; // extract key

                if (!key.empty()) {
                    std::string value = store_.get(key);
                    if (!value.empty()) {
                        response = value + "\n";
                    } else {
                        response = "NOT_FOUND\n";
                    }
                } else {
                    response = "ERROR: GET requires key\n";
                }
            } else if (command == "DEL") {
                std::string key;
                iss >> key; // extract key

                if (!key.empty()) {
                    bool deleted = store_.remove(key);
                    response = deleted ? "DELETED\n" : "NOT_FOUND\n";
                } else {
                    response = "ERROR: DEL requires key\n";
                }
            } else {
                response = "ERROR: Unknown command\n";
            }

            // send response and handle errors
            int bytes_written = write(client_socket, response.c_str(), response.length());
            if (bytes_written < 0) { // connnection most likely broken
                close(client_socket);
                return;
            }
        }
    }
    
    close(client_socket);
}
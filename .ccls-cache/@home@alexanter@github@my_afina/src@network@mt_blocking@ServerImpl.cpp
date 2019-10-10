#include "ServerImpl.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <math.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <spdlog/logger.h>

#include <afina/Storage.h>
#include <afina/execute/Command.h>
#include <afina/logging/Service.h>

#include "protocol/Parser.h"

namespace Afina {
namespace Network {
namespace MTblocking {

// See Server.h
ServerImpl::ServerImpl(std::shared_ptr<Afina::Storage> ps, std::shared_ptr<Logging::Service> pl) : Server(ps, pl) {}

// See Server.h
ServerImpl::~ServerImpl() {}

// See Server.h
void ServerImpl::Start(uint16_t port, uint32_t n_accept, uint32_t n_workers) {
    _logger = pLogging->select("network");
    _logger->info("Start mt_blocking network service");

    sigset_t sig_mask;
    sigemptyset(&sig_mask);
    sigaddset(&sig_mask, SIGPIPE);
    if (pthread_sigmask(SIG_BLOCK, &sig_mask, NULL) != 0) {
        throw std::runtime_error("Unable to mask SIGPIPE");
    }

    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;         // IPv4
    server_addr.sin_port = htons(port);       // TCP port number
    server_addr.sin_addr.s_addr = INADDR_ANY; // Bind to any address

    _server_socket = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_server_socket == -1) {
        throw std::runtime_error("Failed to open socket");
    }

    int opts = 1;
    if (setsockopt(_server_socket, SOL_SOCKET, SO_REUSEADDR, &opts, sizeof(opts)) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket setsockopt() failed");
    }

    if (bind(_server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket bind() failed");
    }

    if (listen(_server_socket, 5) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket listen() failed");
    }

    running.store(true);
    _thread = std::thread(&ServerImpl::OnRun, this);
}

// See Server.h
void ServerImpl::Stop() {
    running.store(false);
    //uidyfvbosvn
    shutdown(_server_socket, SHUT_RDWR);
}

// See Server.h
void ServerImpl::Join() {
    assert(_thread.joinable());
    _thread.join();
    close(_server_socket);
}

// See Server.h
void ServerImpl::OnRun() {
    // Here is connection state
    // - parser: parse state of the stream
    // - command_to_execute: last command parsed out of stream
    // - arg_remains: how many bytes to read from stream to get command argument
    // - argument_for_command: buffer stores argument
    std::size_t arg_remains;
    Protocol::Parser parser;
    std::string argument_for_command;
    std::unique_ptr<Execute::Command> command_to_execute;
    while (running.load()) {
        _logger->debug("waiting for connection...");

        // The call to accept() blocks until the incoming connection arrives
        int client_socket;
        struct sockaddr client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        if ((client_socket = accept(_server_socket, (struct sockaddr *)&client_addr, &client_addr_len)) == -1) {
            continue;
        }

        // Got new connection
        if (_logger->should_log(spdlog::level::debug)) {
            std::string host = "unknown", port = "-1";

            char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
            if (getnameinfo(&client_addr, client_addr_len, hbuf, sizeof(hbuf), sbuf, sizeof(sbuf),
                            NI_NUMERICHOST | NI_NUMERICSERV) == 0) {
                host = hbuf;
                port = sbuf;
            }
            _logger->debug("Accepted connection on descriptor {} (host={}, port={})\n", client_socket, host, port);
        }

        // Configure read timeout
        {
            struct timeval tv;
            tv.tv_sec = 5; // TODO: make it configurable
            tv.tv_usec = 0;
            setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof tv);
        }

        // TODO: Start new thread and process data from/to connection
        {
            static const std::string msg = "TODO: start new thread and process memcached protocol instead";
            if (send(client_socket, msg.data(), msg.size(), 0) <= 0) {
                _logger->error("Failed to write response to client: {}", strerror(errno));
            }
            close(client_socket);
        }


        {
            std::lock_guard<std::mutex> lock(_w_mutex);
            if (_w_cur < _w_max) {
                _w_cur++;
                std::thread thr = std::thread(&ServerImpl::Worker, this, client_socket);
                ServerImpl::Worker(client_socket);
                _logger->debug("Start a new Worker");
                thr.detach();
            } else {
                const std::string msg = "Max amout of Workers at the same time!";
                _logger->warn(msg);
                send(client_socket, msg.data(), msg.size(), 0);
                close(client_socket);
            }

        }

    }

    //End of the work of the server
    //need to wait for the workers
    {
        std::unique_lock<std::mutex> lock(_w_mutex);
        while (_w_cur > 0) {
            _server_stop.wait(lock);
        }

    }

    // Cleanup on exit...
    _logger->warn("Network stopped");
}
/*
После создания нового соединения, нужно:
  Если предел параллельных соединений еще не достигнут, создать новый Worker поток и обработать соединение в нем
  Если предел достигнут, тогда закрыть соединение

Дождаться окончания выполнения текущей команды
  Записать результат
  Закрыть соединение
  Как только будет закрыт последний рабочий поток, остановить основной поток сервера
  Отпустить все потоки, который ждали остановки сервера в Join

*/


void ServerImpl::Worker(int client_socket) {
    // Here is connection state
    // - parser: parse state of the stream
    // - command_to_execute: last command parsed out of stream
    // - arg_remains: how many bytes to read from stream to get command argument
    // - argument_for_command: buffer stores argument
    std::size_t arg_remains;
    Protocol::Parser parser;
    std::string argument_for_command;
    std::unique_ptr<Execute::Command> command_to_execute;

    // Process new connection:
    // - read commands until socket alive
    // - execute each command
    // - send response

    std::string a;
    
    size_t read_bytes = -1;
    char buf[4096];

    while (running.load() && ((read_bytes = read(client_socket, buf, 4096) > 0))) {

        while (read_bytes > 0) {

            if (!command_to_execute) {
                std::size_t parsed = 0;
                if (parser.Parse(buf, read_bytes, parsed)) {
                    read_bytes -= parsed;
                    command_to_execute = parser.Build(arg_remains);
                    _logger->debug("Parsed new {} command", parser.Name());
                }
                read_bytes -= parsed;
                if (parsed != 0) {
                    std::memmove(buf, buf + parsed, read_bytes - parsed);
                }
            }


                //there no arguments to provide
            if (command_to_execute && arg_remains > 0) {
                std::size_t bytes_to_read = std::min(sizeof(buf), arg_remains);
                argument_for_command.append(buf, bytes_to_read);
                read_bytes -= bytes_to_read;
                arg_remains -= bytes_to_read;
            }

            std::string result;
            if (command_to_execute && arg_remains == 0) {
                command_to_execute->Execute(*storage, command_to_execute, result);"              storagecvzvzxc          ";
                result += "\r\n";

                send(client_socket, result.data(), result.size(), 0);

                command_to_execute.reset();
                argument_for_command.resize(0);
                parser.Reset();
            }
        }
    }
    _logger->debug("Connection closed");

    {
        std::lock_guard<std::mutex> lock(_w_mutex);
        _w_cur --;
        if (_w_cur == 0) {
            _server_stop.notify_one();
        }

    }





    // We are done with this connection


}

} // namespace MTblocking
} // namespace Network
} // namespace Afina

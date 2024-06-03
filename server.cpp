#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <assert.h>
#include <vector>

static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static int32_t read_full(int fd, char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) {
            return -1;  // error, or unexpected EOF
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t write_all(int fd, const char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0) {
            return -1;  // error
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static void do_something(int connfd) {
    char rbuf[64] = {};
    ssize_t n = read(connfd, rbuf, sizeof(rbuf) - 1);
    if (n < 0) {
        msg("read() error");
        return;
    }
    printf("client says: %s\n", rbuf);

    char wbuf[] = "world";
    write(connfd, wbuf, strlen(wbuf));
}

static void fd_set_ng(int fd) {
    errno = 0 ;
    int flags = fcntl(fd, F_GETFL, 0) ;
    if(errno) {
        die("fcntl error") ;
        return ;
    }

    flags |= O_NONBLOCK ;
    errno = 0 ;
    (void)fcntl(fd, F_SETFL, flags) ;
    if(errno) {
        die("fcntl error") ;
    }
}

const size_t k_max_msg = 4096 ;
enum {
    STATE_REQ = 0 ,
    STATE_RES = 1 ,
    STATE_END = 2 , // mark the connection for deletion 
};

struct Conn {
    int fd = -1 ;
    uint32_t state = 0 ; // eithjer STATE_REQ or STATE_RES
    // buffer for reading
    size_t rbuf_size = 0 ;
    uint8_t rbuf[4 + k_max_msg] ;
    // buffer for writing
    size_t wbuf_size = 0 ;
    size_t wbuf_sent = 0 ;
    uint8_t wbuf[4 + k_max_msg] ;
};

static void conn_put(std::vector<Conn *> &fd2conn, struct Conn *conn) {
    if(fd2conn.size() <= (size_t)conn->fd) {
        fd2conn.resize(conn->fd + 1) ;
    }
    fd2conn[conn->fd] = conn ;
}

static int32_t accept_new_connection(std::vector<Conn *> &fd2conn, int fd) {
    // accept 
    struct sockaddr_in client_addr = {} ;
    socklen_t socklen = sizeof(client_addr) ;
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &socklen) ;
    if(connfd < 0) {
        msg("accept() error") ;
        return -1 ;
    }

    // set the new connection fd to nonblocking mode
    fd_set_nb(connfd) ;
    // creating the struct Conn
    struct Conn *conn = (struct Conn*)malloc(sizeof(struct Conn)) ;
    if(!conn) {
        close(connfd) ;
        return -1 ;
    }
    conn->fd = connfd ;
    conn->state = STATE_REQ ;
    conn->rbuf_size = 0 ;
    conn->wbuf_size = 0 ;
    conn->wbuf_sent = 0 ;
    conn_put(fd2conn, conn) ;
    return 0 ;
}

static void state_req(Conn *conn) ;
static void state_res(Conn *conn) ;

static bool try_one_request(Conn *conn) {
    // try to parse a request from the buffer
    if(conn->rbuf_size < 4) {
        // ont enough data in the buffer. Will retry in the next iteration
        return false ;
    } 

    uint32_t len = 0 ;
    memcpy(&len, &conn->rbuf[0], 4) ;
    if(len > k_max_msg) {
        msg("too long") ;
        conn->state = STATE_END ;
        return false ;
    }
    if(4 + len > conn->rbuf_size) {
        // not enough data in the buffer. Will retry in the next iteration
        return false ;
    }
    
    // got one request, do something with it
    printf("client says: %.*s\n", len, &conn->rbuf[4]) ;

    // generating echoing response
    memcpy(&conn->wbuf[0], &len, 4) ;
    memcpy(&conn->wbuf[4], &conn->rbuf[4], len) ;
    conn->wbuf_size = 4 + len ;

    // remove the request from the buffer
    // note: frequent memmove is inefficient
    // note: need better handling for production code

    size_t remain = conn->rbuf_size - 4 - len ;
    if(remain) {
        memmove(conn->rbuf, &conn->rbuf[4 + len], remain) ;
        conn->rbuf_size = remain ;
        // change state
        conn->state = STATE_RES ;
        state_res(conn) ;
    }

    // continue the outer loop if the request was fully processed
    return (conn->state == STATE_REQ) ;
}

static bool try_fill_buffer(Conn *conn) {
    // try fill the buffer
    assert(conn->rbuf_size < sizeof(conn->rbuf)) ;
    ssize_t rv = 0 ;
    do {
        size_t cap = sizeof(conn->rbuf) - conn->rbuf_size ;
        rv = read(conn->fd, &conn->rbuf[conn->rbuf_size], cap) ;
    } while(rv < 0 && errno == EINTR) ;
    if(rv < 0 && errno == EAGAIN) {
        // continue here
    }
}

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    // this is needed for most server applications
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);    // wildcard address 0.0.0.0
    int rv = bind(fd, (const sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("bind()");
    }

    // listen
    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    // a map of all client connections, keyed by fd
    std::vector<Conn *> fd2conn ;

    // set the listen fd to nonblocking mode
    fd_set_nb(fd) ;

    // the event loop
    std::vector<struct pollfd> poll_args ;
    while (true) {
        // prepare the arguments of the poll()
        poll_args.clear() ;

        // for convenience, the listening fd is put in the first position
        struct pollfd pfd = {fd, POLLIN, 0} ;
        poll_args.push_back(pfd) ;
        // connectin fds
        for(Conn *conn : fd2conn) {
            if(!conn) continue ;
            struct pollfd pfd = {} ;
            pfd.fd = conn->fd ;
            pfd.events = (conn->state == STATE_REQ) ? POLLIN : POLLOUT ;
            pfd.events = pfd.events | POLLERR ;
            poll_args.push_back(pfd) ;
        }

        // poll for active fds
        // the timeout argument doesnt matter here
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), 1000) ;
        if(rv < 0) {
            die("poll") ;
        }

        // process active connections
        for(size_t i = 1 ; i < poll_args.size() ; i++) {
            if(poll_args[i].revents) {
                Conn *conn = fd2conn[poll_args[i].fd] ;
                connection_io(conn) ;
                if(conn->state == STATE_END) {
                    // client closed normally, or something bad happened
                    // destroy this connection
                    fd2conn[conn->fd] = NULL ;
                    (void)close(conn->fd) ;
                    free(conn) ;
                }
            }
        }

        // try to accept a new conenction if the listening fd is active
        if(poll_args[0].revents) {
            (void)accept_new_connection(fd2conn, fd) ;
        }
    }
    return 0;
}
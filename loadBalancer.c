#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define DIM_BUFF 4096
#define WRITE_ALL(fd, buf, count, n) \
    while (count > 0) { \
        n = write(fd, buf, count); \
        if (n <= 0) { \
            if (n < 0 && errno == EINTR){ \
                continue; \
            } \
            break; \
        } \
        buf += n; \
        count -= n; \
    }
void gestore(int signo) {
    int stato;
    while (waitpid(-1, &stato, WNOHANG) > 0);
}

int main(int argc, char **argv) {
    int listenfd, connfd, nread, port, backend_fd, maxfd, n, roundRobin;
    const int on = 1;
    fd_set rset;
    char buff[DIM_BUFF];
    socklen_t len;
    char *ptr = buff;
    size_t da_scrivere;
    ssize_t esito;
    struct sockaddr_in cliaddr, servaddr, backend_addr;
    if (argc != 2) {
        printf("Error: %s port\n", argv[0]);
        exit(1);
    }

    nread = 0;
    while (argv[1][nread] != '\0') {
        if ((argv[1][nread] < '0') || (argv[1][nread] > '9')) {
            printf("Argomento non intero\n");
            exit(2);
        }
        nread++;
    }

    port = atoi(argv[1]);
    if (port < 1024 || port > 65535) {
        printf("Porta scorretta...\n");
        exit(2);
    }

    memset((char *)&servaddr, 0, sizeof(struct sockaddr_in));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port        = htons(port);

    printf("Inizializzo la socket TCP\n");
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        perror("Apertura socket TCP");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
        perror("TCP setsockopt");
        exit(EXIT_FAILURE);
    }

    if (bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("Bind TCP");
        exit(EXIT_FAILURE);
    }

    if (listen(listenfd, SOMAXCONN) < 0) { //la coda viene affidata al SO con SOMAXCONN
        perror("Listen");
        exit(EXIT_FAILURE);
    }

    signal(SIGCHLD, gestore);

    printf("Inizio del demone\n");
    roundRobin = 0;

    for (;;) {
        len = sizeof(cliaddr);
        if ((connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &len)) < 0) {
             //accept rende bloccante, dunque il processo con la terminazione dei figli manda un SIGCHLD e faccio continue
            if (errno == EINTR) {
                continue;
            } else {
                perror("Accept");
                exit(EXIT_FAILURE);
            }
        }

        printf("Accettata la connessione stream, inizio con le operazioni\n");

        if (fork() == 0) {
            close(listenfd);
                if ((backend_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                    perror("Errore creazione socket verso backend");
                    close(connfd);
                    exit(EXIT_FAILURE);
                }

                memset(&backend_addr, 0, sizeof(backend_addr));
                backend_addr.sin_family = AF_INET;
                backend_addr.sin_port = htons(8001);
                if (inet_pton(AF_INET, "127.0.0.1", &backend_addr.sin_addr) <= 0) {
                    perror("Errore trasformazione ip in binario");
                   close(backend_fd);
                    close(connfd);
                    exit(EXIT_FAILURE);
                }
                if (connect(backend_fd, (struct sockaddr *)&backend_addr, sizeof(backend_addr)) < 0) {
                    perror("Errore connect al backend");
                    close(backend_fd);
                    close(connfd);
                    exit(EXIT_FAILURE);
                }
                printf("Connessione al backend avvennuta\n");

                maxfd = (connfd > backend_fd) ? connfd : backend_fd;

                for (;;) {
                    FD_ZERO(&rset);
                    FD_SET(connfd, &rset);
                    FD_SET(backend_fd, &rset);

                    if (select(maxfd + 1, &rset, NULL, NULL, NULL) < 0) {
                        if (errno == EINTR) continue;
                            perror("Errore select");
                            break;
                        }

                    if (FD_ISSET(connfd, &rset)) {
                        n = read(connfd, buff, sizeof(buff));
                        if (n <= 0) {
                            break;
                        }
                        ptr = buff;
                        da_scrivere = n;
                        WRITE_ALL(backend_fd, ptr, da_scrivere, esito);
                        if (esito < 0 && da_scrivere>0) {
                            perror("Errore scrittura verso backend");
                            break;
                        }
                    }

                    if (FD_ISSET(backend_fd, &rset)) {
                        n = read(backend_fd, buff, sizeof(buff));
                        if (n <= 0) {
                            break;
                        }
                        ptr = buff;
                        da_scrivere = n;
                        WRITE_ALL(connfd, ptr, da_scrivere, esito);
                        if (esito < 0 && da_scrivere>0) {
                            perror("Errore scrittura verso client");
                            break;
                        }
                    }
                }

            printf("Fine operazione stream\n");
            close(backend_fd);
            close(connfd);
            exit(EXIT_SUCCESS);
        }

        close(connfd);
    }

    return 0;
}

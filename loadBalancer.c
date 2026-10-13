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
#include <fcntl.h>
#include <stdarg.h>
#include <time.h>
#define DIM_BUFF 4096
#define MAX_SERVER 5
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
static inline void log_msg(const char *livello, const char *formato, ...) {
    char time_buf[24];
    time_t now = time(NULL);
    struct tm tm_info;
    localtime_r(&now, &tm_info);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &tm_info);

    char final_buf[DIM_BUFF];
    int offset = snprintf(final_buf, sizeof(final_buf), "%s %-5s [%d] ", time_buf, livello, getpid());

    if (offset > 0 && offset < sizeof(final_buf)) {
        va_list args;
        va_start(args, formato);
        int v_len = vsnprintf(final_buf + offset, sizeof(final_buf) - offset, formato, args);
        va_end(args);
        if (v_len > 0) {
            offset += v_len;
        }
    }
    if (offset >= sizeof(final_buf)) {
        offset = sizeof(final_buf) - 2;
    }
    final_buf[offset++] = '\n';
    final_buf[offset] = '\0';
    int fd = (strcmp(livello, "ERROR") == 0 || strcmp(livello, "WARN") == 0) ? STDERR_FILENO : STDOUT_FILENO;
    write(fd, final_buf, offset);
}
int main(int argc, char **argv) {
    int listenfd, connfd, nread, port, backend_fd, maxfd, n, roundRobin, serverList_fd, portaServer, tentativo, idx;
    const int on = 1;
    fd_set rset;
    char buff[DIM_BUFF], backendServer[DIM_BUFF], c, ip_tmp[32], port_tmp[16], client_ip[INET_ADDRSTRLEN], backend_ip[INET_ADDRSTRLEN];
    socklen_t len;
    char *ptr = buff;
    size_t da_scrivere;
    ssize_t esito;
    struct sockaddr_in cliaddr, servaddr;
    struct sockaddr_in backend_addr[MAX_SERVER];
    int num_servers = 0, idx_ip = 0, idx_port = 0,leggendo_porta = 0;
    unsigned long bytes_to_backend, bytes_to_client;
    time_t start_time;
    long duration;

    if (argc != 3) {
        log_msg("ERROR", "%s port fileServer", argv[0]);
        exit(1);
    }

    nread = 0;
    while (argv[1][nread] != '\0') {
        if ((argv[1][nread] < '0') || (argv[1][nread] > '9')) {
            log_msg("ERROR","Argomento non intero");
            exit(2);
        }
        nread++;
    }

    strncpy(backendServer,argv[2], DIM_BUFF-1);
    backendServer[DIM_BUFF-1] = '\0';
    log_msg("INFO", "Preso in input il file %s", backendServer);

    serverList_fd = open(backendServer, O_RDONLY);

         if (serverList_fd < 0) {
             log_msg("ERROR", "Errore apertura file server: %s", strerror(errno));
             exit(EXIT_FAILURE);
         }
         while (read(serverList_fd, &c, 1) > 0) {
             if (c == ' ' || c == '\t') {
                 ip_tmp[idx_ip] = '\0';
                 leggendo_porta = 1;
             }
             else if (c == '\n') {
                 if (idx_ip > 0) {
                     port_tmp[idx_port] = '\0';

                     memset(&backend_addr[num_servers], 0, sizeof(struct sockaddr_in));
                     backend_addr[num_servers].sin_family = AF_INET;
                     portaServer = atoi(port_tmp);
                     if(portaServer<1 ||portaServer>65535){
                         log_msg("ERROR", "Porta non permessa nel file di configurazione: %d", portaServer);

                         exit(EXIT_FAILURE);
                     }
                     backend_addr[num_servers].sin_port = htons(portaServer);

                     if(inet_pton(AF_INET, ip_tmp, &backend_addr[num_servers].sin_addr)!=1){
                         log_msg("ERROR", "File malformato, impossibile convertire IP: %s", ip_tmp);

                         exit(EXIT_FAILURE);
                     }
                     log_msg("INFO","Caricato backend %d: %s:%s", num_servers, ip_tmp, port_tmp);

                     num_servers++;
                     if (num_servers >= MAX_SERVER)
                         break;
                 }
                 idx_ip = 0;
                 idx_port = 0;
                 leggendo_porta = 0;
             }
             else if (c != '\r') {
                 if (leggendo_porta == 0 && idx_ip < sizeof(ip_tmp) - 1) {
                     ip_tmp[idx_ip++] = c;
                 } else if (leggendo_porta == 1 && idx_port < sizeof(port_tmp) - 1) {
                     port_tmp[idx_port++] = c;
                 }
             }
         }

         if (idx_ip > 0 && num_servers < MAX_SERVER) {
             port_tmp[idx_port] = '\0';
             memset(&backend_addr[num_servers], 0, sizeof(struct sockaddr_in));
             backend_addr[num_servers].sin_family = AF_INET;
             portaServer = atoi(port_tmp);
             if(portaServer<1 ||portaServer>65535){
                 log_msg("ERROR", "Porta non permessa nel file di configurazione: %d", portaServer);

                 exit(EXIT_FAILURE);
             }
             backend_addr[num_servers].sin_port = htons(portaServer);

            if( inet_pton(AF_INET, ip_tmp, &backend_addr[num_servers].sin_addr)!= 1){
                log_msg("ERROR", "File malformato, impossibile convertire IP: %s", ip_tmp);

                exit(EXIT_FAILURE);
            }
            log_msg("INFO","Caricato backend %d: %s:%s", num_servers, ip_tmp,port_tmp);
             num_servers++;
         }

         close(serverList_fd);
         if (num_servers==0){
             log_msg("ERROR", "File di configurazione vuoto o malformato");

             exit(EXIT_FAILURE);
         }
    port = atoi(argv[1]);
    if (port < 1024 || port > 65535) {
        log_msg("ERROR", "Porta scorretta...");
        exit(2);
    }
    memset((char *)&servaddr, 0, sizeof(struct sockaddr_in));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port        = htons(port);


    log_msg("INFO", "Inizializzo la socket TCP sulla porta %d", port);
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        log_msg("ERROR", "Apertura socket TCP fallita: %s", strerror(errno));

        exit(EXIT_FAILURE);
    }

    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
        log_msg("ERROR", "TCP setsockopt fallita: %s", strerror(errno));

        exit(EXIT_FAILURE);
    }

    if (bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        log_msg("ERROR", "Bind TCP fallita sulla porta %d: %s", port, strerror(errno));

        exit(EXIT_FAILURE);
    }

    if (listen(listenfd, SOMAXCONN) < 0) { //la coda viene affidata al SO con SOMAXCONN
        log_msg("ERROR", "Listen fallita: %s", strerror(errno));

        exit(EXIT_FAILURE);
    }

    signal(SIGCHLD, gestore);
    log_msg("INFO","Inizio del demone");

    roundRobin = 0;

    for (;;) {
        len = sizeof(cliaddr);
        if ((connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &len)) < 0) {
             //accept rende bloccante, dunque il processo con la terminazione dei figli manda un SIGCHLD e faccio continue
            if (errno == EINTR) {
                continue;
            } else {
                log_msg("ERROR", "Accept fallita: %s", strerror(errno));

                exit(EXIT_FAILURE);
            }
        }
        inet_ntop(AF_INET, &(cliaddr.sin_addr), client_ip, sizeof(client_ip));
        log_msg("INFO", "Accettata connessione da client %s:%d", client_ip, ntohs(cliaddr.sin_port));

        if (fork() == 0) {
            close(listenfd);
            backend_fd = -1;
            for(tentativo= 0;tentativo<num_servers;tentativo++){
                idx = (roundRobin + tentativo) % num_servers;
                if ((backend_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                    log_msg("WARN", "Creazione socket backend fallita al tentativo %d", tentativo);

                    continue;
                }

                 if (connect(backend_fd, (struct sockaddr *)&backend_addr[idx], sizeof(struct sockaddr_in)) == 0){
                     break;
                 }
                 else {
                    inet_ntop(AF_INET, &(backend_addr[idx].sin_addr), buff, INET_ADDRSTRLEN);
                    log_msg("WARN", "connect a %s:%d fallita: %s", buff, ntohs(backend_addr[idx].sin_port), strerror(errno));

                    close(backend_fd);
                    backend_fd = -1;
                }
            }
            if(backend_fd == -1){
                log_msg("ERROR", "Tutti i tentativi di connessione ai server backend sono falliti");

                close(connfd);
                exit(EXIT_FAILURE);
            }

                inet_ntop(AF_INET, &(backend_addr[idx].sin_addr), backend_ip, sizeof(backend_ip));
                log_msg("INFO", "Connessione stabilita con successo al backend %s:%d", backend_ip, ntohs(backend_addr[idx].sin_port));
                maxfd = (connfd > backend_fd) ? connfd : backend_fd;
                bytes_to_backend = 0;
                bytes_to_client = 0;
                start_time = time(NULL);
                for (;;) {
                    FD_ZERO(&rset);
                    FD_SET(connfd, &rset);
                    FD_SET(backend_fd, &rset);

                    if (select(maxfd + 1, &rset, NULL, NULL, NULL) < 0) {
                        if (errno == EINTR) continue;
                        log_msg("ERROR", "Select fallita: %s", strerror(errno));

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
                        if (esito <= 0 && da_scrivere>0) {
                            log_msg("ERROR", "Scrittura verso backend fallita: %s", strerror(errno));

                            break;
                        }
                        bytes_to_backend += n;
                    }

                    if (FD_ISSET(backend_fd, &rset)) {
                        n = read(backend_fd, buff, sizeof(buff));
                        if (n <= 0) {
                            break;
                        }
                        ptr = buff;
                        da_scrivere = n;
                        WRITE_ALL(connfd, ptr, da_scrivere, esito);
                        if (esito <= 0 && da_scrivere>0) {
                            log_msg("ERROR", "Scrittura verso client fallita: %s", strerror(errno));

                            break;
                        }
                        bytes_to_client += n;
                    }
                }
                duration= (long)(time(NULL)-start_time);
                log_msg("INFO", "Riepilogo connessione - Client: %s:%d | Backend: %s:%d | Inviati al backend: %lu bytes | Ricevuti dal backend: %lu bytes | Durata: %ld secondi",
                                    client_ip, ntohs(cliaddr.sin_port),
                                    backend_ip, ntohs(backend_addr[idx].sin_port),
                                    bytes_to_backend, bytes_to_client, duration);

            log_msg("INFO", "Operazione stream terminata, chiusura connessione figlio");
            close(backend_fd);
            close(connfd);
            exit(EXIT_SUCCESS);
        }
        roundRobin++;
        if (roundRobin == num_servers){
            roundRobin = 0;
        }
        close(connfd);
    }

    return 0;
}

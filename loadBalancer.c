#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define DIM_BUFF 256
#define LENGTH_CMD 256
#define RECV_STRING_STREAM(sd, buf) read(sd, buf, LENGTH_CMD)
#define max(a, b)  ((a) > (b) ? (a) : (b))
typedef struct {
    char comando[LENGTH_CMD];
    char argomenti[LENGTH_CMD];
} Pack;


void gestore(int signo){
    int stato;
    wait(&stato);//waitpid(-1, &stato, WNOHANG);
}

int main(int argc, char **argv) {
    int listenfd, connfd, udpfd, nready, maxfdp1, status, pid;
    const int on = 1;
    char buff[DIM_BUFF], comando[LENGTH_CMD], argomenti[LENGTH_CMD];
    fd_set rset;
    int len, nread, port, res;
    struct sockaddr_in cliaddr, servaddr;
    Pack pack;

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

    printf("inizializzo le socket\n");
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        perror("apertura socket TCP ");
        exit(EXIT_FAILURE);
    }
    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
        perror("TCP setsockopt");
        exit(EXIT_FAILURE);
    }
    if (bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("bind TCP");
        exit(EXIT_FAILURE);
    }
    if (listen(listenfd, 5) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    udpfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpfd < 0) {
        perror("apertura socket UDP");
        exit(EXIT_FAILURE);
    }
    if (setsockopt(udpfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
        perror("UDP setsockopt");
        exit(EXIT_FAILURE);
    }
    if (bind(udpfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("bind UDP");
        exit(EXIT_FAILURE);
    }

    signal(SIGCHLD, gestore);
    FD_ZERO(&rset);
    maxfdp1 = max(listenfd, udpfd) + 1;
    printf("inizio del demone\n");
    for (;;) {
        FD_SET(listenfd, &rset);
        FD_SET(udpfd, &rset);
        if ((nready = select(maxfdp1, &rset, NULL, NULL, NULL)) < 0) {
            if (errno == EINTR) continue;
            else {
                perror("select");
                exit(EXIT_FAILURE);
            }
        }
        //-----------------------------------------------------------------------------------------

        // STREAM: esecuzione remota comando (output inviato al client)
        if (FD_ISSET(listenfd, &rset)) {

            len = sizeof(cliaddr);
            if ((connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &len)) < 0) {
                if (errno == EINTR) continue;
                else { perror("accept"); exit(9); }
            }
            printf("accettato la connessione stream, inizio con le operazioni\n");
            if (fork() == 0) {
                close(listenfd);
                while ((nread = RECV_STRING_STREAM(connfd, comando)) > 0) {
                    //MODIFICARE QuA



                }
                if(nread<0){
                    perror("receivefrom");
                }
                    printf("fine operazione stream\n");
                    close(connfd);
                    exit(EXIT_SUCCESS);
            }
            shutdown(connfd, 0);
            shutdown(connfd,1);
            close(connfd);
        }
        //-----------------------------------------------------------------------------------------
        // DATAGRAM: esecuzione comando e invio solo valore di ritorno
        if (FD_ISSET(udpfd, &rset)) {
            printf("accettato la connessione udp, inizio con le operazioni\n");

            len = sizeof(cliaddr);
            nread = recvfrom(udpfd, &pack, sizeof(Pack), 0, (struct sockaddr *)&cliaddr, &len);
            if ((nread)>0) { // comando
                strcpy(comando, pack.comando);
                printf("ricevuto comando %s\n", comando);
                strcpy(argomenti, pack.argomenti);
                printf("ricevuto argomento %s \n", argomenti);
                //MODIFICARE QUA



                if (sendto(udpfd, &res, sizeof(int), 0, (struct sockaddr *)&cliaddr, len) < 0) {
                    perror("sendto udp");
                }
            }else{
                perror("receivefrom");
            }
            printf("fine operazione udp\n");

        }

    }
}

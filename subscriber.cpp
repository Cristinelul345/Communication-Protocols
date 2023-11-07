#include <netinet/in.h>
#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <arpa/inet.h>
#include <zconf.h>
#include <netinet/tcp.h>
#include <cstring>

using namespace std;
struct __attribute__((packed)) tcp_msg_t {
    char ip[16];
    uint16_t udp_port;
    char topic_name[51];
    char type[11];
    char data[1501];
};

// structura pentru un mesaj transmis la server de catre un client TCP
struct __attribute__((packed)) serv_msg_t {
    char type;
    char topic_name[51];
    bool sf;
};

#define MAXBUFF 1552
#define MAXRECV (sizeof(tcp_msg_t) + 1)

bool make_message(serv_msg_t* message, char* buff) {
    // se pune un \0 la final in caz ca nu s-au primit fix 1500 octeti de date
    buff[strlen(buff) - 1] = 0;
    char *token = strtok(buff, " ");
    if(token == nullptr) cout << "Commands are `subscribe <topic> <SF>`, `unsubscribe <topic>`, or `exit`.\n";

    if (!strcmp(token, "subscribe"))
        message->type = 's';
    else if (!strcmp(token, "unsubscribe")) 
        message->type = 'u';
    else 
        cout << "Commands are `subscribe <topic> <SF>`, `unsubscribe <topic>`, or `exit`.\n";

    // se analizeaza al doilea parametru
    token = strtok(nullptr, " ");
    if(token == nullptr) cout << "Commands are `subscribe <topic> <SF>`, `unsubscribe <topic>`, or `exit`.\n";
    if(strlen(token) > 50) cout << "Topic name can be at most 50 characters long.\n";
    strcpy(message->topic_name, token);

    if (message->type == 's') {
        // daca este o comanda de `subscribe`, se analizeaza si al treilea parametru
        token = strtok(nullptr, " ");
        if(token == nullptr) cout << "Commands are `subscribe <topic> <SF>`, `unsubscribe <topic>`, or `exit`.\n";
        if(token[0] != '0' && token[0] != '1') cout << "SF must be either 0 or 1.\n";
        message->sf = token[0] - '0';
    }

    return true;
}

int main(int argc, char** argv) {
    if(argc != 4)
    {
        cout << "Usage: ./subscriber <ID> <IP_SERVER> <PORT_SERVER>.\n";
        exit(0);
    }
    if(strlen(argv[1]) > 10)
    {
        cout << "Subscriber ID should be at most 10 characters long.\n";
        exit(0);
    }

    char buff[MAXRECV];
    int serv_sock, num_bytes, flag = 1;
    sockaddr_in serv_addr;
    tcp_msg_t* recv_msg;
    serv_msg_t sent_msg;
    fd_set fds, tmp_fds;

    // se creeaza socketul dedicat conexiunii la server
    serv_sock = socket(AF_INET, SOCK_STREAM, 0);
    if(serv_sock < 0)
    {
        cout << "Unable to open server socket.\n";
        exit(0);
    }

    // se completeaza datele despre socketul TCP corespunzator conexiunii la server
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(atoi(argv[3]));
    if(inet_aton(argv[2], &serv_addr.sin_addr) == 0)
    {
        cout << "Incorrect <IP_SERVER>. Conversion failed.\n";
        exit(0);
    }

    if(connect(serv_sock, (sockaddr*) &serv_addr, sizeof(serv_addr)) < 0)
    {
        cout << "Unable to connect to server.\n";
        exit(0);
    }
    if(send(serv_sock, argv[1], strlen(argv[1]) + 1, 0) < 0)
    {
        cout << "Unable to send ID to server.\n";
        exit(0);
    }

    // se dezactiveaza algoritmul lui Nagle pentru conexiunea la server
    setsockopt(serv_sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));

    // se seteaza file descriptorii socketilor pentru server si pentru stdin
    FD_ZERO(&fds);
    FD_SET(serv_sock, &fds);
    FD_SET(0, &fds);

    while(true)
    {
        tmp_fds = fds;

        if(select(serv_sock + 1, &tmp_fds, nullptr, nullptr, nullptr) < 0)
        {
            cout << "Unable to select.\n";
            exit(0);
        }

        if (FD_ISSET(0, &tmp_fds))
        {
            // s-a primit un mesaj de la stdin
            memset(buff, 0, MAXRECV);
            fgets(buff, MAXBUFF - 1, stdin);

            if (!strcmp(buff, "exit\n"))
                break;

            // s-a primit o comanda diferita de `exit`
            if (make_message(&sent_msg, buff))
            {
                if(send(serv_sock, (char*) &sent_msg, sizeof(sent_msg), 0) < 0)
                {
                    cout << "Unable to send message to server.\n";
                    exit(0);
                }

                // se afiseaza mesajele corespunzatoare comenzii date
                if(sent_msg.type == 's')
                    cout << "subscribed " <<  sent_msg.topic_name << "\n";
                else
                    cout << "unsubscribed " << sent_msg.topic_name << "\n";

            }
        }
        if (FD_ISSET(serv_sock, &tmp_fds)) {
            // s-a primit un mesaj de la server (adica de la un client UDP al acestuia)
            memset(buff, 0, MAXRECV);
            num_bytes = recv(serv_sock, buff, sizeof(tcp_msg_t), 0);
            if(num_bytes < 0)
            {
                cout << "Error receiving from server.\n";
                exit(0);
            }

            if (num_bytes == 0) break;

            // se afiseaza mesajul primit
            recv_msg = (tcp_msg_t*)buff;
            printf("%s:%hu - %s - %s - %s\n", recv_msg->ip, recv_msg->udp_port,
                   recv_msg->topic_name, recv_msg->type, recv_msg->data);
        }
    }

    close(serv_sock);
    return  0;
}

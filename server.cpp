#include <netinet/tcp.h>
#include <iostream>
#include <arpa/inet.h>
#include <unordered_map>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <unordered_set>
#include <cstdint>
#include <cstring>
#include <string>
#include <netinet/in.h>
#include <cmath>
#include <zconf.h>

#define MAXBUFF 1552
#define topic first
#define topic_data second
#define SF second
using namespace std;
struct client_t
{
    char id[11];
    unordered_map<string, bool> topics;
};
struct topic_t
{
    bool sf;
    int last_msg;
    string topic_name;

    topic_t(): sf(false), last_msg(0), topic_name("") { }

    topic_t(bool _sf, int index, string& name): sf(_sf), last_msg(index), topic_name(name) { }
};
struct __attribute__((packed)) recv_msg_t
{
    char topic_name[50];
    uint8_t type;
    char data[1501];
};
struct __attribute__((packed)) tcp_msg_t
{
    char ip[16];
    uint16_t udp_port;
    char topic_name[51];
    char type[11];
    char data[1501];
};


struct __attribute__((packed)) serv_msg_t
{
    char type;
    char topic_name[51];
    bool sf;
};
#define MAXRECV (sizeof(tcp_msg_t) + 1)

bool decode_message(recv_msg_t* received, tcp_msg_t* to_send)
{
    if(received->type > 3) cout << "Incorrect publisher message type.\n";

    long long int_num;
    double real_num;

    strncpy(to_send->topic_name, received->topic_name, 50);
    to_send->topic_name[50] = 0;

    switch (received->type)
    {
        case 0:
            if(received->data[0] > 1) cout << "Incorrect publisher sign byte.\n";
            int_num = ntohl(*(uint32_t*)(received->data + 1));

            if (received->data[0]) {
                int_num *= -1;
            }

            sprintf(to_send->data, "%lld", int_num);
            strcpy(to_send->type, "INT");
            break;

        case 1:
            real_num = ntohs(*(uint16_t*)(received->data));
            real_num /= 100;
            sprintf(to_send->data, "%.2f", real_num);
            strcpy(to_send->type, "SHORT_REAL");
            break;

        case 2:
            if(received->data[0] > 1) cout << "Incorrect publisher sign byte.\n";

            real_num = ntohl(*(uint32_t*)(received->data + 1));
            real_num /= pow(10, received->data[5]);

            if (received->data[0]) {
                real_num *= -1;
            }

            sprintf(to_send->data, "%lf", real_num);
            strcpy(to_send->type, "FLOAT");
            break;

        default:
            strcpy(to_send->type, "STRING");
            strcpy(to_send->data, received->data);
            break;
    }
    return true;
}

void update_max_fd(int& max_fd, fd_set* active_fds)
{
    for (int j = max_fd; j > 2; --j)
        if(FD_ISSET(j, active_fds))
        {
            max_fd = j;
            break;
        }

}

void close_all(fd_set *fds, int max_fd) {
    for (int i = 2; i <= max_fd; ++i) {
        if (FD_ISSET(i, fds)) {
            close(i);
        }
    }
}

void unsubscribe(int fd, char* topic_name, unordered_map<string, unordered_set<int>>& online, unordered_map<int, client_t>& clients)
{
    auto it = online.find(topic_name);
    if (it != online.end() && it->second.find(fd) != it->second.end())
    {
        it->second.erase(fd);
        clients[fd].topics.erase(topic_name);
    }
}

void subscribe(int fd, serv_msg_t* serv_msg, unordered_map<string, unordered_set<int>>& online, unordered_map<int, client_t>& clients)
{
    online[serv_msg->topic_name].insert(fd);
    clients[fd].topics[serv_msg->topic_name] = serv_msg->sf;
}

void unsubscribe_all(int fd, unordered_map<string, unordered_set<int>>& online, unordered_map<int, client_t>& clients, unordered_map<string, vector<tcp_msg_t>>& buffers, unordered_map<string, vector<topic_t>>& client_topics, unordered_map<string, int>& clients_per_topic)
{
    string crt_id = clients[fd].id;
    string topic_name;

    for (auto& crt_topic : clients[fd].topics)
    {
        topic_name = crt_topic.topic;

        if (online[topic_name].size() == 1)
            online.erase(topic_name);
        else
            online[topic_name].erase(fd);

        if (crt_topic.SF)
        {
            client_topics[crt_id].emplace_back(true, buffers[topic_name].size(), topic_name);
            clients_per_topic[crt_topic.topic]++;
        }
        else client_topics[crt_id].emplace_back(false, -1, topic_name);
    }
    clients.erase(fd);
}

void update_buffer(int index, string& topic_name, unordered_map<string, vector<tcp_msg_t>>& buffers, unordered_map<string, vector<topic_t>>& client_topics)
{
    int min_index = index;
    for (auto& entry : client_topics)
        for (topic_t& crt_topic : entry.topic_data)
            if (crt_topic.topic_name == topic_name && crt_topic.last_msg < min_index)
                min_index = crt_topic.last_msg;

    if (min_index == 0) return;

    vector<tcp_msg_t>& crt_buffer = buffers[topic_name];
    crt_buffer.erase(crt_buffer.begin(), crt_buffer.begin() + min_index);

    for (auto& entry : client_topics)
        for (topic_t& crt_topic : entry.topic_data)
            if (crt_topic.topic_name == topic_name)
                crt_topic.last_msg -= min_index;

}

void send_buffered_messages(int fd, char* id, unordered_map<string, unordered_set<int>>& online, unordered_map<string, vector<topic_t>>& client_topics, unordered_map<string, vector<tcp_msg_t>>& buffers, unordered_map<string, int>& clients_per_topic, client_t& crt_client)
{
    int len;

    for (auto& buffered_topic : client_topics[id])
    {
        if (buffered_topic.sf)
        {
            vector<tcp_msg_t>& crt_buffer = buffers[buffered_topic.topic_name];
            len = crt_buffer.size();

            for (int j = buffered_topic.last_msg; j < len; ++j)
                if(send(fd, (char*) &crt_buffer[j], sizeof(tcp_msg_t), 0) < 0)
                {
                    cout << "Unable to send message to TCP client\n";
                    exit(0);
                }

            if(!--clients_per_topic[buffered_topic.topic_name]) buffers.erase(buffered_topic.topic_name);
            else update_buffer(buffered_topic.last_msg, buffered_topic.topic_name, buffers, client_topics);

        }
        crt_client.topics.insert({buffered_topic.topic_name, buffered_topic.sf});
        online[buffered_topic.topic_name].insert(fd);
    }
}

int main(int argc, char** argv)
{
    if(argc != 2)
    {
        cout << "Usage: ./server <PORT>\n";
        exit(0);
    }
    // buffere de mesaje pentru clientii TCP cu SF = 1, indexate dupa numele topicurilor
    unordered_map<string, vector<tcp_msg_t>> buffers;

    // asigura legatura dintre un topic si file descriptorii clientilor TCP abonati la el
    unordered_map<string, unordered_set<int>> online;

    // clientii TCP cu SF care sunt offline, pentru care se retin fd-ul si pozitia ultimului mesaj
    // primit din vectorul de mesaje bufferate al respectivului topic
    unordered_map<int, client_t> clients;  // retine file descriptorii clientilor si clientii insisi

    // retine topicele unui client offline, identificat dupa id-ul sau
    unordered_map<string, vector<topic_t>> client_topics;

    // retine numarul de clienti abonati la fiecare topic pentru a putea sterge bufferele
    // topicurilor catre care nu mai e nimeni abonat
    unordered_map<string, int> clients_per_topic;

    char buff[MAXBUFF];
    int udp_sock, port_num, tcp_sock, max_fd, new_sock, bytes_recv, flag = 1;
    socklen_t udp_socklen = sizeof(sockaddr), tcp_socklen = sizeof(sockaddr);
    sockaddr_in udp_addr, tcp_addr, new_tcp;
    recv_msg_t* udp_msg;
    tcp_msg_t tcp_msg;
    serv_msg_t* serv_msg;
    client_t crt_client;
    fd_set active_fds, tmp_fds;
    bool exit_msg = false;
    string crt_topic;

    // se creeaza socketul UDP
    udp_sock = socket(PF_INET, SOCK_DGRAM, 0);
    if(udp_sock < 0)
    {
        cout << "Unable to create UDP socket.\n";
        exit(0);
    }

    // se creeaza socketul TCP
    tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
    if(tcp_sock < 0)
    {
        cout << "Unable to create TCP socket.\n";
        exit(0);
    }

    port_num = atoi(argv[1]);
    if(port_num < 1024)
    {
        cout << "Incorrect port number.\n";
        exit(0);
    }

    // se completeaza informatiile despre socketul UDP si despre cel pasiv TCP
    udp_addr.sin_family = tcp_addr.sin_family = AF_INET;
    udp_addr.sin_port = tcp_addr.sin_port = htons(port_num);
    udp_addr.sin_addr.s_addr = tcp_addr.sin_addr.s_addr = INADDR_ANY;

    if(bind(udp_sock, (sockaddr*) &udp_addr, sizeof(sockaddr_in)) < 0)
    {
        cout << "Unable to bind UDP socket.\n";
        exit(0);
    }

    if(bind(tcp_sock, (sockaddr*) &tcp_addr, sizeof(sockaddr_in)) < 0)
    {
        cout << "Unable to bind TCP socket.\n";
        exit(0);
    }

    if(listen(tcp_sock, INT_MAX) < 0)
    {
        cout << "Unable to listen on the TCP socket.\n";
        exit(0);
    }

    // se seteaza file descriptorii socketilor creati pana acum
    FD_ZERO(&active_fds);
    FD_SET(tcp_sock, &active_fds);
    FD_SET(udp_sock, &active_fds);
    FD_SET(0, &active_fds);
    max_fd = tcp_sock;

    // serverul ruleaza pana cand se primeste "exit" de la tastatura
    while(!exit_msg)
    {
        tmp_fds = active_fds;
        memset(buff, 0, MAXBUFF);

        if(select(max_fd + 1, &tmp_fds, nullptr, nullptr, nullptr) < 0)
        {
            cout << "Unable to select.\n";
            exit(0);
        }

        for (int i = 0; i <= max_fd; i++)
        {
            if (FD_ISSET(i, &tmp_fds))
            {
                if (i == 0)
                {
                    // s-a primit o comanda de la stdin
                    fgets(buff, MAXBUFF - 1, stdin);
                    if (!strcmp(buff, "exit\n"))
                    {
                        exit_msg = true;
                        break;
                    }
                    else cout << "Only accepted command is `exit`.\n";

                }
                else if (i == udp_sock)
                {
                    // s-a primit un mesaj de la un client UDP
                    if(recvfrom(udp_sock, buff, MAXBUFF, 0, (sockaddr*) &udp_addr, &udp_socklen) < 0)
                    {
                        cout << "Nothing received from UDP socket.\n";
                        exit(0);
                    }
                    // se converteste mesajul in formatul ce va fi transmis catre clientii TCP
                    // sau bufferat
                    tcp_msg.udp_port = ntohs(udp_addr.sin_port);
                    strcpy(tcp_msg.ip, inet_ntoa(udp_addr.sin_addr));
                    udp_msg = (recv_msg_t*)buff;

                    if (decode_message(udp_msg, &tcp_msg))
                    {
                        if (buffers.find(tcp_msg.topic_name) != buffers.end())
                            buffers[tcp_msg.topic_name].push_back(tcp_msg);
                        if (online.find(tcp_msg.topic_name) != online.end())
                            for (int fd : online[tcp_msg.topic_name])
                                if(send(fd, (char*) &tcp_msg, sizeof(tcp_msg_t), 0) < 0)
                                {
                                    cout << "Unable to send message to TCP client\n";
                                    exit(0);
                                }

                    }
                }
                else if(i == tcp_sock)
                {
                    // s-a primit o noua cerere de conexiune TCP
                    new_sock = accept(i, (sockaddr*) &new_tcp, &tcp_socklen);
                    if(new_sock < 0) cout << "Unable to accept new client.\n";

                    // se dezactiveaza algoritmul lui Nagle pentru conexiunea la clientul TCP
                    setsockopt(new_sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));

                    FD_SET(new_sock, &active_fds);

                    if(new_sock > max_fd) max_fd = new_sock;

                    if(recv(new_sock, buff, MAXBUFF - 1, 0) < 0)
                    {
                        cout << "No client ID received.\n";
                        exit(0);
                    }

                    strcpy(crt_client.id, buff);
                    crt_client.topics.clear();
                    send_buffered_messages(new_sock, buff, online, client_topics, buffers, clients_per_topic, crt_client);

                    clients.insert({new_sock, crt_client});
                    client_topics.erase(buff);

                    printf("New client %s connected from %s:%hu.\n", buff, inet_ntoa(new_tcp.sin_addr), ntohs(new_tcp.sin_port));
                }
                else
                {
                    // se primeste o comanda de la un client TCP
                    bytes_recv = recv(i, buff, MAXBUFF - 1, 0);
                    if(bytes_recv < 0) cout << "Nothing received from TCP subscriber.\n";

                    if (bytes_recv == 0)
                    {
                        printf("Client %s disconnected.\n", clients[i].id);

                        FD_CLR(i, &active_fds);
                        update_max_fd(max_fd, &active_fds);
                        unsubscribe_all(i, online, clients, buffers, client_topics, clients_per_topic);
                        close(i);
                    }
                    else
                    {
                        // s-a primit o comanda de subscribe sau unsubscribe
                        serv_msg = (serv_msg_t*)buff;

                        if (serv_msg->type == 'u') {
                            unsubscribe(i, serv_msg->topic_name, online, clients);
                        } else {
                            subscribe(i, serv_msg, online, clients);
                        }
                    }
                }
            }
        }
    }

    // se inchid toti socketii inca in folosinta
    close_all(&active_fds, max_fd);

    return 0;
}


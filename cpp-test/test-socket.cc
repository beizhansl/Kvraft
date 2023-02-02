#include <sys/socket.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <assert.h>
#include <unistd.h>
int main(int argc, char* argv[]){
    if(argc < 2){
        printf(" usage ipaddress,port_num");
        return 1;
    }
    char * ip = argv[1];
    int port = atoi(argv[2]);
    //socket address
    struct sockaddr_in server_address;
    bzero(&server_address,sizeof(server_address));
    server_address.sin_family = AF_INET;
    inet_pton(AF_INET,ip,&server_address.sin_addr);
    server_address.sin_port = htons(port);

    //socket create
    int sockfd = socket(PF_INET,SOCK_STREAM,0);
    assert(sockfd >=0);
    if(connect(sockfd, (struct sockaddr*)&server_address,sizeof(server_address))<0){
        printf("connect failed");
        return 1;
    }else{
        const char* oob_data = "a";
        const char* normal_data = "123";
        send( sockfd, normal_data, strlen(normal_data), 0);
        sleep(5);
        // send( sockfd, oob_data,strlen(oob_data),MSG_OOB);
        send( sockfd, normal_data, strlen(normal_data),0);
    }
    close( sockfd );
    return 0;
}
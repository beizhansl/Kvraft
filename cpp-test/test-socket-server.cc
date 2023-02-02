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
    
    //sockfd bind
    int ret = bind(sockfd, (struct sockaddr*)&server_address,sizeof(server_address));
    assert(ret != -1);

    //sockfd listen //just create two queue,half and full
    ret = listen(sockfd,5);
    
    // connect progress is in kernel, use in userspace!
    // Then we use accept to get connection(new sockfd) from queue
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int connfd = accept( sockfd, (struct sockaddr*)&client_addr,&client_len);
    if( connfd < 0 ){
        printf("error,accept failed\n");
    }else{
        char buff[1024];
        sockaddr_in conn_addr;
        socklen_t conn_addrlen = sizeof(conn_addr);
        int ret = getsockname( connfd,(struct sockaddr*)&conn_addr,&conn_addrlen);
        char* ptr1 = inet_ntoa(conn_addr.sin_addr);
        printf("now conn address ip is %s port is %d\n",ptr1,conn_addr.sin_port);
        ptr1 = inet_ntoa(client_addr.sin_addr);
        printf("now conn address ip is %s port is %d\n",ptr1,client_addr.sin_port);
        ret = getpeername( connfd,(struct sockaddr*)&conn_addr,&conn_addrlen);
        ptr1 = inet_ntoa(conn_addr.sin_addr);
        printf("now conn address ip is %s port is %d\n",ptr1,conn_addr.sin_port);
        memset(buff,0,sizeof(buff));
        ret = recv( connfd,buff,1023,0);
        printf("got %d bytes %s\n",ret,buff);
        memset(buff,0,sizeof(buff));
        ret = recv( connfd,buff,1023,MSG_OOB);
        printf("got %d bytes %s\n",ret,buff);
        memset(buff,0,sizeof(buff));
        ret = recv( connfd,buff,1023,0);
        printf("got %d bytes %s\n",ret,buff);
        close(connfd);
    }

    close(sockfd);
    return 0;
}
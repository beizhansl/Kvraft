#include<sys/types.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<unistd.h>
#include<fcntl.h>
#include<sys/epoll.h>
#include<poll.h>
#include<iostream>
#include<string.h>
#include<vector>
#include<errno.h>
#include<iostream>
#include <signal.h>

using namespace std;

int setSocketNonblocking(int fd)
{
    //将监听socker设置为非阻塞的
    int oldSocketFlag = fcntl(fd, F_GETFL, 0);
    int newSocketFlag = oldSocketFlag | O_NONBLOCK;
    if(fcntl(fd, F_SETFL, newSocketFlag)==-1) 
    {
        close(fd);
        cout << "set listenfd to nonblock error" << endl;
        return -1;
    }
    return oldSocketFlag;
}


int main()
{
    //创建一个监听socket
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd == -1)
    {
        cout << "create listen socket error" << endl;
        return -1;
    }
    
    setSocketNonblocking(listenfd);

    //初始化服务器地址
    
    struct sockaddr_in bindaddr;
    bindaddr.sin_family = AF_INET;
    bindaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    bindaddr.sin_port = htons(3000);

    if(bind(listenfd, (struct sockaddr*)&bindaddr, sizeof(bindaddr))==-1)
    {
        cout << "bind listen socker error." << endl;
        close(listenfd);
        return -1;
    }
    
    //启动监听
    if(listen(listenfd, SOMAXCONN)==-1)
    {
        cout << "listen error." << endl;
        close(listenfd);
        return -1;
    }
    
    //复用地址和端口号
    int on = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof(on));
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEPORT, (char*)&on, sizeof(on));


    //创建epollfd
    int epollfd = epoll_create(1);
    if(epollfd == -1)
    {
        cout << "create epollfd error." << endl;
        close(listenfd);
        return -1;
    }
    
    epoll_event listen_fd_event;
    listen_fd_event.data.fd = listenfd;
    listen_fd_event.events = EPOLLIN;
    listen_fd_event.events |= EPOLLET;
    
    //将监听sokcet绑定到epollfd上去
    if(epoll_ctl(epollfd, EPOLL_CTL_ADD, listenfd,&listen_fd_event)==-1)
    {
        cout << "epoll_ctl error" << endl;
        close(listenfd);
        return -1;
    }
    
    int n;
    while(true)
    {
        epoll_event epoll_events[1024];
        n = epoll_wait(epollfd, epoll_events, 1024, 1000);
        if(n<0)
        {
            //被信号中断
            if(errno == EINTR) continue;
            //出错,退出
            break;
        }
        else if(n==0)
        {
            //超时,继续
            continue;
        }
        for(size_t i = 0; i<n;i++)
        {
            //事件可读
            if(epoll_events[i].events & EPOLLIN)
            {
                if(epoll_events[i].data.fd == listenfd)
                {
                    //侦听socket,接受新连接
                    struct sockaddr_in clientaddr;
                    socklen_t clientaddrlen = sizeof(clientaddr);
                    int clientfd = accept(listenfd, (struct sockaddr*)&clientaddr, &clientaddrlen);
                    if(clientfd != -1)
                    {
                        int oldSocketFlag = fcntl(clientfd, F_GETFL,0);
                        int newSocketFlag = oldSocketFlag | O_NONBLOCK;
                        if(fcntl(clientfd, F_SETFD, newSocketFlag)==-1)
                        {
                            close(clientfd);
                            cout << "set clientfd to nonblocking error." << endl;
                        }
                        else 
                        {
                            epoll_event client_fd_event;
                            client_fd_event.data.fd = clientfd;
                            client_fd_event.events = EPOLLIN;
                            client_fd_event.events |= EPOLLET; //设置为边缘出发
                            if(epoll_ctl(epollfd, EPOLL_CTL_ADD, clientfd, &client_fd_event)!=-1)
                            {
                                cout << "new client accepted,clientfd: " << clientfd << endl;
                            }
                            else 
                            {
                                cout << "add client fd to epollfd error" << endl;
                                close(clientfd);
                            }
                        }
                    }
                }
                else
                {
                    //普通clientfd
                    char ch;
                    int m = recv(epoll_events[i].data.fd, &ch, 1, 0);
                    if(m==0)
                    {
                        //对端关闭了连接，从epollfd上移除clientfd
                        if(epoll_ctl(epollfd, EPOLL_CTL_DEL, epoll_events[i].data.fd,NULL)!=-1)
                        {
                            cout << "client disconnected,clientfd:" <<epoll_events[i].data.fd << endl;
                        }
                        close(epoll_events[i].data.fd);
                    }
                    else if(m<0)
                    {
                        //出错
                        if(errno!= EWOULDBLOCK && errno !=EINTR)
                        {
                            if(epoll_ctl(epollfd, EPOLL_CTL_DEL, epoll_events[i].data.fd,NULL)!=-1)
                            {
                                cout << "client disconnected,clientfd:" <<epoll_events[i].data.fd << endl;
                            }
                            close(epoll_events[i].data.fd);
                        }
                        break;
                    }
                    else 
                    {
                        //正常收到数据
                        cout << "recv from client:" << epoll_events[i].data.fd << " " << ch << endl; 
                    }
                }
            }
            else if(epoll_events[i].events & POLLERR)
            {
                        // TODO 暂不处理
            }      
        }
    }
    close(listenfd);
    return 0;
}
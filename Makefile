CFLAGS = g++ -std=c++11 -g -Werror -Wall

all: server subscriber

server: server.cpp

subscriber: subscriber.cpp

clean:
	rm -f server subscriber
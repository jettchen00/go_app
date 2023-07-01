.PHONY : all
all : bin/udpsvr
	@echo "make all begin"


.PHONY : udpsvr
udpsvr : bin/udpsvr
	@echo "make udpsvr begin"

.PHONY : install
install : 
	@echo "make install begin"

mod_tidy :
	go mod tidy

UDP_SERVER_SRC = $(wildcard cmd/udp_server/*.go)
bin/udpsvr : mod_tidy $(UDP_SERVER_SRC)
	go build -o bin/udpsvr $(UDP_SERVER_SRC)

.PHONY : clean
clean :
	rm -rf bin/udpsvr

package main

import( 
	"log"
	"fmt"
	"net"
	"os"
)
//Runs as a client, connects with leader in order to break partition
func main(){
	if (len(os.Args[1:]) != 0) {
		conn1, err1 := net.Dial("tcp", ":" + os.Args[1])
			if err1 != nil {      
				log.Fatal(err1)
		}
		conn2, err2 := net.Dial("tcp", ":" + os.Args[2])
			if err1 != nil {      
				log.Fatal(err2)
		}
		conn1.Write([]byte("break"))
		conn2.Write([]byte("break"))
	}else {
		fmt.Println("No arguments received")
	
	}

}
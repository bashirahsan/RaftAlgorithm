package main

import( 
	"log"
	"fmt"
	"net"
	"os"
)

func main(){
	if (len(os.Args[1:]) != 0) {
		conn, err := net.Dial("tcp", ":" + os.Args[1])
			if err != nil {      
				log.Fatal(err)
		}
		messages := make([]byte, 4096)
		conn.Write([]byte("Ahsan"))
		n,_ := conn.Read(messages)
		fmt.Println("Server response: " + string(messages[0:n]))
	}else {
		fmt.Println("No arguments received")
	
	}

}
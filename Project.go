package main

import( 
	"log"
	"fmt"
	"net"
	"os"
	"time"
	"math/rand"
	"strconv"
)

var conns = make([]net.Conn,0) // storing connections
var nNode = 5 // number of nodes needed for initial election 
var leaderconn net.Conn	//The connection with leader 
var clientconn net.Conn	 //The connection with the client
var clientvalue = "" //The value set by client
var consistentvalue = "" //The value consistent at all clients
var resultVar = 0

func handleConnection(conn net.Conn) { //Handles the connection when client connects
	messages := make([]byte, 4096)
	n,_ := conn.Read(messages)
	//fmt.Println(string(messages[0:n]))
	if (string(messages[0]) == "N"){
		conns = append(conns, conn)
		log.Println("A client has connected", conn.RemoteAddr())
	}else {
		clientconn = conn
		clientvalue = string(messages[0:n])
	}
}
func serverFunc() { //Accpets the incloming connections and calls handleConnection(conn net.COnn)
	ln, err := net.Listen("tcp", ":" + os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
		
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		
		go handleConnection(conn)
	}
}
func AskForVote(electionTerm int, no int) (int, string) {// candidate asking for vote
	messages := make([]byte, 4096)
	votecount := 1
	state := "candidate"
	fmt.Println("I am the candidate")
	for i:=0; i<len(conns); i++{
		conns[i].Write([]byte(strconv.Itoa(electionTerm)))
	}
	time.Sleep(time.Duration(no)*time.Millisecond)
	for j:=0; j<len(conns); j++{
		conns[j].SetReadDeadline(time.Now().Add(time.Millisecond))
		n,_ := conns[j].Read(messages)
		
		if (n != 0){
			if (string(messages[0]) == "y"){
				votecount++
			}else if(string(messages[1]) == "l" && n>1){
				leaderconn = conns[j]
				conns = append(conns[:j], conns[j+1:]...)
				return 0,"follower"
				
			}
		}
		if (votecount >= (((len(conns)+1)/2)+1)){
			state = "leader"
			return 1, state
		}
	}
	return 0,state
}
func WaitForelectionTimeout(sv int, no int, electionTerm int) int{ //Timeout for nodes to elect a leader (includes candidate timeout plus vote count)
	whatToRead := 1 // giving vote
	messages := make([]byte, 4096)
	for i:=0; i<no; {
		for j:=0; j<len(conns); j++{
			err := conns[j].SetReadDeadline(time.Now().Add(time.Millisecond))
			if err != nil {
				log.Println(err)
				continue
			}
			n,_ := conns[j].Read(messages) 
			if (n != 0){
				if (whatToRead == 1){
					ets := string(messages[0:n])
					et,_ := strconv.Atoi(ets)
					if (electionTerm == et){
						_,err := conns[j].Write([]byte("yes"))// candidate is here, give vote
						if err != nil {
							log.Println(err)
							continue
						}
						i = 0
						sv = 0
						whatToRead = 2
						break
					}
				}else if (whatToRead == 2){
					if (string(messages[0]) == "l") {
						leaderconn = conns[j]
						conns = append(conns[:j], conns[j+1:]...)
						fmt.Println("leader is with port no: " + string(messages[0:n]))
						return 2 // leader is here
					}
				}
			}
		}
		i += len(conns)
	}
	return  sv
}
func RemovePartition(electionTerm int,state string) { //invokes when client sends message with "break" to the leader nodes
	emptyBuffer()
	fmt.Println("I'm in remove partition")
	time.Sleep(10*time.Second)
	etString := strconv.Itoa(electionTerm)
	otherEt := 0 //other leader's election term
	if state == "leader"{
		for i:=0;i<len(conns);i++{
	 		conns[i].Write([]byte(string(etString)+" "+string(state[0])))
		}
		messages := make([]byte, 4096)
		flag := true
		leaderIndex := -1
		for flag == true{
			for i := 0; i<len(conns);i++{
				conns[i].SetReadDeadline(time.Now().Add(time.Millisecond))
				n,_ := conns[i].Read(messages)
				msg := string(messages[0:n])
				if n>0{
					fmt.Println(msg)
					if msg[2] == 'l'{
						otherEt,_ = strconv.Atoi(msg[0:1])
						flag = false
						leaderIndex = i
					}
				}
			}
		}
		fmt.Printf("leader is at index: ")
		fmt.Println(leaderIndex)
	}
	if electionTerm>otherEt{
		fmt.Printf("Im the Leader with electionTerm..")
		fmt.Println(electionTerm)
		for i:=0;i<len(conns);i++{
			conns[i].Write([]byte(etString))
		}
		time.Sleep(10*time.Millisecond)
		resultVar = electionTerm
		Leader(electionTerm,"leader")
	}else{
		fmt.Println("I have to be follower.")
		if state == "follower" {
			conns = append(conns,leaderconn)
		}
		messages := make([]byte, 4096)
		flag := true
		for flag == true{
			for i := 0; i<len(conns);i++{
				conns[i].SetReadDeadline(time.Now().Add(time.Millisecond))
				n,_ := conns[i].Read(messages)
				if n==1{
					//We have to update election term 
					electionTerm,_ = strconv.Atoi(string(messages[0:1]))
					resultVar = electionTerm
					leaderconn = conns[i]
					conns = append(conns[:i], conns[i+1:]...)
					Follower()
					flag = false
				}
			}
		}


	}

}
func writeFile(){ //Writes the most current value to log.txt
	fmt.Println("Write file called with: "+consistentvalue)
	f, err := os.OpenFile("log.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	_, err = fmt.Fprintln(f, "Current Value: " + consistentvalue) 
	if err != nil {
		fmt.Println(err)
		f.Close()
	}
	err = f.Close()
	if err != nil {
		fmt.Println(err)
	}

}
func Leader(electionTerm int, state string) { //The function that runs when a node becomes a leader
	fmt.Println("My State is " + state)
	fmt.Printf("Election Term: ")
	fmt.Println(electionTerm)
	for i:=0; i<len(conns); i++{
		conns[i].Write([]byte("localaddress: " + os.Args[1]))
	}
	messages := make([]byte, 4096)
	for i:=0; i<len(conns); i++{// to empty buffer
		conns[i].SetReadDeadline(time.Now().Add(10*time.Millisecond))
		conns[i].Read(messages)
	}
	fmt.Println("************Sending Heart Beat************") //starts sending heart beat
	voteCount := 1
	tempFlag := 0
	lenconns := len(conns)
	for{ 
		if len(clientvalue)>0{

			if (clientvalue == "partition"){
				for i:=1; i<lenconns; i++{
					conns[i].Write([]byte("Set: "+clientvalue))
				}
				lenconns = 1
				conns[0].Write([]byte("Set: Nopartition"))
			}else if (clientvalue == "break") {
				for i:=0; i<lenconns; i++{
					conns[i].Write([]byte("Set: "+clientvalue))
				}
				clientvalue = ""
				RemovePartition(electionTerm,state)
				time.Sleep(30*time.Millisecond)
				return

			}else{
				for i:=0; i<lenconns; i++{
					conns[i].Write([]byte("Set: "+clientvalue))
				}
			}
		}else{
			for i:=0; i<lenconns; i++{// to empty buffer
				conns[i].Write([]byte("Heartbeat from leader."))
				tempFlag = 1
			}
		}
		fmt.Println("Wrote to all connected...")
		for i:=0; i<lenconns; i++{//Acknowledgement of heart beat.
			conns[i].SetReadDeadline(time.Now().Add(time.Second))
			n,_ := conns[i].Read(messages)
			if(n>0){
				if (string(messages[0]) == "S"){
					fmt.Println("From follower: "+ string(messages[0:n]))
					voteCount += 1
				}else{
					fmt.Println("From follower: "+string(messages[0:n]))
				}
			}
		}
		if voteCount >= (((len(conns)+1)/2)+1){
			for i:=0; i<lenconns; i++{
				conns[i].Write([]byte(clientvalue))
			}
			consistentvalue = clientvalue
			writeFile()
			fmt.Println("Changed value to: "+ consistentvalue)
			clientconn.Write([]byte("Changed value to: "+ consistentvalue))
			clientvalue = ""
		}else if (tempFlag != 1){
			for i:=0; i<lenconns; i++ {
				conns[i].Write([]byte("Can't change value..."))
			}
			clientconn.Write([]byte("Can't change value..."))
			clientvalue = ""
		}
		fmt.Println("Recieved Acknowledgement from all clients...")
		tempFlag = 0
		voteCount = 1
		time.Sleep(time.Second)//heartbeat timeout
	}
}

func Follower() { //The function that runs when a node becomes a follower
	fmt.Println("Hey I am the follower")
	fmt.Println("************Recieving Heart Beat************")
	messages := make([]byte, 4096)
	time.Sleep(2*time.Second)//Sleep so that leader finishes empty buffer and sends heartbeat.
	count := 0
	for {
		if ( count == 3 ){//wait for 3 seconds 
			fmt.Println("Leader is dead, send for re-election")
			return
		}
		leaderconn.SetReadDeadline(time.Now().Add(time.Second))
		n,_:= leaderconn.Read(messages)
		if(n>0){//when recieve a heartbeat, send Ack.
			count = 0 
			fmt.Println("Received: "+string(messages[0:n]))
			if (string(messages[0:n])=="Set: partition"){
				conns = append(conns,leaderconn)
				fmt.Println("Leader is with us")
				return

			}else if (string(messages[0:n])=="Set: break") {
				RemovePartition(0,"follower")
				time.Sleep(30*time.Millisecond)
				return

			}else if (string(messages[0]) == "S"){
				leaderconn.Write([]byte("Set Acknowledgement."))
				//I should wait here for leder to send wether to change value or not based on majority vote.
				leaderconn.SetReadDeadline(time.Time{})
				n,_ := leaderconn.Read(messages)
				if n>0{
					if (string(messages[0])=="C"){
						fmt.Println("Received: "+ string(messages[0:n]))

					}else{
						fmt.Println("Received: "+string(messages[0:n]))
						consistentvalue = string(messages[0:n])
						fmt.Println("Changed value to: "+ consistentvalue)
					}
				}

			}else{
				leaderconn.Write([]byte("Acknowledgement."))
			}
		}else{
			count += 1//not recieved a heart beat 
		}
		time.Sleep(10*time.Millisecond)

	}

}

func ElectionProcess(electionTerm int){ //runs the entire election process handles different cases (when two nodes time out at same time etc)
	state := "follower"
	sv := 1
	no := rand.Intn(300 - 150 + 1) + 150
	fmt.Println("Sleeping Milliseconds:", no)
	sv = WaitForelectionTimeout(sv, no, electionTerm) // wait til requested time
	if (sv == 1){// if state = candidate
		result := 0
		result, state = AskForVote(electionTerm, no) 
		if (result == 1) {// becomes leader
			Leader(electionTerm, state) 
			return
		} else if (result == 0){ // does not become a leader
			if (state == "follower"){
				fmt.Println("I am the follower now")
				Follower()
				return
			}
			messages := make([]byte, 4096)
			for i:=0; i<len(conns); i++ {
				conns[i].SetReadDeadline(time.Now().Add(time.Millisecond))
				n,_ := conns[i].Read(messages)
				if (n != 0) {
					if (string(messages[0]) == "l" || (string(messages[1]) == "l" && n>1)){
						leaderconn = conns[i]
						conns = append(conns[:i], conns[i+1:]...)
						fmt.Println("I am the Follower: " + string(messages[0:n]))
						Follower()
						return
					}
				}
			}
			
			fmt.Println("Lets go for a re-election")
			return
		}
	}else if(sv == 2){
		Follower()
		return
	}else if(sv == 0){
		fmt.Println("Noone became leader........................")
		return
	}
	return
}
func emptyBuffer(){
	messages := make([]byte, 4096)
	for i:=0; i<len(conns); i++{
		conns[i].SetReadDeadline(time.Now().Add(time.Second))
		conns[i].Read(messages)
	}
}

/////////////////////////////////Input Format//////////////////////////////////////////////////////////////////////////////////////////////////////
//Expected input => go run Project.go 2022 2021 2020
//mylistenport, mydailport(if any)..., 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func main(){
	rand.Seed(time.Now().UTC().UnixNano())
	argLen := len(os.Args[1:])
	electionTerm := 1
	if (argLen != 0){// if there is any argument
		go serverFunc() //Stat listening
		if (argLen > 1) {// checking if any address is given to dail
			for i:=2; i<=argLen ; i++{// dailing to other nodes
				conn, err := net.Dial("tcp", ":" + os.Args[i])
				if err != nil {      
					log.Fatal(err)
				}
				conn.Write([]byte("Node"))
				conns = append(conns,conn)
			}
		}
		for len(conns) != nNode-1{}
		time.Sleep(time.Second)
		fmt.Println("Connected to all") 
		//////////////////////RAFT Algorithm//////////////////////////////////////////////
		for {
			if (len(conns) >= 1) {// run til there is any connection
				ElectionProcess(electionTerm)
			}else {
				fmt.Println("No connections available")
				break
			}
			if resultVar != 0{
				electionTerm = resultVar
				resultVar = 0
			}
			electionTerm++
		}
	}else{// if there is no argument given
		fmt.Println("No arguments received.")
	}
}
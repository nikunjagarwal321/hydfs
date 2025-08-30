1. Implement Log Generator - go run log_generator.go
2. Implement Grep with all options - go run grep_helper.go
3. Implement Connection of 3 clients - 
4. Implement Connection of 5 clients and call all other clients from any 1 client
5. From 1 client, call
   5.a. log generator for all other clients 
   5.b. grep for all other clients (GREP is pushed to all clients --> for infrequent patterns)
6. Make it fault tolerant : If any client is down, it should not affect the functioning of other clients
7. Think of optimizing : TO BRAINSTORM MORE
   7.a. Build Inverted Index and store it : either in each client or copy that to all the clients
8. Write unit tests


RUN:
go run main.go config.go service.go server.go client.go  ClientA
go run main.go config.go service.go server.go client.go  ClientB      
go run main.go config.go service.go server.go client.go  ClientC
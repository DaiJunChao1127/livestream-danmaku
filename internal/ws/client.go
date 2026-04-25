package ws

import (
	"sync"

	"golang.org/x/net/websocket"
)

type Client struct {
	UserID 		uint64
	username 	string
	RoomID 		string
	Socket 		*websocket.Conn // the websocket connection that the server holds for each client
	Send 		chan []byte     // a channel to send messages to the client
	done 		chan struct{}   // done is used to signal the goroutines to stop
	once        sync.Once		// once ensures that the Close operations are only performed once
}

func NewClient(uid uint64, name, room string, conn *websocket.Conn) *Client {
	return &Client{
		UserID: 	uid,
		username: 	name,
		RoomID: 	room,
		Socket: 	conn,
		Send:		make(chan []byte),
		done: 		make(chan struct{}),
	}
}


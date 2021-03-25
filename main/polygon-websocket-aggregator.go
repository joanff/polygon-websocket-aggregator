package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	websocket "github.com/gorilla/websocket"
)

var APIKEY string
var CHANNELS string
// = "XT.*"


type Pairbar struct {
	mu sync.Mutex
	pair string
	timerange time.Time
	v  map[string]interface{}
}

var bars = make(map[string]map[time.Time]*Pairbar, 1000)

func transform(msgmap map[string]interface{}) {
	// transform receive time to time range, so can update to the time range key in the map
	//fmt.Println("Running transform function......")
	//fmt.Println(msgmap)
	ticker := msgmap["ev"].(string)
	//fmt.Println(ticker)
	if strings.Compare(ticker,"status")==0{
		return
	}
	recordts := int64(msgmap["t"].(float64))
	receivets := int64(msgmap["r"].(float64))
	//fmt.Println(recordts)
	recordts_unix := time.Unix(0, recordts * int64(time.Millisecond))
	receivets_unix := time.Unix(0, receivets * int64(time.Millisecond))
	diff := receivets_unix.Sub(recordts_unix).Hours()
	if diff>=1{
		// if receive ts is an hour later than tradets skip this record
		return
	}
	//fmt.Println("diff..... in Hour",diff)
	recordts_range := recordts_unix.Truncate(time.Second*30)
	//fmt.Println(recordts_unix)
	//fmt.Println(recordts_range)
	pair := msgmap["pair"].(string)
	_, ok := bars[pair]
	if !ok{
		bars[pair] = make(map[time.Time]*Pairbar)
	}
	_, ok = bars[pair][recordts_range]
	if !ok{
		var add = &Pairbar{pair:pair, timerange:recordts_range, v:make(map[string]interface{})}
		bars[pair][recordts_range] = add
	}

	go bars[pair][recordts_range].Update(recordts_unix, msgmap)
}


func (s *Pairbar) Update(recordts_unix time.Time, msgmap map[string]interface{}) {
	// update the bar according to this new record
	price := msgmap["p"].(float64)
	size := msgmap["s"].(float64)
	s.mu.Lock()
	var newtimerange = len(s.v)==0
	if newtimerange || recordts_unix.Before(s.v["earliest_ts"].(time.Time)){
		s.v["earliest_ts"] = recordts_unix
		s.v["open"] = price
	}
	if newtimerange || recordts_unix.After(s.v["latest_ts"].(time.Time)){
		s.v["latest_ts"] = recordts_unix
		s.v["close"] = price
	}
	if newtimerange || price>s.v["high"].(float64){
		s.v["high"] = price
	}
	if newtimerange || price<s.v["low"].(float64){
		s.v["low"] = price
	}
	if newtimerange{
		s.v["volume"] = size
	}else{
		s.v["volume"] = s.v["volume"].(float64)+size
	}
	s.mu.Unlock()
	//print("Done updating bar\n")
	s.Show()
}

func (s *Pairbar) Show() {
	// show new bar of this timerange and pair
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf( "%s - pair:%s, open: %f, close: %f, high: %f, low: %f, volume: %f\n", s.timerange.String(), s.pair, s.v["open"], s.v["close"],s.v["high"],s.v["low"],s.v["volume"])
}
func main() {

	fmt.Println("Enter the websocket API key")
	fmt.Scanln(&APIKEY)
	fmt.Println("Enter a crytpo ticker: eg:XT.*")
	fmt.Scanln(&CHANNELS)
	//fmt.Printf("%s", CHANNELS)
	c, _, err := websocket.DefaultDialer.Dial("wss://socket.polygon.io/crypto", nil)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("{\"action\":\"auth\",\"params\":\"%s\"}", APIKEY)))
	_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("{\"action\":\"subscribe\",\"params\":\"%s\"}", CHANNELS)))

	chanMessages := make(chan []byte, 10000)
	chanMaps := make(chan map[string]interface{}, 10000)

	go func() {
		for msgBytes := range chanMessages {
			var arr []map[string]interface{}
			err = json.Unmarshal(msgBytes, &arr)
			for _, item := range arr {
				chanMaps <- item
			}
		}
	}()
	go func(){
		for singlemap := range chanMaps{
			transform(singlemap)
		}
	}()

	for {
		_, p, err := c.ReadMessage()
		if err != nil {
			log.Println(err)
			return
		}
		chanMessages <- p
	}

}
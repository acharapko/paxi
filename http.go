package paxi

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ailidani/paxi/log"
)

// http request header names
const (
	HTTPClientID  = "id"
	HTTPCommandID = "cid"
	HTTPTimestamp = "timestamp"
	HTTPNodeID    = "id"
	HTTPSlot      = "slot"
	HTTPNoVal     = "nv"
)

// serve serves the http REST API request from clients
func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRoot)
	mux.HandleFunc("/pqr/", n.handlePaxosQuorumRead)
	mux.HandleFunc("/history", n.handleHistory)
	mux.HandleFunc("/crash", n.handleCrash)
	mux.HandleFunc("/drop", n.handleDrop)
	// http string should be in form of ":8080"
	url, err := url.Parse(config.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + url.Port()
	n.server = &http.Server{
		Addr:    port,
		Handler: mux,
	}
	log.Info("http server starting on ", port)
	log.Fatal(n.server.ListenAndServe())
}

func (n *node) issueRequest(cmd Command, w http.ResponseWriter, reqtype reqtype, bslot int) {
	req := Request{
		Command:   cmd,
		Timestamp: time.Now().UnixNano(),
		NodeID:    n.id,
		ReqType:   reqtype,
		BSlot:     bslot,
		c:         make(chan Reply, 1),
	}

	n.MessageChan <- req

	reply := <-req.c

	if reply.Err != nil {
		http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(HTTPClientID, string(reply.Command.ClientID))
	w.Header().Set(HTTPCommandID, strconv.Itoa(reply.Command.CommandID))
	w.Header().Set(HTTPSlot, strconv.Itoa(reply.Slot))
	strDataToWrite := ""
	if reply.Value == nil {
		w.Header().Set(HTTPNoVal, "true")
	} else {
		strDataToWrite = string(reply.Value)
	}
	_, err := io.WriteString(w, strDataToWrite)
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handlePaxosQuorumRead(w http.ResponseWriter, r *http.Request) {
	var cmd Command
	var err error
	cmd.ClientID = ID(r.Header.Get(HTTPClientID))
	cmd.CommandID, err = strconv.Atoi(r.Header.Get(HTTPCommandID))
	if err != nil {
		log.Error(err)
	}

	bslot, err := strconv.Atoi(r.Header.Get(HTTPSlot))
	if err != nil {
		log.Error(err)
	}

	if len(r.URL.Path) > 5 {
		i, err := strconv.Atoi(r.URL.Path[5:])
		if err != nil {
			http.Error(w, "invalid path", http.StatusBadRequest)
			log.Error(err)
			return
		}
		cmd.Key = Key(i)
	} else {
		log.Error("Key is missing for PQR")
		return
	}

	n.issueRequest(cmd, w, REQ_PAXOS_QUORUM_READ, bslot)
}

func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var cmd Command
	var err error
	cmd.ClientID = ID(r.Header.Get(HTTPClientID))
	cmd.CommandID, err = strconv.Atoi(r.Header.Get(HTTPCommandID))
	if err != nil {
		log.Error(err)
	}
	if len(r.URL.Path) > 1 {
		i, err := strconv.Atoi(r.URL.Path[1:])
		if err != nil {
			http.Error(w, "invalid path", http.StatusBadRequest)
			log.Error(err)
			return
		}
		cmd.Key = Key(i)
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Error("error reading body: ", err)
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			cmd.Value = Value(body)
		} else {
			cmd.Value = nil
		}
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("error reading body: ", err)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		json.Unmarshal(body, &cmd)
	}

	n.issueRequest(cmd, w, REQ_REGULAR, -1)
}

func (n *node) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(HTTPNodeID, string(n.id))
	k, err := strconv.Atoi(r.URL.Query().Get("key"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide key", http.StatusBadRequest)
		return
	}
	h := n.Database.History(Key(k))
	b, _ := json.Marshal(h)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handleCrash(w http.ResponseWriter, r *http.Request) {
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Socket.Crash(t)
	// timer := time.NewTimer(time.Duration(t) * time.Second)
	// go func() {
	// 	n.server.Close()
	// 	<-timer.C
	// 	log.Error(n.server.ListenAndServe())
	// }()
}

func (n *node) handleDrop(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Drop(ID(id), t)
}

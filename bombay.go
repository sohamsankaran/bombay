package main

import (
        "fmt"
    "log"
    "net/http"
    "sync"
    "flag"
    "os"
    "os/exec"
    //"encoding/hex"
    //"syscall"
    //"encoding/json"
    //"io/ioutil"
    "github.com/ant0ine/go-json-rest/rest"
    "github.com/DiegoAlbertoTorres/alternator"
    "net/rpc"
)

var config alternator.Config

var sigChan chan os.Signal

type Node_properties struct {
    Space_total int             `json:"space_total"`
    Space_remaining int         `json:"pace_remaining"`
    Uptime float32              `json:"uptime"`
    Avg_bandwidth int           `json:"avg_bandwidth"`
    Peak_bandwidth int          `json:"peak_bandwidth"`
    Seq_read_speed int          `json:"seq_read_speed"`
    Rand_read_speed int         `json:"rand_read_speed"`
    Failure_rate float32        `json:"failure_rate"`
}

type Get_req struct {
    Key string          `json:"key"`
    Liveness int        `json:"liveness"`
}

type Get_res struct {
    Err int             `json:"err"`
    Value string        `json:"value"`
}

type Put_req struct {
    Key string                      `json:"key"`
    Value string                    `json:"value"`
    Space_req int                   `json:"space_req",omitempty`
    Uptime float32                  `json:"uptime", omitempty`
    Avg_bandwidth int               `json:"avg_bandwidth",omitempty`
    Peak_bandwidth int              `json:"peak_bandwidth",omitempty`
    Durability_time int             `json:"durability_time",omitempty`
    Durability_percentage float32   `json:"durability_percentage",omitempty`
}

type Put_res struct {
    Err int                         `json:"err"`
    Req_satisfied int               `json:"req_satisfied"`
    Space_req int                   `json:"space_req",omitempty`
    Uptime float32                  `json:"uptime", omitempty`
    Avg_bandwidth int               `json:"avg_bandwidth",omitempty`
    Peak_bandwidth int              `json:"peak_bandwidth",omitempty`
    Durability_time int             `json:"durability_time",omitempty`
    Durability_percentage float32   `json:"durability_percentage",omitempty`
}


var node_properties = Node_properties{}

var lock = sync.RWMutex{}

var port string
var command string
var alt_port string
var alt_joinPort string
var alt_joinAddr string
var tal_port string
var tal_timeout int


func main() {
    flag.StringVar(&port, "port", "1337", "port that the bombay service will use for communication.")
    flag.StringVar(&command, "command", "", "name of command.")
    flag.StringVar(&tal_port, "profileport", "1413", "port that bombay will start the profiler service (talese) on.")
    flag.IntVar(&tal_timeout, "profiletimeout", 1000, "sets the amount of time in miliseconds between node profile refreshes.")
    flag.StringVar(&alt_port, "dataport", "0", "port that bombay will start the datastore service (alternator) on.")
    flag.StringVar(&alt_joinAddr, "joinaddr", "127.0.0.1", "joins the ring that the node at [joinaddr]:[joinport] belongs to.")
    flag.StringVar(&alt_joinPort, "joinport", "0", "joins the ring that the node at [joinaddr]:[joinport] belongs to.")
    flag.IntVar(&config.MemberSyncTime, "memberSyncTime", 1000, "sets the time between membership syncs with a random node.")
    flag.IntVar(&config.HeartbeatTime, "heartbeatTime", 1000, "sets the amount of time between timeouts.")
    flag.IntVar(&config.ResolvePendingTime, "resolvePendingtime", 10000, "sets the amount of time (in ms) between attempts to resolve pending put operations.")
    flag.IntVar(&config.HeartbeatTimeout, "heartbeatTimeout", 400, "sets the amount of time before a heartbeat times out.")
    flag.IntVar(&config.PutMDTimeout, "putMDTimeout", 2000, "sets the amount of time before a PutMD times out.")
    flag.IntVar(&config.PutDataTimeout, "putDataTimeout", 2000, "sets the amount of time before a PutData times out.")
    flag.IntVar(&config.N, "n", 2, "sets the amount of nodes that replicate metadata.")
    flag.StringVar(&config.DotPath, "dotPath", os.Getenv("HOME")+"/.alternator/", "sets the directory for alternator's data.")
    flag.BoolVar(&config.FullKeys, "fullKeys", false, "if true all keys (hashes) are printed completely.")
    flag.BoolVar(&config.CPUProfile, "cpuprofile", false, "write cpu profile to file")
    flag.Parse()

    if command != "" {
        // stub
    } else {

        // check if alternator and talese instances exist unbounded to a bombay instance - if so, kill them

        //progList := []string{"talese", "alternator"}

        //killProgs(progList)

        // start alternator instance

        runAlternator(alt_port, alt_joinPort, alt_joinAddr)

        // start talese instance
        runTalese(port, tal_timeout)

        api := rest.NewApi()
        api.Use(rest.DefaultDevStack...)
        router, err := rest.MakeRouter(
            rest.Get("/status", GetStatus),
            rest.Post("/get", GetKey),
            rest.Post("/put", PutKey),
            rest.Post("/delete", DeleteKey),
            rest.Post("/updateprofile", UpdateProfile))
        if err != nil {
            log.Fatal(err)
        }
        api.SetApp(router)
        log.Fatal(http.ListenAndServe((":"+ port), api.MakeHandler()))
    }
    
    return
}

func printError(err error) {
  if err != nil {
    os.Stderr.WriteString(fmt.Sprintf("==> Error: %s\n", err.Error()))
  }
}

func runAlternator(port string, joinPort string, joinAddr string) {
    //pstr := os.Getenv("GOPATH")+"/bin/alternator"
    pstr := "./alternator" 
    cmd := exec.Command(pstr, "--port", port, "--target", joinAddr, "--join", joinPort)
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    err := cmd.Start()
    printError(err)
}

func runTalese(port string, timeout int) {
    //pstr := os.Getenv("GOPATH")+"/src/bombay/talese.py"
    pstr := "./talese.py"
    cmd := exec.Command(pstr, "--port", port, "--timeout", string(timeout))
    err := cmd.Start()
    printError(err)
}

//func killProgs(progList []string) {
//
//    for _,prog := range progList {
//        out, _ := exec.Command("pgrep", prog).Output()
//        fmt.Println(string(out))
//    }
//
//}

func GetStatus(w rest.ResponseWriter, r *rest.Request) {
    lock.RLock()
    w.WriteJson(node_properties)
    lock.RUnlock()
}

func UpdateProfile(w rest.ResponseWriter, r *rest.Request) {
    np_temp := Node_properties{}
    err := r.DecodeJsonPayload(&np_temp)
    if err != nil {
        rest.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    lock.Lock()
    node_properties = np_temp
    lock.Unlock()
    w.WriteHeader(http.StatusOK)
}

func GetKey(w rest.ResponseWriter, r *rest.Request) {
    get_req := Get_req{}
    get_res := Get_res{}
    get_res.Value = "" // empty value
    get_res.Err = 0 // default no error
    err := r.DecodeJsonPayload(&get_req)
    if err != nil {
        rest.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    if get_req.Key == "" { // key field is empty
        get_res.Err = 1 // set err = 1 to signify no key requested
        w.WriteJson(get_res)
        return
    }
    name := get_req.Key + "_k"
    get_res.Value, get_res.Err = getk(name)
    w.WriteJson(get_res)
}

func PutKey(w rest.ResponseWriter, r *rest.Request) {
    put_req := Put_req{}
    put_res := Put_res{}
    put_res.Err = 0 // default no error
    err := r.DecodeJsonPayload(&put_req)
    if err != nil {
        rest.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    fmt.Println(put_req.Key + " : " + put_req.Value)
    if put_req.Key == "" { // key field is empty
        put_res.Err = 1 
        w.WriteJson(put_res)
        return
    }
    name := put_req.Key + "_k"
    dlist := kamino(put_req, &put_res)
    rerr := putk(name, put_req.Value, dlist)
    if rerr != 0 {
        put_res.Err = rerr
    }
    w.WriteJson(put_res)
}

func DeleteKey(w rest.ResponseWriter, r *rest.Request) {
    // stub
    w.WriteHeader(http.StatusOK)
}

func getk(k string) (v string, rerr int) {
    v = ""
    rerr = 0 // default no error
    if k == "" { // key field is empty
        rerr = 1 // set err = 1 to signify no key requested
        return
    }
    client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":"+alt_port)
    var val []byte
    fmt.Println("Getting " + k)
    if err != nil {
        rerr = 2
        fmt.Println("could not reach alternator")
        return
    }
    err = client.Call("Node."+"Get", k, &val)
    if err != nil {
        rerr = 3
        fmt.Println("RPC Failed")
        return
    }
    v = string(val)
    return
}

func putk(k string, v string, dests []alternator.Key) (rerr int) {
    rerr = 0 //default no error
    if len(dests) < 1 {
        fmt.Println("No destination listed")
        rerr = 1
        return
    }
    if k == "" {
        fmt.Println("No key given")
        rerr = 1
        return
    }
    client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":"+alt_port)
    if err != nil {
        rerr = 2
        fmt.Println("could not reach alternator")
        return
    }
    val := []byte(v)
    fmt.Println("Putting pair " + k + "," + v)
    putArgs := alternator.PutArgs{Name: k, V: val, Replicants: dests, Success: 0}
    err = client.Call("Node."+"Put", &putArgs, &struct{}{})
    if err != nil {
        rerr = 3
        fmt.Println("RPC Failed")
        return
    }
    fmt.Println("Success!")
    return
}

func getmembers() (members []alternator.Peer, rerr int){
    client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":"+alt_port)
    fmt.Println("Getting members")
    if err != nil {
        rerr = 2
        fmt.Println("could not reach alternator")
        return
    }
    err = client.Call("Node."+"GetMembers", struct{}{}, &members)
    if err != nil {
        rerr = 3
        fmt.Println("RPC Failed")
        return
    }
    return
}

func kamino(put_req Put_req, pres *Put_res) (dests []alternator.Key) {
    // currently is crap, just gets all members
    mlist,_ := getmembers()
    for _,cmem := range mlist {
        dests = append(dests, cmem.ID)
    }
    return
}

package main

import (
        "fmt"
    "log"
    "net/http"
    "sync"
    "flag"
    "os"
    "os/exec"
    "strconv"
    //"encoding/hex"
    //"syscall"
    "encoding/json"
    //"io/ioutil"
    "github.com/ant0ine/go-json-rest/rest"
    "github.com/DiegoAlbertoTorres/alternator"
    "net/rpc"
    "github.com/deckarep/golang-set"
)

var config alternator.Config

var sigChan chan os.Signal

type Node_properties struct {
    Machine_id string           `json:"machine_id"`
    Space_total int             `json:"space_total"`
    Space_remaining int         `json:"pace_remaining"`
    Uptime float64              `json:"uptime"`
    Avg_bandwidth int           `json:"avg_bandwidth"`
    Peak_bandwidth int          `json:"peak_bandwidth"`
    Seq_read_speed int          `json:"seq_read_speed"`
    Rand_read_speed int         `json:"rand_read_speed"`
    Failure_rate float64        `json:"failure_rate"`
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
    Uptime float64                  `json:"uptime", omitempty`
    Avg_bandwidth int               `json:"avg_bandwidth",omitempty`
    Peak_bandwidth int              `json:"peak_bandwidth",omitempty`
    Durability_time int             `json:"durability_time",omitempty`
    Durability_percentage float64   `json:"durability_percentage",omitempty`
}

type Put_res struct {
    Err int                         `json:"err"`
    Req_satisfied int               `json:"req_satisfied"`
    Space_req int                   `json:"space_req",omitempty`
    Uptime float64                  `json:"uptime", omitempty`
    Avg_bandwidth int               `json:"avg_bandwidth",omitempty`
    Peak_bandwidth int              `json:"peak_bandwidth",omitempty`
    Durability_time int             `json:"durability_time",omitempty`
    Durability_percentage float64   `json:"durability_percentage",omitempty`
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
    flag.IntVar(&tal_timeout, "profiletimeout", 10, "sets the amount of time in miliseconds between node profile refreshes.")
    flag.StringVar(&alt_port, "dataport", "55482", "port that bombay will start the datastore service (alternator) on.")
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

    //http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000

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
    //cmd.Stdout = os.Stdout
    //cmd.Stderr = os.Stderr
    err := cmd.Start()
    printError(err)
}

func runTalese(port string, timeout int) {
    //pstr := os.Getenv("GOPATH")+"/src/bombay/talese.py"
    pstr := "./talese.py"
    cmd := exec.Command(pstr, "--port", port, "--timeout", strconv.Itoa(timeout))
    //cmd.Stdout = os.Stdout
    //cmd.Stderr = os.Stderr
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
    dlist := getallids()
    //name := node_properties.Machine_id + "_m"
    name := "http://172.28.153.53:" + alt_port + "_m"
    data, err := json.Marshal(node_properties)
    if err != nil {
        rest.Error(w, err.Error(), http.StatusInternalServerError)
        return
    } 
    rerr := putk(name, data, dlist)
    if rerr != 0 {
        rest.Error(w, strconv.Itoa(rerr), http.StatusInternalServerError)
        return
    }
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
    var val []byte
    val, get_res.Err = getk(name)
    get_res.Value = string(val)
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
    fmt.Println("Put request parameters success value: " + strconv.Itoa(put_res.Req_satisfied))
    //dlist := getallids()
    val := []byte(put_req.Value)
    rerr := putk(name, val, dlist)
    if rerr != 0 {
        put_res.Err = rerr
    }
    w.WriteJson(put_res)
}

func DeleteKey(w rest.ResponseWriter, r *rest.Request) {
    // stub
    w.WriteHeader(http.StatusOK)
}

func getk(k string) (val []byte, rerr int) {
    rerr = 0 // default no error
    if k == "" { // key field is empty
        rerr = 1 // set err = 1 to signify no key requested
        return
    }
    client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":"+alt_port)
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
    return
}

func putk(k string, val []byte, dests []alternator.Key) (rerr int) {
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
    //fmt.Println("Putting pair " + k + "," + v)
    fmt.Println("Putting in " + k)
    putArgs := alternator.PutArgs{Name: k, V: val, Replicators: dests, Success: 0}
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
    // only uses uptime for now
    mlist,_ := getmembers()
    mmap := make(map[string]alternator.Key)
    propmap := make(map[string]*Node_properties)
    nodeset := mapset.NewSet()
    for _,cmem := range mlist {
        //dests = append(dests, cmem.ID)
        nodeset.Add(cmem.Address)
        admod := "http://" + cmem.Address + "_m"
        mmap[cmem.Address] = cmem.ID
        cprop := Node_properties{}
        cdat,_ := getk(admod)
        err := json.Unmarshal(cdat, &cprop)
        if err != nil {
            fmt.Println("Failed to Unmarshal " + admod)
        }
        propmap[cmem.Address] = &cprop
    }
    npset := nodeset.PowerSet()
    nplist := npset.ToSlice()
    var cmatchs []interface{}
    cmatchi := 0.0
    pres.Req_satisfied = 0
    for _,cset := range nplist {
        clist := cset.(mapset.Set).ToSlice()
        cfailp := 1.0
        for _,cnode := range clist {
            cfailp = cfailp * (1.0 - propmap[cnode.(string)].Uptime)
        }
        cuptime := 1.0 - cfailp
        if cuptime >= put_req.Uptime {
            pres.Req_satisfied = 1
            cmatchi = cuptime
            cmatchs = clist
            break
        }
        if cuptime > cmatchi {
            cmatchi = cuptime
            cmatchs = clist
        }
    }
    pres.Uptime = cmatchi
    for _,cmod := range cmatchs {
        dests = append(dests, mmap[cmod.(string)])
    }
    return
}

func getallids() (dests []alternator.Key) {
    // get a list of all member ids
    mlist,_ := getmembers()
    for _,cmem := range mlist {
        dests = append(dests, cmem.ID)
        fmt.Println("http://" + cmem.Address + "_m")
    }
    return
}

package main

import (
    "fmt"
    "net/http"
    "github.com/golang/protobuf/proto"
    "proto/test"
    "proto/stock"
    "bytes"
    "encoding/json"
    //"os"
    "gopkg.in/redis.v3"
    "time"
)

var STOCK_DETAIL_KEY_PREFIX = "cls_stock_detail_"//cls_stock_detail_<stock_id>
var STOCK_KLINE_DAY_DETAIL_PREFIX = "cls_kline_day_"//cls_stock_detail_<stock_id>_<timestamp>
var STOCK_KLINE_DAY_LIST_PREFIX = "cls_kline_day_list_" //cls_kline_day_list_<timestamp>
var STOCK_PRICE_RANK_PREFIX = "cls_stock_price_rank_"//cls_stock_price_rank_<stock_id>

var rds *redis.Client

func formatTime(timestamp int64) int64 {
    loc, _ := time.LoadLocation("Asia/Shanghai")
    tm := time.Unix(timestamp, 0)
    t := tm.Format("2006-01-02")
    tm2, _ := time.ParseInLocation("2006-01-02", t, loc)

    return  tm2.Unix()
}

func initRedis()(client *redis.Client, err error) {
    client = redis.NewClient(&redis.Options{
        Addr : "localhost:8878",
        Password: "",
        DB: 0,
    })

    _, err = client.Ping().Result()
    if err != nil  {
        return nil, err
    }

    return client, nil
}

func doRealtimeDataDetail(realtimeData *stock.RealtimeData) {
    /*rds, err := initRedis()
    if err != nil {
        return
    }*/

    //存详情
    detailKey := STOCK_DETAIL_KEY_PREFIX + realtimeData.GetMSzLabel()
    detail, err := json.Marshal(realtimeData)
    if err == nil {
        //n := bytes.Index(detail, []byte{0})
        detail_str := string(detail[:])
        rds.Set(detailKey, detail_str, 0)
    }

    //存最新价格排序
    rankSortedSetKey := STOCK_PRICE_RANK_PREFIX + realtimeData.GetMSzLabel()
    scoreMember := redis.Z{float64(realtimeData.GetMFNewPrice()), detailKey}
    rds.ZAdd(rankSortedSetKey, scoreMember)

}

func handler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hi there, I love %s!\n", r.URL.Path[1:])
    fmt.Println("Hi there")
    buf := new(bytes.Buffer)
    buf.ReadFrom(r.Body)
    newTest := &test.Test{}
    err := proto.Unmarshal(buf.Bytes(), newTest)
    if err != nil {
        fmt.Println("unmarshaling error: ")
    }
    fmt.Println(newTest.GetValue());
}

func realtimeDataHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Println("realtime data")
    buf := new(bytes.Buffer)
    buf.ReadFrom(r.Body)
    data := &stock.RealtimeDataRequest{}
    err := proto.Unmarshal(buf.Bytes(), data)
    if err != nil {
        fmt.Println("unmarshaling error: ")
    }
    fmt.Println(data.GetFrom())
    //fmt.Println(data.GetData())
    realtimeData := data.GetData()

    for _, value := range realtimeData {
        doRealtimeDataDetail(value)
    }
}

func financeDataHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Println("finance")
    buf := new(bytes.Buffer)
    buf.ReadFrom(r.Body)
    data := &stock.FinanceDataRequest{}
    err := proto.Unmarshal(buf.Bytes(), data)

    if err != nil {
        fmt.Println("unmarshaling error: ")
    }

    finance_data := data.GetData()
    for index, value :=  range finance_data {
        fmt.Printf("%d, %s \n", index, value.GetMSzLabel())
    }
}

func doKLineData(stockId string, klineDatas []*stock.KLineData) {
    //TODO 计算周k和月k
    members := make([]redis.Z, 5)
    sortedsetKey := STOCK_KLINE_DAY_LIST_PREFIX + stockId
    var membersPair =  make([]string, 10)
    for index, value := range klineDatas {
        time32 := value.GetMTime()
        time64 := int64(time32)
        formatTimestamp := formatTime(time64)
        memberKey := STOCK_KLINE_DAY_DETAIL_PREFIX + stockId + "_" + fmt.Sprintf("%d", formatTimestamp)
        i := index % 5
        s, _ := json.Marshal(value)
        str := string(s[:])
        membersPair = append(membersPair, memberKey, str)
        members[i] = redis.Z{float64(formatTimestamp), memberKey}
        if i == 4 {
            rds.ZAdd(sortedsetKey, members...)
            rds.MSet(membersPair...)
            membersPair = make([]string, 10)
        }

    }

}

func kLineDataHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Println("kline")
    buf := new(bytes.Buffer)
    buf.ReadFrom(r.Body)
    data := &stock.KLineDataRequest{}
    err := proto.Unmarshal(buf.Bytes(), data)
    if err != nil {
        fmt.Println("kline unmarshaling error:")
        fmt.Println(err)
    }

    stockId := data.GetMSzLabel()
    klineDatas := data.GetData()
    doKLineData(stockId, klineDatas)

}

func main() {
    var err error
    rds, err  = initRedis()
    if err != nil {
        fmt.Println("redis connect error")
        return
    }
    http.HandleFunc("/message", handler)
    http.HandleFunc("/realtime", realtimeDataHandler)
    http.HandleFunc("/finance", financeDataHandler)
    http.HandleFunc("/kline", kLineDataHandler)
    http.ListenAndServe(":8081", nil)
}

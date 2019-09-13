package agdb

import (
	"encoding/json"
	"sync"
	"time"
)


type resolution uint8

const (
	Second   resolution = 1
	Minute   resolution = 2
	Hour     resolution = 3
	Day      resolution = 4
	Month    resolution = 6
	Year     resolution = 7
)

type Aggregator struct {
	Resolution resolution
	MaxRetain  time.Duration
	rw sync.Mutex
	data []*map[string]int64
	current *map[string]int64
}

func NewAggregator(res resolution,maxRetain time.Duration) *Aggregator {
	agg := Aggregator{
		Resolution:res,
		MaxRetain:maxRetain,
		rw : sync.Mutex{},
		data: []*map[string]int64{},
	}
	agg.move()
	if res == Second{
		go func() {
			for{
				time.Sleep(1*time.Second)
				agg.move()
			}
		}()
	}else{
		go func() {
			for{
				time.Sleep(agg.rTime())
				agg.move()
			}
		}()
	}
	return &agg
}


func (agg *Aggregator)Increment(key string,value int)  {
	agg.rw.Lock()
	if _, ok := (*agg.current)[key]; !ok {
		(*agg.current)[key] = 0
	}
	(*agg.current)[key] += int64(value)
	agg.rw.Unlock()
}

func (agg *Aggregator)Decrement(key string,value int)  {
	agg.rw.Lock()
	if _, ok := (*agg.current)[key]; !ok {
		(*agg.current)[key] = 0
	}
	(*agg.current)[key] -= int64(value)
	agg.rw.Unlock()
}

func (agg *Aggregator)move()  {
	agg.rw.Lock()
	m := map[string]int64{
		"_AGTIME_":int64(time.Now().Unix()),
	}
	agg.data = append(agg.data,&m)
	agg.current = &m
	if time.Now().Unix() - (*agg.data[0])["_AGTIME_"] > int64(agg.MaxRetain.Seconds()){
		agg.data = agg.data[1:]
	}
	agg.rw.Unlock()

}

func (agg *Aggregator)rTime() time.Duration {
	now :=  time.Now()
	var t2 time.Time

	if agg.Resolution == Minute{
		t2 = time.Date(now.Year(),now.Month(),now.Day(),now.Hour(),now.Minute(),0,0,now.Location())
		t2 = t2.Add(time.Minute)
	}
	if agg.Resolution == Hour{
		t2 = time.Date(now.Year(),now.Month(),now.Day(),now.Hour(),0,0,0,now.Location())
		t2 = t2.Add(time.Hour)
	}
	if agg.Resolution == Day{
		t2 = time.Date(now.Year(),now.Month(),now.Day(),0,0,0,0,now.Location())
		t2 = t2.AddDate(0,0,1)
	}


	if agg.Resolution == Month{
		t2 = time.Date(now.Year(),now.Month(),1,0,0,0,0,now.Location())
		t2 = t2.AddDate(0,1, 0)
	}

	if agg.Resolution == Year{
		t2 = time.Date(now.Year(),1,1,0,0,0,0,now.Location())
		t2 = t2.AddDate(1,0, 0)
	}
	r := t2.Sub(now)
	return r
}

func (agg *Aggregator)Pack() ([]byte,error) {
	var b []byte
	var err error
	var dup []map[string]int64
	for _,item := range agg.data{
		dup = append(dup,*item)
	}
	b,err = json.Marshal(dup)
	return b,err
}

func (agg *Aggregator)Load(b []byte) error {
	agg.rw.Lock()
	var load []map[string]int64
	err := json.Unmarshal(b,&load)
	if err != nil{
		return err
	}
	agg.data = []*map[string]int64{}
	now := time.Now()
	for _,item := range load{
		if now.Unix() - item["_AGTIME_"] < int64(agg.MaxRetain.Seconds()) {
			agg.data = append(agg.data, &item)
		}
	}
	if len(agg.data) > 0 && agg.Resolution != Second{
		last := agg.data[len(agg.data) - 1]
		t := time.Unix((*last)["_AGTIME_"],0)
		if agg.Resolution == Minute && (t.Minute() == now.Minute() && t.Hour() == now.Hour() && t.Day() == now.Day() && t.Month() == t.Month() && t.Year() == t.Year()){
			agg.current = last
			return nil
		} else if agg.Resolution == Hour && (t.Hour() == now.Hour() && t.Day() == now.Day() && t.Month() == t.Month() && t.Year() == t.Year()){
			agg.current = last
			return nil
		}else if agg.Resolution == Day && (t.Day() == now.Day() && t.Month() == t.Month() && t.Year() == t.Year()){
			agg.current = last
			return nil
		}else if agg.Resolution == Month && (t.Month() == t.Month() && t.Year() == t.Year()){
			agg.current = last
			return nil
		}else if agg.Resolution == Year && (t.Year() == t.Year()){
			agg.current = last
			return nil
		}

	}

	agg.move()

	agg.rw.Unlock()
	return nil
}

func (agg *Aggregator)Get(keys []string,start,end time.Time) map[string]int64 {
	res := map[string]int64{}
	now := time.Now().Unix()
	for _,item := range agg.data{
		if now - (*item)["_AGTIME_"] < int64(agg.MaxRetain.Seconds()) {
			for key, val := range *item {
				if len(keys) == 0 || agg.contains(keys,key) {
					if _, ok := res[key]; !ok {
						res[key] = 0
					}
					res[key] += val
				}
			}
		}

	}
	delete(res,"_AGTIME_")
	return res

}

func (agg *Aggregator)contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
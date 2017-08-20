// based on https://github.com/kkdai/basiclist

package slist

import (
    "errors"
    "fmt"
    "log"
    "github.com/valyala/fasthttp"
    "time"
    "github.com/bearbin/go-age"  // TODO: get rid of it
)

type BasicNote struct {
    key      int
    val      interface{}
    nextNode *BasicNote
}

type BasicList struct {
    head *BasicNote
}

// TODO: make naming nice
type Idx_locations_avg struct {
    // sort key = Visit

    // key
    Visited_at int  // visit
    Birth_date int  // user
    Gender string   // user

    // data
    Mark int        // visit
}

type Idx_users_visits struct {
    Visited_at int  // sorting key, but also used in Raw rendering

    // key
    Visit int       // visit
    Distance int    // location
    Country string  // location

    // data
    Mark int        // visit
    Place string    // location

    Raw  []byte
}

//NewBasicList : Init structure for basic Sorted Linked List.
func NewBasicList() *BasicList {
    var empty interface{}
    return &BasicList{head: &BasicNote{key: 0, val: empty, nextNode: nil}}
}

func (b *BasicList) Insert(key int, value interface{}) {
	if b.head == nil {
		// fmt.Println("note is empty")
		b.head = &BasicNote{key: key, val: value, nextNode: nil}
	} else {
		var currentNode *BasicNote
		currentNode = b.head
		var previouNote *BasicNote
		var found bool
		newNode := &BasicNote{key: key, val: value, nextNode: nil}

		for {
			if currentNode.key > key {
				newNode.nextNode = previouNote.nextNode
				previouNote.nextNode = newNode
				found = true
				break
			}

			if currentNode.nextNode == nil {
				break
			}

			previouNote = currentNode
			currentNode = currentNode.nextNode
		}

		if found == false {
			currentNode.nextNode = newNode
		}
	}
}

func (b *BasicList) Search(key int) (interface{}, error) {
	log.Println("dump");
    currentNode := b.head
	for {
		if currentNode.key == key {
			return currentNode.val, nil
		}

		if currentNode.nextNode == nil {
			break
		}
		currentNode = currentNode.nextNode
	}
	return nil, errors.New("Not found.")
}

func (b *BasicList) SearchIdx(key int) (interface{}, error) {
	currentNode := b.head
	for {
		if currentNode.key == key {
			//log.Println("SearchIdx:", currentNode.val)
			return currentNode.val, nil
		}

		if currentNode.nextNode == nil {
			break
		}
		currentNode = currentNode.nextNode
	}
	return nil, errors.New("Not found.")
}

func(b *BasicList) SearchAll(key int) () {
	currentNode := b.head
    
	for {
		if currentNode.key == key {
			log.Println("FOUND:", currentNode.val)
		} else if currentNode.key > key {
            return
        }
		currentNode = currentNode.nextNode
	}
}

func (b *BasicList) Remove(key int) (*Idx_locations_avg, error) {
	currentNode := b.head
	var previouNote *BasicNote
	for {
        //log.Println(currentNode.key, key)
		if currentNode.key == key {
			previouNote.nextNode = currentNode.nextNode
			return currentNode.val.(*Idx_locations_avg), nil
		}

		if currentNode.nextNode == nil {
			break
		}
		previouNote = currentNode
		currentNode = currentNode.nextNode
	}
	return nil, errors.New("Not found key.")
}

func (b *BasicList) RemoveByVisit(Visit int) (*Idx_users_visits, error) {
    currentNode := b.head

    var previouNote *BasicNote
    for {
        if currentNode.val != nil {
            val := currentNode.val.(*Idx_users_visits)
            if val.Visit == Visit {
                previouNote.nextNode = currentNode.nextNode
                return currentNode.val.(*Idx_users_visits), nil
            }
        }

        if currentNode.nextNode == nil {
            break
        }
        previouNote = currentNode
        currentNode = currentNode.nextNode
    }
    return nil, errors.New("Not found key.")
}

func (b *BasicList) DisplayAll() {
	fmt.Println("")
	fmt.Printf("head->")
	currentNode := b.head
	for {
		fmt.Printf("[key:%d][val:%v]->\n", currentNode.key, currentNode.val)
		if currentNode.nextNode == nil {
			break
		}
		currentNode = currentNode.nextNode
	}
	fmt.Printf("nil\n")
}

func (b *BasicList) CalcAvg(skipFromDate bool, skipToDate bool, skipFromAge bool, skipToAge bool, skipGender bool, fromDate int, toDate int, fromAge int, toAge int, gender string)(avg string) {
	if b.head.nextNode == nil {  // no marks of this location
		return "0.0"
	}

	currentNode := b.head.nextNode
	sum := 0
	cnt := 0
	for {
        // Visited_at int
        // Birth_date int
        // Gender string
        // Mark int  // data

		val := currentNode.val.(*Idx_locations_avg)
		
		// 3600*24*365.25 = 31557600
		currentAge := age.Age(time.Unix(int64(val.Birth_date), 0))
        //log.Println(val.Visited_at, val.Birth_date, val.Gender, val.Mark)
		//log.Println("age =", currentAge)
		//log.Println(age.Age(time.Unix(1502626582-3600*24*364, 0)))
		
		matched :=
		    (skipFromDate || val.Visited_at > fromDate) &&
			(skipToDate || val.Visited_at < toDate) &&
			(skipFromAge || currentAge >= fromAge) &&
			(skipToAge || currentAge < toAge) &&
			(skipGender || gender == val.Gender)
		
		if matched {
            //log.Println("matched", val.Visited_at, val.Birth_date, val.Gender, val.Mark)
			sum += val.Mark
			cnt++
		}

		if currentNode.nextNode == nil {
			break
		}
		currentNode = currentNode.nextNode
	}

	//strconv.FormatFloat(10.900, 'f', -1, 64) => '10.9'
	if cnt == 0 {
		return "0.0"
	}
	return fmt.Sprintf("%.6g", float64(sum) / float64(cnt))
}

func (b *BasicList) VisitsHandler(ctx *fasthttp.RequestCtx, skipFromDate bool, skipToDate bool, skipCountry bool, skipToDistance bool, fromDate int, toDate int, country string, toDistance int) () {
    ctx.Write([]byte("{\"visits\":["))

	if b.head.nextNode == nil {  // no visits of this user
		ctx.Write([]byte("]}"))
        return
	}

	currentNode := b.head.nextNode

	first_entry := true
	
	for {
		val := currentNode.val.(*Idx_users_visits)
		Visited_at := currentNode.key
        //log.Println(val.Visit, Visited_at, val.Distance, val.Country, val.Mark, val.Place)

		matched :=
		    (skipFromDate || Visited_at > fromDate) &&
			(skipToDate || Visited_at < toDate) &&
			(skipCountry || val.Country == country) &&
			(skipToDistance || val.Distance < toDistance)

		if matched {
			if first_entry {
				first_entry = false
			} else {
				ctx.Write([]byte(","))
			}
            ctx.Write(val.Raw)
		}

		if currentNode.nextNode == nil {
			break
		}

		currentNode = currentNode.nextNode
	}
    ctx.Write([]byte("]}"))
}

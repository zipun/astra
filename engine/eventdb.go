package engine

//Using BadgerDB as persistance store instead of RocksDB and lighten up CGO dependency

import(
  "errors"

  glog "github.com/golang/glog"
	db "github.com/dgraph-io/badger"
)

type EventDB struct {
  Datapath   string
	eventDB   *db.DB
	Name       string
  options    db.Options
	Err        error
}

//EventrangeRocksDB - Structure to hold range for reading from RocksDB
type Eventrange struct {
	Start int
	Limit int
}

//OpenDB - function to connect to Event Database
func (s *EventDB) OpenDB() error{
  //Default options sync write is set to true
  s.options = db.DefaultOptions(s.Datapath)
  s.options.SyncWrites = true //This is turned on in default options however resetting this one up just incase
  s.eventDB, s.Err = db.Open(s.options)
  if s.Err != nil {
	  glog.Fatal("Error creating badger database. Failed due to %s[%s]\n", s.Err, s.Datapath)
    return s.Err
  }
  return nil
}

//WritetoDB - for writing events to store
func (s *EventDB) WritetoDB(k string, v string) error {
  if s.eventDB != nil{
    s.Err = nil
    s.Err = s.eventDB.Update(func (trx *db.Txn) error{
      ne := db.NewEntry([]byte(k), []byte(v))
      err := trx.SetEntry(ne)
      if err != nil {
        glog.Errorf("Write operation failed with err: %s\n", s.Err)
      }
      return err
    })
    return s.Err
  }
  return errors.New("Event DB does not exist")
}

//WriteBatchtoDB - function to write batches to DB
func (s *EventDB) WriteBatchtoDB(kvmap map[string]string) error {
  if s.eventDB != nil{
    wb := s.eventDB.NewWriteBatch()
    defer wb.Cancel() //this will take care of cleaning incase of errors
    s.Err = nil
    for key := range kvmap {
			s.Err = wb.Set([]byte(key), []byte(kvmap[key]))
      if s.Err != nil {
        glog.Errorf("Write batch operation failed with err: %s\n", s.Err)
        return s.Err
      }
		}
    wb.Flush()//This will take care of commit
    return s.Err
  }
  return errors.New("Event DB does not exist")
}

//ReadFromDB - function to read from event DB
func (s *EventDB) ReadFromDB(k string) (string,error) {
  var v string
  if s.eventDB != nil {
    s.Err = nil
    var item *db.Item
    s.Err = s.eventDB.View(func (trx *db.Txn) error {
      item, s.Err = trx.Get([]byte(k))
      if s.Err != nil {
        glog.Errorf("Read operation failed with Key not found Error: %s\n", s.Err)
        return s.Err
      }
      s.Err = item.Value(func (val []byte) error {
        v = string(append([]byte{}, val...))
        return nil
      })
      return s.Err
    })
    return v, s.Err
  }
  return v, errors.New("Event DB does not exist")
}

//ReadListFromDB - function to read a list of values from DB
func (s *EventDB) ReadListFromDB(count int) ([]string, error){
  vlist := make([]string, count)
  defer func(){vlist = nil}()
  if s.eventDB != nil {
    s.Err = nil
    s.Err = s.eventDB.View(func (trx *db.Txn) error {
      itr := trx.NewIterator(db.DefaultIteratorOptions)
      defer itr.Close()
      index := 0
      for itr.Rewind(); itr.Valid() && (index<count); itr.Next(){
        item := itr.Item()
        //k := item.Key()
        s.Err = item.Value(func (val []byte) error{
          v := append([]byte{}, val...)
          vlist[index] = string(v)
          return s.Err
        })
        index = index + 1
      }
      return s.Err
    })
    return vlist, s.Err
  }
  return vlist, errors.New("Event DB does not exist")
}

//ReadListCountFromDB - function to read list of values from DB and return the count of list
func (s *EventDB) ReadListCountFromDB(count int) ([]string, int, error){
  vlist := make([]string, count)
  defer func(){vlist = nil}()
  if s.eventDB != nil {
    s.Err = nil
    index := 0
    s.Err = s.eventDB.View(func (trx *db.Txn) error {
      itr := trx.NewIterator(db.DefaultIteratorOptions)
      defer itr.Close()
      for itr.Rewind(); itr.Valid() && (index<count); itr.Next(){
        item := itr.Item()
        //k := item.Key()
        s.Err = item.Value(func (val []byte) error{
          v := append([]byte{}, val...)
          vlist[index] = string(v)
          return s.Err
        })
        index = index + 1
      }
      return s.Err
    })
    return vlist, index, s.Err
  }
  return vlist, 0, errors.New("Event DB does not exist")
}

//ReadAllListFromDB - function to read all the values from DB
func (s *EventDB) ReadAllListFromDB() (map[string]string, error){
  vmap := make(map[string]string)
  defer func(){vmap=nil}()
  if s.eventDB != nil {
    s.Err = nil
    s.Err = s.eventDB.View(func (trx *db.Txn) error {
      itr := trx.NewIterator(db.DefaultIteratorOptions)
      defer itr.Close()
      for itr.Rewind(); itr.Valid(); itr.Next(){
        item := itr.Item()
        k := item.Key()
        s.Err = item.Value(func (val []byte) error{
          v := append([]byte{}, val...)
          vmap[string(k)] = string(v)
          return s.Err
        })
      }
      return s.Err
    })
    return vmap, s.Err
  }
  return vmap, errors.New("Event DB does not exist")
}

/*
ReadKeyRangeFromDB - function used to read data provided a range
*/
func (s *EventDB) ReadKeyRangeFromDB(readRange Eventrange) ([]string, error) {
  vlist := make([]string, readRange.Limit)
  defer func(){vlist = nil}()
  if s.eventDB != nil {
    s.Err = nil
    s.Err = s.eventDB.View(func (trx *db.Txn) error {
      itr := trx.NewIterator(db.DefaultIteratorOptions)
      defer itr.Close()
      index := 0
      count := 0
      for itr.Rewind(); itr.Valid() && (index<readRange.Limit); itr.Next(){
        if count >= readRange.Start {
          item := itr.Item()
          //k := item.Key()
          s.Err = item.Value(func (val []byte) error{
            v := append([]byte{}, val...)
            vlist[index] = string(v)
            return s.Err
          })
          index = index + 1
        }
        count = count + 1
      }//End of for loop
      return s.Err
    })
    return vlist, s.Err
  }
  return vlist, errors.New("Event DB does not exist")
}

//DeleteFromDB - function to delete key from data store
func (s *EventDB) DeleteFromDB(k string) (bool, error){
  if s.eventDB != nil {
    s.Err = nil
    s.Err = s.eventDB.Update(func(txn *db.Txn) error {
      err := txn.Delete([]byte(k))
      return err
    })
    if s.Err != nil{
      glog.Errorf("Deleted operation failed for Key: %s, Err: %s\n", k, s.Err)
      return true, s.Err
    }
    return true, s.Err
  }
  return false, errors.New("Event DB does not exist")
}

//CloseDB - function to close connection to EventDB
func (s *EventDB) CloseDB() error{
  if s.eventDB != nil {
    return s.eventDB.Close()
  }
  return nil
}

package engine

// #cgo LDFLAGS: -L./lib/rocksdb -lstdc++ -lm -lbz2 -lz -lsnappy -lrocksdb
import "C"
import (
	"errors"

	glog "github.com/glog-master"
	rocksdbapi "github.com/tecbot/gorocksdb"
)

//RocksDBInstance - Structure to hold multiple instances of RocksDB
type RocksDBInstance struct {
	Datapath   string
	RocksdbRef *rocksdbapi.DB
	Name       string
	Err        error
}

//EventrangeRocksDB - Structure to hold range for reading from RocksDB
type EventrangeRocksDB struct {
	Start int
	Limit int
}

/*
ConnectRocksDB - function used to open a connection to RocksDB
*/
func (r *RocksDBInstance) ConnectRocksDB() error {
	opts := rocksdbapi.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetUseFsync(true)
	defer opts.Destroy()
	r.Err = nil
	r.RocksdbRef, r.Err = rocksdbapi.OpenDb(opts, r.Datapath)
	if r.Err != nil {
		glog.Errorf("Error creating rocks database. Failed due to %s[%s]\n", r.Err, r.Datapath)
		return r.Err
	}
	//glog.Infof("Data store created successfully...")
	return r.Err
}

/*
WritetoRocksDB - function used to write data to RocksDB
*/
func (r *RocksDBInstance) WritetoRocksDB(key string, value string) error {
	if r.RocksdbRef != nil {
		wopts := rocksdbapi.NewDefaultWriteOptions()
		defer wopts.Destroy()
		wopts.SetSync(true)
		r.Err = nil
		r.Err = r.RocksdbRef.Put(wopts, []byte(key), []byte(value))
		if r.Err != nil {
			glog.Errorf("Write operation failed badly with err: %s\n", r.Err)
			return r.Err
		}
		//glog.Infoln("write operation successful")
		return r.Err
	}
	return errors.New("rocksdb is holding invalid reference")
}

/*
WriteBatchtoRocksDB - function used to write data to RocksDB
*/
func (r *RocksDBInstance) WriteBatchtoRocksDB(kv map[string]string) error {
	if r.RocksdbRef != nil {
		wopts := rocksdbapi.NewDefaultWriteOptions()
		wopts.SetSync(true)
		defer wopts.Destroy()
		wbatch := rocksdbapi.NewWriteBatch()
		defer wbatch.Destroy()

		r.Err = nil
		for key := range kv {
			wbatch.Put([]byte(key), []byte(kv[key]))
		}
		r.Err = r.RocksdbRef.Write(wopts, wbatch)
		if r.Err != nil {
			glog.Errorf("Write operation failed badly with err: %s\n", r.Err)
			return r.Err
		}
		//glog.Infoln("write operation successful")
		return nil
	}
	return errors.New("rocksdb is holding invalid reference")
}

/*
ReadfromRocksDB - function used to read data from RocksDB provided a key
*/
func (r *RocksDBInstance) ReadfromRocksDB(key string) (string, error) {
	if r.RocksdbRef != nil {
		ropts := rocksdbapi.NewDefaultReadOptions()
		defer ropts.Destroy()
		data, err := r.RocksdbRef.Get(ropts, []byte(key))
		defer data.Free()
		if err != nil {
			glog.Errorf("error reading key from database. Error: %s\n", err)
			return "", err
		}
		if data.Size() > 0 {
			glog.Errorf("read value for key successfully. value: %s\n", key)
		}
		return string(data.Data()[:]), err
	}
	return "", errors.New("rocksdb is holding invalid reference")
}

/*
ReadRocksDBEntries - function used to read data based on key count from RocksDB
*/
func (r *RocksDBInstance) ReadRocksDBEntries(msgcount int) ([]string, error) {
	msgarray := make([]string, msgcount)
	defer func() { msgarray = nil }()
	if r.RocksdbRef != nil {
		ropts := rocksdbapi.NewDefaultReadOptions()
		defer ropts.Destroy()
		readiter := r.RocksdbRef.NewIterator(ropts)
		defer readiter.Close()
		count := 0
		for readiter.SeekToFirst(); readiter.Valid() && count < msgcount; readiter.Next() {
			key := readiter.Key().Data()
			val := readiter.Value().Data()
			//msgarray[count] = fmt.Sprintf("{\"key\" : %s, \"val\": %s}", key, val)
			msgarray[count] = string(val)
			glog.Errorf("data retrieved from iterator with key: %s and value: %s \n", key, val)
			readiter.Key().Free()
			readiter.Value().Free()
			count++
		}
		return msgarray, nil
	}
	return msgarray, errors.New("rocksdb is holding invalid reference")
}

/*
ReadRocksDBEntriesWithcount - function used to read data based on key count from RocksDB
*/
func (r *RocksDBInstance) ReadRocksDBEntriesWithcount(msgcount int) ([]string, int, error) {
	msgarray := make([]string, msgcount)
	defer func() { msgarray = nil }()
	if r.RocksdbRef != nil {
		ropts := rocksdbapi.NewDefaultReadOptions()
		defer ropts.Destroy()
		readiter := r.RocksdbRef.NewIterator(ropts)
		defer readiter.Close()
		count := 0
		for readiter.SeekToFirst(); readiter.Valid() && count < msgcount; readiter.Next() {
			key := readiter.Key().Data()
			val := readiter.Value().Data()
			//msgarray[count] = fmt.Sprintf("{\"key\" : %s, \"val\": %s}", key, val)
			msgarray[count] = string(val)
			glog.Errorf("data retrieved from iterator with key: %s and value: %s \n", key, val)
			readiter.Key().Free()
			readiter.Value().Free()
			count++
		}
		return msgarray, count, nil
	}
	return msgarray, 0, errors.New("rocksdb is holding invalid reference")
}

/*
ReadRocksDBEntriesAll - function used to read data based on key count from RocksDB
*/
func (r *RocksDBInstance) ReadRocksDBEntriesAll() (map[string]string, error) {
	msgmap := make(map[string]string)
	defer func() { msgmap = nil }()
	if r.RocksdbRef != nil {
		ropts := rocksdbapi.NewDefaultReadOptions()
		defer ropts.Destroy()
		readiter := r.RocksdbRef.NewIterator(ropts)
		defer readiter.Close()
		count := 0
		for readiter.SeekToFirst(); readiter.Valid(); readiter.Next() {
			key := readiter.Key().Data()
			val := readiter.Value().Data()
			//msgmap[string(key)] = fmt.Sprintf("{\"key\" : %s, \"val\": %s}", key, val)
			msgmap[string(key)] = string(val)
			glog.Errorf("data retrieved from iterator with key: %s and value: %s \n", key, val)
			readiter.Key().Free()
			readiter.Value().Free()
			count++
		}
		return msgmap, nil
	}
	return msgmap, errors.New("rocksdb is holding invalid reference")
}

/*
ReadRocksDBonRange - function used to read data provided a range
*/
func (r *RocksDBInstance) ReadRocksDBonRange(readRange EventrangeRocksDB) ([]string, error) {
	msgarray := make([]string, readRange.Limit)
	defer func() { msgarray = nil }()
	if r.RocksdbRef != nil {
		ropts := rocksdbapi.NewDefaultReadOptions()
		defer ropts.Destroy()
		readiter := r.RocksdbRef.NewIterator(ropts)
		defer readiter.Close()
		count := 0
		currentkey := 0
		//glog.Infof("request to retrieve data from wal [%s] with range: [%d %d]\n", r.Name, readRange.Start, readRange.Limit)
		for readiter.SeekToFirst(); readiter.Valid() && count < readRange.Limit; readiter.Next() {
			if currentkey >= (readRange.Start - 1) {
				key := readiter.Key().Data()
				val := readiter.Value().Data()
				//msgarray[count] = fmt.Sprintf("{ \"key\" : \"%s\", \"val\": %s}", key, val)
				msgarray[count] = string(val)
				glog.Infof("data retrieved from wal [%s] with key: %s\n", r.Name, key)
				readiter.Key().Free()
				readiter.Value().Free()
				count++
			}
		}
		return msgarray, nil
	}
	return msgarray, errors.New("rocksdb is holding invalid reference")
}

/*
DeletefromRocksDB - function to delete data from RocksDB provided a key
*/
func (r *RocksDBInstance) DeletefromRocksDB(key string) (bool, error) {
	if r.RocksdbRef != nil {
		r.Err = nil
		wopts := rocksdbapi.NewDefaultWriteOptions()
		defer wopts.Destroy()
		//wopts.SetSync(true) - This is commented by JP for performance related improvements
		eventstr, _ := r.ReadfromRocksDB(key)
		if len(eventstr) > 0 {
			r.Err = r.RocksdbRef.Delete(wopts, []byte(key))
			if r.Err != nil {
				glog.Errorf("deleting key failed..error..%s\n", r.Err)
				return false, r.Err
			}
			glog.Infof("deleting key successful..Key deleted..%s\n", key)
			return true, r.Err
		}
		glog.Infof("Cannot delete key as it is not found in DB. Key: %s\n", key)
		return false, errors.New("Key not found to delete from DB")
	}
	return false, errors.New("rocksdb is holding invalid reference")
}

/*
CloseRocksDB - function to close RocksDB connection that is open
*/
func (r *RocksDBInstance) CloseRocksDB() error {
	if r.RocksdbRef != nil {
		r.RocksdbRef.Close()
		r.RocksdbRef = nil
		return nil
	}
	return errors.New("rocksdb is holding invalid reference")
}

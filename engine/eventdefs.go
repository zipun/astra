package engine

import (
	"encoding/json"
	"strconv"

	"github.com/golang/glog"
)

//Source type int to represent supported types
type Source int

//Assumptions in this case are that SRC is only supported from the constants below
//The numbers assigned here are weightage that might/will be used incase of aggregations
const (
	GCM   Source = 1
	OMEGA        = 2
	AXIS         = 3
	FSL          = 4
	CFO          = 5
)

/*
 The general naming convention followed here are:
  Event - refers to source Event
  AckEvent - refers to NATS ack Event
*/

/*
GCMEvent - type used for GCM event processing
*/
type GCMEvent struct {
	DPID          string `json:"DPID"`
	BUID          string `json:"BUID"`
	DPIDStatus    string `json:"DPIDStatus"`
	CompanyNumber string `json:"CompanyNumber"`
	EventDate     string `json:"EventDate"`
	UID           string
}

/*
GCMNONUSEvent - type used for GCM non US event processing
"DPID":"2005780294739","BUID":"3535","DPIDStatus":"OA","CompanyNumber":"29","EventDate":"2/25/2018 11:24:09 PM","IRN_NUM":"JP0135-6641-73757","COUNTRY_CODE":"jp","UID":"414D5120534F4D505244435530322020B0DF1C5A3490332B"
*/
type GCMNONUSEvent struct {
	DPID          string `json:"DPID"`
	BUID          string `json:"BUID"`
	DPIDStatus    string `json:"DPIDStatus"`
	CompanyNumber string `json:"CompanyNumber"`
	EventDate     string `json:"EventDate"`
	IRNNumber     string `json:"IRN_NUM"`
	CountryCode   string `json:"COUNTRY_CODE"`
	UID           string
}

/*
OmegaEvent json event type declaration
*/
type OmegaEvent struct {
	Messageid     string            `json:"msg_id"`
	Eventkey      string            `json:"event_key"`
	Priority      string            `json:"priority"`
	Senddate      string            `json:"send_date"`
	Receivedate   string            `json:"receive_date"`
	Correlationid string            `json:"correlation_id"`
	Eventname     string            `json:"event_name"`
	Eventdata     string            `json:"event_data"`
	Parameters    map[string]string `json:"parameter_list"`
	UID           string            `json:"eventuid"`
}

/*
AxisEvent json event type declaration
*/
type AxisEvent struct {
	Messageid       string `json:"msg_id"`
	OrderNumber     string `json:"order_number"`
	Buid            string `json:"buid"`
	Taskcode        string `json:"task_code"`
	Taskcompdate    string `json:"task_comp_date"`
	Subtaskcode     string `json:"sub_task_code"`
	TCFlag          string `json:"tc_flag"`
	Createddate     string `json:"creation_date"`
	RDDcode         string `json:"rdd_date"`
	Heldcode        string `json:"held_code"`
	CompanyNumber   string `json:"company_num"`
	Flexbillingcode string `json:"flex_billing_code"`
	Col1            string `json:"col1"`
	Col2            string `json:"col2"`
	Col3            string `json:"col3"`
	Col4            string `json:"col4"`
	UID             string `json:"eventuid"`
}

/*
FSLEvent json event type declaration
//{"key":"12343543","val":{"src_msg_id":"12343543","order_number":"1234","fabd":"2016-04-18 04:26:38","fsbd":"2016-04-18 04:26:38","global_bu_id":"","emc_po_number":"11","channel_status":"4000","status_desc":"Vendor Ack"}}
*/
type FSLEvent struct {
	Messageid         string `json:"src_msg_id"`
	OrderNumber       string `json:"order_number"`
	Fabd              string `json:"fabd"`
	Fsbd              string `json:"fsbd"`
	Buid              string `json:"global_bu_id"`
	EMCPONumber       string `json:"emc_po_number"`
	ChannelStatus     string `json:"channel_status"`
	StatusDescription string `json:"status_desc"`
}

/*
FSLAckEvent json event type declaration
//{"key":"12343543","val":{"src_msg_id":"12343543","order_number":"1234","fabd":"2016-04-18 04:26:38","fsbd":"2016-04-18 04:26:38","global_bu_id":"","emc_po_number":"11","channel_status":"4000","status_desc":"Vendor Ack"}}
*/
type FSLAckEvent struct {
	Messageid         string `json:"src_msg_id"`
	OrderNumber       string `json:"order_number"`
	Fabd              string `json:"fabd"`
	Fsbd              string `json:"fsbd"`
	Buid              string `json:"global_bu_id"`
	EMCPONumber       string `json:"emc_po_number"`
	ChannelStatus     string `json:"channel_status"`
	StatusDescription string `json:"status_desc"`
}

/*
OmegaAckEvent - Event used for Omega based workflow
*/
type OmegaAckEvent struct {
	Messageid     string            `json:"msg_id"`
	Eventkey      string            `json:"event_key"`
	Priority      string            `json:"priority"`
	Senddate      string            `json:"send_date"`
	Receivedate   string            `json:"receive_date"`
	Correlationid string            `json:"correlation_id"`
	Eventname     string            `json:"event_name"`
	Eventdata     string            `json:"event_data"`
	Parameters    map[string]string `json:"parameter_list"`
	UID           string            `json:"eventuid"`
	Ordernumber   string            `json:"order_number"`
	BUID          string            `json:"buid"`
}

/*
CFOAckEvent - Event used for Omega based workflow
*/
type CFOAckEvent struct {
	EventUID    string `json:"event_id"`
	Ordernumber string `json:"order_number"`
	Orderstatus string `json:"workorder_status"`
	Dpidnum     string `json:"dpid_num"`
}

/*
AxisAckEvent - Event used for Axis based workflow
*/
type AxisAckEvent struct {
	Messageid       string `json:"msg_id"`
	OrderNumber     string `json:"order_number"`
	Buid            string `json:"buid"`
	Taskcode        string `json:"task_code"`
	Taskcompdate    string `json:"task_comp_date"`
	Subtaskcode     string `json:"sub_task_code"`
	TCFlag          string `json:"tc_flag"`
	Createddate     string `json:"creation_date"`
	RDDcode         string `json:"rdd_date"`
	Heldcode        string `json:"held_code"`
	CompanyNumber   string `json:"company_num"`
	Flexbillingcode string `json:"flex_billing_code"`
	Col1            string `json:"col1"`
	Col2            string `json:"col2"`
	Col3            string `json:"col3"`
	Col4            string `json:"col4"`
	UID             string `json:"eventuid"`
}

/*
GCMAckEvent - type used for GCM event processing
*/
type GCMAckEvent struct {
	DPID          string `json:"DPID"`
	BUID          string `json:"BUID"`
	DPIDStatus    string `json:"DPIDStatus"`
	CompanyNumber string `json:"CompanyNumber"`
	EventDate     string `json:"EventDate"`
	UID           string
}

/*
GCMNONUSAckEvent - type used for GCM Non US event processing
"DPID":"2005780294739","BUID":"3535","DPIDStatus":"OA","CompanyNumber":"29","EventDate":"2/25/2018 11:24:09 PM","IRN_NUM":"JP0135-6641-73757","COUNTRY_CODE":"jp","UID":"414D5120534F4D505244435530322020B0DF1C5A3490332B"
*/
type GCMNONUSAckEvent struct {
	DPID          string `json:"DPID"`
	BUID          string `json:"BUID"`
	DPIDStatus    string `json:"DPIDStatus"`
	CompanyNumber string `json:"CompanyNumber"`
	EventDate     string `json:"EventDate"`
	IRNNumber     string `json:"IRN_NUM"`
	CountryCode   string `json:"COUNTRY_CODE"`
	UID           string
}

//AggrGenericEvent - used to aggregate multiple sources into single sequence
type AggrGenericEvent struct {
	UID      string `json:"UID"`
	Sequence uint64 `json:"SEQ"`
	Src      Source `json:"SRC"`
	SrcEvent string `json:"EVENT"`
	UIDX     int
}

/*
OmegaRocksAckDBKV json event type declaration as well as interface
*/
type OmegaRocksAckDBKV struct {
	Key string        `json:"key"`
	Val OmegaAckEvent `json:"val"`
}

/*
AxisRocksAckDBKV json event type declaration as well as interface
*/
type AxisRocksAckDBKV struct {
	Key string       `json:"key"`
	Val AxisAckEvent `json:"val"`
}

/*
GCMRocksAckDBKV json event type declaration as well as interface
*/
type GCMRocksAckDBKV struct {
	Key string      `json:"key"`
	Val GCMAckEvent `json:"val"`
}

/*
GCMNONUSRocksAckDBKV json event type declaration as well as interface
*/
type GCMNONUSRocksAckDBKV struct {
	Key string           `json:"key"`
	Val GCMNONUSAckEvent `json:"val"`
}

/*
CFORocksAckDBKV json event type declaration as well as interface
*/
type CFORocksAckDBKV struct {
	Key string      `json:"key"`
	Val CFOAckEvent `json:"val"`
}

/*
FSLRocksAckDBKV json event type declaration as well as interface
*/
type FSLRocksAckDBKV struct {
	Key string      `json:"key"`
	Val FSLAckEvent `json:"val"`
}

/*
OmegaAckEventParser - function to extract key and snapshot related details from the event
*/
func OmegaAckEventParser(kv RocksData, partitionsize uint64, partitionposition uint64, partitionlength uint64) (string, int64, string) {
	var data *OmegaRocksAckDBKV
	data = new(OmegaRocksAckDBKV)
	defer func() {
		data.Val.Parameters = nil
		data.Key = ""
		data = nil
	}()

	//glog.Infof("Current event string %s", kv.Event)
	err := json.Unmarshal([]byte(kv.Event), data)
	if err != nil {
		glog.Errorf("Error parsing JSON data. Error: %s\n" + err.Error())
		//panic("Error parsing JSON data.")
	}
	//glog.Infof("Current snapshot string %s", data.Val.UID)
	snapshot, err := strconv.ParseInt(data.Val.UID, 10, 64)
	if err != nil {
		glog.Errorf("Error occured in EventParser. Error msg: %s\n", err)
	}
	seqid := ""
	var sequenceid uint64
	sequenceid, err = strconv.ParseUint(data.Val.UID, 10, 64)
	if data.Val.Parameters["HEADER_ID"] != "" {
		sequenceid, err = strconv.ParseUint(data.Val.Parameters["HEADER_ID"], 10, 64)
	} else if data.Val.Parameters["ORDER_NUMBER"] != "" {
		sequenceid, err = strconv.ParseUint(data.Val.Parameters["ORDER_NUMBER"], 10, 64)
	}
	if err != nil {
		glog.Warningf("Could not set sequence number for event: %s\n", data.Key)
	} else {
		if partitionposition > 1 {
			//glog.Infof("Sequence ID considered. seq: %d\n", sequenceid)
			s := []byte(strconv.Itoa(int(sequenceid)))
			if len(s) > int(partitionposition) {
				if partitionsize < 10 {
					sequenceid = uint64(s[len(s)-int(partitionposition)])
				} else {
					seqid := make([]uint64, partitionlength)
					for idx := 0; idx < int(partitionlength); idx++ {
						seqid[idx], _ = strconv.ParseUint(string(s[len(s)-(int(partitionposition)+idx)]), 10, 64)
					}
					seed := uint64(0)
					for idx := (len(seqid) - 1); idx >= 0; idx-- {
						if idx == (len(seqid) - 1) {
							sequenceid = seqid[idx]
						} else {
							sequenceid = (sequenceid * 10) + seqid[idx]
						}
						seed = seed + seqid[idx]
					}
					sequenceid = sequenceid + seed
				}
			}
		}
		seqid = strconv.FormatUint(sequenceid%partitionsize, 10)
	}
	return data.Key, snapshot, seqid
}

/*
OmegaAckEventParserWithFilter - function to extract key and snapshot related details from the event
*/
func OmegaAckEventParserWithFilter(kv RocksData, partitionsize uint64, partitionposition uint64, partitionlength uint64) (string, string, string) {
	var data *OmegaRocksAckDBKV
	data = new(OmegaRocksAckDBKV)
	defer func() {
		data.Val.Parameters = nil
		data.Key = ""
		data = nil
	}()

	//glog.Infof("Current event string %s", kv.Event)
	err := json.Unmarshal([]byte(kv.Event), data)
	if err != nil {
		glog.Errorf("Error parsing JSON data. Error: %s\n" + err.Error())
		//panic("Error parsing JSON data.")
	}
	//glog.Infof("Current snapshot string %s", data.Val.UID)
	seqid := ""
	var sequenceid uint64
	sequenceid, err = strconv.ParseUint(data.Val.UID, 10, 64)
	if data.Val.Parameters["HEADER_ID"] != "" {
		sequenceid, err = strconv.ParseUint(data.Val.Parameters["HEADER_ID"], 10, 64)
	} else if data.Val.Parameters["ORDER_NUMBER"] != "" {
		sequenceid, err = strconv.ParseUint(data.Val.Parameters["ORDER_NUMBER"], 10, 64)
	}
	if err != nil {
		glog.Warningf("Could not set sequence number for event: %s\n", data.Key)
	} else {
		if partitionposition > 1 {
			//glog.Infof("Sequence ID considered. seq: %d\n", sequenceid)
			s := []byte(strconv.Itoa(int(sequenceid)))
			if len(s) > int(partitionposition) {
				if partitionsize < 10 {
					sequenceid = uint64(s[len(s)-int(partitionposition)])
				} else {
					seqid := make([]uint64, partitionlength)
					for idx := 0; idx < int(partitionlength); idx++ {
						seqid[idx], _ = strconv.ParseUint(string(s[len(s)-(int(partitionposition)+idx)]), 10, 64)
					}
					seed := uint64(0)
					for idx := (len(seqid) - 1); idx >= 0; idx-- {
						if idx == (len(seqid) - 1) {
							sequenceid = seqid[idx]
						} else {
							sequenceid = (sequenceid * 10) + seqid[idx]
						}
						seed = seed + seqid[idx]
					}
					sequenceid = sequenceid + seed
				}
			}
		}
		seqid = strconv.FormatUint(sequenceid%partitionsize, 10)
	}
	buid := "0"
	if id, exists := data.Val.Parameters["BUSINESS_UNIT"]; exists {
		buid = id
	}
	return data.Key, buid, seqid
}

/*
AxisAckEventParser - function to extract key and snapshot related details from the event
*/
func AxisAckEventParser(kv RocksData, partitionsize uint64, partitionposition uint64, partitionlength uint64) (string, int64, string) {
	var data *AxisRocksAckDBKV
	data = new(AxisRocksAckDBKV)
	defer func() {
		data.Key = ""
		data = nil
	}()
	//glog.Infof("Current event string %s", kv.Event)
	err := json.Unmarshal([]byte(kv.Event), &data)
	if err != nil {
		glog.Errorf("Error parsing JSON data. Error: %s\n" + err.Error())
		//panic("Error parsing JSON data.")
	}
	//glog.Infof("Current snapshot string %s", data.Val.UID)
	snapshot, err := strconv.ParseInt(data.Val.UID, 10, 64)
	if err != nil {
		glog.Errorf("Error occured in EventParser. Error msg: %s\n", err)
	}
	seqid := ""
	var sequenceid uint64
	if data.Val.OrderNumber != "" {
		sequenceid, err = strconv.ParseUint(data.Val.OrderNumber, 10, 64)
	}
	if err != nil {
		glog.Warningf("Could not set sequence number for event: %s\n", data.Key)
	} else {
		if partitionposition > 1 {
			//glog.Infof("Sequence ID considered. seq: %d\n", sequenceid)
			s := []byte(strconv.Itoa(int(sequenceid)))
			if len(s) > int(partitionposition) {
				if partitionsize < 10 {
					sequenceid = uint64(s[len(s)-int(partitionposition)])
				} else {
					seqid := make([]uint64, partitionlength)
					for idx := 0; idx < int(partitionlength); idx++ {
						seqid[idx], _ = strconv.ParseUint(string(s[len(s)-(int(partitionposition)+idx)]), 10, 64)
					}
					seed := uint64(0)
					for idx := (len(seqid) - 1); idx >= 0; idx-- {
						if idx == (len(seqid) - 1) {
							sequenceid = seqid[idx]
						} else {
							sequenceid = (sequenceid * 10) + seqid[idx]
						}
						seed = seed + seqid[idx]
					}
					sequenceid = sequenceid + seed
				}
			}
		}
		seqid = strconv.FormatUint(sequenceid%partitionsize, 10)
	}
	return data.Key, snapshot, seqid
}

/*
GcmAckEventParser - function to extract key and snapshot related details from the event
*/
func GcmAckEventParser(kv RocksData, partitionsize uint64, partitionposition uint64, partitionlength uint64) (string, int64, string) {
	var data *GCMRocksAckDBKV
	data = new(GCMRocksAckDBKV)
	defer func() {
		data.Key = ""
		data.Val.BUID = ""
		data.Val.CompanyNumber = ""
		data.Val.DPID = ""
		data.Val.DPIDStatus = ""
		data.Val.EventDate = ""
		data.Val.UID = ""
		data = nil
	}()
	//glog.Infof("Current event string %s", kv.Event)
	err := json.Unmarshal([]byte(kv.Event), &data)
	if err != nil {
		glog.Errorf("Error parsing JSON data. Error: %s\n" + err.Error())
		//panic("Error parsing JSON data.")
	}
	//glog.Infof("Current snapshot string %s", data.Val.UID)
	seqid := ""
	var sequenceid uint64
	if data.Val.DPID != "" {
		sequenceid, err = strconv.ParseUint(data.Val.DPID, 10, 64)
	}
	if err != nil {
		glog.Warningf("Could not set sequence number for event: %s\n", data.Key)
	} else {
		if partitionposition > 1 {
			//glog.Infof("Sequence ID considered. seq: %d\n", sequenceid)
			s := []byte(strconv.Itoa(int(sequenceid)))
			if len(s) > int(partitionposition) {
				if partitionsize < 10 {
					sequenceid = uint64(s[len(s)-int(partitionposition)])
				} else {
					seqid := make([]uint64, partitionlength)
					for idx := 0; idx < int(partitionlength); idx++ {
						seqid[idx], _ = strconv.ParseUint(string(s[len(s)-(int(partitionposition)+idx)]), 10, 64)
					}
					seed := uint64(0)
					for idx := (len(seqid) - 1); idx >= 0; idx-- {
						if idx == (len(seqid) - 1) {
							sequenceid = seqid[idx]
						} else {
							sequenceid = (sequenceid * 10) + seqid[idx]
						}
						seed = seed + seqid[idx]
					}
					sequenceid = sequenceid + seed
				}
			}
		}
		seqid = strconv.FormatUint(sequenceid%partitionsize, 10)
	}
	return data.Key, 0, seqid
}

/*
GcmNONUSAckEventParser - function to extract key and snapshot related details from the event
*/
func GcmNONUSAckEventParser(kv RocksData, partitionsize uint64, partitionposition uint64, partitionlength uint64) (string, int64, string) {
	var data *GCMNONUSRocksAckDBKV
	data = new(GCMNONUSRocksAckDBKV)
	defer func() {
		data.Key = ""
		data.Val.BUID = ""
		data.Val.CompanyNumber = ""
		data.Val.DPID = ""
		data.Val.DPIDStatus = ""
		data.Val.EventDate = ""
		data.Val.UID = ""
		data.Val.IRNNumber = ""
		data.Val.CountryCode = ""
		data = nil
	}()
	//glog.Infof("Current event string %s", kv.Event)
	err := json.Unmarshal([]byte(kv.Event), &data)
	if err != nil {
		glog.Errorf("Error parsing JSON data. Error: %s\n" + err.Error())
		//panic("Error parsing JSON data.")
	}
	//glog.Infof("Current snapshot string %s", data.Val.UID)
	seqid := ""
	var sequenceid uint64
	if data.Val.DPID != "" {
		sequenceid, err = strconv.ParseUint(data.Val.DPID, 10, 64)
	}
	if err != nil {
		glog.Warningf("Could not set sequence number for event: %s\n", data.Key)
	} else {
		if partitionposition > 1 {
			//glog.Infof("Sequence ID considered. seq: %d\n", sequenceid)
			s := []byte(strconv.Itoa(int(sequenceid)))
			if len(s) > int(partitionposition) {
				if partitionsize < 10 {
					sequenceid = uint64(s[len(s)-int(partitionposition)])
				} else {
					seqid := make([]uint64, partitionlength)
					for idx := 0; idx < int(partitionlength); idx++ {
						seqid[idx], _ = strconv.ParseUint(string(s[len(s)-(int(partitionposition)+idx)]), 10, 64)
					}
					seed := uint64(0)
					for idx := (len(seqid) - 1); idx >= 0; idx-- {
						if idx == (len(seqid) - 1) {
							sequenceid = seqid[idx]
						} else {
							sequenceid = (sequenceid * 10) + seqid[idx]
						}
						seed = seed + seqid[idx]
					}
					sequenceid = sequenceid + seed
				}
			}
		}
		seqid = strconv.FormatUint(sequenceid%partitionsize, 10)
	}
	return data.Key, 0, seqid
}

/*
CFOAckEventParser - function to extract key and snapshot related details from the event
*/
func CFOAckEventParser(kv RocksData, partitionsize uint64, partitionposition uint64, partitionlength uint64) (string, int64, string) {
	var data *CFORocksAckDBKV
	data = new(CFORocksAckDBKV)
	defer func() {
		data.Key = ""
		data.Val.Dpidnum = ""
		data.Val.EventUID = ""
		data.Val.Ordernumber = ""
		data.Val.Orderstatus = ""
		data = nil
	}()
	//glog.Infof("Current event string %s", kv.Event)
	err := json.Unmarshal([]byte(kv.Event), &data)
	//	glog.Errorf("Parse Data: %s\n" + data)
	if err != nil {
		glog.Errorf("Error parsing JSON data. Error: %s\n" + err.Error())
		//panic("Error parsing JSON data.")
	}
	//glog.Infof("Current snapshot string %s", data.Val.UID)
	snapshot, err := strconv.ParseInt(data.Val.EventUID, 10, 64)
	if err != nil {
		glog.Errorf("Error occured in EventParser. Error msg: %s\n", err)
	}
	return data.Key, snapshot, ""
}

/*
FSLAckEventParser - function to extract key and snapshot related details from the event
*/
func FSLAckEventParser(kv RocksData, partitionsize uint64, partitionposition uint64, partitionlength uint64) (string, int64, string) {
	var data *FSLRocksAckDBKV
	data = new(FSLRocksAckDBKV)
	defer func() {
		data.Key = ""
		data.Val.Buid = ""
		data.Val.ChannelStatus = ""
		data.Val.EMCPONumber = ""
		data.Val.Fabd = ""
		data.Val.Fsbd = ""
		data.Val.OrderNumber = ""
		data.Val.Messageid = ""
		data.Val.StatusDescription = ""
		data = nil
	}()
	//glog.Infof("Current event string %s", kv.Event)
	err := json.Unmarshal([]byte(kv.Event), &data)
	//	glog.Errorf("Parse Data: %s\n" + data)
	if err != nil {
		glog.Errorf("Error parsing JSON data. Error: %s\n" + err.Error())
		//panic("Error parsing JSON data.")
	}
	//glog.Infof("Current snapshot string %s", data.Val.UID)
	seqid := ""
	var sequenceid uint64
	if data.Val.OrderNumber != "" {
		sequenceid, err = strconv.ParseUint(data.Val.OrderNumber, 10, 64)
	}
	if err != nil {
		glog.Warningf("Could not set sequence number for event: %s\n", data.Key)
	} else {
		if partitionposition > 1 {
			//glog.Infof("Sequence ID considered. seq: %d\n", sequenceid)
			s := []byte(strconv.Itoa(int(sequenceid)))
			if len(s) > int(partitionposition) {
				if partitionsize < 10 {
					sequenceid = uint64(s[len(s)-int(partitionposition)])
				} else {
					seqid := make([]uint64, partitionlength)
					for idx := 0; idx < int(partitionlength); idx++ {
						seqid[idx], _ = strconv.ParseUint(string(s[len(s)-(int(partitionposition)+idx)]), 10, 64)
					}
					seed := uint64(0)
					for idx := (len(seqid) - 1); idx >= 0; idx-- {
						if idx == (len(seqid) - 1) {
							sequenceid = seqid[idx]
						} else {
							sequenceid = (sequenceid * 10) + seqid[idx]
						}
						seed = seed + seqid[idx]
					}
					sequenceid = sequenceid + seed
				}
			}
		}
		seqid = strconv.FormatUint(sequenceid%partitionsize, 10)
	}
	return data.Key, 0, seqid
}

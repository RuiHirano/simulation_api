package api

import (
	"context"
	"errors"

	//"fmt"
	"io"
	"log"

	//"sync"
	"time"

	//"os"
	//"os/signal"

	"github.com/google/uuid"

	//"github.com/bwmarrin/snowflake"

	simpb "github.com/RuiHirano/synerex_simulation_beta/api/sim_proto"
	"github.com/golang/protobuf/ptypes"
	proto "github.com/synerex/synerex_api"
	//"github.com/synerex/synerex_alpha/nodeapi"
	//"google.golang.org/grpc"
)

// IDType for all ID in Synergic Exchange
type IDType uint64

/*var (
	node       *snowflake.Node // package variable for keeping unique ID.
	nid        *nodeapi.NodeID
	nupd       *nodeapi.NodeUpdate
	numu       sync.RWMutex
	myNodeName string
	conn       *grpc.ClientConn
	clt        nodeapi.NodeClient
	funcSlice  []func()
)*/

// DemandOpts is sender options for Demand
type DemandOpts struct {
	ID        uint64
	Target    uint64
	Name      string
	JSON      string
	SimDemand *simpb.SimDemand
}

// SupplyOpts is sender options for Supply
type SupplyOpts struct {
	ID        uint64
	Target    uint64
	Name      string
	JSON      string
	SimSupply *simpb.SimSupply
}

// SMServiceClient Wrappter Structure for market client
type SMServiceClient struct {
	ClientID   IDType
	ProviderID uint64
	MType      uint64
	Client     proto.SynerexClient
	ArgJson    string
	MbusID     IDType
}

// NewSMServiceClient Creates wrapper structre SMServiceClient from SynerexClient
func NewSMServiceClient(clt proto.SynerexClient, mtype uint64, providerID uint64, argJson string) *SMServiceClient {
	uid, _ := uuid.NewRandom()
	s := &SMServiceClient{
		ClientID:   IDType(uid.ID()),
		ProviderID: providerID,
		MType:      mtype,
		Client:     clt,
		ArgJson:    argJson,
	}
	return s
}

// GenerateIntID for generate uniquie ID
func GenerateIntID() uint64 {
	uid, _ := uuid.NewRandom()
	return uint64(uid.ID())
}

func (clt SMServiceClient) getChannel() *proto.Channel {
	return &proto.Channel{ClientId: uint64(clt.ClientID), ChannelType: uint32(clt.MType), ProviderId: clt.ProviderID, ArgJson: clt.ArgJson}
}

// IsSupplyTarget is a helper function to check target
func (clt *SMServiceClient) IsSupplyTarget(sp proto.Supply, idlist []uint64) bool {
	spid := sp.TargetId
	for _, id := range idlist {
		if id == spid {
			return true
		}
	}
	return false
}

// IsDemandTarget is a helper function to check target
func (clt *SMServiceClient) IsDemandTarget(dm proto.Demand, idlist []uint64) bool {
	dmid := dm.TargetId
	for _, id := range idlist {
		if id == dmid {
			return true
		}
	}
	return false
}

// ProposeSupply send proposal Supply message to server
func (clt *SMServiceClient) ProposeSupply(spo *SupplyOpts) uint64 {
	pid := GenerateIntID()
	ts := ptypes.TimestampNow()
	sp := &proto.Supply{
		Id:          pid,
		SenderId:    uint64(clt.ClientID),
		TargetId:    spo.Target,
		ChannelType: clt.MType,
		Ts:          ts,
		SupplyName:  spo.Name,
		ArgJson:     spo.JSON,
	}

	/*if spo.SimSupply != nil {
		sp.WithSimSupply(spo.SimSupply)
	}*/

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := clt.Client.ProposeSupply(ctx, sp)
	//log.Printf("%v.Test err %v, [%v]", clt, err, sp)
	if err != nil {
		log.Printf("%v.ProposeSupply err %v, [%v]", clt, err, sp)
		return 0 // should check...
	}
	return pid
}

// SelectSupply send select message to server
func (clt *SMServiceClient) SelectSupply(sp proto.Supply) (uint64, error) {
	tgt := &proto.Target{
		Id:          GenerateIntID(),
		SenderId:    uint64(clt.ClientID),
		TargetId:    sp.Id, /// Message Id of Supply (not SenderId),
		ChannelType: sp.ChannelType,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := clt.Client.SelectSupply(ctx, tgt)
	if err != nil {
		log.Printf("%v.SelectSupply err %v", clt, err)
		return 0, err
	}
	log.Println("SelectSupply Response:", resp)
	// if mbus is OK, start mbus!
	clt.MbusID = IDType(resp.MbusId)
	if clt.MbusID != 0 {
		//		clt.SubscribeMbus()
	}

	return uint64(clt.MbusID), nil
}

// SelectDemand send select message to server
func (clt *SMServiceClient) SelectDemand(dm proto.Demand) error {
	tgt := &proto.Target{
		Id:          GenerateIntID(),
		SenderId:    uint64(clt.ClientID),
		TargetId:    dm.Id,
		ChannelType: dm.ChannelType,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := clt.Client.SelectDemand(ctx, tgt)
	if err != nil {
		log.Printf("%v.SelectDemand err %v", clt, err)
		return err
	}
	log.Println("SelectDemand Response:", resp)
	return nil
}

// SubscribeSupply  Wrapper function for SMServiceClient
func (clt *SMServiceClient) SubscribeSupply(ctx context.Context, spcb func(*SMServiceClient, *proto.Supply)) error {
	ch := clt.getChannel()
	smc, err := clt.Client.SubscribeSyncSupply(ctx, ch)
	//log.Printf("Test3 %v", ch)
	//wg.Done()
	if err != nil {
		log.Printf("SubscribeSupply Error...\n")
		return err
	} else {
		log.Print("Connect Synerex Server!\n")
	}
	for {
		var sp *proto.Supply
		sp, err = smc.Recv() // receive Demand
		//log.Printf("\x1b[30m\x1b[47m SXUTIL: SUPPLY\x1b[0m\n")
		if err != nil {
			if err == io.EOF {
				log.Print("End Supply subscribe OK")
			} else {
				log.Printf("SMServiceClient SubscribeSupply error %v\n", err)
			}
			break
		}
		//		log.Println("Receive SS:", sp)
		// call Callback!
		spcb(clt, sp)
	}
	return err
}

// SubscribeDemand  Wrapper function for SMServiceClient
func (clt *SMServiceClient) SubscribeDemand(ctx context.Context, dmcb func(*SMServiceClient, *proto.Demand)) error {
	ch := clt.getChannel()
	dmc, err := clt.Client.SubscribeSyncDemand(ctx, ch)
	//log.Printf("Test3 %v", ch)
	//wg.Done()
	if err != nil {
		log.Printf("SubscribeDemand Error...\n")
		return err // sender should handle error...
	} else {
		log.Print("Connect Synerex Server!\n")
	}
	for {
		var dm *proto.Demand
		dm, err = dmc.Recv() // receive Demand
		//log.Printf("\x1b[30m\x1b[47m SXUTIL: DEMAND\x1b[0m\n")
		if err != nil {
			if err == io.EOF {
				log.Print("End Demand subscribe OK")
			} else {
				log.Printf("SMServiceClient SubscribeDemand error %v\n", err)
			}
			break
		}
		//		log.Println("Receive SD:",*dm)
		// call Callback!
		dmcb(clt, dm)
	}
	return err
}

// SubscribeMbus  Wrapper function for SMServiceClient
func (clt *SMServiceClient) SubscribeMbus(ctx context.Context, mbcb func(*SMServiceClient, *proto.MbusMsg)) error {

	mb := &proto.Mbus{
		ClientId: uint64(clt.ClientID),
		MbusId:   uint64(clt.MbusID),
	}

	smc, err := clt.Client.SubscribeMbus(ctx, mb)
	if err != nil {
		log.Printf("%v Synerex_SubscribeMbusClient Error %v", clt, err)
		return err // sender should handle error...
	}
	for {
		var mes *proto.MbusMsg
		mes, err = smc.Recv() // receive Demand
		if err != nil {
			if err == io.EOF {
				log.Print("End Mbus subscribe OK")
			} else {
				log.Printf("%v SMServiceClient SubscribeMbus error %v", clt, err)
			}
			break
		}
		//		log.Printf("Receive Mbus Message %v", *mes)
		// call Callback!
		mbcb(clt, mes)
	}
	return err
}

func (clt *SMServiceClient) SendMsg(ctx context.Context, msg *proto.MbusMsg) error {
	if clt.MbusID == 0 {
		return errors.New("No Mbus opened!")
	}
	msg.MsgId = GenerateIntID()
	msg.SenderId = uint64(clt.ClientID)
	msg.MbusId = uint64(clt.MbusID)
	_, err := clt.Client.SendMsg(ctx, msg)

	return err
}

func (clt *SMServiceClient) CloseMbus(ctx context.Context) error {
	if clt.MbusID == 0 {
		return errors.New("No Mbus opened!")
	}
	mbus := &proto.Mbus{
		ClientId: uint64(clt.ClientID),
		MbusId:   uint64(clt.MbusID),
	}
	_, err := clt.Client.CloseMbus(ctx, mbus)
	if err == nil {
		clt.MbusID = 0
	}
	return err
}

// RegisterDemand sends Typed Demand to Server
func (clt *SMServiceClient) RegisterDemand(dmo *DemandOpts) uint64 {
	id := GenerateIntID()
	ts := ptypes.TimestampNow()
	dm := &proto.Demand{
		Id:          id,
		SenderId:    uint64(clt.ClientID),
		ChannelType: uint32(clt.MType),
		DemandName:  dmo.Name,
		Ts:          ts,
		ArgJson:     dmo.JSON,
	}

	/*if dmo.SimDemand != nil {
		dm.WithSimDemand(dmo.SimDemand)
	}*/

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := clt.Client.RegisterDemand(ctx, dm)

	//	resp, err := clt.Client.RegisterDemand(ctx, &dm)
	if err != nil {
		log.Printf("%v.RegisterDemand err %v", clt, err)
		return 0
	}
	//	log.Println(resp)
	dmo.ID = id // assign ID
	return id
}

// RegisterSupply sends Typed Supply to Server
func (clt *SMServiceClient) RegisterSupply(spo *SupplyOpts) uint64 {
	id := GenerateIntID()
	ts := ptypes.TimestampNow()
	sp := &proto.Supply{
		Id:          id,
		SenderId:    uint64(clt.ClientID),
		ChannelType: uint32(clt.MType),
		SupplyName:  spo.Name,
		Ts:          ts,
		ArgJson:     spo.JSON,
	}

	/*if spo.SimSupply != nil {
		sp.WithSimSupply(spo.SimSupply)
	}*/

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//	resp , err := clt.Client.RegisterSupply(ctx, &dm)

	_, err := clt.Client.RegisterSupply(ctx, sp)
	if err != nil {
		log.Printf("Error for sending:RegisterSupply to  Synerex Server as %v ", err)
		return 0
	}
	//	log.Println("RegiterSupply:", smo, resp)
	spo.ID = id // assign ID
	return id
}

//////////////////////////
// add sync function////////
/////////////////////////
// RegisterDemand sends Typed Demand to Server
func (clt *SMServiceClient) SyncDemand(dmo *DemandOpts) uint64 {
	id := GenerateIntID()
	ts := ptypes.TimestampNow()
	dm := &proto.Demand{
		Id:          id,
		SenderId:    uint64(clt.ClientID),
		ChannelType: uint32(clt.MType),
		DemandName:  dmo.Name,
		Ts:          ts,
		ArgJson:     dmo.JSON,
	}

	/*if dmo.SimDemand != nil {
		dm.WithSimDemand(dmo.SimDemand)
	}*/

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := clt.Client.SyncDemand(ctx, dm)

	//	resp, err := clt.Client.SyncDemand(ctx, &dm)
	if err != nil {
		log.Printf("%v.SyncDemand err %v", clt, err)
		return 0
	}
	//	log.Println(resp)
	dmo.ID = id // assign ID
	return id
}

// SyncSupply sends Typed Supply to Server
func (clt *SMServiceClient) SyncSupply(spo *SupplyOpts) uint64 {
	id := GenerateIntID()
	ts := ptypes.TimestampNow()
	sp := &proto.Supply{
		Id:          id,
		SenderId:    uint64(clt.ClientID),
		ChannelType: uint32(clt.MType),
		SupplyName:  spo.Name,
		Ts:          ts,
		ArgJson:     spo.JSON,
	}

	/*if spo.SimSupply != nil {
		sp.WithSimSupply(spo.SimSupply)
	}*/

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//	resp , err := clt.Client.SyncSupply(ctx, &dm)

	_, err := clt.Client.SyncSupply(ctx, sp)
	if err != nil {
		log.Printf("Error for sending:SyncSupply to  Synerex Server as %v ", err)
		return 0
	}
	//	log.Println("RegiterSupply:", smo, resp)
	spo.ID = id // assign ID
	return id
}

// Confirm sends confirm message to sender
func (clt *SMServiceClient) Confirm(id IDType) error {
	tg := &proto.Target{
		Id:          GenerateIntID(),
		SenderId:    uint64(clt.ClientID),
		TargetId:    uint64(id),
		ChannelType: uint32(clt.MType),
		MbusId:      uint64(id),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := clt.Client.Confirm(ctx, tg)
	if err != nil {
		log.Printf("%v Confirm Failier %v", clt, err)
		return err
	}
	clt.MbusID = id
	log.Println("Confirm Success:", resp)
	return nil
}

// Demand
// NewDemand returns empty Demand.
func NewDemand() *proto.Demand {
	return &proto.Demand{}
}

// NewSupply returns empty Supply.
func NewSupply() *proto.Supply {
	return &proto.Supply{}
}

/*func (dm *proto.Demand) WithSimDemand(r *simpb.SimDemand) *proto.Demand {
	dm.ArgOneof = &proto.Demand_SimDemand{r}
	return dm
}

func (sp *proto.Supply) WithSimSupply(c *simpb.SimSupply) *proto.Supply {
	sp.ArgOneof = &proto.Supply_SimSupply{c}
	return sp
}*/

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/RuiHirano/synerex_simulation_beta/api/sim_proto"
	"github.com/google/uuid"
	synpb "github.com/synerex/synerex_api"
)

var (
	mu        sync.Mutex
	waitChMap map[pb.SupplyType]chan *synpb.Supply
	//spMesMap            map[pb.SupplyType]*Message
	//logger              *util.Logger
	CHANNEL_BUFFER_SIZE int
)

func init() {
	waitChMap = make(map[pb.SupplyType]chan *synpb.Supply)
	//logger = util.NewLogger()
	CHANNEL_BUFFER_SIZE = 10
}

type SimAPI struct {
	Client *SMServiceClient
	Waiter *Waiter
}

func NewSimAPI() *SimAPI {
	s := &SimAPI{
		Waiter: NewWaiter(),
	}
	return s
}

////////////////////////////////////////////////////////////
////////////        Supply Demand Function       ///////////
///////////////////////////////////////////////////////////

func (s *SimAPI) RegistClients(client synpb.SynerexClient, providerId uint64, argJson string) {

	simChType := uint64(1)
	s.Client = NewSMServiceClient(client, simChType, providerId, argJson)

}

// SubscribeAll: 全てのチャネルに登録、SubscribeSupply, SubscribeDemandする
func (s *SimAPI) Subscribe(demandCallback func(*SMServiceClient, *synpb.Demand), supplyCallback func(*SMServiceClient, *synpb.Supply)) error {

	// SubscribeDemand, SubscribeSupply
	go subscribeDemand(s.Client, demandCallback)
	go subscribeSupply(s.Client, supplyCallback)

	return nil
}

func subscribeSupply(client *SMServiceClient, supplyCallback func(*SMServiceClient, *synpb.Supply)) {
	//called as goroutine
	ctx := context.Background() // should check proper context
	client.SubscribeSupply(ctx, supplyCallback)
	// comes here if channel closed
	log.Printf("SMarket Server Closed? Reconnect...")
	time.Sleep(2 * time.Second)
	subscribeSupply(client, supplyCallback)

}

func subscribeDemand(client *SMServiceClient, demandCallback func(*SMServiceClient, *synpb.Demand)) {

	//called as goroutine
	ctx := context.Background() // should check proper context
	client.SubscribeDemand(ctx, demandCallback)
	// comes here if channel closed
	log.Printf("SMarket Server Closed? Reconnect...")
	time.Sleep(2 * time.Second)
	subscribeDemand(client, demandCallback)
}

func sendDemand(sclient *SMServiceClient, simDemand *pb.SimDemand) uint64 {
	nm := ""
	js := ""
	opts := &DemandOpts{Name: nm, JSON: js, SimDemand: simDemand}

	mu.Lock()
	id := sclient.RegisterDemand(opts)
	mu.Unlock()
	return id
}

func sendSupply(sclient *SMServiceClient, simSupply *pb.SimSupply) uint64 {
	nm := ""
	js := ""
	opts := &SupplyOpts{Name: nm, JSON: js, SimSupply: simSupply}

	mu.Lock()
	id := sclient.RegisterSupply(opts)
	mu.Unlock()
	return id
}

//////////////////////////
// add new function////////
/////////////////////////
func (s *SimAPI) SendSyncDemand(simDemand *pb.SimDemand) ([]*synpb.Supply, error) {
	//nm := ""
	//js := ""
	//opts := &DemandOpts{Name: nm, JSON: js, SimDemand: simDemand}

	mu.Lock()
	ctx := context.Background()
	s.Client.SendMsg(ctx, msg)
	msgId := simDemand.GetMsgId()
	CHANNEL_BUFFER_SIZE := 10
	waitCh := make(chan *synpb.Supply, CHANNEL_BUFFER_SIZE)
	s.Waiter.WaitSpChMap[msgId] = waitCh
	s.Waiter.SpMap[msgId] = make([]*synpb.Supply, 0)
	mu.Unlock()

	mu.Lock()
	sclient.SyncDemand(opts)
	mu.Unlock()

	// waitする
	sps := []*synpb.Supply{}
	var err error
	targets := simDemand.GetTargets()
	if len(targets) != 0 {
		msgId := simDemand.GetMsgId()
		sps, err = s.Waiter.WaitSp(msgId, targets, 1000)
		s.Waiter = NewWaiter()
	}

	return sps, err
}

func (s *SimAPI) SendSyncSupply(simSupply *pb.SimSupply) uint64 {
	//nm := ""
	//js := ""
	//opts := &SupplyOpts{Name: nm, JSON: js, SimSupply: simSupply}
	msg := &synpb.MbusMsg{
		MsgId:    0,
		SenderId: 0,
		TargetId: 0,
		MbusId:   0,
		MsgType:  0,
		MsgInfo:  0,
		ArgJson:  GetSimSupplyJson(simSupply),
	}
	mu.Lock()
	ctx := context.Background()
	s.Client.SendMsg(ctx, msg)
	//id := sclient.SyncSupply(opts)
	mu.Unlock()
	return 0
}

func (s *SimAPI) SendSpToWait(sp *synpb.Supply) {
	s.Waiter.SendSpToWait(sp)
}

///////////////////////////////////////////
/////////////    Area API   //////////////
//////////////////////////////////////////

// Areaを送るDemand
func (s *SimAPI) SendAreaInfoRequest(senderId uint64, targets []uint64, areas []*pb.Area) ([]*synpb.Supply, error) {

	uid, _ := uuid.NewRandom()
	sendAreaInfoRequest := &pb.SendAreaInfoRequest{
		Areas: areas,
	}

	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_SEND_AREA_INFO_REQUEST,
		Data:     &pb.SimDemand_SendAreaInfoRequest{sendAreaInfoRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentのセット完了
func (s *SimAPI) SendAreaInfoResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	sendAreaInfoResponse := &pb.SendAreaInfoResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_SEND_AREA_INFO_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_SendAreaInfoResponse{sendAreaInfoResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

///////////////////////////////////////////
/////////////   Agent API   //////////////
//////////////////////////////////////////

// AgentをセットするDemand
func (s *SimAPI) SetAgentRequest(senderId uint64, targets []uint64, agents []*pb.Agent) ([]*synpb.Supply, error) {

	uid, _ := uuid.NewRandom()
	setAgentRequest := &pb.SetAgentRequest{
		Agents: agents,
	}

	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_SET_AGENT_REQUEST,
		Data:     &pb.SimDemand_SetAgentRequest{setAgentRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentのセット完了
func (s *SimAPI) SetAgentResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	setAgentResponse := &pb.SetAgentResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_SET_AGENT_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_SetAgentResponse{setAgentResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

// AgentをセットするDemand
func (s *SimAPI) GetAgentRequest(senderId uint64, targets []uint64) ([]*synpb.Supply, error) {

	uid, _ := uuid.NewRandom()
	getAgentRequest := &pb.GetAgentRequest{}

	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_GET_AGENT_REQUEST,
		Data:     &pb.SimDemand_GetAgentRequest{getAgentRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentのセット完了
func (s *SimAPI) GetAgentResponse(senderId uint64, targets []uint64, msgId uint64, agents []*pb.Agent) uint64 {
	getAgentResponse := &pb.GetAgentResponse{
		Agents: agents,
	}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_GET_AGENT_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_GetAgentResponse{getAgentResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

///////////////////////////////////////////
/////////////   Provider API   //////////////
//////////////////////////////////////////

// Providerを登録するDemand
func (s *SimAPI) ReadyProviderRequest(senderId uint64, targets []uint64, providerInfo *pb.Provider) ([]*synpb.Supply, error) {
	readyProviderRequest := &pb.ReadyProviderRequest{
		Provider: providerInfo,
	}

	uid, _ := uuid.NewRandom()
	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_READY_PROVIDER_REQUEST,
		Data:     &pb.SimDemand_ReadyProviderRequest{readyProviderRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Providerを登録するSupply
func (s *SimAPI) ReadyProviderResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	readyProviderResponse := &pb.ReadyProviderResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_READY_PROVIDER_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_ReadyProviderResponse{readyProviderResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

// Providerを登録するDemand
func (s *SimAPI) RegistProviderRequest(senderId uint64, targets []uint64, providerInfo *pb.Provider) ([]*synpb.Supply, error) {
	registProviderRequest := &pb.RegistProviderRequest{
		Provider: providerInfo,
	}

	uid, _ := uuid.NewRandom()
	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_REGIST_PROVIDER_REQUEST,
		Data:     &pb.SimDemand_RegistProviderRequest{registProviderRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Providerを登録するSupply
func (s *SimAPI) RegistProviderResponse(senderId uint64, targets []uint64, msgId uint64, providerInfo *pb.Provider) uint64 {
	registProviderResponse := &pb.RegistProviderResponse{
		Provider: providerInfo,
	}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_REGIST_PROVIDER_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_RegistProviderResponse{registProviderResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

// Providerを登録するDemand
func (s *SimAPI) UpdateProvidersRequest(senderId uint64, targets []uint64, providers []*pb.Provider) ([]*synpb.Supply, error) {
	updateProvidersRequest := &pb.UpdateProvidersRequest{
		Providers: providers,
	}

	uid, _ := uuid.NewRandom()
	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_UPDATE_PROVIDERS_REQUEST,
		Data:     &pb.SimDemand_UpdateProvidersRequest{updateProvidersRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Providerを登録するSupply
func (s *SimAPI) UpdateProvidersResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	updateProvidersResponse := &pb.UpdateProvidersResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_UPDATE_PROVIDERS_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_UpdateProvidersResponse{updateProvidersResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

///////////////////////////////////////////
/////////////   Clock API   //////////////
//////////////////////////////////////////

func (s *SimAPI) SetClockRequest(senderId uint64, targets []uint64, clockInfo *pb.Clock) ([]*synpb.Supply, error) {
	setClockRequest := &pb.SetClockRequest{
		Clock: clockInfo,
	}

	uid, _ := uuid.NewRandom()
	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_SET_CLOCK_REQUEST,
		Data:     &pb.SimDemand_SetClockRequest{setClockRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentを取得するSupply
func (s *SimAPI) SetClockResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	setClockResponse := &pb.SetClockResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_SET_CLOCK_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_SetClockResponse{setClockResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

func (s *SimAPI) ForwardClockRequest(senderId uint64, targets []uint64) ([]*synpb.Supply, error) {
	forwardClockRequest := &pb.ForwardClockRequest{}

	uid, _ := uuid.NewRandom()
	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_FORWARD_CLOCK_REQUEST,
		Data:     &pb.SimDemand_ForwardClockRequest{forwardClockRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentを取得するSupply
func (s *SimAPI) ForwardClockResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	forwardClockResponse := &pb.ForwardClockResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_FORWARD_CLOCK_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_ForwardClockResponse{forwardClockResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

func (s *SimAPI) ForwardClockInitRequest(senderId uint64, targets []uint64) ([]*synpb.Supply, error) {
	forwardClockInitRequest := &pb.ForwardClockInitRequest{}

	uid, _ := uuid.NewRandom()
	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_FORWARD_CLOCK_INIT_REQUEST,
		Data:     &pb.SimDemand_ForwardClockInitRequest{forwardClockInitRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentを取得するSupply
func (s *SimAPI) ForwardClockInitResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	forwardClockInitResponse := &pb.ForwardClockInitResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_FORWARD_CLOCK_INIT_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_ForwardClockInitResponse{forwardClockInitResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

func (s *SimAPI) StartClockRequest(senderId uint64, targets []uint64) ([]*synpb.Supply, error) {
	startClockRequest := &pb.StartClockRequest{}

	uid, _ := uuid.NewRandom()
	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_START_CLOCK_REQUEST,
		Data:     &pb.SimDemand_StartClockRequest{startClockRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentを取得するSupply
func (s *SimAPI) StartClockResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	startClockResponse := &pb.StartClockResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_START_CLOCK_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_StartClockResponse{startClockResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

func (s *SimAPI) StopClockRequest(senderId uint64, targets []uint64) ([]*synpb.Supply, error) {
	stopClockRequest := &pb.StopClockRequest{}

	uid, _ := uuid.NewRandom()
	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_STOP_CLOCK_REQUEST,
		Data:     &pb.SimDemand_StopClockRequest{stopClockRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentを取得するSupply
func (s *SimAPI) StopClockResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	stopClockResponse := &pb.StopClockResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_STOP_CLOCK_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_StopClockResponse{stopClockResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

///////////////////////////////////////////
/////////////   Pod API   //////////////
//////////////////////////////////////////

// AgentをセットするDemand
func (s *SimAPI) CreatePodRequest(senderId uint64, targets []uint64) ([]*synpb.Supply, error) {

	uid, _ := uuid.NewRandom()
	createPodRequest := &pb.CreatePodRequest{}

	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_SET_AGENT_REQUEST,
		Data:     &pb.SimDemand_CreatePodRequest{createPodRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentのセット完了
func (s *SimAPI) CreatePodResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	createPodResponse := &pb.CreatePodResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_SET_AGENT_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_CreatePodResponse{createPodResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

// AgentをセットするDemand
func (s *SimAPI) DeletePodRequest(senderId uint64, targets []uint64) ([]*synpb.Supply, error) {

	uid, _ := uuid.NewRandom()
	deletePodRequest := &pb.DeletePodRequest{}

	msgId := uint64(uid.ID())
	simDemand := &pb.SimDemand{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.DemandType_GET_AGENT_REQUEST,
		Data:     &pb.SimDemand_DeletePodRequest{deletePodRequest},
		Targets:  targets,
	}

	sps, err := s.SendSyncDemand(simDemand)

	return sps, err
}

// Agentのセット完了
func (s *SimAPI) DeletePodResponse(senderId uint64, targets []uint64, msgId uint64) uint64 {
	deletePodResponse := &pb.DeletePodResponse{}

	simSupply := &pb.SimSupply{
		MsgId:    msgId,
		SenderId: senderId,
		Type:     pb.SupplyType_GET_AGENT_RESPONSE,
		Status:   pb.StatusType_OK,
		Data:     &pb.SimSupply_DeletePodResponse{deletePodResponse},
		Targets:  targets,
	}

	s.SendSyncSupply(simSupply)

	return msgId
}

// Marshal/UnMershal SimSupply/SimDemand
func GetSimSupply(argJson string) (*pb.SimSupply, error) {
	simSupply := &pb.SimSupply{}
	err := json.Unmarshal([]byte(argJson), simSupply)
	if err != nil {
		return nil, err
	}
	return simSupply, nil
}

// Marshal/UnMershal SimSupply/SimDemand
func GetSimSupplyJson(simSupply *pb.SimSupply) (string, error) {
	byte, err := json.Marshal(simSupply)
	if err != nil {
		return "", err
	}
	return string(byte), nil
}

// Marshal/UnMershal SimSupply/SimDemand
func GetSimDemand(argJson string) (*pb.SimDemand, error) {
	simDemand := &pb.SimDemand{}
	err := json.Unmarshal([]byte(argJson), simDemand)
	if err != nil {
		return nil, err
	}
	return simDemand, nil
}

// Marshal/UnMershal SimSupply/SimDemand
func GetSimDemandJson(simDemand *pb.SimDemand) (string, error) {
	byte, err := json.Marshal(simDemand)
	if err != nil {
		return "", err
	}
	return string(byte), nil
}

///////////////////////////////////////////
/////////////      Wait      //////////////
//////////////////////////////////////////

type Waiter struct {
	WaitSpChMap map[uint64]chan *synpb.Supply
	SpMap       map[uint64][]*synpb.Supply
}

func NewWaiter() *Waiter {
	w := &Waiter{
		WaitSpChMap: make(map[uint64]chan *synpb.Supply),
		SpMap:       make(map[uint64][]*synpb.Supply),
	}
	return w
}

func (w *Waiter) WaitSp(msgId uint64, targets []uint64, timeout uint64) ([]*synpb.Supply, error) {

	waitCh := w.WaitSpChMap[msgId]

	var err error
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			select {
			case sp, _ := <-waitCh:
				mu.Lock()
				//log.Printf("getSP %v, %v\n", GetSimSupply(sp.GetArgJson()).GetSenderId(), GetSimSupply(sp.GetArgJson()).GetMsgId())
				// spのidがidListに入っているか
				simSupply, _ := GetSimSupply(sp.GetArgJson())
				if simSupply.GetMsgId() == msgId {
					//mu.Lock()
					w.SpMap[simSupply.GetMsgId()] = append(w.SpMap[simSupply.GetMsgId()], sp)
					//mu.Unlock()
					//log.Printf("msgID spId %v, msgId %v targets %v\n", w.SpMap[GetSimSupply(sp.GetArgJson()).GetMsgId()], msgId, targets)

					// 同期が終了したかどうか
					if w.isFinishSpSync(msgId, targets) {
						//logger.Debug("Finish Wait!")
						mu.Unlock()
						wg.Done()
						return
					}
				}
				mu.Unlock()
			case <-time.After(time.Duration(timeout) * time.Millisecond):
				noIds := []uint64{}
				noSps := []*synpb.Supply{} // test
				var sp2 *synpb.Supply
				for _, tgt := range targets {
					isExist := false
					for _, sp := range w.SpMap[msgId] {
						sp2 = sp
						simSupply, _ := GetSimSupply(sp.GetArgJson())
						if tgt == simSupply.GetSenderId() {
							isExist = true
						}
					}
					if isExist == false {
						noIds = append(noIds, tgt)
						noSps = append(noSps, sp2)
					}
				}
				//logger.Error("Sync Error... noids %v, msgId %v \n%v\n\n", noIds, msgId, noSps)
				err = fmt.Errorf("Timeout Error")
				wg.Done()
				return
			}
		}
	}()
	wg.Wait()
	return w.SpMap[msgId], err
}

func (w *Waiter) SendSpToWait(sp *synpb.Supply) {
	//log.Printf("getSP2 %v, %v\n", GetSimSupply(sp.GetArgJson()).GetSenderId(), GetSimSupply(sp.GetArgJson()).GetMsgId())
	mu.Lock()
	simSupply, _ := GetSimSupply(sp.GetArgJson())
	waitCh := w.WaitSpChMap[simSupply.GetMsgId()]
	mu.Unlock()
	waitCh <- sp
}

func (w *Waiter) isFinishSpSync(msgId uint64, targets []uint64) bool {

	for _, pid := range targets {
		isExist := false
		for _, sp := range w.SpMap[msgId] {
			simSupply, _ := GetSimSupply(sp.GetArgJson())
			senderId := simSupply.GetSenderId()
			if senderId == pid {
				isExist = true
			}
		}
		if isExist == false {
			return false
		}
	}

	return true
}

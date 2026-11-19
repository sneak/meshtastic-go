package gmtcli

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/lmatte7/gomesh"
	"github.com/lmatte7/gomesh/github.com/meshtastic/gomeshproto"
	"github.com/urfave/cli/v2"
)

func getMeshNodeByID(radio gomesh.Radio, id int64) (*MeshNode, error) {
	meshNodes, err := getMeshNodes(radio)
	if err != nil {
		return nil, err
	}
	for _, node := range meshNodes {
		if node.NodeNumber == int(id) {
			return &node, nil
		}
	}
	return nil, fmt.Errorf("Node with ID %d not found", id)
}

func getMeshNodeByIDFromNodeList(
	nodeList []MeshNode,
	id int64,
) (*MeshNode, error) {
	for _, node := range nodeList {
		if node.NodeNumber == int(id) {
			return &node, nil
		}
	}
	return nil, fmt.Errorf("Node with ID %d not found", id)
}

type Channel struct {
	Index int    `json:"index"`
	Name  string `json:"name"`
}

type MeshPacket struct {
	ID             int              `json:"id,omitempty"`
	From           *MeshNode        `json:"from,omitempty"`
	To             *MeshNode        `json:"to,omitempty"`
	ChannelNum     int              `json:"channelNum"`
	Channel        *Channel         `json:"channel,omitempty"`
	Port           string           `json:"port,omitempty"`
	Payload        []byte           `json:"payload,omitempty"`
	PayloadDecoded []byte           `json:"payloadDecoded,omitempty"`
	RemoveMeLater  string           `json:"tmp,omitempty"`
	RawPayload     *json.RawMessage `json:"raw,omitempty"`
	ReceiveTime    *time.Time       `json:"receiveTime,omitempty"`
	Priority       string           `json:"priority,omitempty"`
	WantAck        bool             `json:"wantAck"`
	HopLimit       int              `json:"hopLimit,omitempty"`
	HopStart       int              `json:"hopStart,omitempty"`
	HopCount       int              `json:"hopCount,omitempty"`
	ViaMQTT        bool             `json:"viaMQTT"`
	RSSI           string           `json:"rssi,omitempty"`
	Delayed        string           `json:"delayed,omitempty"`
	Broadcast      bool             `json:"broadcast,omitempty"`
	IsFavorite     bool             `json:"isFavorite"`
	IsIgnored      bool             `json:"isIgnored"`
}

func jsonPrettyPrint(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

func writeError(err error) {
	// print errors to stderr
	fmt.Fprintf(
		cli.ErrWriter,
		"Error: %s\n",
		err,
	)
}

func debugLog(msg string) {
	if os.Getenv("DEBUG") == "true" {
		fmt.Fprintf(
			cli.ErrWriter,
			"Debug: %s\n",
			msg,
		)
	}
}

func debugPrintf(format string, a ...interface{}) {
	if os.Getenv("DEBUG") == "true" {
		fmt.Fprintf(
			cli.ErrWriter,
			"DEBUG: %s\n",
			fmt.Sprintf(format, a...),
		)
	}
}

func getNextMessage(radio gomesh.Radio) ([]*gomeshproto.FromRadio, error) {
	debugPrintf("Getting next message...")
	msg, err := radio.ReadResponse(false)
	debugPrintf("msg: %#v", msg)
	debugPrintf("err: %#v", err)
	return msg, err
}

func streamPackets(c *cli.Context) error {
	// epoch 1970
	cacheLastUpdate := time.Unix(0, 0)
	radio := getRadio(c)
	cachedNodeList, err := getMeshNodes(radio)
	radio.Close()
	if err != nil {
		debugPrintf("Error getting mesh nodes: %s", err)
	} else {
		debugPrintf("successfully refreshed node list, count: %d", len(cachedNodeList))
		cacheLastUpdate = time.Now()
	}

	for {
		responses, err := getNextMessage(radio)
		if err != nil {
			debugPrintf("Error getting next message: %s", err)
			continue
		}
		for _, response := range responses {
			if packet, ok := response.GetPayloadVariant().(*gomeshproto.FromRadio_Packet); ok {

				// is our node list out of date?
				if time.Since(cacheLastUpdate) > 60*time.Second {
					cachedNodeList, err = getMeshNodes(radio)
					if err != nil {
						debugPrintf("Error getting mesh nodes: %s", err)
					} else {
						debugPrintf("successfully refreshed node list, count: %d", len(cachedNodeList))
						cacheLastUpdate = time.Now()
					}
				}

				fmt.Printf("Received packet: %v\n", packet.Packet)
				fmt.Printf(
					"Received packet: %v\n",
					packet.Packet.GetDecoded(),
				)

				mp := MeshPacket{}
				mp.From, err = getMeshNodeByIDFromNodeList(
					cachedNodeList,
					int64(packet.Packet.From),
				)
				if err != nil {
					debugPrintf("Error getting from node: %s", err)
				}
				mp.To, err = getMeshNodeByIDFromNodeList(
					cachedNodeList,
					int64(packet.Packet.To),
				)

				if err != nil {
					debugPrintf("Error getting to node: %s", err)
				}

				if packet.Packet.To == 4294967295 {
					mp.Broadcast = true
				}

				if err != nil {
					writeError(err)
				}

				mp.ID = int(packet.Packet.GetId())
				mp.Port = packet.Packet.GetDecoded().GetPortnum().String()
				mp.Payload = packet.Packet.GetDecoded().Payload
				mp.Priority = packet.Packet.GetPriority().String()

				// per the firmware, priority unset is treated the same as
				// default, so make them the same in the json
				// https://github.com/meshtastic/protobufs/blob/master/meshtastic/mesh.proto#L1088
				if mp.Priority == "UNSET" {
					mp.Priority = "DEFAULT"
				}
				mp.HopLimit = int(packet.Packet.GetHopLimit())
				mp.HopStart = int(packet.Packet.GetHopStart())
				mp.HopCount = mp.HopStart - mp.HopLimit
				mp.ViaMQTT = packet.Packet.GetViaMqtt()
				mp.RSSI = fmt.Sprintf("%d", packet.Packet.GetRxRssi())
				mp.WantAck = packet.Packet.GetWantAck()
				mp.Delayed = packet.Packet.GetDelayed().String()
				mp.ChannelNum = int(packet.Packet.GetChannel())
				// FIXME lookup channel info

				// FIXME Favorite/Ignored needs new protobuf file in goMesh
				//mp.IsFavorite = packet.Packet.GetIsFavorite()

				// comes in off the wire in epoch seconds
				t := time.Unix(int64(packet.Packet.GetRxTime()), 0).UTC()
				mp.ReceiveTime = &t

				decodedBytes, err := base64.StdEncoding.DecodeString(
					string(mp.Payload),
				)
				if err == nil {
					mp.PayloadDecoded = decodedBytes
				}

				mp.RemoveMeLater = fmt.Sprintf(
					"packet:%#v %+v %v",
					packet.Packet,
					packet.Packet,
					packet.Packet,
				)
				out := jsonPrettyPrint(mp)

				fmt.Println(out)

			}
		}

	}
}

func getReceivedMessages(c *cli.Context) error {
	radio := getRadio(c)
	defer radio.Close()

	if !c.Bool("json") {
		printMessageHeader()
	}
	for {

		responses, err := radio.ReadResponse(false)
		if err != nil {
			return cli.Exit(err.Error(), 0)
		}

		receivedMessages := []*gomeshproto.FromRadio_Packet{}

		for _, response := range responses {
			if packet, ok := response.GetPayloadVariant().(*gomeshproto.FromRadio_Packet); ok {

				fmt.Printf("Received packet: %v\n", packet)
				fmt.Printf("Received packet: %#v\n", packet)
				if packet.Packet.GetDecoded().
					GetPortnum() ==
					gomeshproto.PortNum_TEXT_MESSAGE_APP {
					receivedMessages = append(receivedMessages, packet)
				}
			}
		}

		if len(receivedMessages) > 0 {
			if c.Bool("json") {
				printJsonMessages(receivedMessages)
			} else {
				printMessages(receivedMessages)
			}
			if c.Bool("exit") {
				return nil
			}
		}
	}

}

func sendText(c *cli.Context) error {

	radio := getRadio(c)
	defer radio.Close()

	err := radio.SendTextMessage(
		c.String("message"),
		c.Int64("to"),
		c.Int64("channel"),
	)
	if err != nil {
		return cli.Exit(err, 0)
	}

	return nil
}

func printMessageHeader() {
	fmt.Printf("\n")
	fmt.Printf("Received Messages:\n")
	printDoubleDivider()
	fmt.Printf("| %-15s| ", "From")
	fmt.Printf("%-15s| ", "To")
	fmt.Printf("%-18s| ", "Port Num")
	fmt.Printf("%-10s| ", "Channel")
	fmt.Printf(
		"%-15s ",
		"Payload                                              |\n",
	)
	printSingleDivider()
}

func printMessages(messages []*gomeshproto.FromRadio_Packet) {
	for _, message := range messages {
		fmt.Printf("| %-15s| ", fmt.Sprint(message.Packet.From))
		fmt.Printf("%-15s| ", fmt.Sprint(message.Packet.To))
		fmt.Printf(
			"%-18s| ",
			message.Packet.GetDecoded().GetPortnum().String(),
		)
		fmt.Printf("%-10s| ", fmt.Sprint(message.Packet.Channel))
		re := regexp.MustCompile(`\r?\n`)
		escMesg := re.ReplaceAllString(
			string(message.Packet.GetDecoded().Payload),
			"",
		)
		fmt.Printf("%-53q", escMesg)
		fmt.Printf("%s", "|\n")
	}
}

func printJsonMessages(messages []*gomeshproto.FromRadio_Packet) error {

	type MeshTextMessage struct {
		From         string           `json:"from,omitempty"`
		To           string           `json:"to,omitempty"`
		Portnum      string           `json:"portnum,omitempty"`
		ChannelIndex string           `json:"channel,omitempty"`
		Payload      string           `json:"Payload,omitempty"`
		NodeInfo     *json.RawMessage `json:"node,omitempty"`
		ChannelInfo  *json.RawMessage `json:"channelInfo,omitempty"`
	}

	for _, message := range messages {

		meshTextMessage := MeshTextMessage{
			From:         fmt.Sprint(message.Packet.From),
			To:           fmt.Sprint(message.Packet.To),
			Portnum:      message.Packet.GetDecoded().GetPortnum().String(),
			ChannelIndex: fmt.Sprint(message.Packet.Channel),
			Payload:      string(message.Packet.GetDecoded().Payload),
		}

		output, err := json.Marshal(meshTextMessage)
		if err != nil {
			return err
		}
		fmt.Println(string(output))
	}

	return nil
}

package event

const RedPacketCacheBuildEventType = "redpacket.cache.build_requested"

// RedPacketCacheBuildEvent asks the cache projector to build the send result
// and claimable-share candidates from committed MySQL facts.
type RedPacketCacheBuildEvent struct {
	EventID     int64  `json:"eventId,string"`
	EventType   string `json:"eventType"`
	Version     int    `json:"version"`
	RedPacketID int64  `json:"redPacketId,string"`
}

func NewRedPacketCacheBuildEvent(eventID, redPacketID int64) RedPacketCacheBuildEvent {
	return RedPacketCacheBuildEvent{
		EventID:     eventID,
		EventType:   RedPacketCacheBuildEventType,
		Version:     1,
		RedPacketID: redPacketID,
	}
}

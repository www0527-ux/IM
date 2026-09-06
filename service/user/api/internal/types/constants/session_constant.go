package constants

const (
	SessionTypeSingle int64 = iota
	SessionTypeGroup
	SessionTypeRobot
)

const (
	SessionStatusNormal int64 = iota
	SessionStatusDeleted
)

const (
	UserSessionRoleOwner int64 = iota
	UserSessionRoleAdmin
	UserSessionRoleMember
)

const (
	UserSessionStatusNormal int64 = iota
	UserSessionStatusExited
)

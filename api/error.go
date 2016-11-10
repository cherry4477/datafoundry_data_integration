package api

import (
	"fmt"
)

type Error struct {
	code    uint
	message string
}

var (
	Errors            [NumErrors]*Error
	ErrorNone         *Error
	ErrorUnkown       *Error
	ErrorJsonBuilding *Error
)

const (
	ErrorCodeAny = -1 // this is for testing only

	ErrorCodeNone = 0

	ErrorCodeUnkown            = 1300
	ErrorCodeJsonBuilding      = 1301
	ErrorCodeParseJsonFailed   = 1302
	ErrorCodeUrlNotSupported   = 1303
	ErrorCodeDbNotInitlized    = 1304
	ErrorCodeAuthFailed        = 1305
	ErrorCodePermissionDenied  = 1306
	ErrorCodeInvalidParameters = 1307
	ErrorCodeRecordRepository  = 1308
	ErrorCodeUpdateBalance     = 1309
	ErrorCodeModifyApp         = 1310
	ErrorCodeGetApp            = 1311
	ErrorCodeQueryRepositorys  = 1312
	ErrorCodeGetAiPayMsg       = 1313
	ErrorCodeAmountsInvalid    = 1314
	ErrorCodeAmountsNegative   = 1315
	ErrorCodeAmountsTooBig     = 1316
	ErrorCodeQueryDataitemss   = 1317
	ErrorCodeQueryAttribute    = 1317

	NumErrors = 1500 // about 12k memroy wasted
)

func init() {
	initError(ErrorCodeNone, "OK")
	initError(ErrorCodeUnkown, "unknown error")
	initError(ErrorCodeJsonBuilding, "json building error")
	initError(ErrorCodeParseJsonFailed, "parse json failed")

	initError(ErrorCodeUrlNotSupported, "unsupported url")
	initError(ErrorCodeDbNotInitlized, "db is not inited")
	initError(ErrorCodeAuthFailed, "auth failed")
	initError(ErrorCodePermissionDenied, "permission denied")
	initError(ErrorCodeInvalidParameters, "invalid parameters")

	initError(ErrorCodeRecordRepository, "failed to record repository")
	initError(ErrorCodeUpdateBalance, "failed to update balance")
	initError(ErrorCodeModifyApp, "failed to modify app")
	initError(ErrorCodeGetApp, "failed to retrieve app")
	initError(ErrorCodeQueryRepositorys, "failed to query repositorys")
	initError(ErrorCodeGetAiPayMsg, "failed to get aipay message")
	initError(ErrorCodeAmountsInvalid, "recharge amount has more than two decimals")
	initError(ErrorCodeAmountsNegative, "recharge amount is negative")
	initError(ErrorCodeAmountsTooBig, "recharge amount is too big")
	initError(ErrorCodeQueryDataitemss, "failed to query dataitems")
	initError(ErrorCodeQueryAttribute, "failed to query attributes")

	ErrorNone = GetError(ErrorCodeNone)
	ErrorUnkown = GetError(ErrorCodeUnkown)
	ErrorJsonBuilding = GetError(ErrorCodeJsonBuilding)
}

func initError(code uint, message string) {
	if code < NumErrors {
		Errors[code] = newError(code, message)
	}
}

func GetError(code uint) *Error {
	if code > NumErrors {
		return Errors[ErrorCodeUnkown]
	}

	return Errors[code]
}

func GetError2(code uint, message string) *Error {
	e := GetError(code)
	if e == nil {
		return newError(code, message)
	} else {
		return newError(code, fmt.Sprintf("%s (%s)", e.message, message))
	}
}

func newError(code uint, message string) *Error {
	return &Error{code: code, message: message}
}

func newUnknownError(message string) *Error {
	return &Error{
		code:    ErrorCodeUnkown,
		message: message,
	}
}

func newInvalidParameterError(paramName string) *Error {
	return &Error{
		code:    ErrorCodeInvalidParameters,
		message: fmt.Sprintf("%s: %s", GetError(ErrorCodeInvalidParameters).message, paramName),
	}
}

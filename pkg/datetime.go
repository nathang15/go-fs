package pkg

import "time"

const dateTimeFormat = "2024-05-01 15.04.05"

const longDateTimeFormat = "2024-05-01 15:04:05.000"

func ConvertCurrentTimeToString() string {
	return time.Now().Format(dateTimeFormat)
}

func ConvertTimeToLongString(time time.Time) string {
	return time.Format(longDateTimeFormat)
}

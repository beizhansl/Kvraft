package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

var debugStart time.Time
var debugVerbosity int
var ofile *os.File

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dApply   logTopic = "APPLY"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	var err error
	ofile, err = os.OpenFile("./log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("cannot open log")
	}
	// log.SetOutput(os.Stdout)
	log.SetOutput(ofile)
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	time := time.Since(debugStart).Microseconds()
	time /= 1000
	prefix := fmt.Sprintf("%06d %v ", time, string(topic))
	format = prefix + format
	log.Printf(format, a...)

}

//Debug(dTimer, "S%d Leader, checking heartbeats", rf.me)
func HBLog(who int, term int) {
	Debug(dTimer, "S%d Leader, checking heartbeats at T%d", who, term)
}

//008258 VOTE S1 <- S0 Got vote
func RVLog(from int, to int) {
	Debug(dVote, "S%d <- S%d Got vote", to, from)
}

//008258 VOTE S4 Granting Vote to S1 at T1
func TVLog(from int, to int, term int) {
	Debug(dVote, "S%d Granting Vote to S%d at T%d", from, to, term)
}

func RequestCallFailLog(from int, to int, term int) {
	Debug(dError, "S%d Request Call Fail  S%d at T%d", from, to, term)
}

func AppendCallFailLog(from int, to int, term int) {
	Debug(dError, "S%d Append Call Fail  S%d at T%d", from, to, term)
}
func InstallCallFailLog(from int, to int, term int) {
	Debug(dError, "S%d Install Call Fail  S%d at T%d", from, to, term)
}

func ToFollowerLog(from int, term int) {
	Debug(dInfo, "S%d To Follower at T%d", from, term)
}

func ToCandidateLog(from int, term int) {
	Debug(dInfo, "S%d To Candidate at T%d", from, term)
}

func RequestBroLog(from int, to int, term int) {
	Debug(dInfo, "S%d Request S%d at T%d", from, to, term)
}

func ReadSnapshotLog(from int, index int, term int) {
	Debug(dInfo, "S%d Read Snapshot I%d at T%d", from, index, term)
}

func ToLeaderLog(from int, term int) {
	Debug(dLeader, "S%d To Leader at T%d", from, term)
}

func AddAppendEntriesLog(from int, index int, term int) {
	Debug(dClient, "S%d Add New Log I%d at T%d", from, index, term)
}

func SnapshotLog(from int, index int, term int) {
	Debug(dClient, "S%d Snapshot Log I%d at T%d", from, index, term)
}

func CommitIndexLog(who int, index int, term int) {
	Debug(dCommit, "S%d Commit Index I%d T%d", who, index, term)
}
func ApplyIndexLog(who int, index int, term int) {
	Debug(dApply, "S%d Apply Index I%d T%d", who, index, term)
}
func ApplySnapshotLog(who int, index int, term int) {
	Debug(dApply, "S%d Apply Snapshot I%d T%d", who, index, term)
}
func SpliteLineLog() {
	Debug(dInfo, "------------------------------")
}

package concurrency

import (
	"fmt"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"log"
	"time"
)

//debug=2,info=1
func (c *Concurrency) logDebug(format string, v ...interface{}) {
	if c.LogLevel >= 2 {
		fmt.Println(fmt.Sprintf(format, v...))
	}
}

func (c *Concurrency) logInfo(format string, v ...interface{}) {
	if c.LogLevel >= 1 {
		fmt.Println(fmt.Sprintf(format, v...))
	}
}

func (c *Concurrency) logFault(format string, v ...interface{}) {
	log.Fatal(fmt.Sprintf(format, v...))
}

func (c *Concurrency) initProgress() {
	c.progressMap = map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true, 10: true}
}

func (c *Concurrency) progressPrint(taskName string, taskFinishNum, taskTotalNum int) {
	var progress = float32(taskFinishNum) / float32(taskTotalNum)
	var index = 0

	if taskName == "" {
		taskName = "Task"
	}

	if progress >= 1.0 {
		index = 10
	} else if progress >= 0.9 {
		index = 9
	} else if progress >= 0.8 {
		index = 8
	} else if progress >= 0.7 {
		index = 7
	} else if progress >= 0.6 {
		index = 6
	} else if progress >= 0.5 {
		index = 5
	} else if progress >= 0.4 {
		index = 4
	} else if progress >= 0.3 {
		index = 3
	} else if progress >= 0.2 {
		index = 2
	} else if progress >= 0.1 {
		index = 1
	}

	if c.progressMap[index] {
		c.progressMap[index] = false
		if taskFinishNum == taskTotalNum {
			c.logInfo(fmt.Sprintf("(%s) completed at %s, Takes %s times", taskName, time.Now().Format("15:04:05"), time.Now().Sub(c.startTime)))
		} else {
			c.logInfo(fmt.Sprintf("(%s) progress: %d / %d percentage: %d%%", taskName, taskFinishNum, taskTotalNum, index*10))
		}
	}
}

// 获取CPU占用率
func getCpuPercent() float64 {
	percent, _ := cpu.Percent(time.Second, false)
	if len(percent) > 0 {
		return percent[0]
	}
	return 0
}

// 获取内存占用率
func getMemPercent() float64 {
	memInfo, _ := mem.VirtualMemory()
	return memInfo.UsedPercent
}
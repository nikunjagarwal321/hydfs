package main

import (
	"fmt"
	"math/rand"
	"time"
)

func startSwim() {

	// Get self address from global server
	var selfAddress string
	if globalServer != nil {
		selfAddress = globalServer.Addr
	}

	// Get membership list from global server (only alive and suspect members)
	var members []Member
	if globalServer != nil {
		for _, member := range globalServer.Members.GetAll() {
			if member.Status == StatusAlive || member.Status == StatusSuspect {
				members = append(members, member)
			}
		}
	}

	// Shuffle the list every time
	rand.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})
	fmt.Println("------------- SWIM: Reshuffled membership list -------------")

	// Ping all members one by one excluding self
	for _, member := range members {
		if member.Address != selfAddress {
			fmt.Printf("SWIM: Sending PING to %s\n", member.Address)
			_, err := CallPing(member.Address, selfAddress, member.Address, 123)
			if err != nil {
				fmt.Printf("SWIM: PING failed to %s: %v\n", member.Address, err)
				// Mark member as suspect
				member.Status = StatusSuspect
				member.LastUpdated = time.Now()
				// Update membership list
				if globalServer != nil {
					globalServer.Members.AddOrUpdate(member)
				}
				// Start suspect timer
				go startSuspectTimer(member)
			} else {
				fmt.Printf("SWIM: PING successful to %s\n", member.Address)
			}
		}
	}
}

// startSuspectTimer starts a timer for suspect timeout
func startSuspectTimer(member Member) {
	fmt.Printf("SWIM: Starting suspect timer for %s (15 seconds)\n", member.ID())

	time.Sleep(15 * time.Second) // 15 second suspect timeout

	// Check if still suspected
	if globalServer != nil {
		snapshot := globalServer.Members.Snapshot()
		if currentMember, exists := snapshot[member.ID()]; exists {
			if currentMember.Status == StatusSuspect {
				// Mark as dead (keep in membership list)
				currentMember.Status = StatusDead
				currentMember.LastUpdated = time.Now()
				globalServer.Members.AddOrUpdate(currentMember)

				fmt.Printf("SWIM: Suspect timer expired, marked %s as FAILED\n", member.ID())
			}
		}
	}
}

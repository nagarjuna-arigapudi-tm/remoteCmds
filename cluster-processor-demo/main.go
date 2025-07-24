package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"cluster-ui/remoteCmds"
)

func main() {
	fmt.Println("Cluster Processor Demo")
	fmt.Println("=" + strings.Repeat("=", 50))

	// Configuration
	sshKeyPath := os.Getenv("SSH_KEY_PATH")
	if sshKeyPath == "" {
		sshKeyPath = filepath.Join(os.Getenv("HOME"), ".ssh", "id_rsa")
	}

	username := os.Getenv("SSH_USERNAME")
	if username == "" {
		username = os.Getenv("USER")
	}

	csvFilePath := "master.csv"

	fmt.Printf("SSH Key Path: %s\n", sshKeyPath)
	fmt.Printf("SSH Username: %s\n", username)
	fmt.Printf("CSV File Path: %s\n", csvFilePath)
	fmt.Println()

	// Create cluster processor
	fmt.Println("1. Creating ClusterProcessor instance...")
	cp, err := remoteCmds.NewClusterProcessor(sshKeyPath, username)
	if err != nil {
		log.Fatalf("Failed to create cluster processor: %v", err)
	}
	fmt.Printf("✓ ClusterProcessor created successfully\n")
	fmt.Printf("  Global cluster list: %v\n", cp.ClusterList)
	fmt.Println()

	// Create sample CSV file if it doesn't exist
	if _, err := os.Stat(csvFilePath); os.IsNotExist(err) {
		fmt.Println("2. Creating sample CSV file...")
		err := createSampleCSV(csvFilePath)
		if err != nil {
			log.Fatalf("Failed to create sample CSV: %v", err)
		}
		fmt.Printf("✓ Sample CSV file created: %s\n", csvFilePath)
	} else {
		fmt.Printf("2. Using existing CSV file: %s\n", csvFilePath)
	}
	fmt.Println()

	// Process CSV data
	fmt.Println("3. Processing CSV data...")
	err = cp.ReadCSVAndProcess(csvFilePath)
	if err != nil {
		log.Fatalf("Failed to process CSV: %v", err)
	}
	fmt.Printf("✓ CSV data processed successfully\n")
	fmt.Println()

	// Print cluster summary
	fmt.Println("4. Displaying cluster summary...")
	cp.PrintClusterSummary()
	fmt.Println()

	// Save results to JSON
	fmt.Println("5. Saving results to JSON...")
	err = cp.SaveResultsToJSON("cluster_results.json")
	if err != nil {
		log.Printf("Failed to save JSON: %v", err)
	} else {
		fmt.Printf("✓ Results saved to cluster_results.json\n")
	}
	fmt.Println()

	// Get cluster names
	fmt.Println("6. Getting available cluster names...")
	clusterNames := cp.GetClusterNames()
	fmt.Printf("Available clusters with data: %v\n", clusterNames)
	fmt.Println()

	// Demonstrate cluster operations (if clusters have data)
	if len(clusterNames) > 0 {
		targetCluster := clusterNames[0]
		fmt.Printf("7. Demonstrating operations on cluster: %s\n", targetCluster)

		// Get hostnames from cluster
		hostnames, err := cp.GetHostnamesFromCluster(targetCluster)
		if err != nil {
			log.Printf("Failed to get hostnames: %v", err)
		} else {
			fmt.Printf("Hostnames in %s: %v\n", targetCluster, hostnames)
		}

		// Demonstrate certificate validation functions
		demonstrateCertificateValidation(cp, targetCluster, hostnames)

		// Demonstrate SSH operations (commented out to avoid actual SSH connections)
		demonstrateSSHOperations(cp, targetCluster)
	} else {
		fmt.Println("7. No clusters with data found - skipping cluster operations")
	}

	// Demonstrate standalone certificate functions
	fmt.Println()
	demonstrateStandaloneCertificateFunctions()

	fmt.Println()
	fmt.Println("Demo completed successfully!")
	fmt.Println("=" + strings.Repeat("=", 50))
}

func createSampleCSV(filePath string) error {
	content := `Hostname,Type,#,Cluster Name
server-01.prod.com,application,5,prod-cluster-01
server-02.prod.com,database,3,prod-cluster-01
server-03.prod.com,cache,2,prod-cluster-01
web-01.prod.com,web,4,prod-cluster-02
web-02.prod.com,web,4,prod-cluster-02
db-01.prod.com,database,8,prod-cluster-02
dev-server-01.dev.com,application,1,dev-cluster-01
dev-server-02.dev.com,database,1,dev-cluster-01
dev-server-03.dev.com,web,1,dev-cluster-01
staging-app-01.staging.com,application,2,staging-cluster-01
staging-app-02.staging.com,database,2,staging-cluster-01
test-server-01.test.com,application,1,test-cluster-01
test-server-02.test.com,database,1,test-cluster-01
backup-server-01.backup.com,storage,10,backup-cluster-01
backup-server-02.backup.com,storage,10,backup-cluster-01
other-server-01.other.com,application,1,other-cluster-01
random-server-01.random.com,web,1,random-cluster-01
unmatched-server-01.unmatched.com,database,1,unmatched-cluster-01`

	return os.WriteFile(filePath, []byte(content), 0644)
}

func demonstrateCertificateValidation(cp *remoteCmds.ClusterProcessor, clusterName string, hostnames []string) {
	fmt.Printf("\n--- Certificate Validation Demo for %s ---\n", clusterName)

	if len(hostnames) == 0 {
		fmt.Println("No hostnames available for certificate validation demo")
		return
	}

	// Note: These operations would require actual SSH connectivity
	// In a real environment, uncomment and modify as needed

	fmt.Println("Certificate validation functions available:")
	fmt.Printf("• ValidateCertificatesOnCluster() - Check certificates on all hosts in cluster\n")
	fmt.Printf("• Individual certificate validation functions\n")
	fmt.Printf("• Batch certificate operations\n")

	// Example of how to use certificate validation (commented to avoid SSH errors)
	/*
		certPath := "/etc/ssl/certs/server.crt"
		fmt.Printf("Checking certificates at path: %s\n", certPath)

		results, err := cp.ValidateCertificatesOnCluster(clusterName, certPath, username)
		if err != nil {
			fmt.Printf("Certificate validation failed: %v\n", err)
		} else {
			fmt.Printf("Certificate validation results:\n")
			for hostname, days := range results {
				if days >= 0 {
					fmt.Printf("  %s: Certificate expires in %d days\n", hostname, days)
				} else if days == -9999 {
					fmt.Printf("  %s: Error checking certificate\n", hostname)
				} else {
					fmt.Printf("  %s: Certificate expired %d days ago\n", hostname, -days)
				}
			}
		}
	*/

	fmt.Printf("(Certificate validation demo skipped - requires SSH connectivity)\n")
}

func demonstrateSSHOperations(cp *remoteCmds.ClusterProcessor, clusterName string) {
	fmt.Printf("\n--- SSH Operations Demo for %s ---\n", clusterName)

	fmt.Println("SSH operations available:")
	fmt.Printf("• ExecuteCommandsOnCluster() - Run commands on all hosts\n")
	fmt.Printf("• CopyFilesFromCluster() - Copy files from all hosts\n")

	// Example of how to use SSH operations (commented to avoid SSH errors)
	/*
		// Execute command on cluster
		command := "uname -a"
		fmt.Printf("Executing command '%s' on cluster %s\n", command, clusterName)

		results, err := cp.ExecuteCommandsOnCluster(clusterName, command, username)
		if err != nil {
			fmt.Printf("Command execution failed: %v\n", err)
		} else {
			fmt.Printf("Command execution results:\n")
			for hostname, result := range results {
				if result.Error != nil {
					fmt.Printf("  %s: Error - %v\n", hostname, result.Error)
				} else {
					fmt.Printf("  %s: Success - %s\n", hostname, strings.TrimSpace(result.Output))
				}
			}
		}

		// Copy files from cluster
		sourceFile := "/etc/hostname"
		destDir := "./downloads"
		fmt.Printf("Copying file '%s' from cluster %s to %s\n", sourceFile, clusterName, destDir)

		copyResults, err := cp.CopyFilesFromCluster(clusterName, sourceFile, destDir, username)
		if err != nil {
			fmt.Printf("File copy failed: %v\n", err)
		} else {
			fmt.Printf("File copy results:\n")
			for hostname, success := range copyResults {
				if success {
					fmt.Printf("  %s: File copied successfully\n", hostname)
				} else {
					fmt.Printf("  %s: File copy failed\n", hostname)
				}
			}
		}
	*/

	fmt.Printf("(SSH operations demo skipped - requires SSH connectivity)\n")
}

func demonstrateStandaloneCertificateFunctions() {
	fmt.Printf("\n--- Standalone Certificate Functions Demo ---\n")

	// Demonstrate DaysForCertToExpireRemote function
	fmt.Println("Main certificate function: DaysForCertToExpireRemote()")

	// Example usage (commented to avoid SSH errors)
	/*
		hosts := []string{"server01.example.com", "server02.example.com", "server03.example.com"}
		certPath := "/etc/ssl/certs"
		formatString := "%s.crt"

		fmt.Printf("Checking certificate expiry for hosts: %v\n", hosts)
		fmt.Printf("Certificate path: %s\n", certPath)
		fmt.Printf("Format string: %s\n", formatString)

		results, err := remoteCmds.DaysForCertToExpireRemote(hosts, certPath, formatString)
		if err != nil {
			fmt.Printf("Certificate check failed: %v\n", err)
		} else {
			fmt.Printf("Certificate expiry results:\n")
			for hostname, days := range results {
				if days >= 0 {
					fmt.Printf("  %s: Certificate expires in %d days\n", hostname, days)
				} else if days == -9999 {
					fmt.Printf("  %s: Error checking certificate\n", hostname)
				} else {
					fmt.Printf("  %s: Certificate expired %d days ago\n", hostname, -days)
				}
			}
		}
	*/

	fmt.Printf("Function signature: DaysForCertToExpireRemote(remoteHosts []string, remoteCertPath string, formatString string) (map[string]int, error)\n")
	fmt.Printf("Parameters:\n")
	fmt.Printf("  - remoteHosts: []string - List of hostnames to check\n")
	fmt.Printf("  - remoteCertPath: string - Base path where certificates are located\n")
	fmt.Printf("  - formatString: string - Format to derive cert name (e.g., \"%%s.crt\")\n")
	fmt.Printf("Returns:\n")
	fmt.Printf("  - map[string]int - hostname -> days until expiry (negative if expired)\n")
	fmt.Printf("  - error - any error that occurred\n")

	fmt.Printf("\nOther certificate functions available:\n")
	fmt.Printf("• GetCertificateInfo() - Get detailed certificate information\n")
	fmt.Printf("• ValidateCertificateChain() - Validate certificate chain\n")
	fmt.Printf("• BatchValidateCertificates() - Validate multiple certificates\n")
	fmt.Printf("• CheckRemoteCertificateExpiry() - Check remote HTTPS certificates\n")
	fmt.Printf("• GenerateCertificateReport() - Generate comprehensive reports\n")
	fmt.Printf("• CheckCertificatePermissions() - Check file permissions\n")

	fmt.Printf("(Certificate functions demo skipped - requires SSH connectivity)\n")
}

// Additional utility functions for demonstration

func demonstrateAdvancedFeatures(cp *remoteCmds.ClusterProcessor) {
	fmt.Printf("\n--- Advanced Features Demo ---\n")

	// Show how to work with the cluster data directly
	fmt.Println("Direct access to cluster data:")
	for clusterName, hosts := range cp.Data {
		fmt.Printf("Cluster %s has %d hosts:\n", clusterName, len(hosts))
		for _, host := range hosts {
			fmt.Printf("  - %s (%s) count: %d\n", host.HostName, host.Type, host.Count)
		}
	}

	// Show how to filter hosts by type
	fmt.Println("\nFiltering hosts by type:")
	webServers := filterHostsByType(cp, "web")
	dbServers := filterHostsByType(cp, "database")
	appServers := filterHostsByType(cp, "application")

	fmt.Printf("Web servers: %d\n", len(webServers))
	fmt.Printf("Database servers: %d\n", len(dbServers))
	fmt.Printf("Application servers: %d\n", len(appServers))
}

func filterHostsByType(cp *remoteCmds.ClusterProcessor, hostType string) []remoteCmds.HostData {
	var filtered []remoteCmds.HostData

	for _, hosts := range cp.Data {
		for _, host := range hosts {
			if strings.ToLower(host.Type) == strings.ToLower(hostType) {
				filtered = append(filtered, host)
			}
		}
	}

	return filtered
}

// Example of how to create a custom certificate validation workflow
func customCertificateWorkflow() {
	fmt.Printf("\n--- Custom Certificate Workflow Example ---\n")

	fmt.Println("Example workflow for certificate management:")
	fmt.Println("1. Read cluster configuration from CSV")
	fmt.Println("2. For each cluster:")
	fmt.Println("   a. Get list of hostnames")
	fmt.Println("   b. Check certificate expiry using DaysForCertToExpireRemote()")
	fmt.Println("   c. Generate alerts for certificates expiring within 30 days")
	fmt.Println("   d. Generate reports for expired certificates")
	fmt.Println("3. Send notifications to administrators")
	fmt.Println("4. Update certificate tracking database")

	fmt.Println("\nCode structure:")
	fmt.Println(`
// Create processor
cp, err := remoteCmds.NewClusterProcessor(sshKey, username)
if err != nil {
    log.Fatal(err)
}

// Process cluster data
err = cp.ReadCSVAndProcess("master.csv")
if err != nil {
    log.Fatal(err)
}

// Check certificates for each cluster
for _, clusterName := range cp.GetClusterNames() {
    hostnames, _ := cp.GetHostnamesFromCluster(clusterName)
    
    // Check certificate expiry
    results, err := remoteCmds.DaysForCertToExpireRemote(
        hostnames, 
        "/etc/ssl/certs", 
        "%s.crt",
    )
    
    // Process results
    for hostname, days := range results {
        if days < 30 && days >= 0 {
            // Certificate expires soon - send alert
        } else if days < 0 {
            // Certificate expired - urgent action needed
        }
    }
}
`)
}

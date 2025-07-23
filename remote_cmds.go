package remoteCmds

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Global cluster list - equivalent to Python clusterList
var ClusterList = []string{
	"prod-cluster-01",
	"prod-cluster-02",
	"dev-cluster-01",
	"staging-cluster-01",
	"test-cluster-01",
	"backup-cluster-01",
}

// HostData represents a single host entry in a cluster
type HostData struct {
	HostName string `json:"hostName"`
	Type     string `json:"type"`
	Count    int    `json:"count"`
}

// ClusterData represents the processed cluster data
type ClusterData map[string][]HostData

// CSVRecord represents a row from the master.csv file
type CSVRecord struct {
	Hostname    string
	Type        string
	Count       string
	ClusterName string
}

// ClusterProcessor handles cluster data processing operations
type ClusterProcessor struct {
	ClusterList []string
	Data        ClusterData
	SSHConfig   *ssh.ClientConfig
}

// NewClusterProcessor creates a new cluster processor instance
func NewClusterProcessor(sshKeyPath, username string) (*ClusterProcessor, error) {
	sshConfig, err := NewSSHConfig(sshKeyPath, username)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSH config: %v", err)
	}

	return &ClusterProcessor{
		ClusterList: ClusterList,
		Data:        make(ClusterData),
		SSHConfig:   sshConfig,
	}, nil
}

// Result represents the outcome of command execution on a remote host
type Result struct {
	HostName string
	Output   string
	Error    error
}

// getSSHConfig creates an SSH client configuration using a private key
func getSSHConfig(privateKeyPath string, user string) (*ssh.ClientConfig, error) {
	privateKey, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key: %v", err)
	}

	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	return &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Note: In production, use proper host key verification
	}, nil
}

// execCommands executes a slice of shell commands on the given SSH session
func execCommands(session *ssh.Session, commands []string) (string, error) {
	var combinedOutput bytes.Buffer
	session.Stdout = &combinedOutput
	session.Stderr = &combinedOutput

	// Combine commands with semicolons
	combinedCmd := ""
	for i, cmd := range commands {
		combinedCmd += cmd
		if i < len(commands)-1 {
			combinedCmd += "; "
		}
	}

	err := session.Run(combinedCmd)
	return combinedOutput.String(), err
}

// ExecOnRemoteHost executes shell commands on a single remote host
func ExecOnRemoteHost(hostName string, shellCmds []string, sshConfig *ssh.ClientConfig) (*Result, error) {
	client, err := ssh.Dial("tcp", hostName+":22", sshConfig)
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to dial host: %v", err),
		}, err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to create session: %v", err),
		}, err
	}
	defer session.Close()

	output, err := execCommands(session, shellCmds)
	return &Result{
		HostName: hostName,
		Output:   output,
		Error:    err,
	}, err
}

// ExecOnMultipleRemoteHosts executes shell commands on multiple remote hosts in parallel
func ExecOnMultipleRemoteHosts(hostNames []string, shellCmds []string, sshConfig *ssh.ClientConfig) []*Result {
	var wg sync.WaitGroup
	resultChan := make(chan *Result, len(hostNames))
	results := make([]*Result, 0, len(hostNames))

	// Execute commands on each host in parallel
	for _, hostName := range hostNames {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()

			result, err := ExecOnRemoteHost(host, shellCmds, sshConfig)
			if err != nil {
				log.Printf("Error executing commands on %s: %v", host, err)
			}
			resultChan <- result
		}(hostName)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for result := range resultChan {
		results = append(results, result)
	}

	return results
}

// CopyFileToRemoteHost copies a local file to a remote host
func CopyFileToRemoteHost(hostName string, localPath string, remotePath string, sshConfig *ssh.ClientConfig) (*Result, error) {
	// Create SSH client
	client, err := ssh.Dial("tcp", hostName+":22", sshConfig)
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to dial host: %v", err),
		}, err
	}
	defer client.Close()

	// Create SFTP client
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to create SFTP client: %v", err),
		}, err
	}
	defer sftpClient.Close()

	// Open local file
	localFile, err := os.Open(localPath)
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to open local file: %v", err),
		}, err
	}
	defer localFile.Close()

	// Ensure remote directory exists
	remoteDir := filepath.Dir(remotePath)
	err = sftpClient.MkdirAll(remoteDir)
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to create remote directory: %v", err),
		}, err
	}

	// Create remote file
	remoteFile, err := sftpClient.Create(remotePath)
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to create remote file: %v", err),
		}, err
	}
	defer remoteFile.Close()

	// Copy file contents
	_, err = io.Copy(remoteFile, localFile)
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to copy file contents: %v", err),
		}, err
	}

	return &Result{
		HostName: hostName,
		Output:   fmt.Sprintf("Successfully copied %s to %s:%s", localPath, hostName, remotePath),
		Error:    nil,
	}, nil
}

// CopyToRemoteHost copies a file or directory to a remote host and sets permissions
func CopyToRemoteHost(hostName string, localPath string, remotePath string, owner string, permissions string, isDirectory bool, sshConfig *ssh.ClientConfig, sshConsecutiveRetry int) (*Result, error) {
	var client *ssh.Client
	var err error
	remainingRetries := sshConsecutiveRetry

	// Try to establish SSH connection with retries
	for remainingRetries > 0 {
		client, err = ssh.Dial("tcp", hostName+":22", sshConfig)
		if err == nil {
			// Connection successful, reset retry count and break
			remainingRetries = sshConsecutiveRetry
			break
		}

		log.Printf("Failed to connect to %s, retrying... (%d attempts remaining): %v", hostName, remainingRetries-1, err)
		remainingRetries--
		if remainingRetries > 0 {
			time.Sleep(2 * time.Second) // Wait before retrying
		}
	}

	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to dial host after %d attempts: %v", sshConsecutiveRetry, err),
		}, err
	}
	defer client.Close()

	// Create SFTP client with retries
	var sftpClient *sftp.Client
	remainingRetries = sshConsecutiveRetry

	for remainingRetries > 0 {
		sftpClient, err = sftp.NewClient(client)
		if err == nil {
			// Connection successful, reset retry count and break
			remainingRetries = sshConsecutiveRetry
			break
		}

		log.Printf("Failed to create SFTP client for %s, retrying... (%d attempts remaining): %v", hostName, remainingRetries-1, err)
		remainingRetries--
		if remainingRetries > 0 {
			time.Sleep(2 * time.Second) // Wait before retrying
		}
	}

	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to create SFTP client after %d attempts: %v", sshConsecutiveRetry, err),
		}, err
	}
	defer sftpClient.Close()

	// Create remote directory
	if err := sftpClient.MkdirAll(filepath.Dir(remotePath)); err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to create remote directory: %v", err),
		}, err
	}

	if isDirectory {
		// Walk through the source directory
		err := filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Calculate relative path
			relPath, err := filepath.Rel(localPath, path)
			if err != nil {
				return fmt.Errorf("failed to get relative path: %v", err)
			}

			// Construct remote path
			remoteFilePath := filepath.Join(remotePath, relPath)

			if info.IsDir() {
				// Create directory on remote
				return sftpClient.MkdirAll(remoteFilePath)
			}

			// Create parent directories if they don't exist
			if err := sftpClient.MkdirAll(filepath.Dir(remoteFilePath)); err != nil {
				return fmt.Errorf("failed to create remote directory: %v", err)
			}

			// Copy file
			srcFile, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open source file: %v", err)
			}
			defer srcFile.Close()

			dstFile, err := sftpClient.Create(remoteFilePath)
			if err != nil {
				return fmt.Errorf("failed to create remote file: %v", err)
			}
			defer dstFile.Close()

			_, err = io.Copy(dstFile, srcFile)
			return err
		})

		if err != nil {
			return &Result{
				HostName: hostName,
				Error:    fmt.Errorf("failed to copy directory: %v", err),
			}, err
		}
	} else {
		// Copy single file
		srcFile, err := os.Open(localPath)
		if err != nil {
			return &Result{
				HostName: hostName,
				Error:    fmt.Errorf("failed to open source file: %v", err),
			}, err
		}
		defer srcFile.Close()

		dstFile, err := sftpClient.Create(remotePath)
		if err != nil {
			return &Result{
				HostName: hostName,
				Error:    fmt.Errorf("failed to create remote file: %v", err),
			}, err
		}
		defer dstFile.Close()

		if _, err := io.Copy(dstFile, srcFile); err != nil {
			return &Result{
				HostName: hostName,
				Error:    fmt.Errorf("failed to copy file: %v", err),
			}, err
		}
	}

	// Create SSH session for chmod/chown commands
	session, err := client.NewSession()
	if err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to create session: %v", err),
		}, err
	}
	defer session.Close()

	// Set ownership and permissions
	cmd := fmt.Sprintf("chown -R %s %s && chmod -R %s %s", owner, remotePath, permissions, remotePath)
	if err := session.Run(cmd); err != nil {
		return &Result{
			HostName: hostName,
			Error:    fmt.Errorf("failed to set permissions: %v", err),
		}, err
	}

	return &Result{
		HostName: hostName,
		Output:   fmt.Sprintf("Successfully copied %s to %s:%s and set permissions", localPath, hostName, remotePath),
		Error:    nil,
	}, nil
}

// CopyToMultipleRemoteHosts copies a file or directory to multiple remote hosts in parallel
func CopyToMultipleRemoteHosts(hostNames []string, localPath string, remotePath string, owner string, permissions string, isDirectory bool, sshConfig *ssh.ClientConfig, sshConsecutiveRetry int) []*Result {
	var wg sync.WaitGroup
	resultChan := make(chan *Result, len(hostNames))
	results := make([]*Result, 0, len(hostNames))

	// Copy to each host in parallel
	for _, hostName := range hostNames {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()

			result, err := CopyToRemoteHost(host, localPath, remotePath, owner, permissions, isDirectory, sshConfig, sshConsecutiveRetry)
			if err != nil {
				log.Printf("Error copying to %s: %v", host, err)
			}
			resultChan <- result
		}(hostName)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for result := range resultChan {
		results = append(results, result)
	}

	return results
}

// CertValidationResult represents the result of certificate validation
type CertValidationResult struct {
	Host            string
	IsValid         bool
	HasValidPerms   bool
	HasValidDays    bool
	HostInSAN       bool
	Errors          []string
	ExpirationDate  string
	RemainingDays   int
	Permissions     string
	SubjectName     string
	SubjectAltNames []string
}

// ValidateCert checks certificate validity on remote hosts
func ValidateCert(remoteHosts []string, remoteCertPath string, expDays int) ([]*CertValidationResult, error) {
	results := make([]*CertValidationResult, 0, len(remoteHosts))

	for _, host := range remoteHosts {
		result := &CertValidationResult{
			Host:   host,
			Errors: make([]string, 0),
		}

		// Create SSH config
		currentUser := os.Getenv("USER")
		if currentUser == "" {
			currentUser = "root"
		}
		sshConfig, err := NewSSHConfig("~/.ssh/id_rsa", currentUser)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("SSH config error: %v", err))
			results = append(results, result)
			continue
		}

		// Check file permissions
		statCmd := fmt.Sprintf("stat -c '%%a %%U %%G' %s", remoteCertPath)
		statResult, err := ExecOnRemoteHost(host, []string{statCmd}, sshConfig)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to check permissions: %v", err))
		} else {
			parts := strings.Fields(statResult.Output)
			if len(parts) == 3 {
				result.Permissions = parts[0]
				// Check if permissions are 600 or 644
				result.HasValidPerms = parts[0] == "600" || parts[0] == "644"
				if !result.HasValidPerms {
					result.Errors = append(result.Errors, fmt.Sprintf("Invalid permissions: %s (expected 600 or 644)", parts[0]))
				}
			}
		}

		// Get certificate info using OpenSSL
		certCmd := fmt.Sprintf(`openssl x509 -in %s -text -noout`, remoteCertPath)
		certResult, err := ExecOnRemoteHost(host, []string{certCmd}, sshConfig)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to read certificate: %v", err))
			results = append(results, result)
			continue
		}

		// Parse certificate info
		certText := certResult.Output

		// Extract expiration date
		expiryCmd := fmt.Sprintf(`openssl x509 -in %s -enddate -noout | cut -d= -f2-`, remoteCertPath)
		expiryResult, err := ExecOnRemoteHost(host, []string{expiryCmd}, sshConfig)
		if err == nil {
			result.ExpirationDate = strings.TrimSpace(expiryResult.Output)

			// Calculate remaining days
			daysCmd := fmt.Sprintf(`openssl x509 -in %s -checkend %d || echo "EXPIRED"`,
				remoteCertPath, expDays*86400) // Convert days to seconds
			daysResult, err := ExecOnRemoteHost(host, []string{daysCmd}, sshConfig)
			if err == nil {
				result.HasValidDays = !strings.Contains(daysResult.Output, "EXPIRED")
				if !result.HasValidDays {
					result.Errors = append(result.Errors, fmt.Sprintf("Certificate will expire within %d days", expDays))
				}
			}
		}

		// Extract Subject Name
		subjectCmd := fmt.Sprintf(`openssl x509 -in %s -subject -noout | sed 's/^subject=//g'`, remoteCertPath)
		subjectResult, err := ExecOnRemoteHost(host, []string{subjectCmd}, sshConfig)
		if err == nil {
			result.SubjectName = strings.TrimSpace(subjectResult.Output)
		}

		// Extract Subject Alternative Names
		if strings.Contains(certText, "Subject Alternative Name") {
			sanCmd := fmt.Sprintf(`openssl x509 -in %s -text -noout | grep -A1 "Subject Alternative Name" | tail -n1`, remoteCertPath)
			sanResult, err := ExecOnRemoteHost(host, []string{sanCmd}, sshConfig)
			if err == nil {
				sans := strings.TrimSpace(sanResult.Output)
				result.SubjectAltNames = parseSANs(sans)

				// Check if host is in SANs
				hostname := host
				if strings.Contains(host, ":") {
					hostname = strings.Split(host, ":")[0]
				}
				result.HostInSAN = containsHost(result.SubjectAltNames, hostname)
				if !result.HostInSAN {
					result.Errors = append(result.Errors, "Host not found in certificate SANs")
				}
			}
		}

		// Additional checks
		if !strings.Contains(certText, "Digital Signature") {
			result.Errors = append(result.Errors, "Certificate missing Digital Signature usage")
		}
		if !strings.Contains(certText, "Key Encipherment") {
			result.Errors = append(result.Errors, "Certificate missing Key Encipherment usage")
		}

		// Set overall validity
		result.IsValid = len(result.Errors) == 0

		results = append(results, result)
	}

	return results, nil
}

// Helper function to parse Subject Alternative Names
func parseSANs(sans string) []string {
	var results []string
	parts := strings.Split(sans, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "DNS:") {
			results = append(results, strings.TrimPrefix(part, "DNS:"))
		} else if strings.HasPrefix(part, "IP:") {
			results = append(results, strings.TrimPrefix(part, "IP:"))
		}
	}
	return results
}

// Helper function to check if a host is in the SANs list
func containsHost(sans []string, host string) bool {
	for _, san := range sans {
		if san == host {
			return true
		}
		// Check wildcard domains
		if strings.HasPrefix(san, "*.") {
			domain := strings.TrimPrefix(san, "*.")
			if strings.HasSuffix(host, domain) && strings.Count(host, ".") == strings.Count(domain, ".")+1 {
				return true
			}
		}
	}
	return false
}

// Helper function to create a new SSH configuration
func NewSSHConfig(privateKeyPath, user string) (*ssh.ClientConfig, error) {
	return getSSHConfig(privateKeyPath, user)
}

// CheckSshConnectivity attempts to establish an SSH session to a remote host
func CheckSshConnectivity(hostname string, sshConfig *ssh.ClientConfig) (bool, error) {
	// Try to establish SSH connection
	client, err := ssh.Dial("tcp", hostname+":22", sshConfig)
	if err != nil {
		return false, fmt.Errorf("failed to dial host: %v", err)
	}
	defer client.Close()

	// Try to create a session
	session, err := client.NewSession()
	if err != nil {
		return false, fmt.Errorf("failed to create session: %v", err)
	}
	defer session.Close()

	return true, nil
}

// CertExpiryResult represents the result of certificate expiry check for a single host
type CertExpiryResult struct {
	HostName     string
	CertPath     string
	DaysToExpiry int
	Error        error
	ExpiryDate   string
	IsExpired    bool
}

// daysForCertToExpireRemote checks certificate expiry on multiple remote hosts
// Parameters:
//   - remoteHosts: slice of hostnames to check
//   - remoteCertPath: base path where certificates are located on remote hosts
//   - formatString: format string to derive cert name from hostname (e.g., "%s.crt")
//
// Returns:
//   - map[string]int: hostname -> days until expiry (negative if expired)
//   - error: if there's a general error with the operation
//
// The function derives certificate names using fmt.Sprintf(formatString, hostname)
// and returns the number of days until expiry for each host.
// Negative values indicate the certificate has already expired.
func DaysForCertToExpireRemote(remoteHosts []string, remoteCertPath string, formatString string) (map[string]int, error) {
	if len(remoteHosts) == 0 {
		return nil, fmt.Errorf("no remote hosts provided")
	}

	if formatString == "" {
		return nil, fmt.Errorf("format string cannot be empty")
	}

	// Create SSH config using default settings
	currentUser := os.Getenv("USER")
	if currentUser == "" {
		currentUser = "root"
	}

	sshConfig, err := NewSSHConfig("~/.ssh/id_rsa", currentUser)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSH config: %v", err)
	}

	results := make(map[string]int)
	var wg sync.WaitGroup
	var mutex sync.Mutex

	// Process each host in parallel
	for _, hostname := range remoteHosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()

			// Derive certificate name using format string
			certName := fmt.Sprintf(formatString, host)
			fullCertPath := filepath.Join(remoteCertPath, certName)

			// Get certificate expiry information
			daysToExpiry, err := getCertExpiryDays(host, fullCertPath, sshConfig)

			mutex.Lock()
			if err != nil {
				log.Printf("Error checking certificate expiry for %s: %v", host, err)
				// Set to a large negative number to indicate error/unknown state
				results[host] = -9999
			} else {
				results[host] = daysToExpiry
			}
			mutex.Unlock()
		}(hostname)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	return results, nil
}

// getCertExpiryDays gets the number of days until certificate expiry for a single host
func getCertExpiryDays(hostname, certPath string, sshConfig *ssh.ClientConfig) (int, error) {
	// Create SSH client
	client, err := ssh.Dial("tcp", hostname+":22", sshConfig)
	if err != nil {
		return -9999, fmt.Errorf("failed to dial host %s: %v", hostname, err)
	}
	defer client.Close()

	// Create SSH session
	session, err := client.NewSession()
	if err != nil {
		return -9999, fmt.Errorf("failed to create session for %s: %v", hostname, err)
	}
	defer session.Close()

	// Check if certificate file exists
	checkCmd := fmt.Sprintf("test -f %s", certPath)
	if err := session.Run(checkCmd); err != nil {
		return -9999, fmt.Errorf("certificate file %s not found on host %s", certPath, hostname)
	}

	// Create new session for the actual certificate check
	session2, err := client.NewSession()
	if err != nil {
		return -9999, fmt.Errorf("failed to create second session for %s: %v", hostname, err)
	}
	defer session2.Close()

	// Get certificate expiry date using OpenSSL
	// This command gets the expiry date and calculates days remaining
	cmd := fmt.Sprintf(`
		# Get the expiry date in seconds since epoch
		EXPIRY_DATE=$(openssl x509 -in %s -enddate -noout | cut -d= -f2)
		EXPIRY_EPOCH=$(date -d "$EXPIRY_DATE" +%%s 2>/dev/null || date -j -f "%%b %%d %%H:%%M:%%S %%Y %%Z" "$EXPIRY_DATE" +%%s 2>/dev/null)
		
		# Get current date in seconds since epoch
		CURRENT_EPOCH=$(date +%%s)
		
		# Calculate difference in seconds and convert to days
		DIFF_SECONDS=$((EXPIRY_EPOCH - CURRENT_EPOCH))
		DAYS=$((DIFF_SECONDS / 86400))
		
		echo $DAYS
	`, certPath)

	var output bytes.Buffer
	session2.Stdout = &output
	session2.Stderr = &output

	if err := session2.Run(cmd); err != nil {
		// If the complex command fails, try a simpler approach
		session3, err := client.NewSession()
		if err != nil {
			return -9999, fmt.Errorf("failed to create third session for %s: %v", hostname, err)
		}
		defer session3.Close()

		// Fallback: use openssl to check if cert expires within a large number of days
		// and then use a binary search approach or direct date parsing
		fallbackCmd := fmt.Sprintf(`
			# Get expiry date
			EXPIRY=$(openssl x509 -in %s -enddate -noout | cut -d= -f2)
			echo "EXPIRY:$EXPIRY"
			
			# Try to get current date
			CURRENT=$(date)
			echo "CURRENT:$CURRENT"
			
			# Check if certificate is expired (will exit with non-zero if expired)
			openssl x509 -in %s -checkend 0 >/dev/null 2>&1
			if [ $? -ne 0 ]; then
				echo "STATUS:EXPIRED"
			else
				echo "STATUS:VALID"
			fi
		`, certPath, certPath)

		var fallbackOutput bytes.Buffer
		session3.Stdout = &fallbackOutput
		session3.Stderr = &fallbackOutput

		if err := session3.Run(fallbackCmd); err != nil {
			return -9999, fmt.Errorf("failed to check certificate on %s: %v", hostname, err)
		}

		// Parse fallback output
		fallbackResult := fallbackOutput.String()
		if strings.Contains(fallbackResult, "STATUS:EXPIRED") {
			// Certificate is expired, try to determine how long ago
			return parseExpiredCertificate(hostname, certPath, sshConfig)
		} else if strings.Contains(fallbackResult, "STATUS:VALID") {
			// Certificate is valid, try to determine days remaining using checkend
			return parseValidCertificate(hostname, certPath, sshConfig)
		}

		return -9999, fmt.Errorf("unable to determine certificate status for %s", hostname)
	}

	// Parse the output to get days
	outputStr := strings.TrimSpace(output.String())
	var days int
	if _, err := fmt.Sscanf(outputStr, "%d", &days); err != nil {
		return -9999, fmt.Errorf("failed to parse days output '%s' for %s: %v", outputStr, hostname, err)
	}

	return days, nil
}

// parseExpiredCertificate determines how many days ago an expired certificate expired
func parseExpiredCertificate(hostname, certPath string, sshConfig *ssh.ClientConfig) (int, error) {
	client, err := ssh.Dial("tcp", hostname+":22", sshConfig)
	if err != nil {
		return -9999, fmt.Errorf("failed to dial host %s: %v", hostname, err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return -9999, fmt.Errorf("failed to create session for %s: %v", hostname, err)
	}
	defer session.Close()

	// Try to calculate days since expiry
	cmd := fmt.Sprintf(`
		# Get the expiry date
		EXPIRY_DATE=$(openssl x509 -in %s -enddate -noout | cut -d= -f2)
		
		# Try different date parsing approaches
		EXPIRY_EPOCH=$(date -d "$EXPIRY_DATE" +%%s 2>/dev/null)
		if [ -z "$EXPIRY_EPOCH" ]; then
			# Try BSD date format (macOS)
			EXPIRY_EPOCH=$(date -j -f "%%b %%d %%H:%%M:%%S %%Y %%Z" "$EXPIRY_DATE" +%%s 2>/dev/null)
		fi
		
		if [ -n "$EXPIRY_EPOCH" ]; then
			CURRENT_EPOCH=$(date +%%s)
			DIFF_SECONDS=$((CURRENT_EPOCH - EXPIRY_EPOCH))
			DAYS=$((DIFF_SECONDS / 86400))
			echo $DAYS
		else
			echo "ERROR"
		fi
	`, certPath)

	var output bytes.Buffer
	session.Stdout = &output
	session.Stderr = &output

	if err := session.Run(cmd); err != nil {
		return -30, nil // Default to 30 days expired if we can't determine exact date
	}

	outputStr := strings.TrimSpace(output.String())
	if outputStr == "ERROR" {
		return -30, nil // Default to 30 days expired
	}

	var daysSinceExpiry int
	if _, err := fmt.Sscanf(outputStr, "%d", &daysSinceExpiry); err != nil {
		return -30, nil // Default to 30 days expired
	}

	// Return negative value to indicate expiry
	return -daysSinceExpiry, nil
}

// parseValidCertificate determines how many days until a valid certificate expires
func parseValidCertificate(hostname, certPath string, sshConfig *ssh.ClientConfig) (int, error) {
	client, err := ssh.Dial("tcp", hostname+":22", sshConfig)
	if err != nil {
		return -9999, fmt.Errorf("failed to dial host %s: %v", hostname, err)
	}
	defer client.Close()

	// Use binary search approach to find approximate days until expiry
	// Start with common ranges
	testDays := []int{1, 7, 30, 90, 180, 365, 730, 1095} // 1 day to 3 years

	var lastValidDays int = 0

	for _, days := range testDays {
		session, err := client.NewSession()
		if err != nil {
			continue
		}

		// Check if certificate expires within 'days' days
		cmd := fmt.Sprintf("openssl x509 -in %s -checkend %d", certPath, days*86400)
		err = session.Run(cmd)
		session.Close()

		if err != nil {
			// Certificate expires within 'days' days
			// The actual expiry is between lastValidDays and days
			return estimateDaysBetween(hostname, certPath, sshConfig, lastValidDays, days)
		}
		lastValidDays = days
	}

	// Certificate expires in more than 3 years, return a large positive number
	return 1095, nil
}

// estimateDaysBetween uses binary search to find more precise expiry days
func estimateDaysBetween(hostname, certPath string, sshConfig *ssh.ClientConfig, minDays, maxDays int) (int, error) {
	client, err := ssh.Dial("tcp", hostname+":22", sshConfig)
	if err != nil {
		return minDays, fmt.Errorf("failed to dial host %s: %v", hostname, err)
	}
	defer client.Close()

	// Binary search between minDays and maxDays
	for maxDays-minDays > 1 {
		midDays := (minDays + maxDays) / 2

		session, err := client.NewSession()
		if err != nil {
			break
		}

		cmd := fmt.Sprintf("openssl x509 -in %s -checkend %d", certPath, midDays*86400)
		err = session.Run(cmd)
		session.Close()

		if err != nil {
			// Certificate expires within midDays days
			maxDays = midDays
		} else {
			// Certificate does not expire within midDays days
			minDays = midDays
		}
	}

	return minDays, nil
}

// CertificateInfo represents detailed information about a certificate
type CertificateInfo struct {
	FilePath           string
	Format             string
	Subject            string
	Issuer             string
	SerialNumber       string
	NotValidBefore     string
	NotValidAfter      string
	DaysUntilExpiry    int
	IsExpired          bool
	SignatureAlgorithm string
	PublicKeyType      string
	PublicKeySize      int
	SubjectAltNames    []string
	KeyUsage           []string
	ExtendedKeyUsage   []string
}

// CertificateValidationResult represents the result of certificate validation
type CertificateValidationResult struct {
	FilePath        string
	IsValidFormat   bool
	IsExpired       bool
	DaysUntilExpiry int
	Warnings        []string
	Errors          []string
	IsValid         bool
	Summary         string
}

// GetCertificateInfo retrieves detailed information about a certificate on a remote host
func GetCertificateInfo(hostname, certPath string, sshConfig *ssh.ClientConfig) (*CertificateInfo, error) {
	client, err := ssh.Dial("tcp", hostname+":22", sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial host %s: %v", hostname, err)
	}
	defer client.Close()

	// Check if certificate file exists
	session, err := client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create session for %s: %v", hostname, err)
	}
	defer session.Close()

	checkCmd := fmt.Sprintf("test -f %s", certPath)
	if err := session.Run(checkCmd); err != nil {
		return nil, fmt.Errorf("certificate file %s not found on host %s", certPath, hostname)
	}

	certInfo := &CertificateInfo{
		FilePath: certPath,
		Format:   "PEM", // Assume PEM format for now
	}

	// Get certificate text output
	session2, err := client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create session for %s: %v", hostname, err)
	}
	defer session2.Close()

	certTextCmd := fmt.Sprintf("openssl x509 -in %s -text -noout", certPath)
	var certTextOutput bytes.Buffer
	session2.Stdout = &certTextOutput
	session2.Stderr = &certTextOutput

	if err := session2.Run(certTextCmd); err != nil {
		return nil, fmt.Errorf("failed to read certificate on %s: %v", hostname, err)
	}

	certText := certTextOutput.String()

	// Extract Subject
	if subject := extractCertField(client, certPath, "subject"); subject != "" {
		certInfo.Subject = subject
	}

	// Extract Issuer
	if issuer := extractCertField(client, certPath, "issuer"); issuer != "" {
		certInfo.Issuer = issuer
	}

	// Extract Serial Number
	if serial := extractCertField(client, certPath, "serial"); serial != "" {
		certInfo.SerialNumber = serial
	}

	// Extract validity dates
	if startDate := extractCertField(client, certPath, "startdate"); startDate != "" {
		certInfo.NotValidBefore = startDate
	}

	if endDate := extractCertField(client, certPath, "enddate"); endDate != "" {
		certInfo.NotValidAfter = endDate
	}

	// Get days until expiry
	if days, err := getCertExpiryDays(hostname, certPath, sshConfig); err == nil {
		certInfo.DaysUntilExpiry = days
		certInfo.IsExpired = days < 0
	}

	// Extract signature algorithm
	if strings.Contains(certText, "Signature Algorithm:") {
		lines := strings.Split(certText, "\n")
		for _, line := range lines {
			if strings.Contains(line, "Signature Algorithm:") {
				parts := strings.Split(line, ":")
				if len(parts) > 1 {
					certInfo.SignatureAlgorithm = strings.TrimSpace(parts[1])
					break
				}
			}
		}
	}

	// Extract public key information
	if strings.Contains(certText, "Public Key Algorithm:") {
		lines := strings.Split(certText, "\n")
		for i, line := range lines {
			if strings.Contains(line, "Public Key Algorithm:") {
				parts := strings.Split(line, ":")
				if len(parts) > 1 {
					certInfo.PublicKeyType = strings.TrimSpace(parts[1])
				}
				// Look for key size in next few lines
				for j := i + 1; j < len(lines) && j < i+5; j++ {
					if strings.Contains(lines[j], "bit") {
						var keySize int
						if _, err := fmt.Sscanf(lines[j], "%d", &keySize); err == nil {
							certInfo.PublicKeySize = keySize
						}
						break
					}
				}
				break
			}
		}
	}

	// Extract Subject Alternative Names
	certInfo.SubjectAltNames = extractSubjectAltNames(client, certPath)

	// Extract Key Usage
	certInfo.KeyUsage = extractKeyUsage(certText)

	// Extract Extended Key Usage
	certInfo.ExtendedKeyUsage = extractExtendedKeyUsage(certText)

	return certInfo, nil
}

// extractCertField extracts a specific field from a certificate using OpenSSL
func extractCertField(client *ssh.Client, certPath, field string) string {
	session, err := client.NewSession()
	if err != nil {
		return ""
	}
	defer session.Close()

	var cmd string
	switch field {
	case "subject":
		cmd = fmt.Sprintf("openssl x509 -in %s -subject -noout | sed 's/^subject=//g'", certPath)
	case "issuer":
		cmd = fmt.Sprintf("openssl x509 -in %s -issuer -noout | sed 's/^issuer=//g'", certPath)
	case "serial":
		cmd = fmt.Sprintf("openssl x509 -in %s -serial -noout | sed 's/^serial=//g'", certPath)
	case "startdate":
		cmd = fmt.Sprintf("openssl x509 -in %s -startdate -noout | sed 's/^notBefore=//g'", certPath)
	case "enddate":
		cmd = fmt.Sprintf("openssl x509 -in %s -enddate -noout | sed 's/^notAfter=//g'", certPath)
	default:
		return ""
	}

	var output bytes.Buffer
	session.Stdout = &output
	session.Stderr = &output

	if err := session.Run(cmd); err != nil {
		return ""
	}

	return strings.TrimSpace(output.String())
}

// extractSubjectAltNames extracts Subject Alternative Names from a certificate
func extractSubjectAltNames(client *ssh.Client, certPath string) []string {
	session, err := client.NewSession()
	if err != nil {
		return nil
	}
	defer session.Close()

	cmd := fmt.Sprintf("openssl x509 -in %s -text -noout | grep -A1 'Subject Alternative Name' | tail -n1", certPath)
	var output bytes.Buffer
	session.Stdout = &output
	session.Stderr = &output

	if err := session.Run(cmd); err != nil {
		return nil
	}

	sans := strings.TrimSpace(output.String())
	return parseSANs(sans)
}

// extractKeyUsage extracts Key Usage from certificate text
func extractKeyUsage(certText string) []string {
	var keyUsage []string
	lines := strings.Split(certText, "\n")

	for i, line := range lines {
		if strings.Contains(line, "Key Usage:") {
			// Key usage might be on the same line or next line
			usageLine := line
			if i+1 < len(lines) && !strings.Contains(line, ":") {
				usageLine = lines[i+1]
			}

			// Extract usage after the colon
			if parts := strings.Split(usageLine, ":"); len(parts) > 1 {
				usages := strings.Split(parts[1], ",")
				for _, usage := range usages {
					keyUsage = append(keyUsage, strings.TrimSpace(usage))
				}
			}
			break
		}
	}

	return keyUsage
}

// extractExtendedKeyUsage extracts Extended Key Usage from certificate text
func extractExtendedKeyUsage(certText string) []string {
	var extKeyUsage []string
	lines := strings.Split(certText, "\n")

	for i, line := range lines {
		if strings.Contains(line, "Extended Key Usage:") {
			// Extended key usage might be on the same line or next line
			usageLine := line
			if i+1 < len(lines) && !strings.Contains(line, ":") {
				usageLine = lines[i+1]
			}

			// Extract usage after the colon
			if parts := strings.Split(usageLine, ":"); len(parts) > 1 {
				usages := strings.Split(parts[1], ",")
				for _, usage := range usages {
					extKeyUsage = append(extKeyUsage, strings.TrimSpace(usage))
				}
			}
			break
		}
	}

	return extKeyUsage
}

// ValidateCertificateChain validates a certificate and returns validation results
func ValidateCertificateChain(hostname, certPath string, sshConfig *ssh.ClientConfig) (*CertificateValidationResult, error) {
	result := &CertificateValidationResult{
		FilePath:      certPath,
		IsValidFormat: true,
		Warnings:      make([]string, 0),
		Errors:        make([]string, 0),
	}

	// Get certificate info
	certInfo, err := GetCertificateInfo(hostname, certPath, sshConfig)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("Failed to read certificate: %v", err))
		result.IsValid = false
		result.Summary = "Certificate invalid - cannot read"
		return result, nil
	}

	result.DaysUntilExpiry = certInfo.DaysUntilExpiry
	result.IsExpired = certInfo.IsExpired

	// Check if certificate is expired
	if certInfo.IsExpired {
		result.Errors = append(result.Errors, fmt.Sprintf("Certificate expired %d days ago", -certInfo.DaysUntilExpiry))
	}

	// Check if certificate expires soon (within 30 days)
	if !certInfo.IsExpired && certInfo.DaysUntilExpiry <= 30 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Certificate expires in %d days", certInfo.DaysUntilExpiry))
	}

	// Check if certificate is not yet valid (future dated)
	client, err := ssh.Dial("tcp", hostname+":22", sshConfig)
	if err == nil {
		session, err := client.NewSession()
		if err == nil {
			checkCmd := fmt.Sprintf("openssl x509 -in %s -checkend -%d", certPath, 86400) // Check if valid now
			if err := session.Run(checkCmd); err != nil {
				result.Errors = append(result.Errors, "Certificate is not yet valid")
			}
			session.Close()
		}
		client.Close()
	}

	// Validate key usage
	if len(certInfo.KeyUsage) == 0 {
		result.Warnings = append(result.Warnings, "No key usage specified")
	} else {
		hasDigitalSignature := false
		hasKeyEncipherment := false
		for _, usage := range certInfo.KeyUsage {
			if strings.Contains(strings.ToLower(usage), "digital signature") {
				hasDigitalSignature = true
			}
			if strings.Contains(strings.ToLower(usage), "key encipherment") {
				hasKeyEncipherment = true
			}
		}
		if !hasDigitalSignature {
			result.Warnings = append(result.Warnings, "Certificate missing Digital Signature usage")
		}
		if !hasKeyEncipherment {
			result.Warnings = append(result.Warnings, "Certificate missing Key Encipherment usage")
		}
	}

	// Check key size
	if certInfo.PublicKeySize > 0 && certInfo.PublicKeySize < 2048 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Weak key size: %d bits (recommended: 2048+)", certInfo.PublicKeySize))
	}

	// Set overall validity
	result.IsValid = len(result.Errors) == 0

	// Create summary
	if result.IsValid {
		result.Summary = "Certificate valid"
		if len(result.Warnings) > 0 {
			result.Summary += fmt.Sprintf(" with %d warning(s)", len(result.Warnings))
		}
	} else {
		result.Summary = "Certificate invalid"
	}

	return result, nil
}

// BatchValidateCertificates validates multiple certificates on remote hosts
func BatchValidateCertificates(hostCertPairs map[string]string, sshConfig *ssh.ClientConfig) (map[string]*CertificateValidationResult, error) {
	results := make(map[string]*CertificateValidationResult)
	var wg sync.WaitGroup
	var mutex sync.Mutex

	for hostname, certPath := range hostCertPairs {
		wg.Add(1)
		go func(host, path string) {
			defer wg.Done()

			result, err := ValidateCertificateChain(host, path, sshConfig)
			if err != nil {
				result = &CertificateValidationResult{
					FilePath: path,
					IsValid:  false,
					Errors:   []string{fmt.Sprintf("Validation error: %v", err)},
					Summary:  "Validation failed",
				}
			}

			mutex.Lock()
			results[host] = result
			mutex.Unlock()
		}(hostname, certPath)
	}

	wg.Wait()
	return results, nil
}

// CheckRemoteCertificateExpiry checks certificate expiry for a remote HTTPS endpoint
func CheckRemoteCertificateExpiry(hostname string, port int, timeout int, sshConfig *ssh.ClientConfig) (int, error) {
	if port == 0 {
		port = 443
	}
	if timeout == 0 {
		timeout = 10
	}

	// Use a remote host to check the certificate (assuming we have SSH access to a host that can reach the target)
	// This is a workaround since we're working through SSH
	client, err := ssh.Dial("tcp", hostname+":22", sshConfig)
	if err != nil {
		return -9999, fmt.Errorf("failed to dial host %s: %v", hostname, err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return -9999, fmt.Errorf("failed to create session for %s: %v", hostname, err)
	}
	defer session.Close()

	// Use openssl s_client to get remote certificate info
	cmd := fmt.Sprintf(`
		timeout %d openssl s_client -connect %s:%d -servername %s </dev/null 2>/dev/null | \
		openssl x509 -enddate -noout 2>/dev/null | cut -d= -f2 | \
		xargs -I {} date -d "{}" +%%s 2>/dev/null | \
		xargs -I {} bash -c 'echo $(( ({} - $(date +%%s)) / 86400 ))'
	`, timeout, hostname, port, hostname)

	var output bytes.Buffer
	session.Stdout = &output
	session.Stderr = &output

	if err := session.Run(cmd); err != nil {
		return -9999, fmt.Errorf("failed to check remote certificate for %s:%d: %v", hostname, port, err)
	}

	outputStr := strings.TrimSpace(output.String())
	var days int
	if _, err := fmt.Sscanf(outputStr, "%d", &days); err != nil {
		return -9999, fmt.Errorf("failed to parse days output '%s' for %s:%d: %v", outputStr, hostname, port, err)
	}

	return days, nil
}

// GetCertificateInfoBatch retrieves certificate information for multiple hosts
func GetCertificateInfoBatch(hostCertPairs map[string]string, sshConfig *ssh.ClientConfig) (map[string]*CertificateInfo, error) {
	results := make(map[string]*CertificateInfo)
	var wg sync.WaitGroup
	var mutex sync.Mutex

	for hostname, certPath := range hostCertPairs {
		wg.Add(1)
		go func(host, path string) {
			defer wg.Done()

			certInfo, err := GetCertificateInfo(host, path, sshConfig)

			mutex.Lock()
			if err != nil {
				log.Printf("Error getting certificate info for %s: %v", host, err)
				results[host] = nil
			} else {
				results[host] = certInfo
			}
			mutex.Unlock()
		}(hostname, certPath)
	}

	wg.Wait()
	return results, nil
}

// CheckCertificatePermissions checks file permissions of certificate files on remote hosts
func CheckCertificatePermissions(hostCertPairs map[string]string, sshConfig *ssh.ClientConfig) (map[string]map[string]interface{}, error) {
	results := make(map[string]map[string]interface{})
	var wg sync.WaitGroup
	var mutex sync.Mutex

	for hostname, certPath := range hostCertPairs {
		wg.Add(1)
		go func(host, path string) {
			defer wg.Done()

			client, err := ssh.Dial("tcp", host+":22", sshConfig)
			if err != nil {
				mutex.Lock()
				results[host] = map[string]interface{}{
					"error": fmt.Sprintf("Failed to connect: %v", err),
				}
				mutex.Unlock()
				return
			}
			defer client.Close()

			session, err := client.NewSession()
			if err != nil {
				mutex.Lock()
				results[host] = map[string]interface{}{
					"error": fmt.Sprintf("Failed to create session: %v", err),
				}
				mutex.Unlock()
				return
			}
			defer session.Close()

			// Get file permissions, owner, and group
			cmd := fmt.Sprintf("stat -c '%%a %%U %%G %%s' %s 2>/dev/null || ls -la %s 2>/dev/null", path, path)
			var output bytes.Buffer
			session.Stdout = &output
			session.Stderr = &output

			permInfo := map[string]interface{}{
				"path": path,
			}

			if err := session.Run(cmd); err != nil {
				permInfo["error"] = fmt.Sprintf("Failed to get file info: %v", err)
			} else {
				outputStr := strings.TrimSpace(output.String())
				parts := strings.Fields(outputStr)

				if len(parts) >= 4 {
					permInfo["permissions"] = parts[0]
					permInfo["owner"] = parts[1]
					permInfo["group"] = parts[2]
					permInfo["size"] = parts[3]

					// Check if permissions are secure (600, 644, or 400)
					perms := parts[0]
					isSecure := perms == "600" || perms == "644" || perms == "400"
					permInfo["is_secure"] = isSecure

					if !isSecure {
						permInfo["warning"] = fmt.Sprintf("Insecure permissions: %s (recommended: 600, 644, or 400)", perms)
					}
				} else {
					permInfo["error"] = "Unable to parse file information"
				}
			}

			mutex.Lock()
			results[host] = permInfo
			mutex.Unlock()
		}(hostname, certPath)
	}

	wg.Wait()
	return results, nil
}

// GenerateCertificateReport generates a comprehensive certificate report for multiple hosts
func GenerateCertificateReport(hostCertPairs map[string]string, sshConfig *ssh.ClientConfig) (map[string]interface{}, error) {
	report := make(map[string]interface{})

	// Get certificate information
	certInfos, err := GetCertificateInfoBatch(hostCertPairs, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get certificate information: %v", err)
	}

	// Validate certificates
	validationResults, err := BatchValidateCertificates(hostCertPairs, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to validate certificates: %v", err)
	}

	// Check permissions
	permissionResults, err := CheckCertificatePermissions(hostCertPairs, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to check permissions: %v", err)
	}

	// Compile report
	hostReports := make(map[string]interface{})
	var totalHosts, validCerts, expiredCerts, expiringSoon int

	for hostname, certPath := range hostCertPairs {
		totalHosts++

		hostReport := map[string]interface{}{
			"hostname":  hostname,
			"cert_path": certPath,
			"timestamp": time.Now().Format(time.RFC3339),
		}

		// Add certificate info
		if certInfo := certInfos[hostname]; certInfo != nil {
			hostReport["certificate_info"] = certInfo

			if certInfo.IsExpired {
				expiredCerts++
			} else if certInfo.DaysUntilExpiry <= 30 {
				expiringSoon++
			} else {
				validCerts++
			}
		}

		// Add validation results
		if validation := validationResults[hostname]; validation != nil {
			hostReport["validation"] = validation
		}

		// Add permission info
		if permissions := permissionResults[hostname]; permissions != nil {
			hostReport["permissions"] = permissions
		}

		hostReports[hostname] = hostReport
	}

	// Summary statistics
	summary := map[string]interface{}{
		"total_hosts":      totalHosts,
		"valid_certs":      validCerts,
		"expired_certs":    expiredCerts,
		"expiring_soon":    expiringSoon,
		"report_generated": time.Now().Format(time.RFC3339),
	}

	report["summary"] = summary
	report["hosts"] = hostReports

	return report, nil
}

// ReadCSVAndProcess reads master.csv file and creates dictionaries for each cluster in ClusterList
// Equivalent to Python's read_csv_and_process function
func (cp *ClusterProcessor) ReadCSVAndProcess(csvFilePath string) error {
	// Initialize result dictionary with empty lists for each cluster
	cp.Data = make(ClusterData)
	for _, cluster := range cp.ClusterList {
		cp.Data[strings.ToLower(cluster)] = make([]HostData, 0)
	}

	// Check if CSV file exists
	if _, err := os.Stat(csvFilePath); os.IsNotExist(err) {
		return fmt.Errorf("CSV file '%s' not found", csvFilePath)
	}

	file, err := os.Open(csvFilePath)
	if err != nil {
		return fmt.Errorf("could not open file '%s': %v", csvFilePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("error reading CSV header: %v", err)
	}

	// Verify required columns exist
	requiredColumns := []string{"Hostname", "Type", "#", "Cluster Name"}
	columnIndices := make(map[string]int)

	for i, col := range header {
		columnIndices[col] = i
	}

	for _, reqCol := range requiredColumns {
		if _, exists := columnIndices[reqCol]; !exists {
			return fmt.Errorf("missing required column: %s", reqCol)
		}
	}

	// Process each row in the CSV
	rowCount := 0
	matchedCount := 0

	for {
		row, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("error reading CSV row: %v", err)
		}

		rowCount++

		if len(row) <= columnIndices["Cluster Name"] {
			continue
		}

		clusterName := strings.TrimSpace(row[columnIndices["Cluster Name"]])
		if clusterName == "" {
			continue
		}

		// Check if cluster name matches any item in ClusterList (case insensitive)
		for _, cluster := range cp.ClusterList {
			if strings.EqualFold(clusterName, cluster) {
				// Convert count to integer
				countStr := strings.TrimSpace(row[columnIndices["#"]])
				count, err := strconv.Atoi(countStr)
				if err != nil {
					count = 0 // Default to 0 if conversion fails
				}

				// Create host data
				hostData := HostData{
					HostName: strings.TrimSpace(row[columnIndices["Hostname"]]),
					Type:     strings.TrimSpace(row[columnIndices["Type"]]),
					Count:    count,
				}

				// Append to corresponding cluster list
				cp.Data[strings.ToLower(cluster)] = append(cp.Data[strings.ToLower(cluster)], hostData)
				matchedCount++
				break
			}
		}
	}

	log.Printf("Processed %d rows from CSV file", rowCount)
	log.Printf("Found %d matching entries for clusters in ClusterList", matchedCount)

	return nil
}

// PrintClusterSummary prints a summary of the processed cluster data
// Equivalent to Python's print_cluster_summary function
func (cp *ClusterProcessor) PrintClusterSummary() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("CLUSTER DATA SUMMARY")
	fmt.Println(strings.Repeat("=", 60))

	for clusterName, hosts := range cp.Data {
		// Find original cluster name from ClusterList (preserve case)
		originalName := clusterName
		for _, c := range cp.ClusterList {
			if strings.ToLower(c) == clusterName {
				originalName = c
				break
			}
		}

		fmt.Printf("\nCluster: %s\n", originalName)
		fmt.Printf("Number of hosts: %d\n", len(hosts))

		if len(hosts) > 0 {
			fmt.Println("Hosts:")
			for i, host := range hosts {
				fmt.Printf("  %d. Hostname: %s, Type: %s, Count: %d\n",
					i+1, host.HostName, host.Type, host.Count)
			}
		} else {
			fmt.Println("  No matching hosts found.")
		}
	}
}

// SaveResultsToJSON saves the processed cluster data to a JSON file
// Equivalent to Python's save_results_to_json function
func (cp *ClusterProcessor) SaveResultsToJSON(outputFile string) error {
	if outputFile == "" {
		outputFile = "cluster_results.json"
	}

	// Convert keys back to original case for output
	outputData := make(map[string][]HostData)
	for clusterName, hosts := range cp.Data {
		originalName := clusterName
		for _, c := range cp.ClusterList {
			if strings.ToLower(c) == clusterName {
				originalName = c
				break
			}
		}
		outputData[originalName] = hosts
	}

	jsonData, err := json.MarshalIndent(outputData, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling to JSON: %v", err)
	}

	err = os.WriteFile(outputFile, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("error writing JSON file: %v", err)
	}

	log.Printf("Results saved to '%s'", outputFile)
	return nil
}

// ExecuteCommandsOnCluster executes a command on all hosts in a specific cluster
// Equivalent to Python's execute_commands_on_cluster function
func (cp *ClusterProcessor) ExecuteCommandsOnCluster(clusterName, command, username string) (map[string]*Result, error) {
	results := make(map[string]*Result)

	// Find cluster (case insensitive)
	var targetCluster []HostData
	for clusterKey, hosts := range cp.Data {
		if strings.EqualFold(clusterKey, clusterName) {
			targetCluster = hosts
			break
		}
	}

	if targetCluster == nil {
		return results, fmt.Errorf("cluster '%s' not found in processed data", clusterName)
	}

	log.Printf("Executing command on all hosts in cluster '%s':", clusterName)
	log.Printf("Command: %s", command)
	log.Printf("Number of hosts: %d", len(targetCluster))
	log.Println(strings.Repeat("-", 50))

	for _, host := range targetCluster {
		hostname := host.HostName
		log.Printf("[%s] Executing command...", hostname)

		result, err := ExecOnRemoteHost(hostname, []string{command}, cp.SSHConfig)
		if err != nil {
			log.Printf("[%s] Failed: %v", hostname, err)
			results[hostname] = &Result{
				HostName: hostname,
				Output:   "",
				Error:    err,
			}
		} else {
			results[hostname] = result
		}
	}

	return results, nil
}

// CopyFilesFromCluster copies files from all hosts in a specific cluster
// Equivalent to Python's copy_files_from_cluster function
func (cp *ClusterProcessor) CopyFilesFromCluster(clusterName, sourceLocation, destinationBase, username string) (map[string]bool, error) {
	results := make(map[string]bool)

	// Find cluster (case insensitive)
	var targetCluster []HostData
	for clusterKey, hosts := range cp.Data {
		if strings.EqualFold(clusterKey, clusterName) {
			targetCluster = hosts
			break
		}
	}

	if targetCluster == nil {
		return results, fmt.Errorf("cluster '%s' not found in processed data", clusterName)
	}

	log.Printf("Copying files from all hosts in cluster '%s':", clusterName)
	log.Printf("Source: %s", sourceLocation)
	log.Printf("Destination base: %s", destinationBase)
	log.Printf("Number of hosts: %d", len(targetCluster))
	log.Println(strings.Repeat("-", 50))

	// Create base destination directory
	err := os.MkdirAll(destinationBase, 0755)
	if err != nil {
		return results, fmt.Errorf("failed to create destination directory: %v", err)
	}

	for _, host := range targetCluster {
		hostname := host.HostName
		log.Printf("[%s] Copying file...", hostname)

		// Create unique destination path for each host
		filename := filepath.Base(sourceLocation)
		destinationPath := filepath.Join(destinationBase, fmt.Sprintf("%s_%s", hostname, filename))

		// Create SSH client for file copy
		client, err := ssh.Dial("tcp", hostname+":22", cp.SSHConfig)
		if err != nil {
			log.Printf("[%s] Failed to connect: %v", hostname, err)
			results[hostname] = false
			continue
		}

		// Create SFTP client
		sftpClient, err := sftp.NewClient(client)
		if err != nil {
			log.Printf("[%s] Failed to create SFTP client: %v", hostname, err)
			client.Close()
			results[hostname] = false
			continue
		}

		// Open remote file
		remoteFile, err := sftpClient.Open(sourceLocation)
		if err != nil {
			log.Printf("[%s] Failed to open remote file: %v", hostname, err)
			sftpClient.Close()
			client.Close()
			results[hostname] = false
			continue
		}

		// Create local file
		localFile, err := os.Create(destinationPath)
		if err != nil {
			log.Printf("[%s] Failed to create local file: %v", hostname, err)
			remoteFile.Close()
			sftpClient.Close()
			client.Close()
			results[hostname] = false
			continue
		}

		// Copy file contents
		_, err = io.Copy(localFile, remoteFile)
		if err != nil {
			log.Printf("[%s] Failed to copy file: %v", hostname, err)
			results[hostname] = false
		} else {
			log.Printf("[%s] File copied successfully", hostname)
			results[hostname] = true
		}

		// Clean up
		localFile.Close()
		remoteFile.Close()
		sftpClient.Close()
		client.Close()
	}

	return results, nil
}

// ValidateCertificatesOnCluster checks certificate expiry on all hosts in a specific cluster
// Equivalent to Python's validate_certificates_on_cluster function
func (cp *ClusterProcessor) ValidateCertificatesOnCluster(clusterName, certPath, username string) (map[string]int, error) {
	results := make(map[string]int)

	// Find cluster (case insensitive)
	var targetCluster []HostData
	for clusterKey, hosts := range cp.Data {
		if strings.EqualFold(clusterKey, clusterName) {
			targetCluster = hosts
			break
		}
	}

	if targetCluster == nil {
		return results, fmt.Errorf("cluster '%s' not found in processed data", clusterName)
	}

	log.Printf("Checking certificate expiry on all hosts in cluster '%s':", clusterName)
	log.Printf("Certificate path: %s", certPath)
	log.Printf("Number of hosts: %d", len(targetCluster))
	log.Println(strings.Repeat("-", 50))

	for _, host := range targetCluster {
		hostname := host.HostName
		log.Printf("[%s] Checking certificate...", hostname)

		// Get certificate expiry information
		daysToExpiry, err := getCertExpiryDays(hostname, certPath, cp.SSHConfig)
		if err != nil {
			log.Printf("[%s] Failed: %v", hostname, err)
			results[hostname] = -9999
		} else {
			results[hostname] = daysToExpiry
			if daysToExpiry >= 0 {
				log.Printf("[%s] ✓ Certificate expires in %d days", hostname, daysToExpiry)
			} else {
				log.Printf("[%s] ✗ Certificate expired %d days ago", hostname, -daysToExpiry)
			}
		}
	}

	return results, nil
}

// GetHostnamesFromCluster returns a slice of hostnames for a given cluster
func (cp *ClusterProcessor) GetHostnamesFromCluster(clusterName string) ([]string, error) {
	var hostnames []string

	// Find cluster (case insensitive)
	var targetCluster []HostData
	for clusterKey, hosts := range cp.Data {
		if strings.EqualFold(clusterKey, clusterName) {
			targetCluster = hosts
			break
		}
	}

	if targetCluster == nil {
		return hostnames, fmt.Errorf("cluster '%s' not found in processed data", clusterName)
	}

	for _, host := range targetCluster {
		hostnames = append(hostnames, host.HostName)
	}

	return hostnames, nil
}

// GetClusterNames returns a list of all cluster names that have data
func (cp *ClusterProcessor) GetClusterNames() []string {
	var clusterNames []string

	for clusterName, hosts := range cp.Data {
		if len(hosts) > 0 {
			// Find original cluster name from ClusterList (preserve case)
			originalName := clusterName
			for _, c := range cp.ClusterList {
				if strings.ToLower(c) == clusterName {
					originalName = c
					break
				}
			}
			clusterNames = append(clusterNames, originalName)
		}
	}

	return clusterNames
}

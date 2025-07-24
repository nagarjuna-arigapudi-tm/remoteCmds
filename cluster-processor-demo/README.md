# Cluster Processor Demo

This demo program showcases the functionality of the ClusterProcessor package, which provides comprehensive cluster management and certificate validation capabilities equivalent to the Python `cert-validation/cluster_processor.py` functionality.

## Overview

The Cluster Processor Demo demonstrates:
- CSV processing with global cluster list matching
- SSH operations across multiple hosts
- Certificate validation and expiry checking
- Batch operations on clusters
- JSON export capabilities

## Features Demonstrated

### 1. Core Cluster Processing
- **CSV Data Processing**: Reads `master.csv` and matches against global cluster list
- **Case-Insensitive Matching**: Flexible cluster name matching
- **Data Structure Creation**: Creates structured data with hostname, type, and count
- **JSON Export**: Saves processed results to JSON format

### 2. SSH Operations
- **Cluster-wide Command Execution**: Run commands on all hosts in a cluster
- **File Operations**: Copy files from/to multiple hosts
- **Parallel Processing**: Concurrent operations for better performance

### 3. Certificate Validation
- **Certificate Expiry Checking**: Main function `DaysForCertToExpireRemote()`
- **Detailed Certificate Information**: Extract subject, issuer, SANs, etc.
- **Batch Certificate Validation**: Process multiple certificates simultaneously
- **Remote HTTPS Certificate Checking**: Validate certificates on remote endpoints
- **Comprehensive Reporting**: Generate detailed certificate reports

## Prerequisites

- Go 1.16 or higher
- SSH access to target hosts (for actual SSH operations)
- Valid SSH keys configured

## Installation

1. Clone or navigate to the demo directory:
```bash
cd cluster-ui/cmd/cluster-processor-demo
```

2. Install dependencies:
```bash
make install-deps
# or manually:
go get golang.org/x/crypto/ssh
go get github.com/pkg/sftp
```

## Usage

### Basic Demo Run
```bash
# Build and run the demo
make run

# Or build separately
make build
./cluster-processor-demo
```

### With Custom SSH Configuration
```bash
# Set environment variables for SSH
export SSH_KEY_PATH="/path/to/your/ssh/key"
export SSH_USERNAME="your_username"
make run

# Or run directly with variables
SSH_KEY_PATH=~/.ssh/my_key SSH_USERNAME=myuser make run
```

### Available Make Targets
```bash
make help          # Show all available targets
make build         # Build the demo binary
make clean         # Clean build artifacts
make deps          # Download and tidy dependencies
make test          # Run tests (placeholder)
```

## Demo Flow

The demo program follows this sequence:

1. **Configuration Setup**
   - Reads SSH key path and username from environment variables
   - Sets default values if not provided

2. **ClusterProcessor Creation**
   - Creates a new ClusterProcessor instance
   - Displays the global cluster list

3. **Sample Data Generation**
   - Creates a sample `master.csv` file if it doesn't exist
   - Contains realistic cluster data for demonstration

4. **CSV Processing**
   - Processes the CSV file against the global cluster list
   - Shows matching statistics

5. **Data Display**
   - Prints a formatted cluster summary
   - Shows all matched hosts organized by cluster

6. **JSON Export**
   - Saves processed results to `cluster_results.json`

7. **Cluster Operations Demo**
   - Demonstrates available SSH and certificate functions
   - Shows function signatures and usage examples

## Sample CSV Format

The demo creates a sample CSV with this structure:
```csv
Hostname,Type,#,Cluster Name
server-01.prod.com,application,5,prod-cluster-01
server-02.prod.com,database,3,prod-cluster-01
web-01.prod.com,web,4,prod-cluster-02
dev-server-01.dev.com,application,1,dev-cluster-01
```

## Key Functions Demonstrated

### Main Certificate Function
```go
func DaysForCertToExpireRemote(remoteHosts []string, remoteCertPath string, formatString string) (map[string]int, error)
```
- **Parameters**:
  - `remoteHosts`: List of hostnames to check
  - `remoteCertPath`: Base path where certificates are located
  - `formatString`: Format to derive cert name (e.g., "%s.crt")
- **Returns**: Map of hostname to days until expiry (negative if expired)

### ClusterProcessor Methods
```go
// CSV Processing
cp.ReadCSVAndProcess(csvFilePath)
cp.PrintClusterSummary()
cp.SaveResultsToJSON(outputFile)

// Cluster Operations
cp.ExecuteCommandsOnCluster(clusterName, command, username)
cp.CopyFilesFromCluster(clusterName, sourceLocation, destBase, username)
cp.ValidateCertificatesOnCluster(clusterName, certPath, username)

// Utility Functions
cp.GetHostnamesFromCluster(clusterName)
cp.GetClusterNames()
```

### Certificate Validation Functions
```go
// Individual certificate operations
GetCertificateInfo(hostname, certPath, sshConfig)
ValidateCertificateChain(hostname, certPath, sshConfig)
CheckRemoteCertificateExpiry(hostname, port, timeout, sshConfig)

// Batch operations
BatchValidateCertificates(hostCertPairs, sshConfig)
GenerateCertificateReport(hostCertPairs, sshConfig)
CheckCertificatePermissions(hostCertPairs, sshConfig)
```

## Example Output

```
Cluster Processor Demo
==================================================
SSH Key Path: /Users/username/.ssh/id_rsa
SSH Username: username
CSV File Path: master.csv

1. Creating ClusterProcessor instance...
✓ ClusterProcessor created successfully
  Global cluster list: [prod-cluster-01 prod-cluster-02 dev-cluster-01 staging-cluster-01 test-cluster-01 backup-cluster-01]

2. Creating sample CSV file...
✓ Sample CSV file created: master.csv

3. Processing CSV data...
✓ CSV data processed successfully

4. Displaying cluster summary...

============================================================
CLUSTER DATA SUMMARY
============================================================

Cluster: prod-cluster-01
Number of hosts: 3
Hosts:
  1. Hostname: server-01.prod.com, Type: application, Count: 5
  2. Hostname: server-02.prod.com, Type: database, Count: 3
  3. Hostname: server-03.prod.com, Type: cache, Count: 2

Cluster: prod-cluster-02
Number of hosts: 3
Hosts:
  1. Hostname: web-01.prod.com, Type: web, Count: 4
  2. Hostname: web-02.prod.com, Type: web, Count: 4
  3. Hostname: db-01.prod.com, Type: database, Count: 8
...
```

## Real-World Usage

To use the ClusterProcessor in a real environment:

1. **Prepare your CSV file** with actual cluster data
2. **Configure SSH access** to your target hosts
3. **Set environment variables** for SSH key and username
4. **Uncomment the SSH operation examples** in the demo code
5. **Modify certificate paths** to match your environment

## Security Considerations

- SSH keys should have proper permissions (600 or 400)
- Use dedicated service accounts for automated operations
- Validate certificate paths and permissions
- Monitor SSH access logs
- Consider using SSH agent forwarding for enhanced security

## Troubleshooting

### Common Issues

1. **SSH Connection Failures**
   - Verify SSH key path and permissions
   - Check network connectivity to target hosts
   - Ensure SSH service is running on target hosts

2. **Certificate Validation Errors**
   - Verify certificate file paths on remote hosts
   - Check OpenSSL availability on target systems
   - Ensure proper file permissions for certificates

3. **CSV Processing Issues**
   - Verify CSV file format and required columns
   - Check for proper encoding (UTF-8)
   - Ensure cluster names match the global list

## Files Generated

The demo creates these files:
- `master.csv` - Sample cluster data (if not exists)
- `cluster_results.json` - Processed cluster data in JSON format
- `cluster-processor-demo` - Compiled binary

## Extending the Demo

You can extend this demo by:
- Adding interactive menu options
- Implementing real SSH operations
- Adding certificate monitoring workflows
- Creating custom reporting formats
- Integrating with external systems

## Support

For issues or questions about the ClusterProcessor functionality, refer to the main `remoteCmds` package documentation or examine the source code in `cluster-ui/remoteCmds/remote_cmds.go`.

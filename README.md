# Worker Hablla Integration

## Overview
This repository contains a Node.js automation worker designed to synchronize data between the Hablla API and Google Sheets[cite: 1, 3]. It handles card processing, attendant reports, and historical data cleanup within a structured CI/CD pipeline[cite: 3].

## Repository Integration
This worker operates in conjunction with the `worker-google-auth` repository[cite: 3]. 
* **Trigger Mechanism**: It is activated via a `repository_dispatch` event named `google_token_ready`[cite: 3].
* **Token Handover**: The authentication worker generates a Google Access Token and dispatches it directly to this repository's workflow payload[cite: 3].
* **Decoupling**: This architecture ensures that the integration worker does not need to manage long-lived Google Service Account keys, receiving only short-lived bearer tokens[cite: 3].

## Technical Architecture

### Folder Structure
* **.github/workflows/main.yml**: Defines the GitHub Actions automation logic and environment mapping[cite: 3].
* **index.js**: The core logic for API communication, data parsing, and spreadsheet manipulation[cite: 3].
* **package.json**: Manages project metadata and the `axios` dependency[cite: 1].

### Functional Stages
1. **Metadata Retrieval**: Identifies specific sheet IDs within the target spreadsheet[cite: 3].
2. **Collaborator Mapping**: Fetches a centralized database of employees to cross-reference IDs with names[cite: 3].
3. **Hablla Authentication**: Performs a secure login to the Hablla API using encrypted secrets[cite: 3].
4. **Optimized Cleanup**: Scans the Google Sheet from bottom to top to identify and remove records older than 7 days[cite: 3]. It includes a stop condition after 20 consecutive old rows to prevent unnecessary API calls[cite: 3].
5. **Data Synchronization**: Fetches new cards from the Hablla API v3 and appends them to the "Base Hablla Card" sheet[cite: 3].
6. **Reporting**: Generates a daily summary of service metrics (TME, TMA, CSAT) for the "Base Atendente" sheet[cite: 3].

## Security Implementation
* **Zero Hardcoded Secrets**: All sensitive credentials (passwords, IDs, and tokens) are injected via GitHub Secrets or encrypted payloads[cite: 3].
* **Token Scoping**: Uses short-lived Google OAuth2 tokens valid only for the duration of the execution[cite: 3].
* **Safe Logging**: The script confirms process milestones without printing raw JSON payloads or sensitive authorization headers to the console[cite: 3].

## Optimization Features
* **Batch Processing**: Uses `batchUpdate` for row deletions and `append` for data insertion to minimize Google Sheets API quota consumption[cite: 3].
* **Pagination Control**: Implements smart pagination that breaks the loop if no new records are found after two consecutive pages[cite: 3].
* **Rate Limiting**: Includes a `sleep` utility to respect API rate limits during heavy data insertion[cite: 3].

## Setup
1. Ensure all required GitHub Secrets are configured (HABLLA_EMAIL, HABLLA_PASSWORD, SPREADSHEET_ID, etc.)[cite: 3].
2. Deploy the `worker-google-auth` to trigger this workflow automatically[cite: 3].

## License
This project is licensed under the MIT License - see the LICENSE file for details[cite: 1].

## Author

**Patrick Araujo - Security Researcher & Computer Engineer**  
**GitHub**: https://github.com/PkLavc  

# Brighter Alibaba Cloud Integration

A comprehensive integration library for [Brighter](https://www.goparamore.io/) with Alibaba Cloud services, providing distributed messaging, storage, and persistence patterns for cloud-native applications.

## Features

This library provides Brighter integrations for the following Alibaba Cloud services:

### 🗄️ Object Storage Service (OSS)
- **Claim Check Pattern**: Store large message payloads in OSS while passing only references in messages
- Async/sync storage operations
- Configurable bucket management strategies

### 📊 Tablestore
- **Inbox Pattern**: Deduplication and idempotent message processing
- **Outbox Pattern**: Reliable message publishing with transactional guarantees
- **Distributed Lock**: Distributed coordination and synchronization

## Installation

Install the packages via NuGet:

```bash
# For OSS storage provider
dotnet add package Brighter.Transformers.Alibaba

# For Tablestore inbox/outbox
dotnet add package Brighter.Inbox.Tablestore
```

## Getting Started

### OSS Claim Check Pattern

Use OSS to store large message payloads externally:

```csharp
// Configure OSS luggage store
var ossOptions = new OssLuggageOptions
{
    // Your OSS configuration
};
var luggageStore = new OssLuggageStore(ossOptions);
```

### Tablestore Inbox/Outbox

Configure Tablestore for inbox and outbox patterns to ensure reliable message processing and publishing.

#### Tablestore Outbox Index Requirements

For the outbox pattern to work correctly, you must create a secondary index on your Tablestore outbox table with the following configuration:

- **Index Columns**: 
  - `DispatchedAt` (Timestamp type)
  - `Timestamp` (Timestamp type, ascending order)

This index is required for efficiently querying and dispatching messages from the outbox.

**Example using Alibaba Cloud Console:**
1. Navigate to your Tablestore instance
2. Select your outbox table
3. Create a new secondary index
4. Add `DispatchedAt` and `Timestamp` columns
5. Set `Timestamp` to ascending order

## Requirements

- .NET Standard 2.0+, .NET 8.0+, or .NET 9.0+
- Alibaba Cloud account with appropriate service access

## License

GPL-3.0-or-later

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

Rafael Andrade


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

### 📨 Message Service (MNS)
- **Simple Queue Service**: Reliable message queuing for asynchronous communication
- Integration with Brighter's messaging infrastructure

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

## Requirements

- .NET Standard 2.0+, .NET 8.0+, or .NET 9.0+
- Alibaba Cloud account with appropriate service access

## License

GPL-3.0-or-later

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

Rafael Andrade


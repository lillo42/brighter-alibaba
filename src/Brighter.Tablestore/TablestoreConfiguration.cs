using System;
using Aliyun.OTS;
using Paramore.Brighter.Observability;

namespace Brighter.Tablestore;

public class TablestoreConfiguration
{
    public OTSClientConfig? Configuration { get; set; }

    public TablestoreTable? Inbox { get; set; }
    
    public TablestoreTable? Outbox { get; set; }
    
    public TablestoreTable? Locking { get; set; }

    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    public InstrumentationOptions Instrumentation { get; set; } = InstrumentationOptions.All;
}
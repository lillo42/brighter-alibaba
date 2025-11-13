using System;
using Aliyun.OTS;
using Paramore.Brighter.Observability;

namespace Brighter.Tablestore;

public class TablestoreConfiguration(OTSClientConfig config)
{
    public OTSClientConfig Configuration { get; }

    public TablestoreConfiguration(string endPoint, 
        string accessKeyID,
        string accessKeySecret,
        string instanceNam)
        : this(new OTSClientConfig(endPoint, accessKeyID, accessKeySecret, instanceNam))
    {
        
    }
    
    public TablestoreTable? Inbox { get; set; }
    
    public TablestoreTable? Outbox { get; set; }
    
    public TablestoreTable? Locking { get; set; }

    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    public InstrumentationOptions Instrumentation { get; set; } = InstrumentationOptions.All;
}
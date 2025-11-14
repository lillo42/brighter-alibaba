using Aliyun.OTS;

namespace Brighter.Tablestore;

public class TablestoreConnectionProvider(OTSClient client) : IAmATablestoreConnectionProvider
{
    public TablestoreConnectionProvider(TablestoreConfiguration configuration)
        : this(new OTSClient(configuration.Configuration))
    {
        
    }
    
    public OTSClient GetTablestoreClient() 
        => client;
}
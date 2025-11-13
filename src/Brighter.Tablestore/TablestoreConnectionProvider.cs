using Aliyun.OTS;

namespace Brighter.Tablestore;

public class TablestoreConnectionProvider(TablestoreConfiguration configuration) : IAmATablestoreConnectionProvider
{
    public OTSClient GetTablestoreClient()
    {
        return new OTSClient(configuration.Configuration);
    }
}
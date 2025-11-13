using Aliyun.OTS;
using Paramore.Brighter;

namespace Brighter.Tablestore;

public interface IAmATablestoreConnectionProvider : IAmAConnectionProvider
{
    OTSClient GetTablestoreClient();
}
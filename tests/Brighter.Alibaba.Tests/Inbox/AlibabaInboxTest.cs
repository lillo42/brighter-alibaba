using Aliyun.OTS.Request;
using Brighter.Inbox.Tablestore;
using Brighter.Tablestore;
using Paramore.Brighter;

namespace Brighter.Transformers.Alibaba.Inbox;

[InheritsTests]
public class AlibabaInboxTest : InboxTests
{
    private TablestoreInbox? _inbox;
    protected override IAmAnInboxSync Inbox => _inbox!;

    protected override void CreateStore()
    {
        _inbox = new TablestoreInbox(new TablestoreConfiguration(
            AlibabaConfiguration.Endpoint,
            AlibabaConfiguration.AccessKey,
            AlibabaConfiguration.SecretKey,
            "brighter")
        {
            Inbox = new TablestoreTable
            {
                Name = "inbox",
                TimeToLive =  TimeSpan.FromHours(1),
            }
        });
    }
}

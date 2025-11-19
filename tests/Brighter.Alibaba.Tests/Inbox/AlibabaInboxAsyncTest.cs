using Aliyun.OTS;
using Aliyun.OTS.DataModel;
using Aliyun.OTS.Request;
using Brighter.Inbox.Tablestore;
using Brighter.Tablestore;
using Paramore.Brighter;

namespace Brighter.Transformers.Alibaba.Inbox;

[InheritsTests]
public class AlibabaInboxAsyncTest : InboxAsyncTests
{
    private TablestoreInbox? _inbox;
    protected override IAmAnInboxAsync Inbox => _inbox!;

    protected override Task CreateStoreAsync()
    {
         _inbox = new TablestoreInbox(new TablestoreConfiguration
        {
            Configuration = new OTSClientConfig(
                AlibabaConfiguration.TablestoreEndpoint,
                AlibabaConfiguration.AccessKey,
                AlibabaConfiguration.SecretKey,
                "brighter"),
            Inbox = new TablestoreTable
            {
                Name = "inbox",
            }
        });

        return Task.CompletedTask;
    }

    public override async Task AfterEachTestAsync()
    {
        var provider = new TablestoreConnectionProvider(new TablestoreConfiguration
        {
            Configuration = new OTSClientConfig(
                AlibabaConfiguration.TablestoreEndpoint,
                AlibabaConfiguration.AccessKey,
                AlibabaConfiguration.SecretKey,
                "brighter")
        });
        
        var client = provider.GetTablestoreClient();
        foreach (var command in CreatedCommands)
        {
            try
            {
                var pk = new PrimaryKey{ ["Id"] = new ColumnValue(command.Id) };
                await client.DeleteRowAsync(new DeleteRowRequest(new RowDeleteChange("inbox", pk))
                {
                    Condition = new Condition(RowExistenceExpectation.IGNORE),
                });
            }
            catch 
            {
                // Ignoring any error during delete, it's not important at this point
            }
        }
    }
}

using Aliyun.OTS;
using Aliyun.OTS.DataModel;
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
    }

    public override void AfterEachTest()
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
                client.DeleteRow(new DeleteRowRequest(new RowDeleteChange("inbox", pk))
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
